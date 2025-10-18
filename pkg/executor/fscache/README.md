# Fission Function Service Cache (fscache) Explained

This document provides a deep dive into the `pkg/executor/fscache/` package, which is the **performance-critical caching layer** that makes Fission fast.

## Table of Contents

- [Overview](#overview)
- [Why fscache Matters](#why-fscache-matters)
- [Package Structure](#package-structure)
- [Core Data Structures](#core-data-structures)
- [FunctionServiceCache - Simple Cache](#functionservicecache---simple-cache)
- [PoolCache - Advanced Cache](#poolcache---advanced-cache)
- [Queue - FIFO for Waiting Requests](#queue---fifo-for-waiting-requests)
- [Complete Request Flow Examples](#complete-request-flow-examples)
- [Design Patterns](#design-patterns)
- [Performance Characteristics](#performance-characteristics)
- [Code Location Reference](#code-location-reference)

---

## Overview

The `fscache` package implements a **two-tiered caching system** that sits between the executor and Kubernetes:

```
Router → Executor → fscache → Kubernetes API
                      ↓
                 (cache hit)
                      ↓
              Pod Address (< 1ms)
```

**Without cache:** Every function invocation requires Kubernetes API calls (100-500ms)
**With cache:** Sub-millisecond lookups for cached functions

### Two Cache Types

1. **FunctionServiceCache** - Used by `newdeploy` and `container` executors
   - Simple function → service mapping
   - TTL-based expiration
   - Three-way indexing for fast lookups

2. **PoolCache** - Used **exclusively** by `poolmgr` executor
   - Tracks concurrent requests per pod
   - CPU-aware load balancing
   - Request queuing when pods are busy
   - Enforces concurrency limits

---

## Why fscache Matters

### Performance Impact

| Operation | Without Cache | With Cache | Speedup |
|-----------|--------------|------------|---------|
| Function lookup | 100-500ms (K8s API) | < 1ms | 100-500x |
| Cold start | 2-5 seconds | ~100ms (poolmgr) | 20-50x |
| Concurrent requests | N pods created | 1 pod created | N:1 |

### Key Benefits

1. **Sub-millisecond lookups** - Avoid expensive Kubernetes API calls
2. **Intelligent request routing** - Load balance across multiple pods
3. **Prevent thundering herd** - Coordinate concurrent requests
4. **Idle resource cleanup** - Track last-access time for reaping
5. **CPU-aware scheduling** - Avoid overloading pods
6. **Request queuing** - Queue requests instead of rejecting them

---

## Package Structure

```
pkg/executor/fscache/
├── functionServiceCache.go      # Simple cache for newdeploy/container
│   └── FunctionServiceCache     # Main cache struct
│       ├── byFunction           # function → FuncSvc
│       ├── byAddress            # IP:port → function
│       ├── byFunctionUID        # UID → function
│       ├── connFunctionCache    # PoolCache instance
│       ├── PodToFsvc            # pod-name → FuncSvc
│       └── WebsocketFsvc        # websocket tracking
│
├── poolcache.go                 # Advanced cache for poolmgr
│   └── PoolCache                # Concurrent request tracking
│       ├── cache                # function → funcSvcGroup
│       └── requestChannel       # Actor model
│
└── queue.go                     # FIFO queue for waiting requests
    └── Queue                    # Thread-safe queue
        ├── items                # Linked list
        └── mutex                # Lock
```

---

## Core Data Structures

### FuncSvc - The Cached Value

This is what gets cached - represents a specialized function pod:

```go
type FuncSvc struct {
    Name              string                  // Pod name (e.g., "poolmgr-nodejs-default-123-abc")
    Function          *metav1.ObjectMeta      // Function metadata (name, namespace, UID)
    Environment       *fv1.Environment        // Environment spec
    Address           string                  // Routable address (e.g., "http://10.244.0.5:8888")
    KubernetesObjects []apiv1.ObjectReference // References to pod/service
    Executor          fv1.ExecutorType        // poolmgr/newdeploy/container
    CPULimit          resource.Quantity       // CPU limit for the pod

    // Timestamps for idle tracking
    Ctime             time.Time               // Creation time
    Atime             time.Time               // Last access time (updated by TapService)
}
```

**Example:**
```go
fsvc := &FuncSvc{
    Name:     "poolmgr-nodejs-default-hello-abc123",
    Function: &metav1.ObjectMeta{
        Name:      "hello",
        Namespace: "default",
        UID:       "12345678-1234-1234-1234-123456789abc",
    },
    Address:  "http://10.244.0.5:8888",
    Executor: fv1.ExecutorTypePoolmgr,
    Ctime:    time.Now(),
    Atime:    time.Now(),
}
```

---

## FunctionServiceCache - Simple Cache

**File:** `functionServiceCache.go`
**Used by:** newdeploy, container executors

### Data Structure

```go
type FunctionServiceCache struct {
    logger *zap.Logger

    // Three-way indexing for fast lookups
    byFunction    *cache.Cache[crd.CacheKeyUR, *FuncSvc]     // Primary: function → FuncSvc
    byAddress     *cache.Cache[string, metav1.ObjectMeta]    // Reverse: "10.244.0.5:8888" → function
    byFunctionUID *cache.Cache[types.UID, metav1.ObjectMeta] // By UID: "12345..." → function

    // For poolmgr
    connFunctionCache *PoolCache  // Advanced concurrent request tracking

    // Additional mappings
    PodToFsvc     sync.Map  // "pod-name" → *FuncSvc
    WebsocketFsvc sync.Map  // "fsvc-name" → bool (has active websocket)

    // Actor model
    requestChannel chan *fscRequest
}
```

### Cache Keys

```go
// CacheKeyUR: Unique Resource key (namespace + name + resourceVersion)
type CacheKeyUR struct {
    Namespace       string
    Name            string
    ResourceVersion string
}

// Example
key := crd.CacheKeyUR{
    Namespace:       "default",
    Name:            "hello",
    ResourceVersion: "12345",
}
```

### Actor Model Pattern

All cache operations go through a single goroutine to avoid race conditions:

```go
func (fsc *FunctionServiceCache) service() {
    for {
        req := <-fsc.requestChannel
        resp := &fscResponse{}

        switch req.requestType {
        case TOUCH:
            // Update last access time (Atime)
            resp.error = fsc._touchByAddress(req.address)

        case LISTOLD:
            // Find pods idle for longer than req.age
            fscs := fsc.byFunctionUID.Copy()
            funcObjects := make([]*FuncSvc, 0)
            for _, m := range fscs {
                fsvc, _ := fsc.byFunction.Get(crd.CacheKeyURFromMeta(&m))
                if time.Since(fsvc.Atime) > req.age {
                    funcObjects = append(funcObjects, fsvc)
                }
            }
            resp.objects = funcObjects

        case LOG:
            // Dump cache contents for debugging
            funcCopy := fsc.byFunction.Copy()
            for key, fsvc := range funcCopy {
                fsc.logger.Info("cache entry",
                    zap.String("key", key.String()),
                    zap.String("address", fsvc.Address))
            }

        case LISTOLDPOOL:
            // For poolmgr: find idle pods in pool cache
            fscs := fsc.connFunctionCache.ListAvailableValue()
            funcObjects := make([]*FuncSvc, 0)
            for _, fsvc := range fscs {
                if time.Since(fsvc.Atime) > req.age {
                    funcObjects = append(funcObjects, fsvc)
                }
            }
            resp.objects = funcObjects
        }

        req.responseChannel <- resp
    }
}
```

### Key Operations

#### 1. Get by Function

```go
func (fsc *FunctionServiceCache) GetByFunction(m *metav1.ObjectMeta) (*FuncSvc, error) {
    key := crd.CacheKeyURFromMeta(m)

    // Lookup in cache
    fsvc, err := fsc.byFunction.Get(key)
    if err != nil {
        return nil, err  // Cache miss
    }

    // Update last access time
    fsvc.Atime = time.Now()

    // Return a copy (avoid external mutations)
    fsvcCopy := *fsvc
    return &fsvcCopy, nil
}
```

**Usage:**
```go
// In newdeploy executor
fsvc, err := fsc.GetByFunction(&function.ObjectMeta)
if err == nil {
    // Cache hit!
    return fsvc.Address, nil
}
// Cache miss - create deployment
```

#### 2. Add to Cache

```go
func (fsc *FunctionServiceCache) Add(fsvc FuncSvc) (*FuncSvc, error) {
    // Try to add to primary cache
    existing, err := fsc.byFunction.Set(crd.CacheKeyURFromMeta(fsvc.Function), &fsvc)
    if err != nil {
        if IsNameExistError(err) {
            // Already exists - touch it and return
            fsc.TouchByAddress(existing.Address)
            return existing, nil
        }
        return nil, err
    }

    // Set timestamps
    now := time.Now()
    fsvc.Ctime = now
    fsvc.Atime = now

    // Add to reverse lookup caches (ignore duplicates)
    fsc.byAddress.Set(fsvc.Address, *fsvc.Function)
    fsc.byFunctionUID.Set(fsvc.Function.UID, *fsvc.Function)

    return nil, nil
}
```

**Usage:**
```go
// After creating a deployment
fsvc := FuncSvc{
    Function: &fn.ObjectMeta,
    Address:  fmt.Sprintf("http://%s:8888", svcName),
    ...
}
fsc.Add(fsvc)
```

#### 3. Touch by Address (Keep-Alive)

```go
func (fsc *FunctionServiceCache) TouchByAddress(address string) error {
    responseChannel := make(chan *fscResponse)
    fsc.requestChannel <- &fscRequest{
        requestType:     TOUCH,
        address:         address,
        responseChannel: responseChannel,
    }
    resp := <-responseChannel
    return resp.error
}

func (fsc *FunctionServiceCache) _touchByAddress(address string) error {
    // Reverse lookup: address → function
    m, err := fsc.byAddress.Get(address)
    if err != nil {
        return err
    }

    // Get FuncSvc and update Atime
    fsvc, err := fsc.byFunction.Get(crd.CacheKeyURFromMeta(&m))
    if err != nil {
        return err
    }

    fsvc.Atime = time.Now()
    return nil
}
```

**Usage:**
```go
// Router calls this after successfully routing request
executor.TapService("http://10.244.0.5:8888")
```

#### 4. List Old (For Idle Reaping)

```go
func (fsc *FunctionServiceCache) ListOld(age time.Duration) ([]*FuncSvc, error) {
    responseChannel := make(chan *fscResponse)
    fsc.requestChannel <- &fscRequest{
        requestType:     LISTOLD,
        age:             age,
        responseChannel: responseChannel,
    }
    resp := <-responseChannel
    return resp.objects, resp.error
}
```

**Usage:**
```go
// In idle pod reaper
oldFuncs, _ := fsc.ListOld(2 * time.Minute)
for _, fsvc := range oldFuncs {
    if time.Since(fsvc.Atime) > idleTimeout {
        // Delete pod
        k8s.DeletePod(fsvc.KubernetesObjects[0])
        fsc.DeleteEntry(fsvc)
    }
}
```

#### 5. Delete Entry

```go
func (fsc *FunctionServiceCache) DeleteEntry(fsvc *FuncSvc) {
    // Delete from all three caches
    fsc.byFunction.Delete(crd.CacheKeyURFromMeta(fsvc.Function))
    fsc.byAddress.Delete(fsvc.Address)
    fsc.byFunctionUID.Delete(fsvc.Function.UID)

    // Record metrics
    metrics.FuncRunningSummary.WithLabelValues(
        fsvc.Function.Name,
        fsvc.Function.Namespace,
    ).Observe(fsvc.Atime.Sub(fsvc.Ctime).Seconds())
}
```

---

## PoolCache - Advanced Cache

**File:** `poolcache.go`
**Used by:** poolmgr executor **only**

This is where the magic happens. PoolCache enables:
- Multiple concurrent requests per pod
- Intelligent load balancing
- Request queuing (not rejection)
- CPU-aware scheduling

### Data Structure

```go
type PoolCache struct {
    cache          map[crd.CacheKeyURG]*funcSvcGroup
    requestChannel chan *request
    logger         *zap.Logger
}

// CacheKeyURG: Unique Resource Generation key (adds generation to UR)
type CacheKeyURG struct {
    Namespace       string
    Name            string
    ResourceVersion string
    Generation      int64  // Function generation (increments on updates)
    UID             types.UID
}

// Function service group - all pods for one function
type funcSvcGroup struct {
    svcWaiting int                      // # specializations currently in progress
    svcRetain  int                      // Min # pods to keep alive
    svcs       map[string]*funcSvcInfo  // address → pod info
    queue      *Queue                   // FIFO queue of waiting requests
    deleted    bool                     // Function marked for deletion?
}

// Info about one specialized pod
type funcSvcInfo struct {
    val             *FuncSvc          // The cached function service
    activeRequests  int               // Current # of concurrent requests
    currentCPUUsage resource.Quantity // Current CPU usage
    cpuLimit        resource.Quantity // Max CPU before load shedding
}
```

### Request Types

```go
const (
    getValue                 // Get an available pod (or wait/queue)
    setValue                 // Add a newly specialized pod
    markAvailable            // Decrement activeRequests (request completed)
    deleteValue              // Remove pod from cache
    setCPUUtilization        // Update CPU usage
    markSpecializationFailure // Decrement svcWaiting (specialization failed)
    listAvailableValue       // List all idle pods (for reaping)
    logFuncSvc               // Dump cache state
    markDeleted              // Mark function as deleted
)
```

### The Magic: getValue - Intelligent Request Routing

This is the **most complex and important** function in the entire caching system.

```go
case getValue:
    funcSvcGroup, ok := c.cache[req.function]
    if !ok {
        // First request for this function ever
        c.cache[req.function] = NewFuncSvcGroup()
        c.cache[req.function].svcWaiting++
        return error(NotFound)  // Trigger specialization
    }

    // ====================
    // STEP 1: Find an available pod
    // ====================
    found := false
    totalActiveRequests := 0

    for addr := range funcSvcGroup.svcs {
        totalActiveRequests += funcSvcGroup.svcs[addr].activeRequests

        // Check if this pod has capacity
        if funcSvcGroup.svcs[addr].activeRequests < req.requestsPerPod &&
           funcSvcGroup.svcs[addr].currentCPUUsage.Cmp(funcSvcGroup.svcs[addr].cpuLimit) < 1 {

            // Found an available pod!
            funcSvcGroup.svcs[addr].activeRequests++
            logger.Debug("using existing pod",
                zap.String("address", addr),
                zap.Int("activeRequests", funcSvcGroup.svcs[addr].activeRequests))

            resp.value = funcSvcGroup.svcs[addr].val
            found = true
            break
        }
    }

    if found {
        return resp  // Success!
    }

    // ====================
    // STEP 2: All pods busy - can we create a new one?
    // ====================
    concurrencyUsed := len(funcSvcGroup.svcs) +           // # specialized pods
                       (funcSvcGroup.svcWaiting -          // # specializing
                        funcSvcGroup.queue.Len())          // # queued requests

    if req.concurrency > 0 && concurrencyUsed < req.concurrency {
        // Yes! We have concurrency budget
        funcSvcGroup.svcWaiting++
        logger.Debug("triggering new specialization",
            zap.Int("concurrencyUsed", concurrencyUsed),
            zap.Int("concurrencyLimit", req.concurrency))

        return error(NotFound)  // Trigger specialization
    }

    // ====================
    // STEP 3: Can't create new pod - check "virtual capacity"
    // ====================
    // Virtual capacity = total capacity - (active + waiting)
    // Example: 3 pods * 5 req/pod = 15 capacity
    //          10 active + 2 waiting = 12 used
    //          15 - 12 = 3 virtual capacity
    capacity := (concurrencyUsed * req.requestsPerPod) -
                (totalActiveRequests + funcSvcGroup.svcWaiting)

    if capacity > 0 {
        // Yes! Existing pods will free up soon - queue this request
        funcSvcGroup.svcWaiting++

        svcWait := &svcWait{
            svcChannel: make(chan *FuncSvc),
            ctx:        req.ctx,
        }
        funcSvcGroup.queue.Push(svcWait)

        logger.Debug("queueing request",
            zap.Int("queueLen", funcSvcGroup.queue.Len()),
            zap.Int("virtualCapacity", capacity))

        resp.svcWaitValue = svcWait  // Caller will block on svcChannel
        return resp
    }

    // ====================
    // STEP 4: No capacity at all - reject request
    // ====================
    if req.concurrency > 0 && concurrencyUsed >= req.concurrency {
        return error(TooManyRequests)  // 429
    } else {
        funcSvcGroup.svcWaiting++
        return error(NotFound)  // Try to specialize anyway
    }
```

### setValue - Add Specialized Pod

Called when a pod finishes specialization:

```go
case setValue:
    if _, ok := c.cache[req.function]; !ok {
        c.cache[req.function] = NewFuncSvcGroup()
    }

    // Add new pod
    if _, ok := c.cache[req.function].svcs[req.address]; !ok {
        c.cache[req.function].svcs[req.address] = &funcSvcInfo{}
    }

    c.cache[req.function].svcRetain = req.svcsRetain
    c.cache[req.function].svcs[req.address].val = req.value
    c.cache[req.function].svcs[req.address].activeRequests = 1  // Marked busy
    c.cache[req.function].svcs[req.address].cpuLimit = req.cpuUsage

    // Decrement waiting counter
    if c.cache[req.function].svcWaiting > 0 {
        c.cache[req.function].svcWaiting--

        // ====================
        // Wake up queued requests!
        // ====================
        svcCapacity := req.requestsPerPod - 1  // -1 because one request already using it
        queueLen := c.cache[req.function].queue.Len()

        if svcCapacity > queueLen {
            svcCapacity = queueLen
        }

        for i := 0; i <= svcCapacity; {
            popped := c.cache[req.function].queue.Pop()
            if popped == nil {
                break
            }

            // Check if request didn't timeout
            if popped.ctx.Err() == nil {
                popped.svcChannel <- req.value  // Wake up waiting goroutine!
                c.cache[req.function].svcs[req.address].activeRequests++
                i++
            }

            close(popped.svcChannel)
            c.cache[req.function].svcWaiting--
        }
    }

    logger.Debug("added specialized pod",
        zap.String("address", req.address),
        zap.Int("activeRequests", c.cache[req.function].svcs[req.address].activeRequests))
```

### markAvailable - Request Completed

Called when a request completes (via TapService):

```go
case markAvailable:
    if _, ok := c.cache[req.function]; ok {
        if _, ok = c.cache[req.function].svcs[req.address]; ok {
            if c.cache[req.function].svcs[req.address].activeRequests > 0 {
                // Decrement counter
                c.cache[req.function].svcs[req.address].activeRequests--

                logger.Debug("request completed",
                    zap.String("address", req.address),
                    zap.Int("activeRequests",
                        c.cache[req.function].svcs[req.address].activeRequests))
            } else {
                logger.Error("invalid markAvailable - already at 0",
                    zap.String("address", req.address))
            }
        }
    }
```

### Public API

```go
// Get an available pod (or wait/queue/reject)
func (c *PoolCache) GetSvcValue(ctx context.Context, function crd.CacheKeyURG,
                                 requestsPerPod int, concurrency int) (*FuncSvc, error) {
    respChannel := make(chan *response)
    c.requestChannel <- &request{
        ctx:             ctx,
        requestType:     getValue,
        function:        function,
        requestsPerPod:  requestsPerPod,
        concurrency:     concurrency,
        responseChannel: respChannel,
    }
    resp := <-respChannel

    // If we got a wait channel, block until pod available
    if resp.svcWaitValue != nil {
        select {
        case <-ctx.Done():
            return nil, ctx.Err()  // Timeout
        case funcSvc := <-resp.svcWaitValue.svcChannel:
            return funcSvc, nil  // Woken up!
        }
    }

    return resp.value, resp.error
}

// Add a newly specialized pod
func (c *PoolCache) SetSvcValue(ctx context.Context, function crd.CacheKeyURG,
                                 address string, value *FuncSvc,
                                 cpuLimit resource.Quantity,
                                 requestsPerPod, svcsRetain int)

// Mark request completed (decrement activeRequests)
func (c *PoolCache) MarkAvailable(function crd.CacheKeyURG, address string)

// Update CPU usage
func (c *PoolCache) SetCPUUtilization(function crd.CacheKeyURG, address string,
                                       cpuUsage resource.Quantity)

// Delete pod from cache
func (c *PoolCache) DeleteValue(ctx context.Context, function crd.CacheKeyURG,
                                 address string) error

// Mark specialization failed (decrement svcWaiting)
func (c *PoolCache) MarkSpecializationFailure(function crd.CacheKeyURG)

// List all idle pods (for reaping)
func (c *PoolCache) ListAvailableValue() []*FuncSvc

// Mark function deleted
func (c *PoolCache) MarkFuncDeleted(function crd.CacheKeyURG)
```

---

## Queue - FIFO for Waiting Requests

**File:** `queue.go`

Simple thread-safe FIFO queue using Go's `container/list`:

```go
type Queue struct {
    items *list.List  // Doubly-linked list
    mutex sync.Mutex
}

type svcWait struct {
    svcChannel chan *FuncSvc  // Channel to wake up waiting goroutine
    ctx        context.Context // For timeout detection
}
```

### Operations

```go
// Push to back of queue
func (q *Queue) Push(item *svcWait) {
    q.mutex.Lock()
    defer q.mutex.Unlock()
    q.items.PushBack(item)
}

// Pop from front of queue
func (q *Queue) Pop() *svcWait {
    q.mutex.Lock()
    defer q.mutex.Unlock()

    item := q.items.Front()
    if item == nil {
        return nil
    }

    q.items.Remove(item)
    return item.Value.(*svcWait)
}

// Get queue length
func (q *Queue) Len() int {
    q.mutex.Lock()
    defer q.mutex.Unlock()
    return q.items.Len()
}

// Remove expired requests (timeout)
func (q *Queue) Expired() int {
    q.mutex.Lock()
    defer q.mutex.Unlock()

    expired := 0
    svcExpired := []*list.Element{}

    for item := q.items.Front(); item != nil; item = item.Next() {
        svcWait := item.Value.(*svcWait)

        // Check if context canceled (timeout)
        if svcWait.ctx.Err() != nil {
            close(svcWait.svcChannel)
            svcExpired = append(svcExpired, item)
            expired++
        }
    }

    // Remove expired items
    for _, item := range svcExpired {
        q.items.Remove(item)
    }

    return expired
}
```

---

## Complete Request Flow Examples

### Example 1: Simple Flow (newdeploy)

**Scenario:** First invocation of function `hello` using newdeploy executor.

```go
// 1. Router calls executor
address, err := executor.GetServiceForFunction(function)

// 2. Executor checks cache
fsvc, err := fsc.GetByFunction(&function.ObjectMeta)
if err != nil {
    // Cache miss - create deployment
    deployment := createDeployment(function)
    k8s.Create(deployment)

    // Wait for pod to be ready
    pod := waitForPodReady(deployment)

    // Add to cache
    fsvc := FuncSvc{
        Function: &function.ObjectMeta,
        Address:  fmt.Sprintf("http://%s:8888", pod.Status.PodIP),
        ...
    }
    fsc.Add(fsvc)

    return fsvc.Address
}

// Cache hit!
return fsvc.Address
```

### Example 2: Complex Flow (poolmgr with queuing)

**Scenario:** Function `hello` with:
- `concurrency: 3` (max 3 pods)
- `requestsPerPod: 5` (each pod handles 5 concurrent requests)

**Initial state:** 0 pods

| Event | Action | State After |
|-------|--------|-------------|
| Request #1 arrives | getValue → NotFound<br>Trigger specialization<br>svcWaiting=1 | 0 pods<br>1 specializing |
| Request #2 arrives | getValue → NotFound<br>concurrencyUsed=1, concurrency=3<br>Trigger specialization<br>svcWaiting=2 | 0 pods<br>2 specializing |
| Pod #1 ready | setValue → activeRequests=1<br>svcWaiting=1 | 1 pod (1/5 busy)<br>1 specializing |
| Request #3 arrives | getValue → Found Pod #1<br>activeRequests=2 | 1 pod (2/5 busy)<br>1 specializing |
| Requests #4-6 arrive | getValue → Found Pod #1<br>activeRequests=3,4,5 | 1 pod (5/5 busy)<br>1 specializing |
| Request #7 arrives | getValue → All busy<br>concurrencyUsed=2, concurrency=3<br>Trigger specialization<br>svcWaiting=2 | 1 pod (5/5 busy)<br>2 specializing |
| Pod #2 ready | setValue → activeRequests=1<br>svcWaiting=1 | 2 pods (5/5, 1/5)<br>1 specializing |
| Requests #8-11 arrive | getValue → Found Pod #2<br>activeRequests=2,3,4,5 | 2 pods (5/5, 5/5)<br>1 specializing |
| Request #12 arrives | getValue → All busy<br>concurrencyUsed=3, concurrency=3<br>Can't specialize<br>virtualCapacity=(3*5)-(10+1)=4<br>**Queue request** | 2 pods (5/5, 5/5)<br>1 specializing<br>1 queued |
| Requests #13-15 arrive | getValue → **Queue requests** | 2 pods (5/5, 5/5)<br>1 specializing<br>4 queued |
| Pod #3 ready | setValue → activeRequests=1<br>svcWaiting=0<br>**Wake up 4 queued requests!**<br>activeRequests=5 | 3 pods (5/5, 5/5, 5/5)<br>0 queued |
| Request #16 arrives | getValue → All busy<br>concurrencyUsed=3, concurrency=3<br>virtualCapacity=0<br>**Return TooManyRequests (429)** | 3 pods (5/5, 5/5, 5/5) |
| Request #1 completes | markAvailable<br>Pod #1 activeRequests=4 | 3 pods (4/5, 5/5, 5/5) |
| Request #17 arrives | getValue → Found Pod #1<br>activeRequests=5 | 3 pods (5/5, 5/5, 5/5) |

**Code flow for Request #12 (queued):**

```go
// Router goroutine
fsvc, err := poolCache.GetSvcValue(ctx, function, requestsPerPod=5, concurrency=3)

// Inside getValue
capacity := (3 * 5) - (10 + 1) = 4  // Virtual capacity available
svcWait := &svcWait{
    svcChannel: make(chan *FuncSvc),
    ctx:        ctx,
}
queue.Push(svcWait)
return svcWait  // Return wait channel

// Router blocks
select {
case <-ctx.Done():
    return error("timeout")
case funcSvc := <-svcWait.svcChannel:
    // Woken up when Pod #3 ready!
    return funcSvc
}
```

**Code flow when Pod #3 becomes ready:**

```go
// Poolmgr goroutine
poolCache.SetSvcValue(ctx, function, address="10.244.0.7:8888", fsvc, ...)

// Inside setValue
svcs[addr].activeRequests = 1
svcWaiting--  // 1 → 0

// Wake up queued requests
svcCapacity := 5 - 1 = 4  // Pod can take 4 more requests
for i := 0; i < 4 && queue.Len() > 0; i++ {
    popped := queue.Pop()
    if popped.ctx.Err() == nil {  // Not timed out
        popped.svcChannel <- fsvc  // Wake up!
        svcs[addr].activeRequests++
    }
    close(popped.svcChannel)
}
// activeRequests now = 5 (1 + 4 queued)
```

---

## Design Patterns

### 1. Actor Model (Message Passing)

Both caches use a single goroutine processing requests from a channel:

**Advantages:**
- No locks needed for cache operations
- Simple concurrency model
- All operations atomic

**Trade-off:**
- All operations serialized (but very fast, < 1μs per op)

```go
// All public methods send to channel
func (c *PoolCache) GetSvcValue(...) (*FuncSvc, error) {
    respChannel := make(chan *response)
    c.requestChannel <- &request{...}
    resp := <-respChannel
    return resp.value, resp.error
}

// Single goroutine processes all requests
func (c *PoolCache) service() {
    for {
        req := <-c.requestChannel
        // Process request...
        req.responseChannel <- resp
    }
}
```

### 2. Waiting Queue with Channels

Instead of rejecting requests when pods are busy, queue them:

```go
// Create a channel for this request
svcWait := &svcWait{
    svcChannel: make(chan *FuncSvc),
    ctx:        ctx,
}
queue.Push(svcWait)

// Caller blocks on channel
select {
case <-ctx.Done():
    return error("timeout")
case funcSvc := <-svcWait.svcChannel:
    return funcSvc  // Woken up!
}
```

When pod becomes available:
```go
popped := queue.Pop()
popped.svcChannel <- funcSvc  // Wake up waiting goroutine
close(popped.svcChannel)
```

### 3. Three-Way Cache Indexing

FunctionServiceCache indexes the same data three ways:

```go
byFunction    map[CacheKeyUR]*FuncSvc           // function → service
byAddress     map[string]metav1.ObjectMeta      // address → function
byFunctionUID map[types.UID]metav1.ObjectMeta   // UID → function
```

**Why?**
- `byFunction`: Primary lookup (router → executor)
- `byAddress`: Reverse lookup for TapService
- `byFunctionUID`: Fast deletion, orphan cleanup

### 4. Idle Pod Reaping via Atime

Every cache access updates `Atime`:

```go
fsvc.Atime = time.Now()
```

Background goroutine:
```go
oldPods := cache.ListOld(idleTimeout)
for _, pod := range oldPods {
    if time.Since(pod.Atime) > idleTimeout {
        DeletePod(pod)
    }
}
```

### 5. Virtual Capacity Calculation

Allows queuing requests even when all pods are busy:

```go
// Example: 3 pods, 5 req/pod each
// Current: 10 active requests, 2 specializing

totalCapacity := (3 pods) * (5 req/pod) = 15
usedCapacity := 10 active + 2 waiting = 12
virtualCapacity := 15 - 12 = 3  // Can queue 3 requests
```

---

## Performance Characteristics

### Time Complexity

| Operation | FunctionServiceCache | PoolCache |
|-----------|---------------------|-----------|
| Get | O(1) | O(n) where n = # pods for function |
| Add | O(1) | O(1) |
| Delete | O(1) | O(1) |
| Touch | O(1) | O(1) |
| ListOld | O(n) where n = # functions | O(n) where n = # function pods |

### Space Complexity

| Cache | Space |
|-------|-------|
| FunctionServiceCache | O(f) where f = # unique functions |
| PoolCache | O(f * p) where f = # functions, p = avg pods per function |

### Latency

| Operation | Typical Latency |
|-----------|----------------|
| Cache hit (Get) | < 1ms |
| Cache miss (K8s API) | 100-500ms |
| Queue wait | 10-1000ms (depends on request processing time) |

### Throughput

- **FunctionServiceCache**: ~1M ops/sec (single-threaded, no contention)
- **PoolCache**: ~100K ops/sec (more complex logic)

---

## Code Location Reference

```
pkg/executor/fscache/
├── functionServiceCache.go      # Lines of interest:
│   ├── FuncSvc struct           # L54-65
│   ├── FunctionServiceCache     # L68-77
│   ├── MakeFunctionServiceCache # L109-120
│   ├── service() goroutine      # L122-168
│   ├── GetByFunction            # L192-205
│   ├── GetFuncSvc (poolmgr)     # L208-222
│   ├── Add                      # L270-312
│   ├── TouchByAddress           # L315-324
│   ├── DeleteEntry              # L340-370
│   └── ListOld                  # L408-417
│
├── poolcache.go                 # Lines of interest:
│   ├── funcSvcInfo struct       # L50-55
│   ├── funcSvcGroup struct      # L57-63
│   ├── PoolCache struct         # L67-71
│   ├── NewPoolCache             # L99-107
│   ├── service() goroutine      # L116-349
│   ├── getValue                 # L121-185 (most complex!)
│   ├── setValue                 # L186-220
│   ├── markAvailable            # L276-288
│   ├── GetSvcValue              # L359-380
│   ├── SetSvcValue              # L394-407
│   └── MarkAvailable            # L421-429
│
└── queue.go                     # Lines of interest:
    ├── Queue struct             # L8-11
    ├── NewQueue                 # L14-18
    ├── Push                     # L19-23
    ├── Pop                      # L25-38
    ├── Expired                  # L41-64
    └── Len                      # L66-70
```

---

## Summary

The `fscache` package is **Fission's secret weapon** for performance:

### FunctionServiceCache (Simple)
- ✅ Sub-millisecond lookups
- ✅ Three-way indexing for flexibility
- ✅ TTL-based idle reaping
- ✅ Actor model for thread safety

### PoolCache (Advanced)
- ✅ Concurrent request tracking
- ✅ Intelligent load balancing
- ✅ Request queuing (not rejection)
- ✅ CPU-aware scheduling
- ✅ Virtual capacity calculation

### Key Insights

1. **Actor pattern** avoids complex locking
2. **Channel-based queuing** provides better UX than rejection
3. **Virtual capacity** allows smart queueing
4. **Atime tracking** enables efficient garbage collection
5. **Three-way indexing** optimizes different access patterns

This caching layer is what enables Fission to achieve:
- **~100ms cold starts** (poolmgr)
- **< 10ms warm starts**
- **High concurrency** per function
- **Efficient resource utilization**

**Bottom line:** Without this cache, Fission would be 100-500x slower! 🚀
