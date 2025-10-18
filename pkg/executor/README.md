# Fission Executor Component Explained

This document provides a comprehensive explanation of the `pkg/executor/` package, which is the core of Fission's function execution system.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Main Components](#main-components)
  - [Executor Core](#executor-core)
  - [ExecutorType Interface](#executortype-interface)
  - [Function Service Cache (fscache)](#function-service-cache-fscache)
  - [Pool Manager](#pool-manager)
  - [ConfigMap/Secret Controller](#configmapsecret-controller)
  - [Client](#client)
- [Request Flow Examples](#request-flow-examples)
- [Critical Patterns](#critical-patterns)
- [fscache Deep Dive](#fscache-deep-dive)

---

## Overview

The executor package is the **core of Fission's function execution system**. It manages the lifecycle of function pods and routes function invocations to the appropriate containers.

**Architecture Flow:**
```
Router Request → Executor API → Executor Core → ExecutorType (poolmgr/newdeploy/container) → Function Pod
                                      ↓
                              FunctionServiceCache
```

**Package Structure:**
```
pkg/executor/
├── executor.go              # Core coordination logic
├── api.go                   # HTTP API endpoints
├── executortype/            # Pluggable executor types
│   ├── executortype.go      # Interface definition
│   ├── poolmgr/             # Pool-based executor (fast cold start)
│   ├── newdeploy/           # Deployment-based executor (HPA support)
│   └── container/           # Container-based executor
├── fscache/                 # Performance-critical caching
│   ├── functionServiceCache.go  # Simple cache for newdeploy/container
│   ├── poolcache.go         # Advanced cache for poolmgr
│   └── queue.go             # FIFO queue for waiting requests
├── cms/                     # ConfigMap/Secret watcher
├── client/                  # Client library for router
├── metrics/                 # Prometheus metrics
├── reaper/                  # Cleanup old resources
└── util/                    # Helper utilities
```

---

## Architecture

The executor sits between the router and function pods:

1. **Router** calls executor API to get a pod address for a function
2. **Executor** checks cache, potentially creates/specializes pods
3. **Router** forwards HTTP request directly to the pod
4. **Router** calls `TapService` to update last-access time

---

## Main Components

### Executor Core

**Files:** `executor.go`, `api.go`

#### `executor.go` - Core Coordination

The main `Executor` struct manages multiple executor types:

```go
type Executor struct {
    logger        *zap.Logger
    executorTypes map[fv1.ExecutorType]executortype.ExecutorType
    cms           *cms.ConfigSecretController
    fissionClient versioned.Interface
    requestChan   chan *createFuncServiceRequest
    fsCreateWg    sync.Map  // Prevents duplicate specializations
}
```

**Key Function: `serveCreateFuncServices()`**

This goroutine ensures that:
- Multiple concurrent requests for **different functions** are parallelized
- **Only ONE specialization happens per function** (using WaitGroups)
- This prevents the "thundering herd" problem

```go
func (executor *Executor) serveCreateFuncServices(ctx context.Context) {
    for {
        req := <-executor.requestChan
        fnkeyUR := crd.CacheKeyURFromObject(req.function)

        // Check if another request is already specializing this function
        wg, found := executor.fsCreateWg.Load(fnkeyUR)
        if !found {
            // First request - create WaitGroup and specialize
            wg := &sync.WaitGroup{}
            wg.Add(1)
            executor.fsCreateWg.Store(fnkeyUR, wg)

            go func() {
                fsvc, err := executor.createServiceForFunction(ctx, req.function)
                req.respChan <- &createFuncServiceResponse{funcSvc: fsvc, err: err}
                executor.fsCreateWg.Delete(fnkeyUR)
                wg.Done()
            }()
        } else {
            // Concurrent request - wait for first one to finish
            go func() {
                wg.(*sync.WaitGroup).Wait()
                fsvc, err := executor.getFunctionServiceFromCache(ctx, req.function)
                req.respChan <- &createFuncServiceResponse{funcSvc: fsvc, err: err}
            }()
        }
    }
}
```

#### `api.go` - HTTP API

Exposes endpoints for the router to call:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/v2/getServiceForFunction` | POST | Get pod address for a function |
| `/v2/tapServices` | POST | Update last-access time (keep-alive) |
| `/v2/unTapService` | POST | Mark service as inactive |
| `/v2/debugInfo` | GET | Dump cache state for debugging |
| `/healthz` | GET | Health check |

**Example: `getServiceForFunctionAPI()`**

```go
func (executor *Executor) getServiceForFunctionAPI(w http.ResponseWriter, r *http.Request) {
    // 1. Parse function metadata from request body
    fn := &fv1.Function{}
    json.Unmarshal(body, &fn)

    // 2. Check cache first (for poolmgr/newdeploy/container)
    et := executor.executorTypes[fn.Spec.InvokeStrategy.ExecutionStrategy.ExecutorType]
    fsvc, err := et.GetFuncSvcFromCache(ctx, fn)
    if err == nil && et.IsValid(ctx, fsvc) {
        // Cache hit!
        w.Write([]byte(fsvc.Address))
        return
    }

    // 3. Cache miss - create/specialize pod
    serviceName, err := executor.getServiceForFunction(ctx, fn)
    w.Write([]byte(serviceName))
}
```

---

### ExecutorType Interface

**File:** `executortype/executortype.go`

This is the **plugin interface** that all executor types must implement:

```go
type ExecutorType interface {
    // Run background jobs
    Run(context.Context, manager.Interface)

    // Get executor type name
    GetTypeName(context.Context) fv1.ExecutorType

    // Get/create function service (may specialize pod)
    GetFuncSvc(context.Context, *fv1.Function) (*fscache.FuncSvc, error)

    // Get from cache only
    GetFuncSvcFromCache(context.Context, *fv1.Function) (*fscache.FuncSvc, error)

    // Check if cached service is still valid
    IsValid(context.Context, *fscache.FuncSvc) bool

    // Update last-access time
    TapService(ctx context.Context, serviceUrl string) error

    // Mark service as inactive
    UnTapService(ctx context.Context, fnMeta *metav1.ObjectMeta, svcHost string)

    // Mark specialization as failed
    MarkSpecializationFailure(ctx context.Context, fnMeta *metav1.ObjectMeta)

    // Delete from cache
    DeleteFuncSvcFromCache(context.Context, *fscache.FuncSvc)

    // Dump debug info
    DumpDebugInfo(context.Context) error

    // Recycle pods (e.g., after ConfigMap change)
    RefreshFuncPods(context.Context, *zap.Logger, fv1.Function) error

    // Adopt orphaned resources from previous executor
    AdoptExistingResources(context.Context)

    // Cleanup old executor instances
    CleanupOldExecutorObjects(context.Context)
}
```

**Three Implementations:**

1. **`poolmgr/`** - Pool-based (fast cold start, ~100ms)
2. **`newdeploy/`** - Deployment-based (supports HPA)
3. **`container/`** - Container-based

---

### Function Service Cache (fscache)

**Location:** `pkg/executor/fscache/`

The **performance-critical component** that maps functions to running pods. See [fscache Deep Dive](#fscache-deep-dive) for detailed explanation.

**Key Concept - FuncSvc:**

```go
type FuncSvc struct {
    Name              string                  // Pod name
    Function          *metav1.ObjectMeta      // Which function
    Environment       *fv1.Environment        // Which environment
    Address           string                  // e.g., "http://10.244.0.5:8888"
    KubernetesObjects []apiv1.ObjectReference // References to pod/svc
    Executor          fv1.ExecutorType        // poolmgr/newdeploy/container
    CPULimit          resource.Quantity
    Ctime             time.Time               // Creation time
    Atime             time.Time               // Last access time (for idle reaping)
}
```

---

### Pool Manager

**Location:** `pkg/executor/executortype/poolmgr/`

The **most complex executor type** - implements Fission's signature fast cold-start.

#### How Pool Manager Works

**1. Pre-warm Pools**

Maintains pools of generic containers per environment:

```go
type GenericPool struct {
    deployment       *appsv1.Deployment  // Generic pods
    replicas         int32               // Pool size
    requestedCPU     int64
    env              *fv1.Environment
}
```

**2. On First Invocation**

1. Grab a generic pod from the pool
2. "Specialize" it by calling the fetcher sidecar to load function code
3. Cache the mapping: `function → specialized pod`

```go
func (gp *GenericPool) getFuncSvc(ctx context.Context, fn *fv1.Function) (*fscache.FuncSvc, error) {
    // 1. Check cache
    fsvc, err := gp.fsCache.GetFuncSvc(ctx, &fn.ObjectMeta, fn.GetRequestPerPod(), fn.GetConcurrency())
    if err == nil {
        return fsvc, nil
    }

    // 2. Get a generic pod from the pool
    pod, err := gp.choosePod(ctx, fn)

    // 3. Specialize it
    err = gp.specializePod(ctx, pod, fn)

    // 4. Cache and return
    fsvc = &fscache.FuncSvc{
        Function: &fn.ObjectMeta,
        Address:  fmt.Sprintf("http://%s:%d", pod.Status.PodIP, port),
        ...
    }
    gp.fsCache.AddFunc(ctx, *fsvc, fn.GetRequestPerPod(), svcsRetain)
    return fsvc, nil
}
```

**3. Subsequent Invocations**

Serve from cache (sub-millisecond lookup).

**4. Idle Reaping**

Background goroutine periodically deletes pods idle > `IdleTimeout`:

```go
func (gpm *GenericPoolManager) idleObjectReaper(ctx context.Context) {
    funcSvcs, err := gpm.fsCache.ListOldForPool(5 * time.Second)

    for _, fsvc := range funcSvcs {
        idlePodReapTime := gpm.defaultIdlePodReapTime  // 2 minutes default

        if time.Since(fsvc.Atime) < idlePodReapTime {
            continue
        }

        // Delete from cache and Kubernetes
        gpm.fsCache.DeleteOldPoolCache(ctx, fsvc, idlePodReapTime)
        reaper.CleanupKubeObject(ctx, gpm.logger, gpm.kubernetesClient, &pod)
    }
}
```

#### Special Features

- **Concurrent requests per pod**: Configurable via `RequestsPerPod`
- **CPU utilization tracking**: For intelligent load balancing
- **WebSocket support**: Marks pods as "in-use" to prevent premature deletion
- **Multi-tenancy**: Can share pods across functions (if `AllowedFunctionsPerContainer=infinite`)

#### Key Files

- `gpm.go` - GenericPoolManager (main orchestrator)
- `gp.go` - GenericPool (manages one environment's pool)
- `poolpodcontroller.go` - Kubernetes controller that watches/manages pool pods
- `readyPodController.go` - Tracks which pods are ready to be specialized

---

### ConfigMap/Secret Controller

**Location:** `pkg/executor/cms/`

Watches ConfigMaps and Secrets referenced by functions:

```go
type ConfigSecretController struct {
    logger        *zap.Logger
    fissionClient versioned.Interface
}
```

**Behavior:**

1. Watches for ConfigMap/Secret changes
2. Finds all functions referencing the changed resource
3. Calls `RefreshFuncPods()` on each executor type
4. Each executor deletes affected pods, forcing re-specialization with new configs

```go
func refreshPods(ctx context.Context, logger *zap.Logger, funcs []fv1.Function,
                 types map[fv1.ExecutorType]executortype.ExecutorType) {
    for _, f := range funcs {
        et := types[f.Spec.InvokeStrategy.ExecutionStrategy.ExecutorType]
        err := et.RefreshFuncPods(ctx, logger, f)
        if err != nil {
            logger.Error("Failed to recycle pods for function after configmap/secret changed",
                zap.Error(err), zap.Any("function", f))
        }
    }
}
```

---

### Client

**Location:** `pkg/executor/client/`

Used by the router to communicate with the executor:

```go
// Get pod address for function
address, err := client.GetServiceForFunction(function)

// Keep-alive (update last-access time)
client.TapService(serviceURL)

// Mark inactive
client.UnTapService(function, serviceURL)
```

---

## Request Flow Examples

### First Invocation of a Function

**Scenario:** User calls `curl http://router/hello` for the first time.

1. **Router** receives HTTP request for function `hello`
2. **Router** calls Executor API:
   ```
   POST /v2/getServiceForFunction
   Body: {function metadata for "hello"}
   ```

3. **Executor** (`getServiceForFunctionAPI`):
   - Checks cache → **miss**
   - Sends request to `serveCreateFuncServices()` goroutine

4. **serveCreateFuncServices**:
   - Creates WaitGroup for function `hello`
   - Calls `createServiceForFunction()` → delegates to executor type

5. **PoolManager** (`GetFuncSvc`):
   - Gets environment for function
   - Gets pool for environment (creates if doesn't exist)
   - Calls `pool.getFuncSvc()`:
     - Grabs a generic pod from the pool
     - Specializes it (POST to fetcher sidecar with function code URL)
     - Waits for specialization to complete (~100ms)
     - Returns address like `http://10.244.0.5:8888`

6. **Executor** caches the mapping and returns address to Router

7. **Router** forwards the user request to `http://10.244.0.5:8888`

8. **Router** calls `POST /v2/tapServices` to update last-access time

**Total time:** ~100-200ms (cold start)

---

### Second Invocation

1. **Router** calls Executor API
2. **Executor** checks cache → **hit!**
3. Returns address immediately (< 1ms)
4. **Router** forwards request

**Total time:** < 10ms (warm start)

---

### Concurrent Requests for Same Function

**Scenario:** 5 requests arrive simultaneously for function `hello` (never invoked before).

1. All 5 requests call executor API
2. **First request**:
   - Cache miss
   - Creates WaitGroup for `hello`
   - Starts specialization in goroutine

3. **Requests 2-5**:
   - Cache miss
   - Find existing WaitGroup for `hello`
   - Wait on WaitGroup (block)

4. **First request completes specialization**:
   - Adds to cache
   - WaitGroup.Done()

5. **Requests 2-5 wake up**:
   - Fetch from cache
   - All get same address

**Result:** Only 1 pod specialized, not 5!

---

## Critical Patterns

### 1. Specialization Coordination

Prevents duplicate pod creation for concurrent requests:

```go
// Only one specialization per function at a time
wg, found := executor.fsCreateWg.Load(fnkeyUR)
if !found {
    wg := &sync.WaitGroup{}
    wg.Add(1)
    executor.fsCreateWg.Store(fnkeyUR, wg)
    go specialize()  // Actually create the pod
    defer wg.Done()
} else {
    wg.Wait()  // Wait for concurrent request to finish
    return getFromCache()
}
```

### 2. Idle Pod Reaping

Runs periodically in poolmgr:

```go
funcSvcs := gpm.fsCache.ListOldForPool(5 * time.Second)
for fsvc := range funcSvcs {
    if time.Since(fsvc.Atime) > idlePodReapTime {
        DeleteOldPoolCache(fsvc)
        CleanupKubeObject(pod)
    }
}
```

### 3. TapService (Keep-Alive)

Router calls this after successfully routing a request to update `Atime`, preventing premature pod deletion:

```go
// Router side
response := http.Get(funcAddress + "/invoke")
executor.TapService(funcAddress)

// Executor side
func TapService(address string) {
    fsvc := cache.GetByAddress(address)
    fsvc.Atime = time.Now()
}
```

### 4. Context Management

All operations accept `context.Context` for:
- Graceful shutdown
- Timeout control
- Request cancellation

```go
fnSpecializationTimeoutContext, cancel := context.WithTimeoutCause(
    req.context,
    time.Duration(specializationTimeout+buffer)*time.Second,
    fmt.Errorf("function specialization timeout (%d)s exceeded", specializationTimeout+buffer))
defer cancel()

fsvc, err := executor.createServiceForFunction(fnSpecializationTimeoutContext, req.function)
```

---

## fscache Deep Dive

The `pkg/executor/fscache/` package implements a **two-tiered caching system**.

### Overview: Two Cache Types

1. **FunctionServiceCache** - Simple cache for newdeploy/container
2. **PoolCache** - Advanced cache for poolmgr (concurrent request tracking)

---

### FunctionServiceCache - Simple Cache

**Data Structure:**

```go
type FunctionServiceCache struct {
    byFunction        *cache.Cache[crd.CacheKeyUR, *FuncSvc]     // function → FuncSvc
    byAddress         *cache.Cache[string, metav1.ObjectMeta]    // IP:port → function
    byFunctionUID     *cache.Cache[types.UID, metav1.ObjectMeta] // UID → function
    connFunctionCache *PoolCache                                 // For poolmgr
    PodToFsvc         sync.Map                                   // pod-name → FuncSvc
    WebsocketFsvc     sync.Map                                   // fsvc-name → bool
    requestChannel    chan *fscRequest                           // Actor model
}
```

**Actor Model Pattern:**

All operations go through a single goroutine to avoid race conditions:

```go
func (fsc *FunctionServiceCache) service() {
    for {
        req := <-fsc.requestChannel
        resp := &fscResponse{}
        switch req.requestType {
        case TOUCH:
            // Update last access time
            resp.error = fsc._touchByAddress(req.address)
        case LISTOLD:
            // Find idle pods older than req.age
            for _, fsvc := range fsc.byFunction.Copy() {
                if time.Since(fsvc.Atime) > req.age {
                    funcObjects = append(funcObjects, fsvc)
                }
            }
            resp.objects = funcObjects
        case LOG:
            // Dump cache for debugging
        case LISTOLDPOOL:
            // For poolmgr: find idle pods
        }
        req.responseChannel <- resp
    }
}
```

**Key Operations:**

```go
// Get function service
fsvc, err := fsc.GetByFunction(&functionMeta)

// Add to cache
fsc.Add(FuncSvc{...})

// Touch (keep-alive)
fsc.TouchByAddress("http://10.244.0.5:8888")

// List old for reaping
oldFuncs := fsc.ListOld(2 * time.Minute)

// Delete
fsc.DeleteEntry(fsvc)
```

---

### PoolCache - Advanced Cache (Poolmgr Only)

Handles **concurrent request routing** and **load balancing** across multiple pods.

**Data Structure:**

```go
type PoolCache struct {
    cache          map[crd.CacheKeyURG]*funcSvcGroup
    requestChannel chan *request
}

type funcSvcGroup struct {
    svcWaiting int                      // # specializations in progress
    svcRetain  int                      // Min # pods to keep alive
    svcs       map[string]*funcSvcInfo  // address → pod info
    queue      *Queue                   // Waiting requests (when all pods busy)
    deleted    bool                     // Function deleted?
}

type funcSvcInfo struct {
    val             *FuncSvc
    activeRequests  int               // Current concurrent requests
    currentCPUUsage resource.Quantity // Current CPU usage
    cpuLimit        resource.Quantity // Max CPU before shedding load
}
```

**The Magic: `getValue()` - Intelligent Request Routing**

This is the **most critical** function in poolmgr:

```go
case getValue:
    funcSvcGroup := c.cache[req.function]

    // STEP 1: Try to find an available pod
    for addr := range funcSvcGroup.svcs {
        if svcs[addr].activeRequests < req.requestsPerPod &&
           svcs[addr].currentCPUUsage < svcs[addr].cpuLimit {
            // Found one! Mark it busy and return
            svcs[addr].activeRequests++
            resp.value = svcs[addr].val
            return
        }
    }

    // STEP 2: All pods busy - check if we can start a new specialization
    concurrencyUsed := len(svcs) + (svcWaiting - queue.Len())

    if req.concurrency > 0 && concurrencyUsed < req.concurrency {
        // Yes! Trigger new specialization
        svcWaiting++
        return error(NotFound)  // Signals: specialize a new pod
    }

    // STEP 3: Can't specialize - check if existing pods have "virtual capacity"
    capacity := (concurrencyUsed * requestsPerPod) - (totalActiveRequests + svcWaiting)

    if capacity > 0 {
        // Yes! Queue this request to wait for a pod to become free
        svcWaiting++
        svcWait := &svcWait{
            svcChannel: make(chan *FuncSvc),
            ctx:        req.ctx,
        }
        queue.Push(svcWait)
        return svcWait  // Caller will block on svcChannel
    }

    // STEP 4: No capacity - reject request
    return error(TooManyRequests)
```

**Example Scenario:**

Function `hello` configuration:
- `concurrency: 3` (max 3 pods)
- `requestsPerPod: 5` (each pod handles 5 concurrent requests)
- Current state: 2 specialized pods, each handling 4 requests

**Request Flow:**

| Request # | Action |
|-----------|--------|
| 11 | Pod #1 at 4/5 → return Pod #1, increment to 5/5 |
| 12 | Pod #1 full, Pod #2 at 4/5 → return Pod #2, increment to 5/5 |
| 13 | Both full, can specialize (2 < 3) → return NotFound, trigger specialization |
| 14 | Both full, can't specialize (3 >= 3), virtual capacity = 4 → **queue request** |
| 15-18 | Queue all requests |

**Pod #3 completes specialization:**

```go
case setValue:
    svcs[newAddr].activeRequests = 1
    svcWaiting--

    // Wake up queued requests!
    svcCapacity := requestsPerPod - svcs[newAddr].activeRequests  // 5 - 1 = 4
    for i := 0; i < svcCapacity && queue.Len() > 0; i++ {
        popped := queue.Pop()
        if popped.ctx.Err() == nil {  // Not timed out
            popped.svcChannel <- funcSvc  // Wake up waiting goroutine!
            svcs[newAddr].activeRequests++
        }
    }
```

**Request completes, calls TapService:**

```go
case markAvailable:
    svcs[addr].activeRequests--  // Decrement counter
```

---

### Queue - FIFO for Waiting Requests

Simple thread-safe FIFO queue:

```go
type Queue struct {
    items *list.List  // Go's doubly-linked list
    mutex sync.Mutex
}

func (q *Queue) Push(item *svcWait)
func (q *Queue) Pop() *svcWait

// Remove requests whose context was canceled (timeout)
func (q *Queue) Expired() int {
    for item := q.items.Front(); item != nil; item = item.Next() {
        svcWait := item.Value.(*svcWait)
        if svcWait.ctx.Err() != nil {
            close(svcWait.svcChannel)
            q.items.Remove(item)
            expired++
        }
    }
    return expired
}
```

---

## Summary

The `pkg/executor/` package is essentially a **smart cache + pod lifecycle manager**:

- **Cache layer**: Maps functions → pod addresses (critical for performance)
- **Specialization orchestrator**: Ensures safe, concurrent pod creation
- **Pluggable backends**: Three executor types with different tradeoffs
- **Resource management**: Idle reaping, ConfigMap/Secret watching, adoption of orphaned pods

### Why Poolmgr is Fast

The **poolmgr** executor achieves ~100ms cold starts by:

1. **Pre-warming pools** of generic containers
2. **Specializing on-demand** rather than creating pods from scratch
3. **Intelligent caching** with concurrent request tracking
4. **Load balancing** across multiple pods
5. **Idle reaping** to free resources

### Key Takeaways

1. **Actor model** for cache operations avoids complex locking
2. **Waiting queue with channels** prevents request rejection during bursts
3. **Three-way indexing** enables fast lookups by function, address, or UID
4. **Atime tracking** enables efficient idle pod reaping
5. **WaitGroups** prevent duplicate specializations for concurrent requests

This is the **secret sauce** that makes Fission's poolmgr executor so fast and efficient!
