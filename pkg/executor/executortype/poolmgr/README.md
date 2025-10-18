# Pool Manager (poolmgr) Functionality & Integration

The `pkg/executor/executortype/poolmgr` folder implements the **Pool Manager** executor type, which is one of Fission's core strategies for achieving **sub-second cold start times** (~100ms). Here's how it works:

## Core Concept

Pool Manager maintains **pre-warmed pools** of generic environment containers. When a function is invoked for the first time, instead of creating a new pod from scratch, it:
1. Takes a ready pod from the warm pool
2. "Specializes" it by loading the function code via the fetcher
3. Routes traffic to the specialized pod

This is much faster than spinning up a new pod.

---

## Key Components

### 1. **GenericPoolManager** (`gpm.go`)
The top-level manager that orchestrates all pool operations.

**Main responsibilities:**
- **Pool lifecycle**: Creates and manages multiple `GenericPool` instances (one per environment)
- **Function service cache**: Maintains `fsCache` mapping functions to service endpoints
- **Resource adoption**: On startup, adopts existing pods from previous executor instances (pkg/executor/executortype/poolmgr/gpm.go:356)
- **Idle reaping**: Cleans up idle function pods after timeout (pkg/executor/executortype/poolmgr/gpm.go:625)
- **WebSocket handling**: Tracks WebSocket connections to prevent premature pod cleanup (pkg/executor/executortype/poolmgr/gpm.go:718)

**Key methods:**
- `GetFuncSvc()` - Main entry point: gets environment, retrieves pool, specializes pod (pkg/executor/executortype/poolmgr/gpm.go:208)
- `service()` - Background goroutine handling pool creation/cleanup requests via channels (pkg/executor/executortype/poolmgr/gpm.go:516)
- `idleObjectReaper()` - Periodically cleans up idle pods based on `IdleTimeout` (pkg/executor/executortype/poolmgr/gpm.go:625)

---

### 2. **GenericPool** (`gp.go`)
Represents a single environment's pool of generic containers.

**Main responsibilities:**
- **Deployment management**: Creates Kubernetes Deployment with N replicas (pkg/executor/executortype/poolmgr/gp_deployment.go:220)
- **Pod selection**: Chooses ready pods from the pool for specialization (pkg/executor/executortype/poolmgr/gp.go:240)
- **Specialization**: Loads function code into generic pods via fetcher (pkg/executor/executortype/poolmgr/gp.go:412)
- **Service creation**: Optionally creates K8s services for specialized pods (when `useSvc=true` or Istio enabled)
- **CPU tracking**: Monitors CPU utilization for load balancing (pkg/executor/executortype/poolmgr/gp.go:183)

**Key workflow in `getFuncSvc()`** (pkg/executor/executortype/poolmgr/gp.go:500):
```
1. Choose ready pod from pool → choosePod()
2. Specialize pod with function code → specializePod()
3. Create service if needed → createSvc()
4. Cache function service → fsCache.AddFunc()
5. Return service address to router
```

---

### 3. **PoolPodController** (`poolpodcontroller.go`)
Watches Kubernetes resources and reconciles pool state.

**Main responsibilities:**
- **Environment reconciliation**: Creates/updates/deletes pools when environments change (pkg/executor/executortype/poolmgr/poolpodcontroller.go:310)
- **Function deletion**: Removes function pods when functions are deleted (pkg/executor/executortype/poolmgr/poolpodcontroller.go:146)
- **ReplicaSet watching**: Cleans up specialized pods when pool deployment scales down (pkg/executor/executortype/poolmgr/poolpodcontroller.go:151)
- **Istio integration**: Creates Istio services for functions if enabled (pkg/executor/executortype/poolmgr/funchandlers.go:44)

**Work queues:**
- `envCreateUpdateQueue` - Handles environment add/update events
- `envDeleteQueue` - Handles environment deletion
- `spCleanupPodQueue` - Cleans up specialized pods

---

### 4. **ReadyPodController** (`readyPodController.go`)
Manages the queue of ready pods available for specialization.

**Main responsibilities:**
- **Ready pod tracking**: Monitors pods with `managed=true` label (generic pool pods)
- **Queue management**: Maintains `readyPodQueue` of available pods (pkg/executor/executortype/poolmgr/readyPodController.go:36)
- **Event handling**: Adds pods to queue when ready, removes when deleted (pkg/executor/executortype/poolmgr/readyPodController.go:13)

---

### 5. **Deployment Management** (`gp_deployment.go`)
Creates and updates Kubernetes Deployments for environment pools.

**Key features:**
- **Pool sizing**: Determines replica count from `Environment.Spec.Poolsize` (default: 3) (pkg/executor/executortype/poolmgr/common.go:21)
- **Container setup**: Injects fetcher sidecar and merges environment container specs (pkg/executor/executortype/poolmgr/gp_deployment.go:203)
- **Graceful shutdown**: Sets 6-minute termination grace period for connection draining (pkg/executor/executortype/poolmgr/gp_deployment.go:85)
- **Istio support**: Configures sidecar injection based on settings (pkg/executor/executortype/poolmgr/gp_deployment.go:100)

---

## Integration with Executor Component

The poolmgr integrates with the main executor through the **ExecutorType interface**:

```go
var _ executortype.ExecutorType = &GenericPoolManager{}
```

**Key interface methods implemented:**

1. **`GetFuncSvc(ctx, fn)`** - Called by router to get a function service
   - Entry point for all function invocations
   - Returns address where function can be called

2. **`GetFuncSvcFromCache(ctx, fn)`** - Fast path: check if function already specialized

3. **`IsValid(ctx, fsvc)`** - Validates cached services are still healthy (pkg/executor/executortype/poolmgr/gpm.go:285)

4. **`TapService(ctx, svcHost)`** - Updates access time when function is called (pkg/executor/executortype/poolmgr/gpm.go:264)

5. **`RefreshFuncPods(ctx, fn)`** - Forces pod refresh for function updates (pkg/executor/executortype/poolmgr/gpm.go:310)

6. **`AdoptExistingResources(ctx)`** - Adopts pods from previous executor instances (pkg/executor/executortype/poolmgr/gpm.go:356)

7. **`CleanupOldExecutorObjects(ctx)`** - Removes orphaned resources (pkg/executor/executortype/poolmgr/gpm.go:492)

---

## Specialization Process

The **specialization** is the key innovation (pkg/executor/executortype/poolmgr/gp.go:412):

1. **Generic pod runs** with base environment image + fetcher sidecar
2. **Fetcher downloads** function code from storage service
3. **Fetcher calls** environment's `/specialize` endpoint with function metadata
4. **Environment loads** the function code into memory
5. **Pod is relabeled** with function metadata (`functionName`, `functionUid`)
6. **Service address cached** in fsCache for future invocations

This happens in **~100ms** vs **several seconds** for starting a new pod!

---

## Architecture Flow

```
Router Request
    ↓
Executor.GetFuncSvc()
    ↓
GenericPoolManager.GetFuncSvc()
    ↓
GenericPool.getFuncSvc()
    ↓
choosePod() → picks from readyPodQueue
    ↓
specializePod() → calls fetcher API
    ↓
Cache & Return service address
    ↓
Router forwards request to specialized pod
```

---

## Key Design Patterns

1. **Channel-based request handling**: Pool requests go through `requestChannel` to avoid race conditions (pkg/executor/executortype/poolmgr/gpm.go:516)

2. **Work queues**: Kubernetes-style controllers with rate-limited queues for reconciliation

3. **Informer caching**: Uses shared informers to watch pods/environments efficiently

4. **Two-tier caching**:
   - `functionEnv` cache: Function → Environment mapping
   - `fsCache`: Function → Service address mapping

5. **Exponential backoff**: Pod selection retries with increasing delays (pkg/executor/executortype/poolmgr/gp.go:250)

---

## File Structure

```
pkg/executor/executortype/poolmgr/
├── gpm.go                    # GenericPoolManager - top-level orchestrator
├── gp.go                     # GenericPool - per-environment pool
├── gp_deployment.go          # Kubernetes Deployment creation/updates
├── poolpodcontroller.go      # Kubernetes resource reconciliation controller
├── readyPodController.go     # Ready pod queue management
├── funchandlers.go           # Function event handlers (Istio integration)
├── common.go                 # Shared utility functions
└── packagehandlers.go        # Package-related handlers
```

---

This architecture enables Fission's **fast cold starts** while maintaining **efficient resource usage** through pool sizing and idle timeout management!
