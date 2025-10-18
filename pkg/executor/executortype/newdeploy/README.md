# New Deploy Executor Functionality & Integration

The `pkg/executor/executortype/newdeploy` folder implements the **NewDeploy** executor type, which is Fission's deployment-based strategy for long-running, production-grade functions. Unlike poolmgr's pre-warmed pool approach, NewDeploy creates dedicated Kubernetes Deployments for each function with full support for autoscaling and production workloads.

## Core Concept

NewDeploy creates a **dedicated Kubernetes Deployment** for each function with:
1. **Kubernetes Deployment** - One deployment per function (not shared)
2. **Kubernetes Service** - ClusterIP service for routing traffic
3. **HorizontalPodAutoscaler (HPA)** - Automatic scaling based on metrics
4. **Pre-specialized pods** - Pods start already specialized with function code

This approach trades faster cold-start (poolmgr's strength) for better production features like horizontal autoscaling, rolling updates, and resource isolation.

---

## Key Components

### 1. **NewDeploy** (`newdeploymgr.go`)
The main executor manager that implements the ExecutorType interface.

**Main responsibilities:**
- **Function lifecycle**: Creates, updates, and deletes function deployments
- **Resource management**: Manages Deployments, Services, and HPAs for each function
- **Autoscaling**: Integrates with Kubernetes HPA for dynamic scaling
- **Idle scaling**: Scales down to minScale when function is idle (pkg/executor/executortype/newdeploy/newdeploymgr.go:781)
- **Cache management**: Maintains function service cache for routing

**Key methods:**
- `GetFuncSvc()` - Entry point: creates or gets function deployment (pkg/executor/executortype/newdeploy/newdeploymgr.go:193)
- `fnCreate()` - Creates Deployment, Service, and HPA for a function (pkg/executor/executortype/newdeploy/newdeploymgr.go:430)
- `updateFunction()` - Handles function updates and rolling updates (pkg/executor/executortype/newdeploy/newdeploymgr.go:529)
- `fnDelete()` - Cleans up all resources when function is deleted (pkg/executor/executortype/newdeploy/newdeploymgr.go:696)
- `idleObjectReaper()` - Scales down idle functions to minScale (pkg/executor/executortype/newdeploy/newdeploymgr.go:781)

**Important fields:**
```go
type NewDeploy struct {
    kubernetesClient kubernetes.Interface
    fissionClient    versioned.Interface
    fsCache          *fscache.FunctionServiceCache
    throttler        *throttler.Throttler  // Prevents concurrent creation of same function
    hpaops           *hpautils.HpaOperations  // HPA management
    defaultIdlePodReapTime time.Duration  // Default: 2 minutes
}
```

---

### 2. **Deployment Management** (`newdeploy.go`)
Handles Kubernetes Deployment creation, update, and lifecycle.

**Main responsibilities:**
- **Deployment creation**: Creates deployments with specialized pods (pkg/executor/executortype/newdeploy/newdeploy.go:41)
- **Resource specification**: Configures CPU/memory limits and requests (pkg/executor/executortype/newdeploy/newdeploy.go:316)
- **Rolling updates**: Manages gradual rollout with 20% maxUnavailable/maxSurge (pkg/executor/executortype/newdeploy/newdeploy.go:273)
- **Specialization**: Adds fetcher to pre-specialize pods on startup (pkg/executor/executortype/newdeploy/newdeploy.go:295)
- **Service creation**: Creates ClusterIP service for each function (pkg/executor/executortype/newdeploy/newdeploy.go:350)

**Key workflow in `createOrGetDeployment()`** (pkg/executor/executortype/newdeploy/newdeploy.go:41):
```
1. Generate deployment spec with specialized containers
2. Check if deployment exists (adopt orphan if needed)
3. Create deployment if not exists
4. Wait for deployment to be ready (AvailableReplicas >= minScale)
5. Return deployment object
```

**Deployment characteristics:**
- **MinScale**: Always scales to at least 1 on first invocation (even if minScale=0)
- **Specialization timeout**: Default 120s, configurable per function
- **Rolling updates**: 20% maxUnavailable and maxSurge for gradual rollout
- **Grace period**: 6 minutes termination grace period (configurable)
- **Revision history**: Disabled (RevisionHistoryLimit=0) to avoid clutter

---

### 3. **Function Event Handlers** (`funchandlers.go`)
Watches Function CRD events and reconciles function state.

**Event handling:**

**AddFunc** (pkg/executor/executortype/newdeploy/funchandlers.go:29):
- Eagerly creates Deployment, Service, and HPA when function is created
- Runs in goroutine for non-blocking operation

**UpdateFunc** (pkg/executor/executortype/newdeploy/funchandlers.go:56):
- Detects changes in function spec (environment, package, secrets, configmaps)
- Triggers rolling update if deployment needs to change
- Updates HPA if scaling parameters changed (minScale, maxScale, metrics)

**DeleteFunc** (pkg/executor/executortype/newdeploy/funchandlers.go:45):
- Deletes Deployment, Service, and HPA
- Removes function from cache

**Critical update triggers:**
- Environment change
- Package reference change
- Secrets/ConfigMaps change
- InvokeStrategy parameters (minScale, maxScale, metrics, behavior)

---

### 4. **Environment Event Handlers** (`envhandlers.go`)
Watches Environment CRD events and updates affected functions.

**UpdateFunc** (pkg/executor/executortype/newdeploy/envhandlers.go:32):
- Detects environment image changes
- Updates **all functions** using that environment
- Triggers rolling updates across all affected functions

**Why this matters:**
When you update an environment's runtime image, all functions using that environment get automatically updated with the new image via rolling updates.

---

### 5. **HPA Integration**
NewDeploy fully supports Kubernetes Horizontal Pod Autoscaler.

**HPA configuration:**
- **MinScale**: Minimum number of replicas (from `InvokeStrategy.ExecutionStrategy.MinScale`)
- **MaxScale**: Maximum number of replicas (from `InvokeStrategy.ExecutionStrategy.MaxScale`)
- **Metrics**: Custom metrics for scaling decisions (CPU, memory, custom metrics)
- **Behavior**: Scaling behavior policies (scale-up/down rates)

**HPA lifecycle:**
1. Created alongside deployment in `fnCreate()` (pkg/executor/executortype/newdeploy/newdeploymgr.go:473)
2. Updated when function's InvokeStrategy changes (pkg/executor/executortype/newdeploy/newdeploymgr.go:576)
3. Deleted when function is deleted (pkg/executor/executortype/newdeploy/newdeploy.go:470)

---

## Integration with Executor Component

The NewDeploy integrates with the main executor through the **ExecutorType interface**:

```go
var _ executortype.ExecutorType = &NewDeploy{}
```

**Key interface methods implemented:**

1. **`GetFuncSvc(ctx, fn)`** - Main entry point for function invocations
   - Calls `createFunction()` which uses throttler to prevent duplicate creation
   - Returns cached service if already exists
   - Creates Deployment + Service + HPA if new function

2. **`GetFuncSvcFromCache(ctx, fn)`** - Fast path using function UID lookup (pkg/executor/executortype/newdeploy/newdeploymgr.go:198)

3. **`IsValid(ctx, fsvc)`** - Validates function resources (pkg/executor/executortype/newdeploy/newdeploymgr.go:232)
   - Checks service exists
   - Verifies deployment has AvailableReplicas >= 1
   - Returns false if resources missing or unhealthy

4. **`TapService(ctx, svcHost)`** - Updates last access time for idle tracking (pkg/executor/executortype/newdeploy/newdeploymgr.go:220)

5. **`RefreshFuncPods(ctx, fn)`** - Forces pod refresh via deployment patch (pkg/executor/executortype/newdeploy/newdeploymgr.go:270)
   - Updates `ResourceVersionCount` environment variable
   - Triggers rolling update to refresh all pods

6. **`AdoptExistingResources(ctx)`** - Adopts resources from previous executor (pkg/executor/executortype/newdeploy/newdeploymgr.go:312)

7. **`CleanupOldExecutorObjects(ctx)`** - Removes orphaned resources (pkg/executor/executortype/newdeploy/newdeploymgr.go:344)

---

## Architecture Flow

### Function Creation Flow
```
Function CRD Created
    ↓
FunctionEventHandler.AddFunc()
    ↓
NewDeploy.createFunction()
    ↓
Throttler.RunOnce() (prevents duplicate creation)
    ↓
NewDeploy.fnCreate()
    ↓
1. Create/Get Service (for fast Istio propagation)
    ↓
2. Create/Get Deployment (with pre-specialized pods)
    ↓
3. Wait for deployment ready (AvailableReplicas >= minScale)
    ↓
4. Create/Get HPA
    ↓
5. Cache FuncSvc
    ↓
Return service address to router
```

### Function Update Flow
```
Function CRD Updated
    ↓
FunctionEventHandler.UpdateFunc()
    ↓
NewDeploy.updateFunction()
    ↓
Detect change type:
  ├─ HPA params changed? → Update HPA
  ├─ Deployment changed? → updateFuncDeployment()
  │                        └─ Triggers rolling update
  └─ Executor type changed? → Delete old / Create new
```

### Idle Scaling Flow
```
idleObjectReaper() (runs every 5s)
    ↓
List old function services (Atime > IdleTimeout)
    ↓
For each idle function:
  ├─ Get current deployment
  ├─ Check: currentReplicas > minScale?
  └─ If yes: scaleDeployment(minScale)
```

---

## Key Design Patterns

### 1. **Throttler Pattern**
Prevents concurrent creation of the same function:
```go
fsvcObj, err := deploy.throttler.RunOnce(string(fn.ObjectMeta.UID), func(ableToCreate bool) {
    if ableToCreate {
        return deploy.fnCreate(ctx, fn)  // Create new
    }
    return deploy.fsCache.GetByFunctionUID(fn.ObjectMeta.UID)  // Return cached
})
```

### 2. **Resource Adoption**
Executors adopt orphaned resources from previous instances:
- Checks `EXECUTOR_INSTANCEID_LABEL` annotation
- Updates instance ID to claim ownership
- Avoids disrupting existing function deployments during executor restart

### 3. **Referenced Resource Tracking**
Tracks ConfigMap/Secret changes via resource version sum (pkg/executor/executortype/newdeploy/newdeploy.go:491):
- Sums resource versions of all secrets and configmaps
- Injects sum as `ResourceVersionCount` env var
- Triggers rolling update when sum changes (without timestamp!)

### 4. **Gradual Rollout Strategy**
- **20% maxUnavailable**: Only 20% of pods can be unavailable during update
- **20% maxSurge**: Only 20% extra pods created during update
- Prevents overwhelming cluster resources during updates

### 5. **Service-First Creation**
Creates Service before Deployment (pkg/executor/executortype/newdeploy/newdeploymgr.go:458):
- Allows Istio/Envoy time to propagate configuration
- Uses deployment wait time productively
- Avoids 404 errors during initial propagation

---

## NewDeploy vs PoolMgr Comparison

| Feature | NewDeploy | PoolMgr |
|---------|-----------|---------|
| **Cold Start** | Slower (~seconds) | Faster (~100ms) |
| **Resource Model** | 1 Deployment per function | Shared pool per environment |
| **Autoscaling** | Full HPA support | No autoscaling |
| **Isolation** | Dedicated pods per function | Shared pool pods |
| **Rolling Updates** | Native Kubernetes rolling updates | Manual pod deletion |
| **Best For** | Production, high-traffic, long-running | Event-driven, short-lived, bursty |
| **Min Replicas** | Configurable (0 or more) | N/A (pool size only) |
| **Idle Behavior** | Scales down to minScale | Individual pod deletion |
| **Resource Overhead** | Higher (deployment per function) | Lower (shared pools) |

---

## Specialized Pod Startup

Unlike poolmgr (which specializes after startup), NewDeploy pods are **pre-specialized**:

1. **Deployment spec includes**:
   - Function code fetch configuration via fetcher
   - Environment variables with function metadata
   - Secrets and ConfigMaps mounted

2. **Pod startup sequence**:
   - Fetcher init container downloads function code
   - Main container starts with function already loaded
   - Pod becomes ready and joins service endpoints

3. **Result**:
   - No specialization delay on first request
   - But slower initial deployment creation
   - Better for long-running, persistent workloads

---

## Idle Scaling Mechanism

The idle object reaper scales functions down to save resources (pkg/executor/executortype/newdeploy/newdeploymgr.go:786):

**Process:**
1. Runs every 5 seconds (configurable via `OBJECT_REAPER_INTERVAL`)
2. Lists cached function services older than 5 seconds
3. For each function service:
   - Get last access time (`Atime`)
   - Get function's `IdleTimeout` (default: 2 minutes)
   - If `time.Since(Atime) >= IdleTimeout`:
     - Get current deployment replica count
     - If `currentReplicas > minScale`:
       - Scale deployment to `minScale`

**Important notes:**
- Does **not** delete resources (unlike poolmgr which deletes pods)
- Only scales down to `minScale`, not to zero
- HPA can scale back up when traffic returns
- Respects function-specific `IdleTimeout` setting

---

## Resource Version Tracking

NewDeploy tracks ConfigMap/Secret changes intelligently (pkg/executor/executortype/newdeploy/newdeploy.go:491):

**Why it exists:**
- Need to trigger rolling update when secrets/configmaps change
- Can't use timestamp (changes on every executor restart)
- Need deterministic value for resource adoption

**Solution:**
- Sum all resource versions of referenced secrets/configmaps
- Inject as `ResourceVersionCount` environment variable
- Deployment spec changes → triggers rolling update
- Same sum on restart → no unwanted rolling update

**Example:**
```go
// Secret "db-creds" has ResourceVersion: "12345"
// ConfigMap "app-config" has ResourceVersion: "67890"
// ResourceVersionCount = 12345 + 67890 = 80235

env: [{
    name: "RESOURCE_VERSION_COUNT"
    value: "80235"
}]
```

---

## File Structure

```
pkg/executor/executortype/newdeploy/
├── newdeploymgr.go       # Main NewDeploy manager and ExecutorType implementation
├── newdeploy.go          # Deployment, Service, and resource creation/management
├── funchandlers.go       # Function CRD event handlers (Add/Update/Delete)
├── envhandlers.go        # Environment CRD event handlers (Update)
└── newdeploymgr_test.go  # Unit tests
```

---

## When to Use NewDeploy

**Choose NewDeploy for:**
- ✅ Production workloads requiring autoscaling
- ✅ Long-running functions with steady traffic
- ✅ Functions requiring resource isolation
- ✅ Workloads needing rolling updates
- ✅ Functions with predictable scaling patterns
- ✅ Services requiring Kubernetes-native deployment features

**Avoid NewDeploy for:**
- ❌ Very short, bursty workloads (use poolmgr)
- ❌ Event-driven functions with sporadic invocations
- ❌ Scenarios requiring fastest cold start
- ❌ Resource-constrained environments (high overhead)

---

This architecture provides **production-grade** function execution with full Kubernetes ecosystem integration at the cost of slower cold starts and higher resource overhead!
