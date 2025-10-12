# Executor Leader Election

## Overview

The Executor component now supports leader election to enable running multiple replicas in a high-availability configuration. With leader election enabled, only one executor instance (the leader) actively creates and manages function resources, while follower instances serve read-only operations from cache and stand ready to take over leadership if needed.

## Architecture

### Option A: Minimal Impact (Current Implementation)

**Leader Responsibilities:**
- Create new function service pods
- Manage function specialization
- Update function service cache
- All write operations to Kubernetes resources

**Follower Responsibilities:**
- Serve function service lookups from cache
- Answer health/readiness checks
- Ready to become leader on failure

**Key Benefits:**
- Prevents duplicate pod creation
- Eliminates race conditions in function specialization
- Maintains cache consistency
- Enables horizontal scaling for read operations
- Two-phase graceful shutdown when losing leadership:
  - Phase 1: Leader services stop cleanly (30s timeout)
  - Phase 2: Complete application shutdown (30s timeout)
- In-flight HTTP requests complete before shutdown
- Metrics properly flushed
- Kubernetes-friendly clean exit

## Configuration

Leader election is configured via environment variables:

### Environment Variables

| Variable | Description | Default | Required |
|----------|-------------|---------|----------|
| `ENABLE_LEADER_ELECTION` | Enable/disable leader election | `false` | No |
| `EXECUTOR_LEASE_NAME` | Name of the Kubernetes Lease resource | `fission-executor` | No |
| `EXECUTOR_LEASE_NAMESPACE` | Namespace for the Lease resource | Function namespace | No |

### Lease Timing (uses defaults from leaderelection package)

- **Lease Duration**: 15s - How long a leader holds the lease
- **Renew Deadline**: 10s - How long a leader tries to renew before giving up
- **Retry Period**: 2s - How often non-leaders attempt to acquire the lease

## Deployment

### Enabling Leader Election

#### Helm Chart Values (Example)

```yaml
executor:
  replicaCount: 3  # Run multiple replicas
  env:
    - name: ENABLE_LEADER_ELECTION
      value: "true"
    - name: EXECUTOR_LEASE_NAME
      value: "fission-executor"
    - name: EXECUTOR_LEASE_NAMESPACE
      value: "fission"
```

#### Kubernetes Deployment (Example)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: executor
  namespace: fission
spec:
  replicas: 3
  selector:
    matchLabels:
      app: executor
  template:
    metadata:
      labels:
        app: executor
    spec:
      serviceAccountName: fission-executor
      containers:
      - name: executor
        image: fission/fission-bundle:latest
        env:
        - name: ENABLE_LEADER_ELECTION
          value: "true"
        - name: EXECUTOR_LEASE_NAME
          value: "fission-executor"
        - name: EXECUTOR_LEASE_NAMESPACE
          value: "fission"
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        livenessProbe:
          httpGet:
            path: /healthz
            port: 8888
        readinessProbe:
          httpGet:
            path: /readyz
            port: 8888
```

### RBAC Requirements

The executor service account needs permissions to manage Lease resources:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: executor-leader-election
  namespace: fission
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "create", "update", "patch", "delete"]
```

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: executor-leader-election
  namespace: fission
subjects:
- kind: ServiceAccount
  name: fission-executor
  namespace: fission
roleRef:
  kind: Role
  name: executor-leader-election
  apiGroup: rbac.authorization.k8s.io
```

## Monitoring

### Health Checks

- **`/healthz`**: Always returns 200 OK (checks if process is alive)
- **`/readyz`**: Returns 503 if leader election is enabled and instance is not the leader

### Logs

Monitor leader election events in executor logs:

**Leadership Acquisition:**
```
INFO executor started leading - initializing leader services
INFO adopting existing resources and cleaning up old executor objects
INFO resource adoption and cleanup complete
INFO starting all informers on leader instance
INFO all informers started successfully
INFO starting executor type reconciliation loops
INFO executor leader initialization complete - now accepting write operations
```

**Leadership Loss (Two-Phase Shutdown):**
```
INFO leadership lost, stopping leader services
INFO all leader services stopped gracefully
INFO leader services cleanup complete, exiting OnStartedLeading callback
INFO stopped leading, initiating complete application shutdown
INFO cancelling main context to stop all application services
INFO waiting for all application services to stop gracefully
INFO all application services stopped gracefully
INFO exiting process after complete shutdown
```

**New Leader Elected:**
```
INFO new executor leader elected {"leader_identity": "executor-abc123"}
```

**Rejected Requests (Follower):**
```
WARN rejecting function service creation request - not leader
```

### Metrics

Track leader election status:
- Monitor which instance is leader via readiness probes
- Track rejected requests on follower instances
- Monitor lease acquisition/renewal events

## Operational Considerations

### Leader Failover

When the leader fails or loses leadership, a **two-phase graceful shutdown** occurs:

**Phase 1: Leader Services Shutdown (in OnStartedLeading callback)**
1. The leader detects it has lost the lease (context cancelled)
2. Leader-specific services stop:
   - Function service creation handler stops accepting new requests
   - Executor type reconciliation loops stop
   - All informers stop
3. Wait up to 30s for all leader-specific goroutines to complete
4. Return from OnStartedLeading callback

**Phase 2: Complete Application Shutdown (in OnStoppedLeading callback)**
5. Main context is cancelled
6. All application services stop:
   - HTTP API server drains existing connections
   - Metrics server flushes data
   - Other background services complete
7. Wait up to 30s for all application goroutines to complete
8. Process exits cleanly (exit code 0)
9. Kubernetes restarts the pod

**Leadership Acquisition**
10. Other replicas detect the lease is available
11. One replica acquires the lease and becomes the new leader (~15s)
12. New leader begins accepting write operations

**Total shutdown time:** Up to 60 seconds (30s per phase)

### Gradual Rollout

For production deployments:

1. **Deploy with leader election disabled** on all replicas
2. **Enable on one replica** first, monitor for issues
3. **Enable on remaining replicas** once validated
4. **Monitor** cache consistency and request success rates

### Disabling Leader Election

To disable leader election (e.g., for rollback):

1. Set `ENABLE_LEADER_ELECTION=false` in deployment
2. Roll out the change to all replicas
3. All instances will accept write operations again

## Troubleshooting

### Issue: All instances report "not ready"

**Cause**: No instance has acquired leadership

**Solutions**:
- Check RBAC permissions for Lease resources
- Verify `EXECUTOR_LEASE_NAMESPACE` is correct
- Check for network issues between executor and API server
- Review executor logs for errors

### Issue: Multiple instances creating function pods

**Cause**: Leader election not properly enabled

**Solutions**:
- Verify `ENABLE_LEADER_ELECTION=true` on all replicas
- Ensure all replicas are using the same `EXECUTOR_LEASE_NAME`
- Check for version mismatches between replicas

### Issue: Frequent leader changes

**Cause**: Network instability or resource constraints

**Solutions**:
- Increase lease duration via future configuration options
- Check executor pod resource limits (CPU/memory)
- Review network policies and connectivity
- Check for high executor restart rates

## Future Enhancements (Option B)

Future versions may implement more robust leader-follower patterns:

- **Followers stop all reconciliation loops** when not leader
- **Followers only serve from cache** (no resource creation)
- **Configurable lease timings** via environment variables
- **Graceful leadership transfer** on shutdown
- **Leader-aware metrics** and monitoring

## Testing

### Unit Tests

Run executor leader election tests:

```bash
go test -v ./pkg/executor -run "TestExecutor.*Leader"
```

### Integration Tests

Test with multiple replicas:

```bash
# Deploy 3 executor replicas with leader election enabled
kubectl apply -f test-executor-ha.yaml

# Verify only one is leader
kubectl get pods -n fission -l app=executor -o json | \
  jq '.items[] | select(.status.containerStatuses[0].ready==true) | .metadata.name'

# Kill the leader and verify failover
kubectl delete pod <leader-pod-name> -n fission

# Wait ~15s and verify new leader is elected
```

## References

- Leader election implementation: `pkg/leaderelection/`
- Executor integration: `pkg/executor/executor.go`
- Callbacks: `pkg/executor/callbacks.go`
- Tests: `pkg/executor/leaderelection_test.go`
