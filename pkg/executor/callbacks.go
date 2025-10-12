/*
Copyright 2024 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package executor

import (
	"context"
	"os"
	"time"

	"go.uber.org/zap"
	"k8s.io/client-go/tools/leaderelection"

	"github.com/fission/fission/pkg/utils/manager"
)

// ExecutorCallbacksProvider implements leaderelection.CallbacksProvider for the executor.
// It manages the transition between leader and follower states.
type ExecutorCallbacksProvider struct {
	logger   *zap.Logger
	executor *Executor
	mgr      manager.Interface
	// cancelFunc is used to stop executor types when losing leadership
	cancelFunc context.CancelFunc
	// mainCancelFunc is used to trigger graceful shutdown of the entire process
	mainCancelFunc context.CancelFunc
	// osExit is used to exit the process (can be overridden in tests)
	osExit func(int)
}

// NewExecutorCallbacksProvider creates a new ExecutorCallbacksProvider.
func NewExecutorCallbacksProvider(logger *zap.Logger, executor *Executor, mgr manager.Interface, mainCancelFunc context.CancelFunc) *ExecutorCallbacksProvider {
	return &ExecutorCallbacksProvider{
		logger:         logger,
		executor:       executor,
		mgr:            mgr,
		mainCancelFunc: mainCancelFunc,
		osExit:         os.Exit, // Use real os.Exit by default
	}
}

// GetCallbacks returns the callbacks for leader election events.
func (ecp *ExecutorCallbacksProvider) GetCallbacks() leaderelection.LeaderCallbacks {
	return leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			ecp.logger.Info("executor started leading - initializing leader services")

			// Create a manager specifically for leader-specific goroutines
			// This allows us to wait for only leader services during shutdown
			leaderMgr := manager.New()
			ecp.cancelFunc = nil // Reset any previous cancel function

			// Adopt existing resources and clean up old executor objects
			// This must be done before starting reconciliation loops
			ecp.executor.adoptAndCleanupResources(ctx)

			// Start all informers (Fission, executor-specific, ConfigMap, Secret)
			// IMPORTANT: Informers should ONLY run on the leader to prevent race conditions
			ecp.executor.startAllInformers(ctx)

			// Start the function service creation request handler
			leaderMgr.Add(ctx, func(ctx context.Context) {
				ecp.executor.serveCreateFuncServices(ctx)
			})

			// Start executor type reconciliation loops (create/update/delete pods, services, etc.)
			// Use leaderMgr so these goroutines are tracked separately
			ecp.executor.startExecutorTypes(ctx, leaderMgr)

			ecp.logger.Info("executor leader initialization complete - now accepting write operations")

			// BLOCK here until leadership is lost (context is cancelled)
			// This is the standard Kubernetes leader election pattern
			<-ctx.Done()

			ecp.logger.Info("leadership lost, stopping leader services")

			// Wait for all leader-specific goroutines to finish gracefully
			if err := leaderMgr.WaitWithTimeout(30 * time.Second); err != nil {
				ecp.logger.Warn("leader services shutdown timeout exceeded", zap.Error(err))
			} else {
				ecp.logger.Info("all leader services stopped gracefully")
			}

			ecp.logger.Info("leader services cleanup complete, exiting OnStartedLeading callback")
			// Returning from this callback will trigger OnStoppedLeading
		},
		OnStoppedLeading: func() {
			ecp.logger.Info("stopped leading, initiating complete application shutdown")

			// Cancel the main context to trigger graceful shutdown of ALL components
			// This stops the API server, metrics server, and any other application-level services
			if ecp.mainCancelFunc != nil {
				ecp.logger.Info("cancelling main context to stop all application services")
				ecp.mainCancelFunc()
			}

			// Wait for ALL application goroutines to shut down gracefully
			// This includes: HTTP server connection draining, metrics flushing, etc.
			ecp.logger.Info("waiting for all application services to stop gracefully")
			if err := ecp.mgr.WaitWithTimeout(30 * time.Second); err != nil {
				ecp.logger.Warn("application shutdown timeout exceeded, forcing exit", zap.Error(err))
			} else {
				ecp.logger.Info("all application services stopped gracefully")
			}

			// Exit the process to allow Kubernetes to restart the pod
			// This ensures a clean state when leadership is lost
			ecp.logger.Info("exiting process after complete shutdown")
			ecp.osExit(0)
		},
		OnNewLeader: func(identity string) {
			ecp.logger.Info("new executor leader elected",
				zap.String("leader_identity", identity))
		},
	}
}
