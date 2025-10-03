/*
Copyright 2024 The Fission Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUTHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package leaderelection

import (
	"context"
	"os"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// CallbacksProvider is an interface that provides the callbacks for the leader election.

type CallbacksProvider interface {
	// GetCallbacks returns the callbacks for the leader election.
	GetCallbacks() leaderelection.LeaderCallbacks
}

// DefaultCallbacksProvider is the default implementation of the CallbacksProvider interface.
// It provides default logging for the leader election events.
type DefaultCallbacksProvider struct {
	Logger    *zap.Logger
	Component string
}

// GetCallbacks returns the callbacks for the leader election.
func (dcp *DefaultCallbacksProvider) GetCallbacks() leaderelection.LeaderCallbacks {
	return leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			dcp.Logger.Info("started leading", zap.String("component", dcp.Component))
		},
		OnStoppedLeading: func() {
			dcp.Logger.Info("stopped leading", zap.String("component", dcp.Component))
		},
		OnNewLeader: func(identity string) {
			dcp.Logger.Info("new leader elected", zap.String("component", dcp.Component), zap.String("leader", identity))
		},
	}
}

// LeaderElectionConfig contains the configuration for the leader election.
type LeaderElectionConfig struct {
	// Logger is the logger used for logging.
	Logger *zap.Logger
	// Component is the component that is using the leader election.
	Component string
	// KubeClient is the Kubernetes clientset.
	KubeClient kubernetes.Interface
	// CallbacksProvider is the provider for the callbacks that are invoked when the leader changes.
	CallbacksProvider CallbacksProvider
	// LeaseLockName is the name of the lease lock.
	LeaseLockName string
	// LeaseLockNamespace is the namespace of the lease lock.
	LeaseLockNamespace string
	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership.
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up.
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of acquiring leadership.
	RetryPeriod time.Duration
}

// LeaderElector is a wrapper around the Kubernetes leader election mechanism.
type LeaderElector struct {
	config *LeaderElectionConfig
}

// NewLeaderElector creates a new LeaderElector.
func NewLeaderElector(config *LeaderElectionConfig) *LeaderElector {
	return &LeaderElector{
		config: config,
	}
}

// Run starts the leader election.
func (le *LeaderElector) Run(ctx context.Context) error {
	// Create the resource lock.
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      le.config.LeaseLockName,
			Namespace: le.config.LeaseLockNamespace,
		},
		Client: le.config.KubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: getPodName(),
		},
	}

	// Create the leader elector.
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: le.config.LeaseDuration,
		RenewDeadline: le.config.RenewDeadline,
		RetryPeriod:   le.config.RetryPeriod,
		Callbacks:     le.config.CallbacksProvider.GetCallbacks(),
	})
	if err != nil {
		le.config.Logger.Error("failed to create leader elector", zap.Error(err))
		return err
	}

	// Start the leader election loop.
	elector.Run(ctx)
	return nil
}

func getPodName() string {
	podName := os.Getenv("POD_NAME")
	if len(podName) == 0 {
		// If the POD_NAME environment variable is not set, generate a unique ID.
		return uuid.New().String()
	}
	return podName
}
