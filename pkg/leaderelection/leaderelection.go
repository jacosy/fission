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

package leaderelection

import (
	"context"
	"errors"
	"os"
	"sync"
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
	// Identity is the unique identifier for this instance. If empty, will use POD_NAME or generate UUID.
	Identity string
	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. Default: 15s
	LeaseDuration time.Duration
	// RenewDeadline is the duration that the acting master will retry
	// refreshing leadership before giving up. Default: 10s
	RenewDeadline time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of acquiring leadership. Default: 2s
	RetryPeriod time.Duration
}

// LeaderElector is a wrapper around the Kubernetes leader election mechanism.
type LeaderElector struct {
	config   *LeaderElectionConfig
	elector  *leaderelection.LeaderElector
	mu       sync.RWMutex
	isLeader bool
	identity string
}

// Default values for leader election timing
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRenewDeadline = 10 * time.Second
	DefaultRetryPeriod   = 2 * time.Second
)

// NewLeaderElector creates a new LeaderElector with validation.
func NewLeaderElector(config *LeaderElectionConfig) (*LeaderElector, error) {
	if err := validateConfig(config); err != nil {
		return nil, err
	}

	// Apply defaults
	applyDefaults(config)

	// Determine identity
	identity := config.Identity
	if identity == "" {
		identity = getIdentity()
	}

	return &LeaderElector{
		config:   config,
		identity: identity,
	}, nil
}

// validateConfig validates the required fields in the config.
func validateConfig(config *LeaderElectionConfig) error {
	if config == nil {
		return errors.New("config cannot be nil")
	}
	if config.KubeClient == nil {
		return errors.New("KubeClient is required")
	}
	if config.LeaseLockName == "" {
		return errors.New("LeaseLockName is required")
	}
	if config.LeaseLockNamespace == "" {
		return errors.New("LeaseLockNamespace is required")
	}
	if config.CallbacksProvider == nil {
		return errors.New("CallbacksProvider is required")
	}
	if config.Logger == nil {
		return errors.New("Logger is required")
	}
	return nil
}

// applyDefaults applies default values to the config.
func applyDefaults(config *LeaderElectionConfig) {
	if config.LeaseDuration == 0 {
		config.LeaseDuration = DefaultLeaseDuration
	}
	if config.RenewDeadline == 0 {
		config.RenewDeadline = DefaultRenewDeadline
	}
	if config.RetryPeriod == 0 {
		config.RetryPeriod = DefaultRetryPeriod
	}
}

// Run starts the leader election.
func (le *LeaderElector) Run(ctx context.Context) error {
	if ctx == nil {
		return errors.New("context cannot be nil")
	}

	// Create the resource lock.
	lock := &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      le.config.LeaseLockName,
			Namespace: le.config.LeaseLockNamespace,
		},
		Client: le.config.KubeClient.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: le.identity,
		},
	}

	// Wrap callbacks to track leadership state
	callbacks := le.config.CallbacksProvider.GetCallbacks()
	wrappedCallbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			le.mu.Lock()
			le.isLeader = true
			le.mu.Unlock()
			if callbacks.OnStartedLeading != nil {
				callbacks.OnStartedLeading(ctx)
			}
		},
		OnStoppedLeading: func() {
			le.mu.Lock()
			le.isLeader = false
			le.mu.Unlock()
			if callbacks.OnStoppedLeading != nil {
				callbacks.OnStoppedLeading()
			}
		},
		OnNewLeader: func(identity string) {
			if callbacks.OnNewLeader != nil {
				callbacks.OnNewLeader(identity)
			}
		},
	}

	// Create the leader elector.
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: le.config.LeaseDuration,
		RenewDeadline: le.config.RenewDeadline,
		RetryPeriod:   le.config.RetryPeriod,
		Callbacks:     wrappedCallbacks,
	})
	if err != nil {
		le.config.Logger.Error("failed to create leader elector", zap.Error(err))
		return err
	}

	le.mu.Lock()
	le.elector = elector
	le.mu.Unlock()

	// Start the leader election loop.
	elector.Run(ctx)
	return nil
}

// IsLeader returns true if this instance is currently the leader.
func (le *LeaderElector) IsLeader() bool {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.isLeader
}

// GetLeader returns the identity of the current leader.
// Returns empty string if leader is unknown.
func (le *LeaderElector) GetLeader() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	if le.elector == nil {
		return ""
	}
	return le.elector.GetLeader()
}

// GetIdentity returns the identity of this instance.
func (le *LeaderElector) GetIdentity() string {
	le.mu.RLock()
	defer le.mu.RUnlock()
	return le.identity
}

// getIdentity returns the identity for this instance.
// It first checks the POD_NAME environment variable, then generates a UUID.
func getIdentity() string {
	podName := os.Getenv("POD_NAME")
	if len(podName) == 0 {
		// If the POD_NAME environment variable is not set, generate a unique ID.
		return uuid.New().String()
	}
	return podName
}
