package leaderelection

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection"
)

// mockCallbacksProvider is a mock implementation of the CallbacksProvider interface.
type mockCallbacksProvider struct {
	callbacks leaderelection.LeaderCallbacks
}

// GetCallbacks returns the callbacks for the leader election.
func (mcp *mockCallbacksProvider) GetCallbacks() leaderelection.LeaderCallbacks {
	return mcp.callbacks
}

func TestNewLeaderElector(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
	}

	le := NewLeaderElector(config)

	if le.config != config {
		t.Errorf("Expected config to be %v, but got %v", config, le.config)
	}
}

func TestRun(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	startedLeading := false
	stoppedLeading := false
	newLeader := ""

	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			startedLeading = true
		},
		OnStoppedLeading: func() {
			stoppedLeading = true
		},
		OnNewLeader: func(identity string) {
			newLeader = identity
		},
	}

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{callbacks: callbacks},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
		LeaseDuration:      15 * time.Second,
		RenewDeadline:      10 * time.Second,
		RetryPeriod:        2 * time.Second,
	}

	le := NewLeaderElector(config)

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		err := le.Run(ctx)
		if err != nil {
			t.Errorf("Expected no error, but got %v", err)
		}
	}()

	time.Sleep(1 * time.Second)
	cancel()
	time.Sleep(1 * time.Second)

	if !startedLeading {
		t.Errorf("OnStartedLeading should have been called")
	}

	if !stoppedLeading {
		t.Errorf("OnStoppedLeading should have been called")
	}

	if newLeader == "" {
		t.Errorf("OnNewLeader should have been called")
	}
}
