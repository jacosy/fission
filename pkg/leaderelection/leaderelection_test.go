package leaderelection

import (
	"context"
	"os"
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

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	if le.config != config {
		t.Errorf("Expected config to be %v, but got %v", config, le.config)
	}

	if le.identity == "" {
		t.Error("Expected identity to be set")
	}
}

func TestNewLeaderElectorWithValidation(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	tests := []struct {
		name        string
		config      *LeaderElectionConfig
		expectError bool
		errorMsg    string
	}{
		{
			name:        "nil config",
			config:      nil,
			expectError: true,
			errorMsg:    "config cannot be nil",
		},
		{
			name: "missing KubeClient",
			config: &LeaderElectionConfig{
				Logger:             logger,
				CallbacksProvider:  &mockCallbacksProvider{},
				LeaseLockName:      "test-lease",
				LeaseLockNamespace: "test-namespace",
			},
			expectError: true,
			errorMsg:    "KubeClient is required",
		},
		{
			name: "missing LeaseLockName",
			config: &LeaderElectionConfig{
				Logger:             logger,
				KubeClient:         kubeClient,
				CallbacksProvider:  &mockCallbacksProvider{},
				LeaseLockNamespace: "test-namespace",
			},
			expectError: true,
			errorMsg:    "LeaseLockName is required",
		},
		{
			name: "missing LeaseLockNamespace",
			config: &LeaderElectionConfig{
				Logger:            logger,
				KubeClient:        kubeClient,
				CallbacksProvider: &mockCallbacksProvider{},
				LeaseLockName:     "test-lease",
			},
			expectError: true,
			errorMsg:    "LeaseLockNamespace is required",
		},
		{
			name: "missing CallbacksProvider",
			config: &LeaderElectionConfig{
				Logger:             logger,
				KubeClient:         kubeClient,
				LeaseLockName:      "test-lease",
				LeaseLockNamespace: "test-namespace",
			},
			expectError: true,
			errorMsg:    "CallbacksProvider is required",
		},
		{
			name: "missing Logger",
			config: &LeaderElectionConfig{
				KubeClient:         kubeClient,
				CallbacksProvider:  &mockCallbacksProvider{},
				LeaseLockName:      "test-lease",
				LeaseLockNamespace: "test-namespace",
			},
			expectError: true,
			errorMsg:    "Logger is required",
		},
		{
			name: "valid config",
			config: &LeaderElectionConfig{
				Logger:             logger,
				KubeClient:         kubeClient,
				CallbacksProvider:  &mockCallbacksProvider{},
				LeaseLockName:      "test-lease",
				LeaseLockNamespace: "test-namespace",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			le, err := NewLeaderElector(tt.config)
			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error '%s', but got none", tt.errorMsg)
				} else if err.Error() != tt.errorMsg {
					t.Errorf("Expected error '%s', but got '%s'", tt.errorMsg, err.Error())
				}
				if le != nil {
					t.Error("Expected nil LeaderElector when error occurs")
				}
			} else {
				if err != nil {
					t.Errorf("Expected no error, but got %v", err)
				}
				if le == nil {
					t.Error("Expected non-nil LeaderElector")
				}
			}
		})
	}
}

func TestDefaultValues(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
		// Durations not set, should use defaults
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	if le.config.LeaseDuration != DefaultLeaseDuration {
		t.Errorf("Expected LeaseDuration to be %v, but got %v", DefaultLeaseDuration, le.config.LeaseDuration)
	}
	if le.config.RenewDeadline != DefaultRenewDeadline {
		t.Errorf("Expected RenewDeadline to be %v, but got %v", DefaultRenewDeadline, le.config.RenewDeadline)
	}
	if le.config.RetryPeriod != DefaultRetryPeriod {
		t.Errorf("Expected RetryPeriod to be %v, but got %v", DefaultRetryPeriod, le.config.RetryPeriod)
	}
}

func TestCustomIdentity(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	customIdentity := "custom-identity-123"
	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
		Identity:           customIdentity,
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	if le.GetIdentity() != customIdentity {
		t.Errorf("Expected identity to be %s, but got %s", customIdentity, le.GetIdentity())
	}
}

func TestIdentityFromEnv(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	// Set POD_NAME environment variable
	podName := "test-pod-123"
	os.Setenv("POD_NAME", podName)
	defer os.Unsetenv("POD_NAME")

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	if le.GetIdentity() != podName {
		t.Errorf("Expected identity to be %s, but got %s", podName, le.GetIdentity())
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

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

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

func TestRunWithNilContext(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error creating elector, but got %v", err)
	}

	err = le.Run(nil)
	if err == nil {
		t.Error("Expected error when running with nil context")
	}
	if err.Error() != "context cannot be nil" {
		t.Errorf("Expected 'context cannot be nil' error, but got '%s'", err.Error())
	}
}

func TestIsLeader(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	startedChan := make(chan struct{})
	stoppedChan := make(chan struct{})

	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			close(startedChan)
		},
		OnStoppedLeading: func() {
			close(stoppedChan)
		},
	}

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{callbacks: callbacks},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	// Initially not leader
	if le.IsLeader() {
		t.Error("Expected IsLeader to be false initially")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		le.Run(ctx)
	}()

	// Wait for leadership
	select {
	case <-startedChan:
		if !le.IsLeader() {
			t.Error("Expected IsLeader to be true after OnStartedLeading")
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for OnStartedLeading")
	}

	// Cancel context and check leadership is lost
	cancel()
	select {
	case <-stoppedChan:
		time.Sleep(100 * time.Millisecond) // Give it time to update state
		if le.IsLeader() {
			t.Error("Expected IsLeader to be false after OnStoppedLeading")
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for OnStoppedLeading")
	}
}

func TestGetLeader(t *testing.T) {
	logger := zaptest.NewLogger(t)
	kubeClient := fake.NewSimpleClientset()

	startedChan := make(chan struct{})

	callbacks := leaderelection.LeaderCallbacks{
		OnStartedLeading: func(ctx context.Context) {
			close(startedChan)
		},
	}

	config := &LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-component",
		KubeClient:         kubeClient,
		CallbacksProvider:  &mockCallbacksProvider{callbacks: callbacks},
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
	}

	le, err := NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Expected no error, but got %v", err)
	}

	// Initially no leader known
	if le.GetLeader() != "" {
		t.Error("Expected GetLeader to return empty string before Run")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		le.Run(ctx)
	}()

	// Wait for leadership
	select {
	case <-startedChan:
		leader := le.GetLeader()
		if leader == "" {
			t.Error("Expected GetLeader to return non-empty string after becoming leader")
		}
		if leader != le.GetIdentity() {
			t.Errorf("Expected leader to be %s, but got %s", le.GetIdentity(), leader)
		}
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for OnStartedLeading")
	}
}

func TestDefaultCallbacksProvider(t *testing.T) {
	logger := zaptest.NewLogger(t)

	provider := &DefaultCallbacksProvider{
		Logger:    logger,
		Component: "test-component",
	}

	callbacks := provider.GetCallbacks()

	// Test that callbacks are not nil
	if callbacks.OnStartedLeading == nil {
		t.Error("Expected OnStartedLeading to be set")
	}
	if callbacks.OnStoppedLeading == nil {
		t.Error("Expected OnStoppedLeading to be set")
	}
	if callbacks.OnNewLeader == nil {
		t.Error("Expected OnNewLeader to be set")
	}

	// Test that callbacks can be invoked without panic
	ctx := context.Background()
	callbacks.OnStartedLeading(ctx)
	callbacks.OnStoppedLeading()
	callbacks.OnNewLeader("some-identity")
}
