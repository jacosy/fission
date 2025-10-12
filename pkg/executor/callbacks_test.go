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
	"testing"
	"time"

	"go.uber.org/zap/zaptest"

	fv1 "github.com/fission/fission/pkg/apis/core/v1"
	"github.com/fission/fission/pkg/executor/executortype"
	fissionfake "github.com/fission/fission/pkg/generated/clientset/versioned/fake"
	"github.com/fission/fission/pkg/utils/manager"
)

func TestNewExecutorCallbacksProvider(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()
	mgr := manager.New()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		leaderEnabled: false,
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := NewExecutorCallbacksProvider(logger, executor, mgr, cancel)
	if provider == nil {
		t.Fatal("Expected non-nil provider")
	}

	if provider.logger != logger {
		t.Error("Expected logger to be set correctly")
	}

	if provider.executor != executor {
		t.Error("Expected executor to be set correctly")
	}

	if provider.mgr != mgr {
		t.Error("Expected mgr to be set correctly")
	}

	if provider.mainCancelFunc == nil {
		t.Error("Expected mainCancelFunc to be set correctly")
	}
}

func TestExecutorCallbacksProvider_GetCallbacks(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()
	mgr := manager.New()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		leaderEnabled: false,
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := NewExecutorCallbacksProvider(logger, executor, mgr, cancel)
	// Override osExit to prevent actual process exit during test
	provider.osExit = func(code int) {
		// Do nothing in test
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
	// OnStartedLeading now blocks until context is cancelled, so run it in a goroutine
	callbackCtx, callbackCancel := context.WithCancel(context.Background())

	done := make(chan bool)
	go func() {
		callbacks.OnStartedLeading(callbackCtx)
		done <- true
	}()

	// Give the callback time to start up
	time.Sleep(100 * time.Millisecond)

	// Cancel the context to trigger shutdown
	callbackCancel()

	// Wait for the callback to complete with timeout
	select {
	case <-done:
		// Success - callback completed
	case <-time.After(5 * time.Second):
		t.Error("OnStartedLeading callback did not complete within timeout")
	}

	// Test OnStoppedLeading
	callbacks.OnStoppedLeading()

	// Test OnNewLeader
	callbacks.OnNewLeader("test-identity")
}
