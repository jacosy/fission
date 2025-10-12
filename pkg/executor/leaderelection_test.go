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
	"sync"
	"testing"

	"go.uber.org/zap/zaptest"
	"k8s.io/client-go/kubernetes/fake"

	fv1 "github.com/fission/fission/pkg/apis/core/v1"
	"github.com/fission/fission/pkg/executor/executortype"
	"github.com/fission/fission/pkg/leaderelection"
	fissionfake "github.com/fission/fission/pkg/generated/clientset/versioned/fake"
	"github.com/fission/fission/pkg/utils/manager"
)

func TestExecutor_IsLeader_WhenDisabled(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		fsCreateWg:    sync.Map{},
		leaderEnabled: false,
	}

	// When leader election is disabled, IsLeader should always return true
	if !executor.IsLeader() {
		t.Error("Expected IsLeader to return true when leader election is disabled")
	}
}

func TestExecutor_IsLeader_WhenEnabledButNoElector(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		fsCreateWg:    sync.Map{},
		leaderEnabled: true,
		leaderElector: nil,
	}

	// When leader election is enabled but no elector exists, should return false
	if executor.IsLeader() {
		t.Error("Expected IsLeader to return false when elector is nil")
	}
}

func TestExecutor_IsLeader_WhenEnabledAndLeader(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()
	kubeClient := fake.NewSimpleClientset()

	mgr := manager.New()
	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		leaderEnabled: true,
	}

	_, cancel := context.WithCancel(context.Background())
	defer cancel()

	config := &leaderelection.LeaderElectionConfig{
		Logger:             logger,
		Component:          "test-executor",
		KubeClient:         kubeClient,
		CallbacksProvider:  NewExecutorCallbacksProvider(logger, executor, mgr, cancel),
		LeaseLockName:      "test-lease",
		LeaseLockNamespace: "test-namespace",
	}

	elector, err := leaderelection.NewLeaderElector(config)
	if err != nil {
		t.Fatalf("Failed to create leader elector: %v", err)
	}

	executor.fsCreateWg = sync.Map{}
	executor.leaderElector = elector

	// Initially should not be leader (before Run is called)
	if executor.IsLeader() {
		t.Error("Expected IsLeader to return false before leadership is acquired")
	}
}

func TestExecutor_RequireLeader_WhenDisabled(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		fsCreateWg:    sync.Map{},
		leaderEnabled: false,
	}

	// When leader election is disabled, RequireLeader should not error
	if err := executor.RequireLeader(); err != nil {
		t.Errorf("Expected no error when leader election is disabled, got: %v", err)
	}
}

func TestExecutor_RequireLeader_WhenNotLeader(t *testing.T) {
	logger := zaptest.NewLogger(t)
	fissionClient := fissionfake.NewSimpleClientset()

	executor := &Executor{
		logger:        logger,
		fissionClient: fissionClient,
		executorTypes: make(map[fv1.ExecutorType]executortype.ExecutorType),
		requestChan:   make(chan *createFuncServiceRequest),
		fsCreateWg:    sync.Map{},
		leaderEnabled: true,
		leaderElector: nil,
	}

	// When not the leader, RequireLeader should return an error
	err := executor.RequireLeader()
	if err == nil {
		t.Error("Expected error when not the leader")
	}
}
