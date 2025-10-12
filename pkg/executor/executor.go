/*
Copyright 2016 The Fission Authors.

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
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dchest/uniuri"
	"go.uber.org/zap"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	k8sCache "k8s.io/client-go/tools/cache"

	fv1 "github.com/fission/fission/pkg/apis/core/v1"
	"github.com/fission/fission/pkg/crd"
	"github.com/fission/fission/pkg/executor/cms"
	"github.com/fission/fission/pkg/executor/executortype"
	"github.com/fission/fission/pkg/executor/executortype/container"
	"github.com/fission/fission/pkg/executor/executortype/newdeploy"
	"github.com/fission/fission/pkg/executor/executortype/poolmgr"
	"github.com/fission/fission/pkg/executor/fscache"
	"github.com/fission/fission/pkg/executor/util"
	fetcherConfig "github.com/fission/fission/pkg/fetcher/config"
	"github.com/fission/fission/pkg/generated/clientset/versioned"
	genInformer "github.com/fission/fission/pkg/generated/informers/externalversions"
	"github.com/fission/fission/pkg/leaderelection"
	"github.com/fission/fission/pkg/utils"
	"github.com/fission/fission/pkg/utils/manager"
	"github.com/fission/fission/pkg/utils/metrics"
	otelUtils "github.com/fission/fission/pkg/utils/otel"
)

type (
	// Executor defines a fission function executor.
	Executor struct {
		logger *zap.Logger

		executorTypes map[fv1.ExecutorType]executortype.ExecutorType
		cms           *cms.ConfigSecretController

		fissionClient versioned.Interface

		requestChan chan *createFuncServiceRequest
		fsCreateWg  sync.Map

		// Leader election support
		leaderElector *leaderelection.LeaderElector
		leaderEnabled bool

		// Informers - only started on the leader instance
		fissionInformers   map[string]genInformer.SharedInformerFactory
		poolmgrInformers   map[string]informers.SharedInformerFactory
		newdeployInformers map[string]informers.SharedInformerFactory
		containerInformers map[string]informers.SharedInformerFactory
		configMapInformers map[string]k8sCache.SharedIndexInformer
		secretInformers    map[string]k8sCache.SharedIndexInformer
	}
	createFuncServiceRequest struct {
		context  context.Context
		function *fv1.Function
		respChan chan *createFuncServiceResponse
	}

	createFuncServiceResponse struct {
		funcSvc *fscache.FuncSvc
		err     error
	}
)

// MakeExecutor returns an Executor for given ExecutorType(s).
// Note: This function creates executor types but does NOT start their reconciliation loops.
// When leader election is enabled, the loops are started only on the leader instance.
// When leader election is disabled, they are started immediately after this function returns.
// The serveCreateFuncServices handler is also not started here - it will be started either
// in the OnStartedLeading callback (when leader election is enabled) or in setupLeaderElection
// (when leader election is disabled).
func MakeExecutor(ctx context.Context, logger *zap.Logger, mgr manager.Interface, cms *cms.ConfigSecretController,
	fissionClient versioned.Interface, types map[fv1.ExecutorType]executortype.ExecutorType) (*Executor, error) {
	executor := &Executor{
		logger:        logger.Named("executor"),
		cms:           cms,
		fissionClient: fissionClient,
		executorTypes: types,

		requestChan: make(chan *createFuncServiceRequest),
	}

	return executor, nil
}

// startExecutorTypes starts all executor type reconciliation loops.
// This should only be called on the leader instance when leader election is enabled.
func (executor *Executor) startExecutorTypes(ctx context.Context, mgr manager.Interface) {
	executor.logger.Info("starting executor type reconciliation loops")
	for _, et := range executor.executorTypes {
		et := et
		mgr.Add(ctx, func(ctx context.Context) {
			et.Run(ctx, mgr)
		})
	}
}

// startAllInformers starts all informers (Fission, executor-specific, ConfigMap, and Secret).
// This should only be called on the leader instance when leader election is enabled.
func (executor *Executor) startAllInformers(ctx context.Context) {
	executor.logger.Info("starting all informers on leader instance")

	// Start Fission resource informers (Functions, Packages, Environments, etc.)
	for _, factory := range executor.fissionInformers {
		factory.Start(ctx.Done())
	}

	// Start poolmgr executor informers
	for _, factory := range executor.poolmgrInformers {
		factory.Start(ctx.Done())
	}

	// Start newdeploy executor informers
	for _, factory := range executor.newdeployInformers {
		factory.Start(ctx.Done())
	}

	// Start container executor informers
	for _, factory := range executor.containerInformers {
		factory.Start(ctx.Done())
	}

	// Start ConfigMap informers
	for _, informer := range executor.configMapInformers {
		go informer.Run(ctx.Done())
	}

	// Start Secret informers
	for _, informer := range executor.secretInformers {
		go informer.Run(ctx.Done())
	}

	executor.logger.Info("all informers started successfully")
}

// adoptAndCleanupResources adopts existing resources and cleans up old executor objects.
// This should only be called on the leader instance when it starts leading.
func (executor *Executor) adoptAndCleanupResources(ctx context.Context) {
	executor.logger.Info("adopting existing resources and cleaning up old executor objects")

	wg := &sync.WaitGroup{}
	for _, et := range executor.executorTypes {
		wg.Add(1)
		go func(et executortype.ExecutorType) {
			defer wg.Done()

			adoptExistingResources, _ := strconv.ParseBool(os.Getenv("ADOPT_EXISTING_RESOURCES"))
			if adoptExistingResources {
				et.AdoptExistingResources(ctx)
			}
			et.CleanupOldExecutorObjects(ctx)
		}(et)
	}

	// Wait for all adoption and cleanup tasks to complete with a timeout
	util.WaitTimeout(wg, 30*time.Second)
	executor.logger.Info("resource adoption and cleanup complete")
}

// All non-cached function service requests go through this goroutine
// serially. It parallelizes requests for different functions, and
// ensures that for a given function, only one request causes a pod to
// get specialized. In other words, it ensures that when there's an
// ongoing request for a certain function, all other requests wait for
// that request to complete.
func (executor *Executor) serveCreateFuncServices(ctx context.Context) {
	for {
		var req *createFuncServiceRequest
		select {
		case <-ctx.Done():
			return
		case req = <-executor.requestChan:
		}
		function := req.function
		fnName := k8sCache.MetaObjectToName(function)
		fnkeyUR := crd.CacheKeyURFromObject(function)

		if req.function.Spec.InvokeStrategy.ExecutionStrategy.ExecutorType == fv1.ExecutorTypePoolmgr {
			go func() {
				buffer := 10 // add some buffer time for specialization
				specializationTimeout := req.function.Spec.InvokeStrategy.ExecutionStrategy.SpecializationTimeout

				// set minimum specialization timeout to avoid illegal input and
				// compatibility problem when applying old spec file that doesn't
				// have specialization timeout field.
				if specializationTimeout < fv1.DefaultSpecializationTimeOut {
					specializationTimeout = fv1.DefaultSpecializationTimeOut
				}

				fnSpecializationTimeoutContext, cancel := context.WithTimeoutCause(req.context,
					time.Duration(specializationTimeout+buffer)*time.Second, fmt.Errorf("function specialization timeout (%d)s exceeded", specializationTimeout+buffer))
				defer cancel()

				fsvc, err := executor.createServiceForFunction(fnSpecializationTimeoutContext, req.function)
				req.respChan <- &createFuncServiceResponse{
					funcSvc: fsvc,
					err:     err,
				}
			}()
			continue
		}

		// Cache miss -- is this first one to request the func?
		wg, found := executor.fsCreateWg.Load(fnkeyUR)
		if !found {
			// create a waitgroup for other requests for
			// the same function to wait on
			wg := &sync.WaitGroup{}
			wg.Add(1)
			executor.fsCreateWg.Store(fnkeyUR, wg)

			// launch a goroutine for each request, to parallelize
			// the specialization of different functions
			go func() {
				// Control overall specialization time by setting function
				// specialization time to context. The reason not to use
				// context from router requests is because a request maybe
				// canceled for unknown reasons and let executor keeps
				// spawning pods that never finish specialization process.
				// Also, even a request failed, a specialized function pod
				// still can serve other subsequent requests.

				buffer := 10 // add some buffer time for specialization
				specializationTimeout := req.function.Spec.InvokeStrategy.ExecutionStrategy.SpecializationTimeout

				// set minimum specialization timeout to avoid illegal input and
				// compatibility problem when applying old spec file that doesn't
				// have specialization timeout field.
				if specializationTimeout < fv1.DefaultSpecializationTimeOut {
					specializationTimeout = fv1.DefaultSpecializationTimeOut
				}

				fnSpecializationTimeoutContext, cancel := context.WithTimeoutCause(req.context,
					time.Duration(specializationTimeout+buffer)*time.Second, fmt.Errorf("function specialization timeout (%d)s exceeded", specializationTimeout+buffer))
				defer cancel()

				fsvc, err := executor.createServiceForFunction(fnSpecializationTimeoutContext, req.function)
				req.respChan <- &createFuncServiceResponse{
					funcSvc: fsvc,
					err:     err,
				}
				executor.fsCreateWg.Delete(fnkeyUR)
				wg.Done()
			}()
		} else {
			// There's an existing request for this function, wait for it to finish
			go func() {
				executor.logger.Debug("waiting for concurrent request for the same function",
					zap.String("function", fnName.String()))
				wg, ok := wg.(*sync.WaitGroup)
				if !ok {
					err := fmt.Errorf("could not convert value to workgroup for function %s", fnName)
					req.respChan <- &createFuncServiceResponse{
						funcSvc: nil,
						err:     err,
					}
				}
				wg.Wait()

				// get the function service from the cache
				fsvc, err := executor.getFunctionServiceFromCache(req.context, req.function)

				// fsCache return error when the entry does not exist/expire.
				// It normally happened if there are multiple requests are
				// waiting for the same function and executor failed to cre-
				// ate service for function.
				err = fmt.Errorf("error getting service for function %s: %w", fnName, err)
				req.respChan <- &createFuncServiceResponse{
					funcSvc: fsvc,
					err:     err,
				}
			}()
		}
	}
}

func (executor *Executor) createServiceForFunction(ctx context.Context, fn *fv1.Function) (*fscache.FuncSvc, error) {
	logger := otelUtils.LoggerWithTraceID(ctx, executor.logger)
	otelUtils.SpanTrackEvent(ctx, "createServiceForFunction", otelUtils.GetAttributesForFunction(fn)...)

	// Check if this instance is the leader before creating new resources
	if err := executor.RequireLeader(); err != nil {
		logger.Warn("rejecting function service creation request - not leader",
			zap.String("function_name", fn.ObjectMeta.Name),
			zap.String("function_namespace", fn.ObjectMeta.Namespace),
			zap.Error(err))
		return nil, err
	}

	logger.Debug("no cached function service found, creating one",
		zap.String("function_name", fn.ObjectMeta.Name),
		zap.String("function_namespace", fn.ObjectMeta.Namespace))

	t := fn.Spec.InvokeStrategy.ExecutionStrategy.ExecutorType
	e, ok := executor.executorTypes[t]
	if !ok {
		return nil, fmt.Errorf("unknown executor type '%s'", t)
	}

	fsvc, fsvcErr := e.GetFuncSvc(ctx, fn)
	if fsvcErr != nil {
		e := "error creating service for function"
		logger.Error(e,
			zap.Error(fsvcErr),
			zap.String("function_name", fn.ObjectMeta.Name),
			zap.String("function_namespace", fn.ObjectMeta.Namespace))
		fsvcErr = fmt.Errorf("[%s] %s: %w", fn.ObjectMeta.Name, e, fsvcErr)
	}

	return fsvc, fsvcErr
}

func (executor *Executor) getFunctionServiceFromCache(ctx context.Context, fn *fv1.Function) (*fscache.FuncSvc, error) {
	otelUtils.SpanTrackEvent(ctx, "getFunctionServiceFromCache", otelUtils.GetAttributesForFunction(fn)...)
	t := fn.Spec.InvokeStrategy.ExecutionStrategy.ExecutorType
	e, ok := executor.executorTypes[t]
	if !ok {
		return nil, fmt.Errorf("unknown executor type '%s'", t)
	}
	return e.GetFuncSvcFromCache(ctx, fn)
}

// IsLeader returns true if this executor instance is the leader.
// If leader election is disabled, always returns true.
func (executor *Executor) IsLeader() bool {
	if !executor.leaderEnabled {
		return true
	}
	if executor.leaderElector == nil {
		return false
	}
	return executor.leaderElector.IsLeader()
}

// RequireLeader returns an error if this executor is not the leader and leader election is enabled.
func (executor *Executor) RequireLeader() error {
	if !executor.IsLeader() {
		leaderIdentity := ""
		if executor.leaderElector != nil {
			leaderIdentity = executor.leaderElector.GetLeader()
		}
		return fmt.Errorf("this executor instance is not the leader (current leader: %s)", leaderIdentity)
	}
	return nil
}

// setupLeaderElection initializes and starts leader election for the executor if enabled.
// It reads configuration from environment variables and starts the leader election goroutine.
// When leader election is disabled, it starts all executor types and informers immediately.
func setupLeaderElection(ctx context.Context, executor *Executor, kubernetesClient kubernetes.Interface, logger *zap.Logger, mgr manager.Interface) error {
	leaderEnabled, _ := strconv.ParseBool(os.Getenv("ENABLE_LEADER_ELECTION"))
	if !leaderEnabled {
		logger.Info("leader election disabled for executor - starting all reconciliation loops and informers immediately")
		executor.leaderEnabled = false

		// When leader election is disabled, adopt resources and start everything immediately
		executor.adoptAndCleanupResources(ctx)

		// Start all informers (only on this instance since leader election is disabled)
		executor.startAllInformers(ctx)

		// Start the function service creation request handler
		mgr.Add(ctx, func(ctx context.Context) {
			executor.serveCreateFuncServices(ctx)
		})

		executor.startExecutorTypes(ctx, mgr)

		return nil
	}

	logger.Info("leader election enabled for executor")

	// Get configuration from environment variables
	leaseName := os.Getenv("EXECUTOR_LEASE_NAME")
	if leaseName == "" {
		leaseName = "fission-executor"
	}

	leaseNamespace := os.Getenv("EXECUTOR_LEASE_NAMESPACE")
	if leaseNamespace == "" {
		leaseNamespace = utils.DefaultNSResolver().FunctionNamespace
	}

	// Create a cancellable context for graceful shutdown
	// This allows the OnStoppedLeading callback to trigger shutdown of all components
	ctx, cancel := context.WithCancel(ctx)

	// Create leader election config with callbacks that control executor types
	leConfig := &leaderelection.LeaderElectionConfig{
		Logger:             logger,
		Component:          "executor",
		KubeClient:         kubernetesClient,
		CallbacksProvider:  NewExecutorCallbacksProvider(logger, executor, mgr, cancel),
		LeaseLockName:      leaseName,
		LeaseLockNamespace: leaseNamespace,
		// Using default durations from leaderelection package
	}

	elector, err := leaderelection.NewLeaderElector(leConfig)
	if err != nil {
		return fmt.Errorf("failed to create leader elector: %w", err)
	}

	executor.leaderElector = elector
	executor.leaderEnabled = true

	// Start leader election in background
	mgr.Add(ctx, func(ctx context.Context) {
		logger.Info("starting leader election for executor")
		if err := elector.Run(ctx); err != nil {
			logger.Error("leader election stopped with error", zap.Error(err))
		}
	})

	return nil
}

// StartExecutor Starts executor and the executor components such as Poolmgr,
// deploymgr and potential future executor types
func StartExecutor(ctx context.Context, clientGen crd.ClientGeneratorInterface, logger *zap.Logger, mgr manager.Interface, port int) error {

	fissionClient, err := clientGen.GetFissionClient()
	if err != nil {
		return fmt.Errorf("error making the fission client: %w", err)
	}
	kubernetesClient, err := clientGen.GetKubernetesClient()
	if err != nil {
		return fmt.Errorf("error making the kube client: %w", err)
	}
	metricsClient, err := clientGen.GetMetricsClient()
	if err != nil {
		logger.Error("error making the metrics client", zap.Error(err))
	}

	err = crd.WaitForFunctionCRDs(ctx, logger, fissionClient)
	if err != nil {
		return fmt.Errorf("error waiting for CRDs: %w", err)
	}

	fetcherConfig, err := fetcherConfig.MakeFetcherConfig("/userfunc")
	if err != nil {
		return fmt.Errorf("error making fetcher config: %w", err)
	}

	executorInstanceID := strings.ToLower(uniuri.NewLen(8))

	podSpecPatch, err := util.GetSpecFromConfigMap(fv1.RuntimePodSpecPath)
	if err != nil && !os.IsNotExist(err) {
		logger.Warn("error reading data for pod spec patch", zap.String("path", fv1.RuntimePodSpecPath), zap.Error(err))
	}

	logger.Info("Starting executor", zap.String("instanceID", executorInstanceID))

	finformerFactory := make(map[string]genInformer.SharedInformerFactory, 0)
	for _, ns := range utils.DefaultNSResolver().FissionResourceNS {
		finformerFactory[ns] = genInformer.NewFilteredSharedInformerFactory(fissionClient, time.Minute*30, ns, nil)
	}

	executorLabel, err := utils.GetInformerLabelByExecutor(fv1.ExecutorTypePoolmgr)
	if err != nil {
		return err
	}
	gpmInformerFactory := utils.GetInformerFactoryByExecutor(kubernetesClient, executorLabel, time.Minute*30)
	gpm, err := poolmgr.MakeGenericPoolManager(ctx,
		logger,
		fissionClient, kubernetesClient, metricsClient,
		fetcherConfig, executorInstanceID,
		finformerFactory,
		gpmInformerFactory, podSpecPatch)
	if err != nil {
		return fmt.Errorf("pool manager creation failed: %w", err)
	}

	executorLabel, err = utils.GetInformerLabelByExecutor(fv1.ExecutorTypeNewdeploy)
	if err != nil {
		return err
	}
	ndmInformerFactory := utils.GetInformerFactoryByExecutor(kubernetesClient, executorLabel, time.Minute*30)
	ndm, err := newdeploy.MakeNewDeploy(ctx,
		logger,
		fissionClient, kubernetesClient,
		fetcherConfig, executorInstanceID,
		finformerFactory,
		ndmInformerFactory, podSpecPatch)
	if err != nil {
		return fmt.Errorf("new deploy manager creation failed: %w", err)
	}

	executorLabel, err = utils.GetInformerLabelByExecutor(fv1.ExecutorTypeContainer)
	if err != nil {
		return err
	}
	cnmInformerFactory := utils.GetInformerFactoryByExecutor(kubernetesClient, executorLabel, time.Minute*30)
	cnm, err := container.MakeContainer(
		ctx, logger,
		fissionClient, kubernetesClient,
		executorInstanceID, finformerFactory,
		cnmInformerFactory)
	if err != nil {
		return fmt.Errorf("container manager creation failed: %w", err)
	}

	executorTypes := make(map[fv1.ExecutorType]executortype.ExecutorType)
	executorTypes[gpm.GetTypeName(ctx)] = gpm
	executorTypes[ndm.GetTypeName(ctx)] = ndm
	executorTypes[cnm.GetTypeName(ctx)] = cnm

	// Note: AdoptExistingResources and CleanupOldExecutorObjects are now called in adoptAndCleanupResources
	// which is invoked either in setupLeaderElection (when leader election is disabled) or in the
	// OnStartedLeading callback (when leader election is enabled and this instance becomes leader)

	// Create ConfigMap and Secret informers but don't start them yet
	configMapInformer := utils.GetK8sInformersForNamespaces(kubernetesClient, time.Minute*30, fv1.ConfigMaps)
	secretInformer := utils.GetK8sInformersForNamespaces(kubernetesClient, time.Minute*30, fv1.Secrets)

	cms, err := cms.MakeConfigSecretController(ctx, logger, fissionClient, kubernetesClient, executorTypes, configMapInformer, secretInformer)
	if err != nil {
		return fmt.Errorf("error creating configmap and secret controller: %w", err)
	}

	api, err := MakeExecutor(ctx, logger, mgr, cms, fissionClient, executorTypes)
	if err != nil {
		return err
	}

	// Store all informer factories in the executor
	// They will be started ONLY when this instance becomes leader (or immediately if leader election is disabled)
	// This prevents multiple informers from watching the same resources and causing race conditions
	api.fissionInformers = finformerFactory
	api.poolmgrInformers = gpmInformerFactory
	api.newdeployInformers = ndmInformerFactory
	api.containerInformers = cnmInformerFactory
	api.configMapInformers = configMapInformer
	api.secretInformers = secretInformer

	// Initialize leader election if enabled
	// This will start executor types and config/secret informers on the leader
	if err := setupLeaderElection(ctx, api, kubernetesClient, logger, mgr); err != nil {
		return fmt.Errorf("failed to setup leader election: %w", err)
	}

	utils.CreateMissingPermissionForSA(ctx, kubernetesClient, logger)

	mgr.Add(ctx, func(ctx context.Context) {
		metrics.ServeMetrics(ctx, "executor", logger, mgr)
	})

	mgr.Add(ctx, func(ctx context.Context) {
		api.Serve(ctx, mgr, port)
	})

	return nil
}
