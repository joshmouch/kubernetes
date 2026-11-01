/*
Copyright 2026 The Kubernetes Authors.

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

package kubelet

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	csitrans "k8s.io/csi-translation-lib"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/kubelet/clustertrustbundle"
	"k8s.io/kubernetes/pkg/kubelet/configmap"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	kubepod "k8s.io/kubernetes/pkg/kubelet/pod"
	"k8s.io/kubernetes/pkg/kubelet/secret"
	"k8s.io/kubernetes/pkg/kubelet/token"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	cachemanager "k8s.io/kubernetes/pkg/kubelet/util/manager"
	volumecache "k8s.io/kubernetes/pkg/kubelet/volumemanager/cache"
	"k8s.io/kubernetes/pkg/kubelet/volumemanager/populator"
	"k8s.io/kubernetes/pkg/volume"
	volumeconfigmap "k8s.io/kubernetes/pkg/volume/configmap"
	"k8s.io/kubernetes/pkg/volume/csimigration"
	"k8s.io/kubernetes/pkg/volume/emptydir"
	volumeutil "k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// Observe the publication boundary using the actual cache/volume consumer.
type deferredResourcePodManager struct {
	kubepod.Manager
	beforePublish func(*v1.Pod)
	afterPublish  func(*v1.Pod)
}

func (m *deferredResourcePodManager) AddPod(pod *v1.Pod) {
	if m.beforePublish != nil {
		m.beforePublish(pod)
	}
	m.Manager.AddPod(pod)
	if m.afterPublish != nil {
		m.afterPublish(pod)
	}
}

func TestDeferredAdmissionSharedStateProvider(t *testing.T) {
	tk, _ := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	uid := types.UID("waiting-no-worker")
	p := &podWorkers{allocationManager: kl.allocationManager, podSyncStatuses: map[types.UID]*podSyncStatus{}, podsSynced: true}
	require.True(t, p.ShouldPodRuntimeBeRemoved(uid), "the control is a genuinely unknown post-sync UID")
	kl.allocationManager.MarkPodAdmissionDeferred(uid, kl.clock.Now())
	require.Empty(t, p.podSyncStatuses, "waiting must not construct a worker or admitted status")
	require.True(t, p.CouldHaveRunningContainers(uid))
	require.False(t, p.ShouldPodBeFinished(uid))
	require.False(t, p.ShouldPodContainersBeTerminating(uid))
	require.False(t, p.ShouldPodRuntimeBeRemoved(uid))
	require.False(t, p.ShouldPodContentBeRemoved(uid))

	// Runtime GC/volume/content readers race with queue removal in production.
	var readers sync.WaitGroup
	done := make(chan struct{})
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-done:
					return
				default:
					p.ShouldPodRuntimeBeRemoved(uid)
					p.ShouldPodContentBeRemoved(uid)
					p.ShouldPodContainersBeTerminating(uid)
				}
			}
		}()
	}
	for i := 0; i < 50; i++ {
		kl.allocationManager.ClearPodAdmission(uid)
		kl.allocationManager.MarkPodAdmissionDeferred(uid, kl.clock.Now())
	}
	kl.allocationManager.ClearPodAdmission(uid)
	close(done)
	readers.Wait()
	require.True(t, p.ShouldPodRuntimeBeRemoved(uid))
	require.True(t, p.ShouldPodContentBeRemoved(uid))
	require.True(t, p.ShouldPodContainersBeTerminating(uid))
	require.False(t, p.CouldHaveRunningContainers(uid))
	require.True(t, p.ShouldPodBeFinished(uid))
}

func TestDeferredAdmissionVolumeCachePopulationAndRelease(t *testing.T) {
	for _, ending := range []string{"expiry", "cancellation"} {
		t.Run(ending, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			tk, h := newDeferredAdmissionKubelet(t)
			defer tk.Cleanup()
			kl := tk.kubelet
			kl.configMapManager = configmap.NewCachingConfigMapManager(tk.fakeKubeClient, cachemanager.GetObjectTTLFromNodeFunc(kl.GetNode))
			kl.secretManager = secret.NewCachingSecretManager(tk.fakeKubeClient, cachemanager.GetObjectTTLFromNodeFunc(kl.GetNode))
			pod := newDeferTestPod("volume-" + ending)
			mode := int32(0644)
			pod.Spec.Volumes = []v1.Volume{{Name: "config", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: "configuration"}, DefaultMode: &mode}}}}
			pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "config", MountPath: "/configuration"}}
			pod.Spec.Containers[0].EnvFrom = []v1.EnvFromSource{{SecretRef: &v1.SecretEnvSource{LocalObjectReference: v1.LocalObjectReference{Name: "credential"}}}}
			_, err := tk.fakeKubeClient.CoreV1().ConfigMaps(pod.Namespace).Create(tCtx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "configuration", Namespace: pod.Namespace}, Data: map[string]string{"key": "native-value"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = tk.fakeKubeClient.CoreV1().Secrets(pod.Namespace).Create(tCtx, &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "credential", Namespace: pod.Namespace}, Data: map[string][]byte{"key": []byte("native-secret")}}, metav1.CreateOptions{})
			require.NoError(t, err)
			_, err = kl.configMapManager.GetConfigMap(pod.Namespace, "configuration")
			require.ErrorContains(t, err, "not registered", "the control reproduces the original cache precondition")

			plugins := append(volumeconfigmap.ProbeVolumePlugins(), emptydir.ProbeVolumePlugins()...)
			kl.volumePluginMgr, err = NewInitializedVolumePluginMgr(kl, kl.secretManager, kl.configMapManager, token.NewManager(kl.kubeClient), &clustertrustbundle.NoopManager{}, plugins, nil)
			require.NoError(t, err)
			plugin, err := kl.volumePluginMgr.FindPluginBySpec(volume.NewSpecFromVolume(&pod.Spec.Volumes[0]))
			require.NoError(t, err)
			mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(&pod.Spec.Volumes[0]), pod)
			require.NoError(t, err)
			// This actual native ConfigMap volume SetUp runs before desired publication,
			// before deferral is marked, and without SyncPod registering anything.
			kl.podManager = &deferredResourcePodManager{Manager: kl.podManager, beforePublish: func(p *v1.Pod) {
				_, getErr := kl.secretManager.GetSecret(p.Namespace, "credential")
				require.NoError(t, getErr)
				require.NoError(t, mounter.SetUp(volume.MounterArgs{}))
			}}
			workers, _, _ := createPodWorkers(klog.FromContext(tCtx))
			workers.allocationManager = kl.allocationManager
			workers.podsSynced = true
			kl.podWorkers = workers
			h.deferResult = true
			kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
			require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
			require.Empty(t, workers.podSyncStatuses)
			data, err := os.ReadFile(filepath.Join(mounter.GetPath(), "key"))
			require.NoError(t, err)
			require.Equal(t, "native-value", string(data))

			// Run the real exported volume populator with the actual shared provider.
			translator := csitrans.New()
			dsw := volumecache.NewDesiredStateOfWorld(kl.volumePluginMgr, volumeutil.NewSELinuxLabelTranslator())
			asw := volumecache.NewActualStateOfWorld(kl.nodeName, kl.volumePluginMgr)
			pop := populator.NewDesiredStateOfWorldPopulator(tk.fakeKubeClient, time.Millisecond, kl.podManager, workers, dsw, asw, csimigration.NewPluginManager(translator), translator, kl.volumePluginMgr)
			ctx, cancel := context.WithTimeout(tCtx, 5*time.Second)
			done := make(chan struct{})
			go func() { defer close(done); pop.Run(ctx, kl.sourcesReady) }()
			defer func() { cancel(); <-done }()
			require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
				return pop.HasAddedPods() && len(dsw.GetVolumesToMount()) == 1, nil
			}))
			pop.ReprocessPod(volumeutil.GetUniquePodName(pod))
			require.False(t, workers.ShouldPodRuntimeBeRemoved(pod.UID), "reconstructed desired volumes remain owned while inventory is pending")
			require.False(t, workers.ShouldPodContentBeRemoved(pod.UID))
			_, err = kl.configMapManager.GetConfigMap(pod.Namespace, "configuration")
			require.NoError(t, err)
			if ending == "expiry" {
				stepClock(t, kl, deferredAdmissionTimeout)
				kl.retryDeferredAdmissions(tCtx)
			} else {
				pod = pod.DeepCopy()
				deleted := metav1.NewTime(kl.clock.Now())
				pod.DeletionTimestamp = &deleted
				kl.HandlePodUpdates(tCtx, []*v1.Pod{pod})
				drainAllWorkers(workers)
			}
			require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
			require.True(t, workers.ShouldPodRuntimeBeRemoved(pod.UID))
			kl.HandlePodRemoves(tCtx, []*v1.Pod{pod})
			drainAllWorkers(workers)
			require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) { return len(dsw.GetVolumesToMount()) == 0, nil }))
			cancel()
			<-done
			unmounter, err := plugin.NewUnmounter("config", pod.UID)
			require.NoError(t, err)
			require.NoError(t, unmounter.TearDown())
			// Existing native post-unmount termination owns cache reference release.
			require.NoError(t, kl.SyncTerminatedPod(tCtx, pod, &kubecontainer.PodStatus{ID: pod.UID}))
			_, err = kl.configMapManager.GetConfigMap(pod.Namespace, "configuration")
			require.ErrorContains(t, err, "not registered")
			_, err = kl.secretManager.GetSecret(pod.Namespace, "credential")
			require.ErrorContains(t, err, "not registered")
		})
	}
}

func TestDeferredAdmissionPublicationRetainsOwnershipDuringEvaluation(t *testing.T) {
	for _, outcome := range []string{"admit", "reject", "defer-expiry", "defer-cancel"} {
		t.Run(outcome, func(t *testing.T) {
			tCtx := ktesting.Init(t)
			tk, h := newDeferredAdmissionKubelet(t)
			defer tk.Cleanup()
			kl := tk.kubelet
			kl.configMapManager = configmap.NewCachingConfigMapManager(tk.fakeKubeClient, cachemanager.GetObjectTTLFromNodeFunc(kl.GetNode))
			pod := newDeferTestPod("publication-" + outcome)
			mode := int32(0644)
			pod.Spec.Volumes = []v1.Volume{{Name: "config", VolumeSource: v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: "publication"}, DefaultMode: &mode}}}}
			pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{{Name: "config", MountPath: "/configuration"}}
			_, err := tk.fakeKubeClient.CoreV1().ConfigMaps(pod.Namespace).Create(tCtx, &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "publication", Namespace: pod.Namespace}, Data: map[string]string{"key": "published-value"}}, metav1.CreateOptions{})
			require.NoError(t, err)
			plugins := append(volumeconfigmap.ProbeVolumePlugins(), emptydir.ProbeVolumePlugins()...)
			kl.volumePluginMgr, err = NewInitializedVolumePluginMgr(kl, kl.secretManager, kl.configMapManager, token.NewManager(kl.kubeClient), &clustertrustbundle.NoopManager{}, plugins, nil)
			require.NoError(t, err)
			workers, _, _ := createPodWorkers(klog.FromContext(tCtx))
			workers.allocationManager = kl.allocationManager
			workers.podsSynced = true
			kl.podWorkers = workers
			published, entered, release, finished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
			kl.podManager = &deferredResourcePodManager{Manager: kl.podManager, afterPublish: func(*v1.Pod) { close(published) }}
			ctx, cancel := context.WithTimeout(tCtx, 5*time.Second)
			defer cancel()
			h.admit = outcome == "admit"
			h.deferResult = outcome == "defer-expiry" || outcome == "defer-cancel"
			h.onAdmit = func(_ *lifecycle.PodAdmitAttributes) {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			}
			go func() { defer close(finished); kl.HandlePodAdditions(ctx, []*v1.Pod{pod}) }()
			var releaseOnce sync.Once
			unhold := func() { releaseOnce.Do(func() { close(release) }) }
			defer func() { unhold(); <-finished }()
			select {
			case <-published:
			case <-ctx.Done():
				t.Fatal("desired Pod was not published")
			}
			select {
			case <-entered:
			case <-ctx.Done():
				t.Fatal("admission handler did not enter")
			}

			// The actual desired UID is visible while its handler is still evaluating,
			// after the first worker sync and before any device-deferral timestamp.
			_, found := kl.podManager.GetPodByUID(pod.UID)
			require.True(t, found)
			require.True(t, kl.allocationManager.IsPodAdmissionPending(pod.UID))
			require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
			require.NotContains(t, kl.allocationManager.SnapshotDeferredAdmissions(), pod.UID)
			require.Empty(t, workers.podSyncStatuses)
			require.True(t, workers.CouldHaveRunningContainers(pod.UID))
			require.False(t, workers.ShouldPodBeFinished(pod.UID))
			require.False(t, workers.ShouldPodRuntimeBeRemoved(pod.UID))
			require.False(t, workers.ShouldPodContentBeRemoved(pod.UID))
			require.False(t, workers.ShouldPodContainersBeTerminating(pod.UID))
			_, err = kl.configMapManager.GetConfigMap(pod.Namespace, "publication")
			require.NoError(t, err)
			plugin, err := kl.volumePluginMgr.FindPluginBySpec(volume.NewSpecFromVolume(&pod.Spec.Volumes[0]))
			require.NoError(t, err)
			mounter, err := plugin.NewMounter(volume.NewSpecFromVolume(&pod.Spec.Volumes[0]), pod)
			require.NoError(t, err)
			require.NoError(t, mounter.SetUp(volume.MounterArgs{}))
			unmounter, err := plugin.NewUnmounter("config", pod.UID)
			require.NoError(t, err)
			defer func() { require.NoError(t, unmounter.TearDown()) }()
			translator := csitrans.New()
			dsw := volumecache.NewDesiredStateOfWorld(kl.volumePluginMgr, volumeutil.NewSELinuxLabelTranslator())
			asw := volumecache.NewActualStateOfWorld(kl.nodeName, kl.volumePluginMgr)
			pop := populator.NewDesiredStateOfWorldPopulator(tk.fakeKubeClient, time.Millisecond, kl.podManager, workers, dsw, asw, csimigration.NewPluginManager(translator), translator, kl.volumePluginMgr)
			popCtx, stopPop := context.WithCancel(ctx)
			popDone := make(chan struct{})
			go func() { defer close(popDone); pop.Run(popCtx, kl.sourcesReady) }()
			defer func() { stopPop(); <-popDone }()
			require.NoError(t, wait.PollUntilContextTimeout(ctx, time.Millisecond, time.Second, true, func(context.Context) (bool, error) {
				return pop.HasAddedPods() && len(dsw.GetVolumesToMount()) == 1, nil
			}))
			unhold()
			<-finished
			drainAllWorkers(workers)
			if outcome == "defer-expiry" || outcome == "defer-cancel" {
				require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
				if outcome == "defer-expiry" {
					stepClock(t, kl, deferredAdmissionTimeout)
					kl.retryDeferredAdmissions(tCtx)
				} else {
					pod = pod.DeepCopy()
					deleted := metav1.NewTime(kl.clock.Now())
					pod.DeletionTimestamp = &deleted
					kl.HandlePodUpdates(tCtx, []*v1.Pod{pod})
					drainAllWorkers(workers)
				}
			}
			require.False(t, kl.allocationManager.IsPodAdmissionPending(pod.UID))
			if outcome == "admit" {
				require.False(t, workers.ShouldPodRuntimeBeRemoved(pod.UID), "the real worker succeeds pending ownership")
			} else {
				require.True(t, workers.ShouldPodRuntimeBeRemoved(pod.UID))
				if outcome != "defer-cancel" {
					checkPodStatus(t, kl, pod, v1.PodFailed)
				}
			}
		})
	}
}

type deferredTrackingConfigMapManager struct {
	configmap.Manager
	registered map[types.UID]bool
}

func (m *deferredTrackingConfigMapManager) RegisterPod(pod *v1.Pod) {
	m.registered[pod.UID] = true
	m.Manager.RegisterPod(pod)
}

type deferredTrackingSecretManager struct {
	secret.Manager
	registered map[types.UID]bool
}

func (m *deferredTrackingSecretManager) RegisterPod(pod *v1.Pod) {
	m.registered[pod.UID] = true
	m.Manager.RegisterPod(pod)
}

func TestDeferredAdmissionMirrorNotificationsDoNotOwnReferencesOrDispatch(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	cm := &deferredTrackingConfigMapManager{Manager: configmap.NewCachingConfigMapManager(tk.fakeKubeClient, cachemanager.GetObjectTTLFromNodeFunc(kl.GetNode)), registered: map[types.UID]bool{}}
	sm := &deferredTrackingSecretManager{Manager: secret.NewCachingSecretManager(tk.fakeKubeClient, cachemanager.GetObjectTTLFromNodeFunc(kl.GetNode)), registered: map[types.UID]bool{}}
	kl.configMapManager, kl.secretManager = cm, sm
	dispatched := recordDispatches(kl)
	h.deferResult = true
	actual := newDeferTestPod("static-waiting")
	actual.Annotations = map[string]string{kubetypes.ConfigSourceAnnotationKey: kubetypes.FileSource}
	kl.HandlePodAdditions(tCtx, []*v1.Pod{actual})
	first := kl.allocationManager.SnapshotDeferredAdmissions()[actual.UID]
	for _, uid := range []types.UID{"mirror-1", "mirror-2"} {
		mirror := actual.DeepCopy()
		mirror.UID = uid
		mirror.Annotations = map[string]string{kubetypes.ConfigSourceAnnotationKey: kubetypes.ApiserverSource, kubetypes.ConfigMirrorAnnotationKey: "static-hash"}
		// A legal empty-reference mirror still creates registeredPods state if
		// RegisterPod is called, and has no worker to release that mirror UID.
		kl.HandlePodAdditions(tCtx, []*v1.Pod{mirror})
		kl.HandlePodUpdates(tCtx, []*v1.Pod{mirror})
		kl.HandlePodReconcile(tCtx, []*v1.Pod{mirror})
		kl.HandlePodRemoves(tCtx, []*v1.Pod{mirror})
		require.NotContains(t, cm.registered, uid)
		require.NotContains(t, sm.registered, uid)
		require.False(t, kl.allocationManager.IsPodAdmissionPending(uid))
		require.NotContains(t, dispatched, actual.UID, "mirror notifications must not bypass actual Pod admission")
		require.True(t, kl.allocationManager.IsPodAdmissionDeferred(actual.UID))
		require.Equal(t, first, kl.allocationManager.SnapshotDeferredAdmissions()[actual.UID])
	}
	require.Equal(t, map[types.UID]bool{actual.UID: true}, cm.registered)
	require.Equal(t, map[types.UID]bool{actual.UID: true}, sm.registered)
}
