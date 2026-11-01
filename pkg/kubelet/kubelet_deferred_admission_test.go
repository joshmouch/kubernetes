/*
Copyright The Kubernetes Authors.

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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/version"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/kubelet/config"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/pkg/kubelet/lifecycle"
	"k8s.io/kubernetes/pkg/kubelet/prober"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/kubernetes/test/utils/ktesting"
	testingclock "k8s.io/utils/clock/testing"
)

// deferAdmitHandler is a controllable admit handler used to drive deferred
// admission tests. When admit is true it admits; otherwise it returns the
// configured defer/reason/message.
type deferAdmitHandler struct {
	calls       int
	onAdmit     func(*lifecycle.PodAdmitAttributes)
	admit       bool
	deferResult bool
	reason      string
	message     string
}

func (h *deferAdmitHandler) Admit(attrs *lifecycle.PodAdmitAttributes) lifecycle.PodAdmitResult {
	h.calls++
	if h.onAdmit != nil {
		h.onAdmit(attrs)
	}
	if h.admit {
		return lifecycle.PodAdmitResult{Admit: true}
	}
	return lifecycle.PodAdmitResult{
		Admit:   false,
		Defer:   h.deferResult,
		Reason:  h.reason,
		Message: h.message,
	}
}

// newDeferredAdmissionKubelet returns a test kubelet whose admission is driven
// by the returned controllable handler. The node is configured so benign pods
// pass the default predicate handlers, leaving admission decisions to the
// returned handler.
func newDeferredAdmissionKubelet(t *testing.T) (*TestKubelet, *deferAdmitHandler) {
	t.Helper()
	testKubelet := newTestKubelet(t, false /* controllerAttachDetachEnabled */)
	kl := testKubelet.kubelet
	kl.nodeLister = testNodeLister{nodes: []*v1.Node{
		{
			ObjectMeta: metav1.ObjectMeta{Name: string(kl.nodeName)},
			Status: v1.NodeStatus{
				Allocatable: v1.ResourceList{
					v1.ResourcePods: *resource.NewQuantity(110, resource.DecimalSI),
				},
			},
		},
	}}
	h := &deferAdmitHandler{reason: "DeviceNotReady", message: "device not ready"}
	kl.allocationManager.AddPodAdmitHandlers(lifecycle.PodAdmitHandlers{h})
	return testKubelet, h
}

func newDeferTestPod(uid string) *v1.Pod {
	return podWithUIDNameNsSpec(types.UID(uid), "pod-"+uid, "ns", v1.PodSpec{
		NodeName:   string(testKubeletHostname),
		Containers: []v1.Container{{Name: "c1"}},
	})
}

// recordDispatches installs a syncPodFn on the fake pod workers that records the
// update type seen per pod UID, so tests can assert how a pod was dispatched.
func recordDispatches(kl *Kubelet) map[types.UID]kubetypes.SyncPodType {
	dispatched := map[types.UID]kubetypes.SyncPodType{}
	kl.podWorkers.(*fakePodWorkers).syncPodFn = func(_ context.Context, updateType kubetypes.SyncPodType, pod, _ *v1.Pod, _ *kubecontainer.PodStatus) (bool, func(), error) {
		if pod != nil {
			dispatched[pod.UID] = updateType
		}
		return false, nil, nil
	}
	return dispatched
}

func stepClock(t *testing.T, kl *Kubelet, d time.Duration) {
	t.Helper()
	fc, ok := kl.clock.(*testingclock.FakeClock)
	require.True(t, ok, "kubelet clock must be a fake clock for deferred admission tests")
	fc.Step(d)
}

// A pod whose admission is deferred is tracked and kept Pending (not rejected).
func TestDeferredAdmissionTracksDeferredPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-1")

	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})

	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "deferred pod should be tracked")
	require.NotContains(t, dispatched, pod.UID, "deferred pod must not be dispatched to the pod workers")
	if status, found := kl.statusManager.GetPodStatus(pod.UID); found {
		require.NotEqual(t, v1.PodFailed, status.Phase, "deferred pod must not be failed")
	}
}

// A previously deferred pod is dispatched with SyncPodCreate once admission succeeds.
func TestDeferredAdmissionRetrySucceeds(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-2")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	// Device becomes available.
	h.admit = true
	stepClock(t, kl, 10*time.Second)
	kl.retryDeferredAdmissions(tCtx)

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "admitted pod should be removed from the deferred map")
	require.Equal(t, kubetypes.SyncPodCreate, dispatched[pod.UID], "admitted pod should be dispatched with SyncPodCreate")
	if status, found := kl.statusManager.GetPodStatus(pod.UID); found {
		require.NotEqual(t, v1.PodFailed, status.Phase, "admitted pod must not be failed")
	}
}

// A deferred pod still within the timeout keeps being deferred.
func TestDeferredAdmissionStillDeferredWithinTimeout(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-3")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	stepClock(t, kl, deferredAdmissionTimeout/2)
	kl.retryDeferredAdmissions(tCtx)

	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "pod within the timeout should still be deferred")
	if status, found := kl.statusManager.GetPodStatus(pod.UID); found {
		require.NotEqual(t, v1.PodFailed, status.Phase, "pod within timeout must not be failed")
	}
}

// A deferred pod past the timeout is permanently rejected, preserving the
// deadline reason, without performing admission after expiry.
func TestDeferredAdmissionTimesOut(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-4")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	// Advance past the deferral timeout; device is still unavailable.
	stepClock(t, kl, deferredAdmissionTimeout+time.Second)
	kl.retryDeferredAdmissions(tCtx)

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "timed-out pod should be removed from the deferred map")
	checkPodStatus(t, kl, pod, v1.PodFailed)
	status, _ := kl.statusManager.GetPodStatus(pod.UID)
	require.Equal(t, "DeferredAdmissionTimeout", status.Reason)
}

// A deferred pod that becomes termination-requested is dropped, not admitted.
func TestDeferredAdmissionTerminationRequestedNotAdmitted(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-5")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	// The pod is now requested for termination, and admission would otherwise succeed.
	kl.podWorkers.(*fakePodWorkers).terminationRequested = map[types.UID]bool{pod.UID: true}
	h.admit = true
	kl.retryDeferredAdmissions(tCtx)

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "terminating pod should be dropped from the deferred map")
	require.NotContains(t, dispatched, pod.UID, "terminating pod must not be admitted/dispatched")
	if status, found := kl.statusManager.GetPodStatus(pod.UID); found {
		require.NotEqual(t, v1.PodFailed, status.Phase, "terminating deferred pod must not be rejected")
	}
}

// A pod removed while deferred is cleaned up from the deferred map.
func TestDeferredAdmissionClearedOnRemove(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-6")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	kl.HandlePodRemoves(tCtx, []*v1.Pod{pod})

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "removed pod should be cleared from the deferred map")
}

// removeOrphanedDeferredAdmissions prunes deferred entries for pods no longer present.
func TestDeferredAdmissionOrphanCleanup(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("defer-7")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	// The remaining set does not contain the deferred pod.
	kl.allocationManager.RemoveOrphanedPods(sets.New[types.UID]("some-other-pod"))

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "orphaned deferred pod should be pruned")
}

// HandlePodSyncs skips dispatch for deferred pods.
func TestDeferredAdmissionSkippedByHandlePodSyncs(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("sync-skip-1")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	kl.HandlePodSyncs(tCtx, []*v1.Pod{pod})

	require.NotContains(t, dispatched, pod.UID, "HandlePodSyncs must not dispatch a deferred pod")
}

// HandlePodUpdates skips dispatch for deferred pods.
func TestDeferredAdmissionSkippedByHandlePodUpdates(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("update-skip-1")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	kl.HandlePodUpdates(tCtx, []*v1.Pod{pod})

	require.NotContains(t, dispatched, pod.UID, "HandlePodUpdates must not dispatch a deferred pod")
}

// HandlePodReconcile skips dispatch for deferred pods.
func TestDeferredAdmissionSkippedByHandlePodReconcile(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet
	dispatched := recordDispatches(kl)

	h.admit = false
	h.deferResult = true
	pod := newDeferTestPod("reconcile-skip-1")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))

	kl.HandlePodReconcile(tCtx, []*v1.Pod{pod})

	require.NotContains(t, dispatched, pod.UID, "HandlePodReconcile must not dispatch a deferred pod")
}

// A non-deferrable rejection in HandlePodAdditions fails the pod immediately and
// does not track it as deferred.
func TestDeferredAdmissionNonDeferrableRejection(t *testing.T) {
	tCtx := ktesting.Init(t)
	testKubelet, h := newDeferredAdmissionKubelet(t)
	defer testKubelet.Cleanup()
	kl := testKubelet.kubelet

	h.admit = false
	h.deferResult = false
	h.reason = "OutOfMemory"
	h.message = "node out of memory"
	pod := newDeferTestPod("reject-1")

	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})

	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "non-deferrable rejection must not be tracked")
	checkPodStatus(t, kl, pod, v1.PodFailed)
}

// probe cleanup consumes the same possibly-running population as cgroup cleanup.
type deferredProbeManager struct {
	prober.Manager
	retained map[types.UID]sets.Empty
}

func (m *deferredProbeManager) CleanupPods(pods map[types.UID]sets.Empty) {
	m.retained = pods
	m.Manager.CleanupPods(pods)
}

func TestDeferredAdmissionCleanupRetainsRuntimeUID(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	dispatched := recordDispatches(kl)
	h.deferResult = true
	pod := newDeferTestPod("restored-runtime")
	runtimePod := &kubecontainer.Pod{ID: pod.UID, Name: pod.Name, Namespace: pod.Namespace, Containers: []*kubecontainer.Container{{Name: "c1"}}}
	tk.fakeRuntime.PodList = []*containertest.FakePod{{Pod: runtimePod}}
	tk.fakeContainerManager.PodContainerManager.AddPodFromCgroups(runtimePod)
	kl.cgroupsPerQOS = true
	probes := &deferredProbeManager{Manager: kl.probeManager}
	kl.probeManager = probes
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})

	require.NoError(t, kl.HandlePodCleanups(tCtx))
	require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
	require.NotContains(t, dispatched, pod.UID)
	require.Empty(t, kl.podWorkers.(*fakePodWorkers).triggeredDeletion, "desired runtime is not an orphan")
	require.Contains(t, probes.retained, pod.UID, "probe and cgroup cleanup must retain deferred runtime ownership")
	tk.fakeContainerManager.PodContainerManager.Lock()
	cgroupCalls := append([]string(nil), tk.fakeContainerManager.PodContainerManager.CalledFunctions...)
	tk.fakeContainerManager.PodContainerManager.Unlock()
	require.NotContains(t, cgroupCalls, "Destroy")

	h.admit = true
	kl.retryDeferredAdmissions(tCtx)
	require.Equal(t, kubetypes.SyncPodCreate, dispatched[pod.UID])
	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
}

func TestDeferredAdmissionCleanupStillTerminatesRuntimeOrphans(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, _ := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	runtimePod := &kubecontainer.Pod{ID: "actual-orphan", Name: "orphan", Namespace: "ns"}
	tk.fakeRuntime.PodList = []*containertest.FakePod{{Pod: runtimePod}}
	require.NoError(t, tk.kubelet.HandlePodCleanups(tCtx))
	require.Contains(t, tk.kubelet.podWorkers.(*fakePodWorkers).triggeredDeletion, runtimePod.ID)
}

func TestDeferredAdmissionDeletionRetriesTerminationBeforeAdmission(t *testing.T) {
	featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.34"))
	for _, scaling := range []bool{false, true} {
		t.Run(map[bool]string{false: "scaling-off", true: "scaling-on"}[scaling], func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.InPlacePodVerticalScaling, scaling)
			tCtx := ktesting.Init(t)
			tk, h := newDeferredAdmissionKubelet(t)
			defer tk.Cleanup()
			kl := tk.kubelet
			dispatched := recordDispatches(kl)
			h.deferResult = true
			pod := newDeferTestPod("cancel-before-inventory")
			kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
			calls := h.calls
			ready := false
			kl.sourcesReady = config.NewSourcesReady(func(_ sets.Set[string]) bool { return ready })
			pod = pod.DeepCopy()
			deleted := metav1.NewTime(kl.clock.Now())
			pod.DeletionTimestamp = &deleted
			h.admit = true
			kl.HandlePodUpdates(tCtx, []*v1.Pod{pod})
			require.True(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID), "retain cancellation until termination has an owner")
			require.NotContains(t, dispatched, pod.UID)
			require.Empty(t, kl.podWorkers.(*fakePodWorkers).triggeredDeletion)
			ready = true
			kl.retryDeferredAdmissions(tCtx)
			require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
			require.Equal(t, calls, h.calls, "a deleting UID must not allocate even after inventory becomes ready")
			require.NotContains(t, dispatched, pod.UID)
			require.Contains(t, kl.podWorkers.(*fakePodWorkers).triggeredDeletion, pod.UID)
		})
	}
}

func TestDeferredAdmissionDeadlinePrecedesAllocationAndIsNotExtended(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	// A deliberately distant clock proves deferral does not use wall time.
	kl.clock = testingclock.NewFakeClock(time.Unix(1000, 0))
	dispatched := recordDispatches(kl)
	h.deferResult = true
	pod := newDeferTestPod("deadline")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	first := kl.allocationManager.SnapshotDeferredAdmissions()[pod.UID]
	require.Equal(t, kl.clock.Now(), first)
	stepClock(t, kl, deferredAdmissionTimeout/2)
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	kl.HandlePodUpdates(tCtx, []*v1.Pod{pod})
	require.Equal(t, first, kl.allocationManager.SnapshotDeferredAdmissions()[pod.UID])
	calls := h.calls
	h.admit = true
	stepClock(t, kl, deferredAdmissionTimeout/2)
	kl.retryDeferredAdmissions(tCtx)
	require.Equal(t, calls, h.calls, "deadline must be checked before an admission handler allocates")
	require.NotContains(t, dispatched, pod.UID)
	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
	checkPodStatus(t, kl, pod, v1.PodFailed)
	status, _ := kl.statusManager.GetPodStatus(pod.UID)
	require.Equal(t, "DeferredAdmissionTimeout", status.Reason)
}

func TestDeferredAdmissionTerminalPodCannotResume(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	dispatched := recordDispatches(kl)
	h.deferResult = true
	pod := newDeferTestPod("terminal")
	kl.HandlePodAdditions(tCtx, []*v1.Pod{pod})
	pod = pod.DeepCopy()
	pod.Status.Phase = v1.PodSucceeded
	kl.podManager.UpdatePod(pod)
	h.admit = true
	calls := h.calls
	kl.retryDeferredAdmissions(tCtx)
	require.Equal(t, calls, h.calls)
	require.NotContains(t, dispatched, pod.UID)
	require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
}

func TestDeferredAdmissionFirstTimestampIsPerPod(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	kl.clock = testingclock.NewFakeClock(time.Unix(1000, 0))
	h.deferResult = true
	first, later := newDeferTestPod("slow-first"), newDeferTestPod("later")
	first.CreationTimestamp = metav1.NewTime(time.Unix(1, 0))
	later.CreationTimestamp = metav1.NewTime(time.Unix(2, 0))
	h.onAdmit = func(attrs *lifecycle.PodAdmitAttributes) {
		if attrs.Pod.UID == first.UID {
			stepClock(t, kl, 2*deferredAdmissionTimeout)
		}
	}
	kl.HandlePodAdditions(tCtx, []*v1.Pod{first, later})
	stamp := kl.allocationManager.SnapshotDeferredAdmissions()[later.UID]
	require.Equal(t, kl.clock.Now(), stamp, "a slow earlier Pod cannot age a later first deferral")
	stepClock(t, kl, time.Second)
	kl.allocationManager.MarkPodAdmissionDeferred(later.UID, kl.clock.Now())
	require.Equal(t, stamp, kl.allocationManager.SnapshotDeferredAdmissions()[later.UID])
}

func TestDeferredAdmissionRetryObservesClockPerUID(t *testing.T) {
	tCtx := ktesting.Init(t)
	tk, h := newDeferredAdmissionKubelet(t)
	defer tk.Cleanup()
	kl := tk.kubelet
	kl.clock = testingclock.NewFakeClock(time.Unix(1000, 0))
	dispatched := recordDispatches(kl)
	h.deferResult = true
	pods := []*v1.Pod{newDeferTestPod("retry-a"), newDeferTestPod("retry-b"), newDeferTestPod("retry-c")}
	kl.HandlePodAdditions(tCtx, pods)
	stepClock(t, kl, deferredAdmissionTimeout/2)
	calls := h.calls
	h.admit = true
	h.onAdmit = func(_ *lifecycle.PodAdmitAttributes) { stepClock(t, kl, deferredAdmissionTimeout) }
	kl.retryDeferredAdmissions(tCtx)
	require.Equal(t, calls+1, h.calls, "regardless of map order, later expired UIDs cannot reach allocation")
	require.Len(t, dispatched, 1)
	for _, pod := range pods {
		require.False(t, kl.allocationManager.IsPodAdmissionDeferred(pod.UID))
		if _, started := dispatched[pod.UID]; !started {
			checkPodStatus(t, kl, pod, v1.PodFailed)
		}
	}
}
