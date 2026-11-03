//go:build linux

/*
Copyright 2015 The Kubernetes Authors.

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

package cm

import (
	"errors"
	"os"
	"path"
	"testing"
	"time"

	cadvisorapi "github.com/google/cadvisor/info/v1"
	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	"github.com/opencontainers/cgroups"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/ktesting"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	cadvisortest "k8s.io/kubernetes/pkg/kubelet/cadvisor/testing"
	"k8s.io/kubernetes/pkg/kubelet/cm/cpumanager"
	cmqos "k8s.io/kubernetes/pkg/kubelet/cm/qos"
	evictionapi "k8s.io/kubernetes/pkg/kubelet/eviction/api"
	kubetypes "k8s.io/kubernetes/pkg/kubelet/types"
	"k8s.io/mount-utils"
	"k8s.io/utils/cpuset"
)

type mockCPUAllocationReader struct {
	cpumanager.Manager
	sets            map[string]cpuset.CPUSet
	isolationLevels map[string]cmqos.ResourceIsolationLevel
}

func (m *mockCPUAllocationReader) GetExclusiveCPUs(podUID, containerName string) cpuset.CPUSet {
	key := podUID + "/" + containerName
	if cset, ok := m.sets[key]; ok {
		return cset
	}
	return cpuset.New()
}

func (m *mockCPUAllocationReader) GetResourceIsolationLevel(pod *v1.Pod, container *v1.Container) cmqos.ResourceIsolationLevel {
	key := string(pod.UID) + "/" + container.Name
	if level, ok := m.isolationLevels[key]; ok {
		return level
	}
	// Fallback to behavior based on sets existence if isolation level not explicitly set,
	// to maintain compatibility with other tests or provide a reasonable default.
	// For TestContainerHasExclusiveCPUs, we set isolationLevels explicitly.
	if _, ok := m.sets[key]; ok {
		return cmqos.ResourceIsolationContainer
	}
	return cmqos.ResourceIsolationHost
}

func fakeContainerMgrMountInt() mount.Interface {
	return mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "memory"},
			},
		})
}

func TestCgroupMountValidationSuccess(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	f, err := validateSystemRequirements(logger, fakeContainerMgrMountInt())
	assert.NoError(t, err)
	if cgroups.IsCgroup2UnifiedMode() {
		assert.True(t, f.cpuHardcapping, "cpu hardcapping is expected to be enabled")
	} else {
		assert.False(t, f.cpuHardcapping, "cpu hardcapping is expected to be disabled")
	}
}

func TestCgroupMountValidationMemoryMissing(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
		})
	_, err := validateSystemRequirements(logger, mountInt)
	assert.Error(t, err)
}

func TestCgroupMountValidationMultipleSubsystem(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset", "memory"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct"},
			},
		})
	_, err := validateSystemRequirements(logger, mountInt)
	assert.NoError(t, err)
}

func TestGetCpuWeight(t *testing.T) {
	assert.Equal(t, uint64(0), getCPUWeight(nil))

	v := uint64(2)
	assert.Equal(t, uint64(1), getCPUWeight(&v))

	v = uint64(262144)
	assert.Equal(t, uint64(10000), getCPUWeight(&v))

	v = uint64(1000000000)
	assert.Equal(t, uint64(10000), getCPUWeight(&v))
}

func TestSoftRequirementsValidationSuccess(t *testing.T) {
	if cgroups.IsCgroup2UnifiedMode() {
		t.Skip("skipping cgroup v1 test on a cgroup v2 system")
	}
	logger, _ := ktesting.NewTestContext(t)
	req := require.New(t)
	tempDir, err := os.MkdirTemp("", "")
	req.NoError(err)
	defer os.RemoveAll(tempDir)
	req.NoError(os.WriteFile(path.Join(tempDir, "cpu.cfs_period_us"), []byte("0"), os.ModePerm))
	req.NoError(os.WriteFile(path.Join(tempDir, "cpu.cfs_quota_us"), []byte("0"), os.ModePerm))
	mountInt := mount.NewFakeMounter(
		[]mount.MountPoint{
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuset"},
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpu"},
				Path:   tempDir,
			},
			{
				Device: "cgroup",
				Type:   "cgroup",
				Opts:   []string{"rw", "relatime", "cpuacct", "memory"},
			},
		})
	f, err := validateSystemRequirements(logger, mountInt)
	assert.NoError(t, err)
	assert.True(t, f.cpuHardcapping, "cpu hardcapping is expected to be enabled")
}

func TestGetCapacity(t *testing.T) {
	ephemeralStorageFromCapacity := int64(2000)
	ephemeralStorageFromCadvisor := int64(8000)

	mockCadvisor := cadvisortest.NewMockInterface(t)
	rootfs := cadvisorapiv2.FsInfo{
		Capacity: 8000,
	}
	mockCadvisor.EXPECT().RootFsInfo().Return(rootfs, nil)
	mockCadvisorError := cadvisortest.NewMockInterface(t)
	mockCadvisorError.EXPECT().RootFsInfo().Return(cadvisorapiv2.FsInfo{}, errors.New("Unable to get rootfs data from cAdvisor interface"))
	cases := []struct {
		name                                 string
		cm                                   *containerManagerImpl
		expectedResourceQuantity             *resource.Quantity
		expectedNoEphemeralStorage           bool
		disablelocalStorageCapacityIsolation bool
	}{
		{
			name: "capacity property has ephemeral-storage",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
				},
			},
			expectedResourceQuantity:   resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
			expectedNoEphemeralStorage: false,
		},
		{
			name: "capacity property does not have ephemeral-storage",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity:          v1.ResourceList{},
			},
			expectedResourceQuantity:   resource.NewQuantity(ephemeralStorageFromCadvisor, resource.BinarySI),
			expectedNoEphemeralStorage: false,
		},
		{
			name: "capacity property does not have ephemeral-storage, error from rootfs",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisorError,
				capacity:          v1.ResourceList{},
			},
			expectedNoEphemeralStorage: true,
		},
		{
			name: "capacity property does not have ephemeral-storage, cadvisor interface is nil",
			cm: &containerManagerImpl{
				cadvisorInterface: nil,
				capacity:          v1.ResourceList{},
			},
			expectedNoEphemeralStorage: true,
		},
		{
			name: "capacity property has ephemeral-storage, but localStorageCapacityIsolation is disabled",
			cm: &containerManagerImpl{
				cadvisorInterface: mockCadvisor,
				capacity: v1.ResourceList{
					v1.ResourceEphemeralStorage: *resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
				},
			},
			expectedResourceQuantity:             resource.NewQuantity(ephemeralStorageFromCapacity, resource.BinarySI),
			expectedNoEphemeralStorage:           true,
			disablelocalStorageCapacityIsolation: true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ret := c.cm.GetCapacity(!c.disablelocalStorageCapacityIsolation)
			if v, exists := ret[v1.ResourceEphemeralStorage]; !exists {
				if !c.expectedNoEphemeralStorage {
					t.Errorf("did not get any ephemeral storage data")
				}
			} else {
				if v.Value() != c.expectedResourceQuantity.Value() {
					t.Errorf("got unexpected %s value, expected %d, got %d", v1.ResourceEphemeralStorage, c.expectedResourceQuantity.Value(), v.Value())
				}
			}
		})
	}
}

func TestNewPodContainerManager(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	info := QOSContainersInfo{
		Guaranteed: CgroupName{"guaranteed"},
		BestEffort: CgroupName{"besteffort"},
		Burstable:  CgroupName{"burstable"},
	}
	QosEnabled := NodeConfig{
		CgroupsPerQOS: true,
	}
	QosDisabled := NodeConfig{
		CgroupsPerQOS: false,
	}

	cases := []struct {
		name string
		cm   *containerManagerImpl
	}{
		{
			name: "CgroupsPerQOS is disabled, return *podContainerManagerNoop",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, ""),
				},

				NodeConfig: QosDisabled,
			},
		},
		{
			name: "CgroupsPerQOS is enabled, return *podContainerManagerImpl",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, ""),
				},

				NodeConfig: QosEnabled,
			},
		},
		{
			name: "CgroupsPerQOS is enabled, use systemd",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, "systemd"),
				},

				NodeConfig: QosEnabled,
			},
		},
		{
			name: "CgroupsPerQOS is disabled, use systemd",
			cm: &containerManagerImpl{
				qosContainerManager: &qosContainerManagerImpl{
					qosContainersInfo: info,
					cgroupManager:     NewCgroupManager(logger, &CgroupSubsystems{}, "systemd"),
				},

				NodeConfig: QosDisabled,
			},
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			pcm := c.cm.NewPodContainerManager()
			if c.cm.NodeConfig.CgroupsPerQOS {
				assert.IsType(t, &podContainerManagerImpl{}, pcm)
				got := pcm.(*podContainerManagerImpl)
				assert.Equal(t, c.cm.subsystems, got.subsystems)
				assert.Equal(t, c.cm.cgroupManager, got.cgroupManager)
				assert.Equal(t, c.cm.PodPidsLimit, got.podPidsLimit)
				assert.Equal(t, c.cm.EnforceCPULimits, got.enforceCPULimits)
				assert.Equal(t, uint64(c.cm.CPUCFSQuotaPeriod/time.Microsecond), got.cpuCFSQuotaPeriod)

			} else {
				assert.IsType(t, &podContainerManagerNoop{}, pcm)
				got := pcm.(*podContainerManagerNoop)
				assert.Equal(t, c.cm.cgroupRoot, got.cgroupRoot)
			}
		})
	}
}

func TestContainerHasExclusiveCPUs(t *testing.T) {
	guaranteedQOSResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	burstableQOSResources := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("1"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}
	guaranteedQOSResourcesNonIntegerCPUNotExclusive := v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
		Limits: v1.ResourceList{
			v1.ResourceCPU:    resource.MustParse("500m"),
			v1.ResourceMemory: resource.MustParse("100Mi"),
		},
	}

	testCases := []struct {
		name                            string
		pod                             *v1.Pod
		containerName                   string
		cpuSets                         map[string]cpuset.CPUSet
		isolationLevels                 map[string]cmqos.ResourceIsolationLevel
		expectExclusiveCPUs             bool
		podLevelResourceManagersEnabled bool
	}{
		{
			name: "No pod-level resources, Guaranteed container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationContainer},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, no exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, non-integer CPU",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResourcesNonIntegerCPUNotExclusive}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Burstable container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: burstableQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, BestEffort container, has exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1"}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{"pod1/c1": cpuset.New(1)},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "Pod-level resources, Guaranteed container, Integer CPUs, no exclusive CPUs assigned",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1", Resources: guaranteedQOSResources}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
		{
			name: "Pod-level resources, BestEffort container",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources:  &guaranteedQOSResources,
					Containers: []v1.Container{{Name: "c1"}},
				},
			},
			containerName:                   "c1",
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourceManagersEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			mockReader := &mockCPUAllocationReader{
				sets:            tc.cpuSets,
				isolationLevels: tc.isolationLevels,
			}
			cm := &containerManagerImpl{
				cpuManager: mockReader,
			}

			var targetContainer *v1.Container
			for i := range tc.pod.Spec.Containers {
				if tc.pod.Spec.Containers[i].Name == tc.containerName {
					targetContainer = &tc.pod.Spec.Containers[i]
					break
				}
			}
			require.NotNil(t, targetContainer, "container %s not found in pod spec", tc.containerName)

			result := cm.ContainerHasExclusiveCPUs(tc.pod, targetContainer)
			assert.Equal(t, tc.expectExclusiveCPUs, result)
		})
	}
}

func TestPodHasExclusiveCPUs(t *testing.T) {
	testCases := []struct {
		name                            string
		pod                             *v1.Pod
		cpuSets                         map[string]cpuset.CPUSet
		isolationLevels                 map[string]cmqos.ResourceIsolationLevel
		expectExclusiveCPUs             bool
		podLevelResourceManagersEnabled bool
	}{
		{
			name: "No exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets:             map[string]cpuset.CPUSet{},
			expectExclusiveCPUs: false,
		},
		{
			name: "One container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			expectExclusiveCPUs: true,
		},
		{
			name: "Init container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					InitContainers: []v1.Container{
						{Name: "init1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/init1": cpuset.New(1),
			},
			expectExclusiveCPUs: true,
		},
		{
			name: "PodLevelResourceManagers enabled, One container with exclusive CPUs",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationContainer},
			expectExclusiveCPUs:             true,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers enabled, One container in pod shared pool (ResourceIsolationPod)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
			},
			isolationLevels:                 map[string]cmqos.ResourceIsolationLevel{"pod1/c1": cmqos.ResourceIsolationPod},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers enabled, Mixed containers (one exclusive, one shared)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("2")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
						{Name: "c2"},
					},
				},
			},
			cpuSets: map[string]cpuset.CPUSet{
				"pod1/c1": cpuset.New(1),
				"pod1/c2": cpuset.New(2),
			},
			isolationLevels: map[string]cmqos.ResourceIsolationLevel{
				"pod1/c1": cmqos.ResourceIsolationContainer,
				"pod1/c2": cmqos.ResourceIsolationPod,
			},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: true,
		},
		{
			name: "PodLevelResourceManagers disabled, Pod Level Resources Only (No Exclusive CPUs)",
			pod: &v1.Pod{
				ObjectMeta: metav1.ObjectMeta{UID: "pod1"},
				Spec: v1.PodSpec{
					Resources: &v1.ResourceRequirements{
						Requests: v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
						Limits:   v1.ResourceList{v1.ResourceCPU: resource.MustParse("1")},
					},
					Containers: []v1.Container{
						{Name: "c1"},
					},
				},
			},
			cpuSets:                         map[string]cpuset.CPUSet{},
			expectExclusiveCPUs:             false,
			podLevelResourceManagersEnabled: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResources, tc.podLevelResourceManagersEnabled)
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, kubefeatures.PodLevelResourceManagers, tc.podLevelResourceManagersEnabled)

			mockReader := &mockCPUAllocationReader{
				sets:            tc.cpuSets,
				isolationLevels: tc.isolationLevels,
			}
			cm := &containerManagerImpl{
				cpuManager: mockReader,
			}

			result := cm.PodHasExclusiveCPUs(tc.pod)
			assert.Equal(t, tc.expectExclusiveCPUs, result)
		})
	}
}

type memoryCapacityCgroupManager struct {
	CgroupManager
	limits []int64
	fail   bool
}

func (m *memoryCapacityCgroupManager) Update(_ klog.Logger, config *CgroupConfig) error {
	m.limits = append(m.limits, *config.ResourceParameters.Memory)
	if m.fail {
		m.fail = false
		return errors.New("memory limit cannot be applied")
	}
	return nil
}

type memoryCapacityCPUManager struct{ cpumanager.Manager }

func (*memoryCapacityCPUManager) GetAllocatableCPUs() cpuset.CPUSet { return cpuset.New() }
func (*memoryCapacityCPUManager) GetAllCPUs() cpuset.CPUSet         { return cpuset.New() }

func TestReconcileMemoryCapacity(t *testing.T) {
	for _, tc := range []struct {
		name       string
		policy     string
		current    uint64
		readError  bool
		writeError bool
		want       string
		wantError  bool
	}{
		{name: "grow", policy: "None", current: 24 << 30, want: "24Gi"},
		{name: "shrink", policy: "None", current: 2 << 30, want: "2Gi"},
		{name: "growth enforcement failure retains accepted capacity", policy: "None", current: 24 << 30, writeError: true, want: "4Gi", wantError: true},
		{name: "shrink enforcement failure still lowers admission", policy: "None", current: 2 << 30, writeError: true, want: "2Gi", wantError: true},
		{name: "provider error", policy: "None", readError: true, want: "4Gi", wantError: true},
		{name: "zero capacity", policy: "None", want: "4Gi", wantError: true},
		{name: "overflow", policy: "None", current: 1 << 63, want: "4Gi", wantError: true},
		{name: "static topology policy remains unchanged", policy: "Static", current: 24 << 30, want: "4Gi"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			logger, _ := ktesting.NewTestContext(t)
			provider := cadvisortest.NewMockInterface(t)
			if tc.policy == "None" {
				var err error
				if tc.readError {
					err = errors.New("machine info unavailable")
				}
				provider.EXPECT().MachineInfo().Return(&cadvisorapi.MachineInfo{MemoryCapacity: tc.current, NumCores: 128}, err).Once()
			}
			groups := &memoryCapacityCgroupManager{fail: tc.writeError}
			original := v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi"), v1.ResourceCPU: resource.MustParse("4"), v1.ResourceName("hugepages-2Mi"): resource.MustParse("8Mi")}
			cm := &containerManagerImpl{NodeConfig: NodeConfig{MemoryManagerPolicy: tc.policy}, cadvisorInterface: provider, capacity: original.DeepCopy(), internalCapacity: original.DeepCopy(), cgroupRoot: NewCgroupName(RootCgroupName, "kubepods"), cgroupManager: groups}
			err := cm.reconcileMemoryCapacity(logger)
			assert.Equal(t, tc.wantError, err != nil)
			want := resource.MustParse(tc.want)
			assert.True(t, cm.capacity[v1.ResourceMemory].Equal(want))
			assert.True(t, cm.internalCapacity[v1.ResourceMemory].Equal(want))
			assert.True(t, cm.GetNodeAllocatableAbsolute()[v1.ResourceMemory].Equal(want))
			assert.True(t, cm.capacity[v1.ResourceCPU].Equal(original[v1.ResourceCPU]))
			assert.True(t, cm.capacity[v1.ResourceName("hugepages-2Mi")].Equal(original[v1.ResourceName("hugepages-2Mi")]))
			copy := cm.GetCapacity(false)
			copy[v1.ResourceMemory] = resource.MustParse("1Mi")
			assert.True(t, cm.GetCapacity(false)[v1.ResourceMemory].Equal(want), "readers must not mutate accepted capacity")
		})
	}
}

func TestNodeAllocatableRetryUsesReconciledMemory(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	provider := cadvisortest.NewMockInterface(t)
	provider.EXPECT().MachineInfo().Return(&cadvisorapi.MachineInfo{MemoryCapacity: 24 << 30}, nil).Once()
	groups := &memoryCapacityCgroupManager{fail: true}
	capacity := v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}
	cm := &containerManagerImpl{NodeConfig: NodeConfig{MemoryManagerPolicy: "None"}, cadvisorInterface: provider, capacity: capacity.DeepCopy(), internalCapacity: capacity.DeepCopy(), cgroupRoot: NewCgroupName(RootCgroupName, "kubepods"), cgroupManager: groups, cpuManager: &memoryCapacityCPUManager{}}
	require.Error(t, cm.updateNodeAllocatableCgroup(logger))
	require.NoError(t, cm.reconcileMemoryCapacity(logger))
	require.NoError(t, cm.updateNodeAllocatableCgroup(logger))
	assert.Equal(t, []int64{4 << 30, 24 << 30, 24 << 30}, groups.limits, "startup retry must not restore the captured boot limit")
}

func TestReconciledMemoryReservationAndEnforcement(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	provider := cadvisortest.NewMockInterface(t)
	provider.EXPECT().MachineInfo().Return(&cadvisorapi.MachineInfo{MemoryCapacity: 24 << 30}, nil).Once()
	groups := &memoryCapacityCgroupManager{}
	capacity := v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}
	cm := &containerManagerImpl{
		NodeConfig: NodeConfig{MemoryManagerPolicy: "None", CgroupsPerQOS: true, NodeAllocatableConfig: NodeAllocatableConfig{
			SystemReserved:         v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
			KubeReserved:           v1.ResourceList{v1.ResourceMemory: resource.MustParse("1Gi")},
			EnforceNodeAllocatable: sets.New(kubetypes.NodeAllocatableEnforcementKey),
			HardEvictionThresholds: []evictionapi.Threshold{{Signal: evictionapi.SignalMemoryAvailable, Value: evictionapi.ThresholdValue{Percentage: 0.25}}},
		}},
		cadvisorInterface: provider, capacity: capacity.DeepCopy(), internalCapacity: capacity.DeepCopy(), cgroupRoot: NewCgroupName(RootCgroupName, "kubepods"), cgroupManager: groups,
	}
	require.NoError(t, cm.reconcileMemoryCapacity(logger))
	assert.Equal(t, []int64{22 << 30}, groups.limits)
	assert.True(t, cm.GetNodeAllocatableAbsolute()[v1.ResourceMemory].Equal(resource.MustParse("22Gi")))
	assert.True(t, cm.GetNodeAllocatableReservation(nil)[v1.ResourceMemory].Equal(resource.MustParse("8Gi")), "percentage eviction reservation must use accepted memory")
}

func TestMemoryCapacityRetriesFailedEnforcement(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	provider := cadvisortest.NewMockInterface(t)
	provider.EXPECT().MachineInfo().Return(&cadvisorapi.MachineInfo{MemoryCapacity: 24 << 30}, nil).Twice()
	groups := &memoryCapacityCgroupManager{fail: true}
	capacity := v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}
	cm := &containerManagerImpl{NodeConfig: NodeConfig{MemoryManagerPolicy: "None"}, cadvisorInterface: provider, capacity: capacity.DeepCopy(), internalCapacity: capacity.DeepCopy(), cgroupRoot: NewCgroupName(RootCgroupName, "kubepods"), cgroupManager: groups}
	require.Error(t, cm.reconcileMemoryCapacity(logger))
	assert.True(t, cm.GetCapacity(false)[v1.ResourceMemory].Equal(resource.MustParse("4Gi")))
	require.NoError(t, cm.reconcileMemoryCapacity(logger))
	assert.True(t, cm.GetCapacity(false)[v1.ResourceMemory].Equal(resource.MustParse("24Gi")))
	assert.Equal(t, []int64{24 << 30, 24 << 30}, groups.limits)
}

func TestMemoryReservationUsesProvidedSnapshot(t *testing.T) {
	cm := &containerManagerImpl{NodeConfig: NodeConfig{NodeAllocatableConfig: NodeAllocatableConfig{HardEvictionThresholds: []evictionapi.Threshold{{Signal: evictionapi.SignalMemoryAvailable, Value: evictionapi.ThresholdValue{Percentage: 0.25}}}}}, capacity: v1.ResourceList{v1.ResourceMemory: resource.MustParse("24Gi")}}
	previous := v1.ResourceList{v1.ResourceMemory: resource.MustParse("4Gi")}
	assert.True(t, cm.GetNodeAllocatableReservation(previous)[v1.ResourceMemory].Equal(resource.MustParse("1Gi")))
	assert.True(t, cm.GetNodeAllocatableReservation(nil)[v1.ResourceMemory].Equal(resource.MustParse("6Gi")))
	cm.capacity = v1.ResourceList{v1.ResourceMemory: resource.MustParse("2Gi")}
	assert.True(t, cm.GetNodeAllocatableReservation(previous)[v1.ResourceMemory].Equal(resource.MustParse("1Gi")), "shrink cannot mix the projected capacity with a newer reservation")
}
