/*
Copyright 2022 The Kubernetes Authors.

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

package v1beta1

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"k8s.io/klog/v2"
	api "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
	"k8s.io/kubernetes/test/utils/ktesting"
)

type lifecycleClient struct {
	socket       string
	run          func()
	disconnected atomic.Int32
}

func (c *lifecycleClient) API() api.DevicePluginClient   { return nil }
func (c *lifecycleClient) Resource() string              { return "example.com/device" }
func (c *lifecycleClient) SocketPath() string            { return c.socket }
func (c *lifecycleClient) Connect(context.Context) error { return nil }
func (c *lifecycleClient) Run(context.Context) {
	if c.run != nil {
		c.run()
	}
}
func (c *lifecycleClient) Disconnect(klog.Logger) error { c.disconnected.Add(1); return nil }

func TestClientReplacementRetainsSuccessor(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	old := &lifecycleClient{socket: "old.sock"}
	next := &lifecycleClient{socket: "new.sock"}
	s := &server{clients: make(map[string]Client)}
	s.registerClient(logger, old.Resource(), old)
	s.registerClient(logger, next.Resource(), next)
	require.EqualValues(t, 1, old.disconnected.Load())
	// The previous Run may finish after replacement. It must close itself,
	// never look up and close the current client for the same resource.
	s.runClient(ctx, old.Resource(), old)
	require.Same(t, next, s.getClient(next.Resource()))
	require.Zero(t, next.disconnected.Load())
	// A delayed watcher deletion for the old socket has the same rule.
	s.DeRegisterPlugin(old.Resource(), old.SocketPath())
	require.Same(t, next, s.getClient(next.Resource()))
	require.Zero(t, next.disconnected.Load())
	s.DeRegisterPlugin(next.Resource(), next.SocketPath())
	require.Nil(t, s.getClient(next.Resource()))
	require.EqualValues(t, 1, next.disconnected.Load())
}

func TestRunClientKeepsOriginalIdentityWhenReplacedDuringRun(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)
	next := &lifecycleClient{socket: "new.sock"}
	s := &server{clients: make(map[string]Client)}
	old := &lifecycleClient{socket: "old.sock"}
	old.run = func() { s.registerClient(logger, old.Resource(), next) }
	s.registerClient(logger, old.Resource(), old)
	s.runClient(ctx, old.Resource(), old)
	require.Same(t, next, s.getClient(next.Resource()))
	require.Zero(t, next.disconnected.Load())
}

type optionsHandshakePlugin struct {
	api.UnimplementedDevicePluginServer
	entered chan struct{}
	release <-chan struct{}
}

func (p *optionsHandshakePlugin) GetDevicePluginOptions(ctx context.Context, _ *api.Empty) (*api.DevicePluginOptions, error) {
	close(p.entered)
	if p.release != nil {
		select {
		case <-p.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &api.DevicePluginOptions{}, nil
}

func (p *optionsHandshakePlugin) ListAndWatch(_ *api.Empty, stream api.DevicePlugin_ListAndWatchServer) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

type handshakeHandler struct {
	mutex        sync.Mutex
	connected    map[string]DevicePlugin
	publications map[string]int
	inventory    int
}

func (h *handshakeHandler) PluginConnected(_ context.Context, resource string, p DevicePlugin, _ *api.DevicePluginOptions) error {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.connected[resource] = p
	h.publications[resource]++
	return nil
}
func (h *handshakeHandler) PluginDisconnected(_ klog.Logger, resource string, p DevicePlugin) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	if h.connected[resource] == p {
		delete(h.connected, resource)
	}
}
func (h *handshakeHandler) PluginListAndWatchReceiver(_ klog.Logger, _ string, _ DevicePlugin, _ *api.ListAndWatchResponse) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.inventory++
}

func newHandshakeServer(t *testing.T) (*server, *handshakeHandler) {
	t.Helper()
	h := &handshakeHandler{connected: make(map[string]DevicePlugin), publications: make(map[string]int)}
	s := &server{clients: make(map[string]Client), chandler: h}
	logger, _ := ktesting.NewTestContext(t)
	t.Cleanup(func() { require.NoError(t, s.Stop(logger)) })
	return s, h
}

func startOptionsPlugin(t *testing.T, held bool) (string, <-chan struct{}, chan struct{}) {
	t.Helper()
	dir, err := os.MkdirTemp("", "dp-options-")
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
	socket := filepath.Join(dir, "plugin.sock")
	listener, err := net.Listen("unix", socket)
	require.NoError(t, err)
	p := &optionsHandshakePlugin{entered: make(chan struct{})}
	var release chan struct{}
	if held {
		release = make(chan struct{})
		p.release = release
	}
	g := grpc.NewServer()
	api.RegisterDevicePluginServer(g, p)
	go g.Serve(listener)
	t.Cleanup(g.Stop)
	return socket, p.entered, release
}

func waitHandshake(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatal("native handshake did not reach controlled boundary")
	}
}

func waitHandshakeResult(t *testing.T, ch <-chan error) error {
	t.Helper()
	select {
	case err := <-ch:
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("native handshake did not terminate")
		return nil
	}
}

func TestUnrelatedPluginProgressDuringHeldOptions(t *testing.T) {
	s, _ := newHandshakeServer(t)
	heldSocket, entered, release := startOptionsPlugin(t, true)
	heldDone := make(chan error, 1)
	go func() { heldDone <- s.connectClient(context.Background(), "example.com/held", heldSocket) }()
	waitHandshake(t, entered)
	fastSocket, _, _ := startOptionsPlugin(t, false)
	fastDone := make(chan error, 1)
	go func() { fastDone <- s.connectClient(context.Background(), "example.com/fast", fastSocket) }()
	require.NoError(t, waitHandshakeResult(t, fastDone))
	close(release)
	require.NoError(t, waitHandshakeResult(t, heldDone))
}

func TestReplacementFencesHeldOptionsPublication(t *testing.T) {
	s, h := newHandshakeServer(t)
	resource := "example.com/device"
	oldSocket, entered, release := startOptionsPlugin(t, true)
	oldDone := make(chan error, 1)
	go func() { oldDone <- s.connectClient(context.Background(), resource, oldSocket) }()
	waitHandshake(t, entered)
	old := s.getClient(resource).(*client)
	newSocket, _, _ := startOptionsPlugin(t, false)
	require.NoError(t, s.connectClient(context.Background(), resource, newSocket))
	next := s.getClient(resource)
	require.Error(t, waitHandshakeResult(t, oldDone))
	close(release)
	func() {
		h.mutex.Lock()
		defer h.mutex.Unlock()
		require.Same(t, next, h.connected[resource])
		require.Equal(t, 1, h.publications[resource])
	}()
	func() {
		old.mutex.Lock()
		defer old.mutex.Unlock()
		require.True(t, old.closed)
		require.Nil(t, old.grpc)
	}()
}

func TestHeldOptionsCannotPublishAfterRemoval(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "deregister", true: "stop"}[stop], func(t *testing.T) {
			s, h := newHandshakeServer(t)
			resource := "example.com/device"
			socket, entered, release := startOptionsPlugin(t, true)
			done := make(chan error, 1)
			go func() { done <- s.connectClient(context.Background(), resource, socket) }()
			waitHandshake(t, entered)
			pending := s.getClient(resource).(*client)
			logger, _ := ktesting.NewTestContext(t)
			if stop {
				require.NoError(t, s.Stop(logger))
			} else {
				s.DeRegisterPlugin(resource, socket)
			}
			close(release)
			require.Error(t, waitHandshakeResult(t, done))
			require.Nil(t, s.getClient(resource))
			func() {
				h.mutex.Lock()
				defer h.mutex.Unlock()
				require.Empty(t, h.connected)
				require.Zero(t, h.publications[resource])
				require.Zero(t, h.inventory)
			}()
			func() {
				pending.mutex.Lock()
				defer pending.mutex.Unlock()
				require.True(t, pending.closed)
				require.Nil(t, pending.grpc)
			}()
			if stop {
				require.Error(t, s.connectClient(context.Background(), resource, socket))
			}
		})
	}
}

func TestRegisterPluginTimeoutCoversOptions(t *testing.T) {
	s, h := newHandshakeServer(t)
	socket, entered, release := startOptionsPlugin(t, true)
	timeout := time.Second
	done := make(chan error, 1)
	go func() { done <- s.RegisterPlugin("example.com/device", socket, nil, &timeout) }()
	waitHandshake(t, entered)
	require.Error(t, waitHandshakeResult(t, done))
	close(release)
	require.Nil(t, s.getClient("example.com/device"))
	func() {
		h.mutex.Lock()
		defer h.mutex.Unlock()
		require.Empty(t, h.connected)
	}()
}

func TestPendingDialCannotPublishAfterRemoval(t *testing.T) {
	for _, stop := range []bool{false, true} {
		t.Run(map[bool]string{false: "deregister", true: "stop"}[stop], func(t *testing.T) {
			s, h := newHandshakeServer(t)
			dir, err := os.MkdirTemp("", "dp-dial-")
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, os.RemoveAll(dir)) })
			socket := filepath.Join(dir, "plugin.sock")
			listener, err := net.Listen("unix", socket)
			require.NoError(t, err)
			t.Cleanup(func() { listener.Close() })
			accepted := make(chan net.Conn, 1)
			go func() {
				conn, err := listener.Accept()
				if err == nil {
					accepted <- conn
				}
			}()
			resource := "example.com/pending-dial"
			done := make(chan error, 1)
			go func() { done <- s.connectClient(context.Background(), resource, socket) }()
			var conn net.Conn
			select {
			case conn = <-accepted:
			case <-time.After(5 * time.Second):
				t.Fatal("dial did not reach controlled transport boundary")
			}
			t.Cleanup(func() { conn.Close() })
			pending := s.getClient(resource).(*client)
			func() {
				pending.mutex.Lock()
				defer pending.mutex.Unlock()
				require.Nil(t, pending.grpc) // Transport handshake has not completed.
			}()
			logger, _ := ktesting.NewTestContext(t)
			if stop {
				require.NoError(t, s.Stop(logger))
			} else {
				s.DeRegisterPlugin(resource, socket)
			}
			conn.Close()
			require.Error(t, waitHandshakeResult(t, done))
			require.Nil(t, s.getClient(resource))
			func() {
				h.mutex.Lock()
				defer h.mutex.Unlock()
				require.Empty(t, h.connected)
				require.Zero(t, h.publications[resource])
				require.Zero(t, h.inventory)
			}()
			func() {
				pending.mutex.Lock()
				defer pending.mutex.Unlock()
				require.True(t, pending.closed)
				require.Nil(t, pending.grpc)
			}()
		})
	}
}
