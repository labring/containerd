/*
   Copyright The containerd Authors.

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

package server

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func runscUnixPair(t *testing.T) (*net.UnixConn, *net.UnixConn) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "s")
	l, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
	require.NoError(t, err)
	defer l.Close()
	a, err := net.DialUnix("unix", nil, &net.UnixAddr{Name: path, Net: "unix"})
	require.NoError(t, err)
	b, err := l.AcceptUnix()
	require.NoError(t, err)
	t.Cleanup(func() { a.Close(); b.Close() })
	return a, b
}

func TestRunscForwardHalfClose(t *testing.T) {
	client, stream := runscUnixPair(t)
	conn, sandbox := runscUnixPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- copyRunscPortForward(ctx, conn, stream) }()
	_, err := client.Write([]byte("request"))
	require.NoError(t, err)
	require.NoError(t, client.CloseWrite())
	request, err := io.ReadAll(sandbox)
	require.NoError(t, err)
	require.Equal(t, "request", string(request))
	// The server responds only after seeing EOF on the request.
	_, err = sandbox.Write([]byte("response after EOF"))
	require.NoError(t, err)
	require.NoError(t, sandbox.CloseWrite())
	response, err := io.ReadAll(client)
	require.NoError(t, err)
	require.Equal(t, "response after EOF", string(response))
	require.NoError(t, <-result)
}

func TestRunscForwardCancellation(t *testing.T) {
	_, stream := runscUnixPair(t)
	conn, _ := runscUnixPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { result <- copyRunscPortForward(ctx, conn, stream) }()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("forwarding pumps did not stop")
	}
}

func TestRunscForwardSetupFailure(t *testing.T) {
	for _, test := range []struct {
		name, body string
		cancel     bool
	}{
		{"failure", "echo helper-failed >&2; exit 42", false},
		{"cancel", "exec sleep 60", true},
		{"no socket connection", "exit 0", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			dir := t.TempDir()
			t.Setenv("TMPDIR", dir)
			binary := filepath.Join(dir, "helper")
			require.NoError(t, os.WriteFile(binary, []byte("#!/bin/sh\n"+test.body+"\n"), 0700))
			client, stream := net.Pipe()
			defer client.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			err := runscPortForward(ctx, binary, "/state/k8s.io", "sandbox", 8080, stream)
			require.Error(t, err)
			if !test.cancel {
				require.Contains(t, err.Error(), "helper-failed")
			}
			files, err := os.ReadDir(dir)
			require.NoError(t, err)
			for _, f := range files {
				require.False(t, strings.HasPrefix(f.Name(), "ctr-runsc-pf-"))
			}
		})
	}
}

func TestRunscForwardInvalidPort(t *testing.T) {
	for _, port := range []int32{0, -1, 65536} {
		client, stream := net.Pipe()
		err := runscPortForward(context.Background(), "/does-not-exist", "/state", "sandbox", port, stream)
		require.ErrorContains(t, err, "invalid port")
		_, err = client.Write([]byte("x"))
		require.True(t, errors.Is(err, io.ErrClosedPipe))
		client.Close()
	}
}

func TestRunscForwardDiagnosticLimit(t *testing.T) {
	b := &portForwardDiagnostic{}
	n, err := b.Write([]byte(strings.Repeat("x", 8192)))
	require.NoError(t, err)
	require.Equal(t, 8192, n)
	require.Len(t, b.data, 4096)
	n, err = b.Write([]byte("more"))
	require.NoError(t, err)
	require.Equal(t, 4, n)
	require.Len(t, b.data, 4096)
}

// finOnlyStream mimics a SPDY stream: Close only signals the peer, and a
// blocked Read returns only once the stream is reset.
type finOnlyStream struct {
	reset chan struct{}
	once  sync.Once
	mu    sync.Mutex
	buf   bytes.Buffer
}

func newFinOnlyStream() *finOnlyStream {
	return &finOnlyStream{reset: make(chan struct{})}
}

func (s *finOnlyStream) Read(p []byte) (int, error) {
	<-s.reset
	return 0, io.EOF
}

func (s *finOnlyStream) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *finOnlyStream) Close() error { return nil }

func (s *finOnlyStream) Reset() error {
	s.once.Do(func() { close(s.reset) })
	return nil
}

func (s *finOnlyStream) received() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.String()
}

func TestRunscForwardResetsBlockedClientRead(t *testing.T) {
	for _, test := range []struct {
		name     string
		finish   func(t *testing.T, sandbox *net.UnixConn, cancel context.CancelFunc)
		expected error
	}{
		{"sandbox EOF", func(t *testing.T, sandbox *net.UnixConn, _ context.CancelFunc) {
			_, err := sandbox.Write([]byte("response"))
			require.NoError(t, err)
			require.NoError(t, sandbox.CloseWrite())
		}, nil},
		{"cancellation", func(_ *testing.T, _ *net.UnixConn, cancel context.CancelFunc) {
			cancel()
		}, context.Canceled},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn, sandbox := runscUnixPair(t)
			stream := newFinOnlyStream()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- copyRunscPortForward(ctx, conn, stream) }()
			test.finish(t, sandbox, cancel)
			select {
			case err := <-result:
				if test.expected == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, test.expected)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("forwarding did not return while the client read stayed blocked")
			}
			if test.expected == nil {
				require.Equal(t, "response", stream.received())
			}
		})
	}
}
