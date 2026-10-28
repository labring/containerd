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
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

// runscPortForward donates a Unix stream to the Sentry using runsc's public CLI.
// runsc exits after donation; the connection lives independently in the Sentry.
func runscPortForward(ctx context.Context, binary, root, id string, port int32, stream io.ReadWriteCloser) error {
	defer stream.Close()
	if port < 1 || port > 65535 {
		return fmt.Errorf("invalid port %d", port)
	}
	// MkdirTemp creates a mode-0700 directory. No TCP listener or runtime socket
	// is exposed to the Pod, and each forwarded connection has its own socket.
	dir, err := os.MkdirTemp("", "ctr-runsc-pf-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)
	socket := filepath.Join(dir, "stream.sock")
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socket, Net: "unix"})
	if err != nil {
		return err
	}
	defer listener.Close()

	setupCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	stopAccept := context.AfterFunc(setupCtx, func() { listener.Close() })
	defer stopAccept()
	cmd := exec.CommandContext(setupCtx, binary, "--root="+root, "port-forward", "--stream="+socket, id, strconv.Itoa(int(port)))
	cmd.WaitDelay = time.Second
	// Bound diagnostic output even if a misconfigured helper emits continuously.
	diagnostic := &portForwardDiagnostic{}
	cmd.Stdout = diagnostic
	cmd.Stderr = diagnostic
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("runsc port-forward: %w: %s", err, diagnostic.data)
	}
	if err := setupCtx.Err(); err != nil {
		return err
	}
	deadline, _ := setupCtx.Deadline()
	if err := listener.SetDeadline(deadline); err != nil {
		return err
	}
	conn, err := listener.AcceptUnix()
	if err != nil {
		return fmt.Errorf("accept runsc stream: %w", err)
	}
	listener.Close()
	cancel()

	return copyRunscPortForward(ctx, conn, stream)
}

// copyRunscPortForward preserves a client's write EOF long enough for the
// sandbox to finish its response, matching the existing netns path's grace period.
func copyRunscPortForward(ctx context.Context, conn *net.UnixConn, stream io.ReadWriteCloser) error {
	toSandbox := make(chan error, 1)
	toClient := make(chan error, 1)
	go func(done chan<- error) {
		_, err := io.Copy(conn, stream)
		conn.CloseWrite()
		done <- err
	}(toSandbox)
	go func(done chan<- error) {
		_, err := io.Copy(stream, conn)
		done <- err
	}(toClient)
	// Close both endpoints on every exit. Each buffered channel is drained once
	// so neither pump can outlive this request.
	defer func() {
		conn.Close()
		stream.Close()
		// A SPDY stream's Close only sends FIN; a concurrent Read stays blocked
		// until the peer closes or the stream is reset. Reset after Close does not
		// emit a frame, it only releases the local reader.
		if r, ok := stream.(interface{ Reset() error }); ok {
			r.Reset()
		}
		if toSandbox != nil {
			<-toSandbox
		}
		if toClient != nil {
			<-toClient
		}
	}()
	select {
	case err := <-toClient:
		toClient = nil
		return err
	case err := <-toSandbox:
		toSandbox = nil
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case err := <-toClient:
		toClient = nil
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// portForwardDiagnostic retains only the first 4 KiB, while consuming all writes.
type portForwardDiagnostic struct{ data []byte }

func (b *portForwardDiagnostic) Write(p []byte) (int, error) {
	n := len(p)
	remaining := 4096 - len(b.data)
	if len(p) > remaining {
		p = p[:remaining]
	}
	b.data = append(b.data, p...)
	return n, nil
}
