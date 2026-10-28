# gVisor CRI port forwarding

This extension adds an optional gVisor forwarding path to containerd 2.2 CRI.
Clients continue to use `kubectl port-forward`, and gVisor keeps
`network=sandbox`. A node administrator must enable the feature for each runtime
handler. Handlers without this configuration retain the existing network
namespace forwarding path.

## How it works

The existing implementation enters the Pod's Linux network namespace and
connects to `localhost:PORT`. A gVisor application's loopback belongs to the
Sentry netstack, so entering the host-side Pod network namespace cannot reach it,
even when Pod IP, Service, and DNS connectivity work.

```text
kubectl -> API server -> kubelet -> containerd CRI streaming
                                      |
                          Private Unix socket per connection
                                      |
                 runsc port-forward --stream SOCKET SANDBOX_ID PORT
                                      |
                         Sentry netstack -> application port
```

Containerd creates a temporary directory with mode 0700, listens on a Unix socket
inside it, and invokes the public runsc CLI. Runsc passes a socket endpoint to
the Sentry and exits; containerd continues to bridge the CRI stream through the
other endpoint. Successful CLI exit indicates that the handoff completed, so it
must not terminate the forwarding stream.

This path uses gVisor's existing interface and does not introduce a new runtime
or Kubernetes streaming API. Runtime and client bugs can still affect forwarding;
see the validation notes below. Application images do not need
forwarding utilities. These settings are an extension in this fork and are not
part of upstream containerd 2.2 configuration.

## Configuration

Add the two forwarding fields to the existing runtime handler tables of the
`io.containerd.cri.v1.runtime` plugin, preserving other settings. Do not declare
an existing TOML table a second time.

```toml
version = 3

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc]
  runtime_type = "io.containerd.runsc.v1"
  snapshotter = "overlayfs"
  runsc_port_forward_binary = "/usr/local/bin/runsc"
  runsc_port_forward_root = "/run/containerd/runsc/k8s.io"

[plugins.'io.containerd.cri.v1.runtime'.containerd.runtimes.runsc-stargz]
  runtime_type = "io.containerd.runsc.v1"
  snapshotter = "stargz"
  runsc_port_forward_binary = "/usr/local/bin/runsc"
  runsc_port_forward_root = "/run/containerd/runsc/k8s.io"
```

| Field | Description |
|---|---|
| `runsc_port_forward_binary` | Absolute path to a runsc executable supporting `port-forward --stream`. Leave both fields empty to disable this path. |
| `runsc_port_forward_root` | Runsc state directory for the running sandbox, including the containerd namespace, such as `k8s.io`. |

Both fields must be configured together, and the runtime type must be
`io.containerd.runsc.v1`. Invalid settings fail CRI configuration validation.
The root must match the running sandbox's state directory. For example, a shim
configured with `root=/run/containerd/runsc` uses the namespace-specific path
shown above. Check sandbox IDs with `runsc --root=... list`. The sandbox ID
passed to runsc is the CRI pod sandbox ID; this layout corresponds to the
default `podsandbox` sandboxer.

The executable and root come only from the node's containerd configuration.
Pod annotations cannot select either value, and commands are invoked without a
shell. This integration targets gVisor sandbox networking. Host network
passthrough, other runtimes, and other runsc versions require separate validation.

The forwarding settings are independent of the snapshotter. The `runsc-stargz`
handler reuses the existing stargz root, socket, cache, and credentials. The Pod
must start successfully before forwarding can work.

## Build and enable

Build Linux amd64 containerd from this branch:

```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath \
  -ldflags "-X github.com/containerd/containerd/v2/version.Version=v2.2.3-labring.gvisor-pf.1 -X github.com/containerd/containerd/v2/version.Revision=$(git rev-parse HEAD)" \
  -o bin/containerd-gvisor-pf ./cmd/containerd
```

Back up the containerd configuration and service definition, install the binary,
set both forwarding fields, and restart containerd. Stargz does not require a
restart or configuration change for this feature. Preserve the containerd
service's `KillMode=process`; CRI requests are briefly interrupted during daemon
restart. Keep the original binary and service configuration for rollback.

Installing the binary alone does not enable forwarding. Confirm the server
version with `ctr version` and check CRI health with `crictl info`, then use the
standard client command:

```bash
kubectl -n <namespace> port-forward pod/<pod> 18080:8080
curl http://127.0.0.1:18080/
```

## Connection lifecycle and errors

- Each connection invokes runsc once. Connection setup has a 10-second timeout;
  this timeout does not limit an established data stream.
- CLI stdout and stderr retain at most 4 KiB of diagnostic output. Subprocess
  errors are returned through the CRI error stream, without falling back to
  host-side localhost.
- Request EOF is passed to the Sentry through the Unix socket's `CloseWrite`.
  The response then has a one-second grace period, matching the existing
  network namespace forwarding path.
- On return, client disconnection, or context cancellation, both endpoints are
  closed, the client stream is reset where its type supports it so a pending
  read cannot stall cleanup, copy goroutines are joined, and the temporary
  socket directory is removed.
- Only TCP streams are supported. API server and kubelet authentication and
  authorization remain in place. Pods do not receive a host runtime socket.

Starting a CLI process per connection adds setup overhead. Use this feature for
debugging access; use Services or Ingress for application traffic. The integration
depends on the public runsc CLI behavior, so run regression tests after upgrading
runsc.

## Validation

Unit tests cover responses after request half-close, cancellation, helper errors
and timeouts, a helper that never connects to the Unix socket, invalid ports,
diagnostic output limits, and configuration validation.

Run on Linux:

```bash
go test ./internal/cri/config
go test ./internal/cri/server -run 'TestRunscForward|Test.*PortForward|Test.*SandboxRuntime'
```

For cluster validation, make the application listen only on `127.0.0.1`. Verify
failure before enabling the feature and success afterward. Cover multiple ports,
concurrent requests, large responses, closed-port errors, and cleanup after
client exit. Include a runc control and place the test Pod on a worker running
the patched daemon.

This 2.2 port carries the forwarding code unchanged from this fork's containerd
1.7 port, where it was exercised on a Kubernetes test cluster with gVisor +
overlayfs and stargz using both ordinary images and eStargz remote snapshots.
The 2.2 build itself has unit test coverage only and has not been exercised on a
cluster yet; repeat the cluster checks above before relying on it.

Testing with official gVisor `release-20261005.0` identified two independent
upstream defects:

- gVisor's imported Unix socket writer can return a positive byte count together
  with `ErrWouldBlock`. Its port-forward wrapper retries the original buffer,
  duplicating bytes already written. Direct `runsc port-forward --stream` also
  reproduces this failure, without containerd or Kubernetes in the data path.
  Tracked as [google/gvisor#15660](https://github.com/google/gvisor/issues/15660);
  the fix in [google/gvisor#15661](https://github.com/google/gvisor/pull/15661)
  retains the byte count and retries only the unwritten suffix.
- client-go 1.28.15 can block a forwarding session after an interrupted download
  because it waits for the error stream before resetting the data stream.
  [Kubernetes PR #128681](https://github.com/kubernetes/kubernetes/pull/128681)
  adds that reset. This failure also reproduces with runc.

With the Sentry fix applied, the 1.7-based cluster run passed exact-byte
sequential-payload checks and ten batches of four concurrent 4 MiB downloads
with all three gVisor combinations. A diagnostic client using client-go 1.28.15
with the upstream reset backported also passed twenty interrupted downloads
followed by successful requests in the same session, while the unmodified client
still failed that interruption test. These results establish separate runtime
and client fixes; large responses are byte-exact only on a gVisor release that
includes the runtime fix.

The client reset is present in client-go v0.32.0. Select kubectl upgrades according
to Kubernetes' supported version skew; a kubectl 1.32 client is outside the
supported range for a Kubernetes 1.28 API server. A controlled client backport
can isolate this fix while planning a supported cluster upgrade.

## Upstream references

- [Containerd 2.2 Linux port forwarding](https://github.com/containerd/containerd/blob/v2.2.3/internal/cri/server/sandbox_portforward_linux.go)
- [Containerd 2.x CRI configuration](https://github.com/containerd/containerd/blob/v2.2.3/docs/cri/config.md)
- [gVisor release-20260914.0 port-forward CLI](https://github.com/google/gvisor/blob/release-20260914.0/runsc/cmd/portforward.go): `doStream` hands off the file descriptor; `portCopy` explains the stream's independent lifetime.
- [gVisor networking architecture](https://gvisor.dev/docs/architecture_guide/networking/)
- [Kubernetes port forwarding](https://kubernetes.io/docs/tasks/access-application-cluster/port-forward-access-application-cluster/)
- [gVisor release-20261005.0 imported-FD forwarding](https://github.com/google/gvisor/blob/release-20261005.0/runsc/boot/portforward/portforward_fd_rw.go)
- [gVisor imported Unix socket partial-write semantics](https://github.com/google/gvisor/blob/release-20261005.0/pkg/sentry/socket/unix/transport/host.go)
- [Kubernetes client stream-reset fix](https://github.com/kubernetes/kubernetes/pull/128681)
- [Kubernetes version skew policy](https://kubernetes.io/releases/version-skew-policy/)
