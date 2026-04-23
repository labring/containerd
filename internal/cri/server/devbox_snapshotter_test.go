package server

import (
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/internal/cri/devboxsnapshotter"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

func TestDevboxSnapshotterOpts(t *testing.T) {
	opt, err := devboxSnapshotterOpts(&runtime.PodSandboxConfig{
		Annotations: map[string]string{
			devboxsnapshotter.SealosDevboxContentIDAnnotation:    "workspace-9",
			devboxsnapshotter.SealosDevboxStorageLimitAnnotation: "8Gi",
			"other.annotation": "ignored",
		},
	})
	if err != nil {
		t.Fatalf("devboxSnapshotterOpts() error = %v", err)
	}
	if opt == nil {
		t.Fatal("devboxSnapshotterOpts() returned nil opt")
	}

	info := &snapshots.Info{Labels: make(map[string]string)}
	if err := opt(info); err != nil {
		t.Fatalf("applying snapshot opt error = %v", err)
	}

	if got := info.Labels[devboxsnapshotter.SealosDevboxContentIDAnnotation]; got != "workspace-9" {
		t.Fatalf("content-id label = %q, want %q", got, "workspace-9")
	}
	if got := info.Labels[devboxsnapshotter.SealosDevboxStorageLimitAnnotation]; got != "8Gi" {
		t.Fatalf("storage-limit label = %q, want %q", got, "8Gi")
	}
	if _, ok := info.Labels["other.annotation"]; ok {
		t.Fatalf("unexpected unrelated annotation preserved: %+v", info.Labels)
	}
}
