package devboxsnapshotter

import "testing"

func TestLabelsFromAnnotations(t *testing.T) {
	labels := LabelsFromAnnotations(map[string]string{
		SealosDevboxContentIDAnnotation:    "workspace-1",
		SealosDevboxStorageLimitAnnotation: "20Gi",
		"other.annotation":                 "ignored",
	})

	if got := labels[SealosDevboxContentIDAnnotation]; got != "workspace-1" {
		t.Fatalf("content-id label = %q, want %q", got, "workspace-1")
	}
	if got := labels[SealosDevboxStorageLimitAnnotation]; got != "20Gi" {
		t.Fatalf("storage-limit label = %q, want %q", got, "20Gi")
	}
	if _, ok := labels["other.annotation"]; ok {
		t.Fatalf("unexpected non-devbox label preserved: %+v", labels)
	}
}

func TestIsWritableSnapshotter(t *testing.T) {
	tests := []struct {
		name        string
		snapshotter string
		want        bool
	}{
		{name: "devbox", snapshotter: DevboxSnapshotter, want: true},
		{name: "stargz", snapshotter: StargzSnapshotter, want: true},
		{name: "overlayfs", snapshotter: "overlayfs", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsWritableSnapshotter(tt.snapshotter); got != tt.want {
				t.Fatalf("IsWritableSnapshotter(%q) = %v, want %v", tt.snapshotter, got, tt.want)
			}
		})
	}
}
