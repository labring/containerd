//go:build linux

package lvm

import (
	"context"
	"testing"

	apis "github.com/openebs/lvm-localpv/pkg/apis/openebs.io/lvm/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBuildLVMCreateArgsThinVolume(t *testing.T) {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "devbox-devboxtemplatepod"},
		Spec: apis.VolumeInfo{
			Capacity:      "10737418240",
			VolGroup:      "devbox-vg",
			ThinProvision: "devbox-vg-thinpool",
		},
	}

	args := buildLVMCreateArgsWithThinPool(context.Background(), vol, true)
	expected := []string{
		"-T", "devbox-vg/devbox-vg-thinpool",
		"-V", "10737418240b",
		"-n", "devbox-devboxtemplatepod",
		"-y",
	}

	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d, args=%v", len(args), len(expected), args)
	}
	for i := range expected {
		if args[i] != expected[i] {
			t.Fatalf("args[%d] = %q, want %q, full args=%v", i, args[i], expected[i], args)
		}
	}
}

func TestBuildLVMCreateArgsThinVolumeCreatesPool(t *testing.T) {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "devbox-devboxtemplatepod"},
		Spec: apis.VolumeInfo{
			Capacity:      "10737418240",
			VolGroup:      "devbox-vg",
			ThinProvision: "devbox-vg-thinpool",
		},
	}

	args := buildLVMCreateArgsWithThinPool(context.Background(), vol, false)
	expected := []string{
		"-L", "10737418240b",
		"-T", "devbox-vg/devbox-vg-thinpool",
		"-V", "10737418240b",
		"-n", "devbox-devboxtemplatepod",
		"-y",
	}

	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d, args=%v", len(args), len(expected), args)
	}
	for i := range expected {
		if args[i] != expected[i] {
			t.Fatalf("args[%d] = %q, want %q, full args=%v", i, args[i], expected[i], args)
		}
	}
}

func TestBuildLVMCreateArgsLinearVolume(t *testing.T) {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{Name: "devbox-linear"},
		Spec: apis.VolumeInfo{
			Capacity: "1073741824",
			VolGroup: "devbox-vg",
		},
	}

	args := buildLVMCreateArgsWithThinPool(context.Background(), vol, false)
	expected := []string{
		"-L", "1073741824b",
		"-n", "devbox-linear",
		"devbox-vg",
		"-y",
	}

	if len(args) != len(expected) {
		t.Fatalf("args len = %d, want %d, args=%v", len(args), len(expected), args)
	}
	for i := range expected {
		if args[i] != expected[i] {
			t.Fatalf("args[%d] = %q, want %q, full args=%v", i, args[i], expected[i], args)
		}
	}
}
