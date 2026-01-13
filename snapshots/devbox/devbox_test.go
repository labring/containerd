//go:build linux

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

package devbox

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"testing"

	apis "github.com/openebs/lvm-localpv/pkg/apis/openebs.io/lvm/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/containerd/containerd/snapshots/devbox/lvm"
)

const (
	testVGName   = "devbox-vg"
	testPoolName = "devbox-vg-thinpool"
)

// TestFindMountPointAndUnmount tests the findMountPointByDevice and unmountLvm functions
// It creates an LV, mounts it to a temporary directory, verifies findMountPointByDevice
// can find the mount point, and then tests unmountLvm to unmount it.
func TestFindMountPointAndUnmount(t *testing.T) {
	ctx := context.Background()

	// Create a temporary directory for the test
	tmpRoot, err := os.MkdirTemp("", "devbox-test-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	// Create a minimal Snapshotter instance for testing
	snapshotter := &Snapshotter{
		lvmVgName:    testVGName,
		ThinPoolName: testPoolName,
	}

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-mount-unmount-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		// Force destroy the volume
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Format the filesystem
	t.Logf("Step 2: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// // Step 3: Create a temporary mount point
	mountPoint := filepath.Join(tmpRoot, "mount-point")
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		t.Fatalf("Failed to create mount point directory: %v", err)
	}
	// defer os.RemoveAll(mountPoint)

	// Step 4: Mount the LV
	t.Logf("Step 4: Mounting %s to %s", devicePath, mountPoint)
	if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
		t.Fatalf("Failed to mount %s to %s: %v", devicePath, mountPoint, err)
	}
	// Ensure unmount at the end (in case test fails)
	mounted := true
	defer func() {
		if mounted {
			if err := syscall.Unmount(mountPoint, 0); err != nil {
				t.Logf("Warning: Failed to unmount %s during cleanup: %v", mountPoint, err)
			}
		}
	}()

	// Step 5: Test findMountPointByDevice
	t.Logf("Step 5: Testing findMountPointByDevice for %s", devicePath)
	foundMountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(foundMountPoints) == 0 {
		t.Fatal("findMountPointByDevice should have found the mount point, but returned empty slice")
	}
	if len(foundMountPoints) != 1 {
		t.Fatalf("findMountPointByDevice should return exactly 1 mount point, but got %d: %v", len(foundMountPoints), foundMountPoints)
	}
	if foundMountPoints[0] != mountPoint {
		t.Fatalf("findMountPointByDevice returned wrong mount point: expected %s, got %s", mountPoint, foundMountPoints[0])
	}
	t.Logf("Successfully found mount point: %s", foundMountPoints[0])

	// Step 6: Test unmountLvm
	t.Logf("Step 6: Testing unmountLvm for %s", mountPoint)
	if err := snapshotter.unmountLvm(ctx, mountPoint); err != nil {
		t.Fatalf("unmountLvm failed: %v", err)
	}
	mounted = false // Mark as unmounted so defer doesn't try again
	t.Logf("Successfully unmounted %s", mountPoint)

	// Step 7: Verify the mount point is no longer mounted
	t.Logf("Step 7: Verifying mount point is no longer mounted")
	foundMountPoints, err = lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed after unmount: %v", err)
	}
	if len(foundMountPoints) != 0 {
		t.Fatalf("findMountPointByDevice should return empty slice after unmount, but got %d mount points: %v", len(foundMountPoints), foundMountPoints)
	}
	t.Logf("Verified: mount point is no longer mounted")

	t.Logf("Test completed successfully")
}

// TestFindMountPointByDevice_UnmountedDevice tests findMountPointByDevice with an unmounted device
func TestFindMountPointByDevice_UnmountedDevice(t *testing.T) {
	ctx := context.Background()

	// Create a test volume
	lvName := fmt.Sprintf("test-unmounted-%d", os.Getpid())
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Create the LV
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)

	// Test findMountPointByDevice on an unmounted device
	mountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(mountPoints) != 0 {
		t.Fatalf("findMountPointByDevice should return empty slice for unmounted device, but got %d mount points: %v", len(mountPoints), mountPoints)
	}

	t.Logf("Test passed: unmounted device correctly returns empty mount point slice")
}

// TestFindMountPointAndUnmount_Concurrent tests the findMountPointByDevice and unmountLvm
// functions under concurrent conditions. It creates multiple LVs and performs mount/unmount
// operations concurrently to verify there are no race conditions or deadlocks.
func TestFindMountPointAndUnmount_Concurrent(t *testing.T) {
	ctx := context.Background()

	// Number of concurrent operations
	const numConcurrent = 10

	// Create a temporary directory for the test
	tmpRoot, err := os.MkdirTemp("", "devbox-test-concurrent-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	// Create a minimal Snapshotter instance for testing
	snapshotter := &Snapshotter{
		lvmVgName:    testVGName,
		ThinPoolName: testPoolName,
	}

	// Track all created LVs for cleanup
	var allVols []*apis.LVMVolume
	var allVolsMutex sync.Mutex

	// Use WaitGroup to wait for all goroutines to complete
	var wg sync.WaitGroup

	// Channel to collect errors from goroutines
	errorChan := make(chan error, numConcurrent)

	// Launch concurrent operations
	for i := 0; i < numConcurrent; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()

			// Generate unique LV name for this goroutine
			lvName := fmt.Sprintf("test-concurrent-%d-%d", os.Getpid(), index)

			// Create the test volume
			vol := &apis.LVMVolume{
				ObjectMeta: metav1.ObjectMeta{
					Name: lvName,
				},
				Spec: apis.VolumeInfo{
					Capacity:      "100M",
					VolGroup:      testVGName,
					ThinProvision: testPoolName,
				},
			}

			// Register for cleanup
			allVolsMutex.Lock()
			allVols = append(allVols, vol)
			allVolsMutex.Unlock()

			// Step 1: Create the LV
			if err := lvm.CreateVolume(ctx, vol); err != nil {
				errorChan <- fmt.Errorf("goroutine %d: failed to create LV %s: %w", index, lvName, err)
				return
			}

			// Verify LV exists
			devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
			if _, err := os.Stat(devicePath); os.IsNotExist(err) {
				errorChan <- fmt.Errorf("goroutine %d: LV %s does not exist: %w", index, devicePath, err)
				return
			}

			// Step 2: Format the filesystem
			cmd := exec.Command("mkfs.ext4", "-F", devicePath)
			output, err := cmd.CombinedOutput()
			if err != nil {
				errorChan <- fmt.Errorf("goroutine %d: failed to format %s: %w, output: %s", index, devicePath, err, string(output))
				return
			}

			// Step 3: Create a temporary mount point
			mountPoint := filepath.Join(tmpRoot, fmt.Sprintf("mount-point-%d", index))
			if err := os.MkdirAll(mountPoint, 0755); err != nil {
				errorChan <- fmt.Errorf("goroutine %d: failed to create mount point: %w", index, err)
				return
			}

			// Step 4: Mount the LV
			if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
				errorChan <- fmt.Errorf("goroutine %d: failed to mount %s to %s: %w", index, devicePath, mountPoint, err)
				return
			}
			// Ensure unmount at the end (in case test fails)
			mounted := true
			defer func() {
				if mounted {
					if err := syscall.Unmount(mountPoint, 0); err != nil {
						t.Logf("Warning: goroutine %d failed to unmount %s during cleanup: %v", index, mountPoint, err)
					}
				}
			}()

			// Step 5: Test findMountPointByDevice (concurrent access)
			foundMountPoints, err := lvm.FindMountPointByDevice(devicePath)
			if err != nil {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice failed: %w", index, err)
				return
			}
			if len(foundMountPoints) == 0 {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice should have found mount point for %s", index, devicePath)
				return
			}
			if len(foundMountPoints) != 1 {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice should return exactly 1 mount point, but got %d: %v", index, len(foundMountPoints), foundMountPoints)
				return
			}
			if foundMountPoints[0] != mountPoint {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice returned wrong mount point: expected %s, got %s", index, mountPoint, foundMountPoints[0])
				return
			}

			// Step 6: Test unmountLvm (concurrent access)
			if err := snapshotter.unmountLvm(ctx, mountPoint); err != nil {
				errorChan <- fmt.Errorf("goroutine %d: unmountLvm failed: %w", index, err)
				return
			}
			mounted = false // Mark as unmounted so defer doesn't try again

			// Step 7: Verify the mount point is no longer mounted
			foundMountPoints, err = lvm.FindMountPointByDevice(devicePath)
			if err != nil {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice failed after unmount: %w", index, err)
				return
			}
			if len(foundMountPoints) != 0 {
				errorChan <- fmt.Errorf("goroutine %d: findMountPointByDevice should return empty slice after unmount, but got %d mount points: %v", index, len(foundMountPoints), foundMountPoints)
				return
			}

			// Success - no error to report
			t.Logf("Goroutine %d: Successfully completed mount/unmount cycle for LV %s", index, lvName)
		}(i)
	}

	// Wait for all goroutines to complete
	wg.Wait()
	close(errorChan)

	// Collect all errors
	var errors []error
	for err := range errorChan {
		errors = append(errors, err)
	}

	// Clean up all LVs
	t.Logf("Cleaning up %d LVs...", len(allVols))
	for _, vol := range allVols {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", vol.Name, err)
		}
	}

	// Report results
	if len(errors) > 0 {
		t.Errorf("Concurrent test failed with %d errors:", len(errors))
		for i, err := range errors {
			t.Errorf("  Error %d: %v", i+1, err)
		}
		t.FailNow()
	}

	t.Logf("Concurrent test passed: all %d goroutines completed successfully", numConcurrent)
}

// TestReadProcMounts tests the readProcMounts function
// It creates an LV, mounts it, and verifies readProcMounts can read and parse /proc/mounts correctly
func TestReadProcMounts(t *testing.T) {
	ctx := context.Background()

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-read-proc-mounts-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Format the filesystem
	t.Logf("Step 2: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// Step 3: Create a temporary mount point
	tmpRoot, err := os.MkdirTemp("", "devbox-test-read-proc-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	mountPoint := filepath.Join(tmpRoot, "mount-point")
	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		t.Fatalf("Failed to create mount point directory: %v", err)
	}

	// Step 4: Mount the LV
	t.Logf("Step 4: Mounting %s to %s", devicePath, mountPoint)
	if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
		t.Fatalf("Failed to mount %s to %s: %v", devicePath, mountPoint, err)
	}
	defer func() {
		if err := syscall.Unmount(mountPoint, 0); err != nil {
			t.Logf("Warning: Failed to unmount %s during cleanup: %v", mountPoint, err)
		}
	}()

	// Step 5: Test readProcMounts
	t.Logf("Step 5: Testing readProcMounts")
	mounts, err := readProcMounts()
	if err != nil {
		t.Fatalf("readProcMounts failed: %v", err)
	}

	if len(mounts) == 0 {
		t.Fatal("readProcMounts returned empty slice, expected at least one mount entry")
	}

	// Verify the mounted LV is in the results
	found := false
	for _, mount := range mounts {
		if len(mount) < 2 {
			continue
		}
		mountDevice := mount[0]
		mountPointFromProc := mount[1]

		// Check if this is our mount
		if mountPointFromProc == mountPoint {
			found = true
			t.Logf("Found mount entry: device=%s, mountpoint=%s", mountDevice, mountPointFromProc)
			// Verify device path matches (may be symlink, so check both)
			if mountDevice == devicePath {
				t.Logf("Device path matches directly: %s", devicePath)
			} else {
				// Check if it's a symlink resolution
				resolvedDevice, err := filepath.EvalSymlinks(devicePath)
				if err == nil && resolvedDevice == mountDevice {
					t.Logf("Device path matches via symlink: %s -> %s", devicePath, resolvedDevice)
				}
			}
			break
		}
	}

	if !found {
		t.Errorf("readProcMounts did not find mount point %s in results", mountPoint)
		t.Logf("Available mount points (first 10):")
		for i, mount := range mounts {
			if i >= 10 {
				break
			}
			if len(mount) >= 2 {
				t.Logf("  %s -> %s", mount[0], mount[1])
			}
		}
	}

	t.Logf("Test passed: readProcMounts successfully read and parsed /proc/mounts")
}

// TestIsMountPoint tests the isMountPoint function
// It creates an LV, mounts it, and verifies isMountPoint can correctly identify mount points
func TestIsMountPoint(t *testing.T) {
	ctx := context.Background()

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-is-mount-point-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Format the filesystem
	t.Logf("Step 2: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// Step 3: Create temporary directories
	tmpRoot, err := os.MkdirTemp("", "devbox-test-is-mount-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	mountPoint := filepath.Join(tmpRoot, "mount-point")
	nonMountPoint := filepath.Join(tmpRoot, "non-mount-point")

	if err := os.MkdirAll(mountPoint, 0755); err != nil {
		t.Fatalf("Failed to create mount point directory: %v", err)
	}
	if err := os.MkdirAll(nonMountPoint, 0755); err != nil {
		t.Fatalf("Failed to create non-mount point directory: %v", err)
	}

	// Step 4: Test isMountPoint on non-mounted directory (should return false)
	t.Logf("Step 4: Testing isMountPoint on non-mounted directory")
	isMounted, err := isMountPoint(nonMountPoint)
	if err != nil {
		t.Fatalf("isMountPoint failed: %v", err)
	}
	if isMounted {
		t.Errorf("isMountPoint returned true for non-mounted directory %s", nonMountPoint)
	} else {
		t.Logf("Correctly identified %s as not a mount point", nonMountPoint)
	}

	// Step 5: Mount the LV
	t.Logf("Step 5: Mounting %s to %s", devicePath, mountPoint)
	if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
		t.Fatalf("Failed to mount %s to %s: %v", devicePath, mountPoint, err)
	}
	defer func() {
		if err := syscall.Unmount(mountPoint, 0); err != nil {
			t.Logf("Warning: Failed to unmount %s during cleanup: %v", mountPoint, err)
		}
	}()

	// Step 6: Test isMountPoint on mounted directory (should return true)
	t.Logf("Step 6: Testing isMountPoint on mounted directory")
	isMounted, err = isMountPoint(mountPoint)
	if err != nil {
		t.Fatalf("isMountPoint failed: %v", err)
	}
	if !isMounted {
		t.Errorf("isMountPoint returned false for mounted directory %s", mountPoint)
	} else {
		t.Logf("Correctly identified %s as a mount point", mountPoint)
	}

	// Step 7: Unmount and verify isMountPoint returns false
	t.Logf("Step 7: Unmounting and verifying isMountPoint returns false")
	if err := syscall.Unmount(mountPoint, 0); err != nil {
		t.Fatalf("Failed to unmount %s: %v", mountPoint, err)
	}

	isMounted, err = isMountPoint(mountPoint)
	if err != nil {
		t.Fatalf("isMountPoint failed after unmount: %v", err)
	}
	if isMounted {
		t.Errorf("isMountPoint returned true for unmounted directory %s", mountPoint)
	} else {
		t.Logf("Correctly identified %s as not a mount point after unmount", mountPoint)
	}

	t.Logf("Test passed: isMountPoint correctly identifies mount points")
}

// TestFindMountPointByDevice tests the findMountPointByDevice function
// It creates an LV, mounts it, and verifies findMountPointByDevice can find the mount point
func TestFindMountPointByDevice(t *testing.T) {
	ctx := context.Background()

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-find-mount-point-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Test findMountPointByDevice on unmounted device (should return empty)
	t.Logf("Step 2: Testing findMountPointByDevice on unmounted device")
	mountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(mountPoints) != 0 {
		t.Errorf("findMountPointByDevice returned %d mount points for unmounted device %s: %v", len(mountPoints), devicePath, mountPoints)
	} else {
		t.Logf("Correctly returned empty slice for unmounted device %s", devicePath)
	}

	// Step 3: Format the filesystem
	t.Logf("Step 3: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// Step 4: Create a temporary mount point
	tmpRoot, err := os.MkdirTemp("", "devbox-test-find-mount-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	expectedMountPoint := filepath.Join(tmpRoot, "mount-point")
	if err := os.MkdirAll(expectedMountPoint, 0755); err != nil {
		t.Fatalf("Failed to create mount point directory: %v", err)
	}

	// Step 5: Mount the LV
	t.Logf("Step 5: Mounting %s to %s", devicePath, expectedMountPoint)
	if err := syscall.Mount(devicePath, expectedMountPoint, "ext4", 0, ""); err != nil {
		t.Fatalf("Failed to mount %s to %s: %v", devicePath, expectedMountPoint, err)
	}
	defer func() {
		if err := syscall.Unmount(expectedMountPoint, 0); err != nil {
			t.Logf("Warning: Failed to unmount %s during cleanup: %v", expectedMountPoint, err)
		}
	}()

	// Step 6: Test findMountPointByDevice on mounted device
	t.Logf("Step 6: Testing findMountPointByDevice on mounted device")
	mountPoints, err = lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(mountPoints) == 0 {
		t.Errorf("findMountPointByDevice returned empty slice for mounted device %s", devicePath)
	} else if len(mountPoints) != 1 {
		t.Errorf("findMountPointByDevice should return exactly 1 mount point, but got %d: %v", len(mountPoints), mountPoints)
	} else if mountPoints[0] != expectedMountPoint {
		t.Errorf("findMountPointByDevice returned wrong mount point: expected %s, got %s", expectedMountPoint, mountPoints[0])
	} else {
		t.Logf("Successfully found mount point: %s", mountPoints[0])
	}

	// Step 7: Unmount and verify findMountPointByDevice returns empty
	t.Logf("Step 7: Unmounting and verifying findMountPointByDevice returns empty")
	if err := syscall.Unmount(expectedMountPoint, 0); err != nil {
		t.Fatalf("Failed to unmount %s: %v", expectedMountPoint, err)
	}

	mountPoints, err = lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed after unmount: %v", err)
	}
	if len(mountPoints) != 0 {
		t.Errorf("findMountPointByDevice returned %d mount points for unmounted device %s: %v", len(mountPoints), devicePath, mountPoints)
	} else {
		t.Logf("Correctly returned empty slice for unmounted device %s", devicePath)
	}

	t.Logf("Test passed: findMountPointByDevice correctly finds mount points")
}

// TestFindMountPointByDevice_MultipleMountPoints tests that findMountPointByDevice
// can find all mount points when a device is mounted to multiple directories
func TestFindMountPointByDevice_MultipleMountPoints(t *testing.T) {
	ctx := context.Background()

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-multi-mount-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Format the filesystem
	t.Logf("Step 2: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// Step 3: Create temporary directory for mount points
	tmpRoot, err := os.MkdirTemp("", "devbox-test-multi-mount-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	// Step 4: Create 5 mount point directories
	numMountPoints := 5
	expectedMountPoints := make([]string, numMountPoints)
	for i := 0; i < numMountPoints; i++ {
		mountPoint := filepath.Join(tmpRoot, fmt.Sprintf("mount-point-%d", i))
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			t.Fatalf("Failed to create mount point directory %s: %v", mountPoint, err)
		}
		expectedMountPoints[i] = mountPoint
	}

	// Step 5: Mount the LV to all 5 directories
	t.Logf("Step 5: Mounting %s to %d directories", devicePath, numMountPoints)
	mountedPoints := make([]string, 0, numMountPoints)
	for i, mountPoint := range expectedMountPoints {
		t.Logf("  Mounting to %s", mountPoint)
		if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
			t.Fatalf("Failed to mount %s to %s: %v", devicePath, mountPoint, err)
		}
		mountedPoints = append(mountedPoints, mountPoint)

		// Verify it's mounted after each mount
		mountPoints, err := lvm.FindMountPointByDevice(devicePath)
		if err != nil {
			t.Fatalf("findMountPointByDevice failed after mounting to %s: %v", mountPoint, err)
		}
		if len(mountPoints) != i+1 {
			t.Errorf("After mounting to %s, expected %d mount points, got %d", mountPoint, i+1, len(mountPoints))
		}
	}

	// Ensure all mounts are unmounted at the end
	defer func() {
		for _, mountPoint := range mountedPoints {
			if err := syscall.Unmount(mountPoint, 0); err != nil {
				t.Logf("Warning: Failed to unmount %s during cleanup: %v", mountPoint, err)
			}
		}
	}()

	// Step 6: Test findMountPointByDevice finds all mount points
	t.Logf("Step 6: Testing findMountPointByDevice finds all %d mount points", numMountPoints)
	foundMountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}

	if len(foundMountPoints) != numMountPoints {
		t.Fatalf("Expected %d mount points, but found %d. Found: %v", numMountPoints, len(foundMountPoints), foundMountPoints)
	}

	// Step 7: Verify all expected mount points are found
	t.Logf("Step 7: Verifying all expected mount points are found")
	foundMap := make(map[string]bool)
	for _, mp := range foundMountPoints {
		foundMap[mp] = true
		t.Logf("  Found mount point: %s", mp)
	}

	for _, expectedMP := range expectedMountPoints {
		if !foundMap[expectedMP] {
			t.Errorf("Expected mount point %s not found in results", expectedMP)
		}
	}

	// Step 8: Verify no unexpected mount points
	if len(foundMountPoints) != len(expectedMountPoints) {
		t.Errorf("Number of found mount points (%d) doesn't match expected (%d)", len(foundMountPoints), len(expectedMountPoints))
	}

	t.Logf("Test passed: findMountPointByDevice correctly found all %d mount points", numMountPoints)
}

// TestCleanupUnmountAllMountPoints tests that the cleanup logic can unmount all mount points
// for a device that is mounted to multiple directories
func TestCleanupUnmountAllMountPoints(t *testing.T) {
	ctx := context.Background()

	// Create a minimal Snapshotter instance for testing
	snapshotter := &Snapshotter{
		lvmVgName:    testVGName,
		ThinPoolName: testPoolName,
	}

	// Generate a unique LV name for this test
	lvName := fmt.Sprintf("test-cleanup-unmount-%d", os.Getpid())

	// Create the test volume
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      "100M",
			VolGroup:      testVGName,
			ThinProvision: testPoolName,
		},
	}

	// Clean up LV at the end
	defer func() {
		if err := lvm.ForceDestroyVolume(ctx, vol); err != nil {
			t.Logf("Warning: Failed to clean up test LV %s: %v", lvName, err)
		}
	}()

	// Step 1: Create the LV
	t.Logf("Step 1: Creating LV %s", lvName)
	if err := lvm.CreateVolume(ctx, vol); err != nil {
		t.Fatalf("Failed to create test volume: %v", err)
	}

	// Verify LV exists
	devicePath := fmt.Sprintf("/dev/%s/%s", testVGName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		t.Fatalf("LVM logical volume %s does not exist: %v", devicePath, err)
	}

	// Step 2: Format the filesystem
	t.Logf("Step 2: Formatting filesystem on %s", devicePath)
	cmd := exec.Command("mkfs.ext4", "-F", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("Failed to create filesystem on %s: %v, output: %s", devicePath, err, string(output))
	}

	// Step 3: Create temporary directory for mount points
	tmpRoot, err := os.MkdirTemp("", "devbox-test-cleanup-")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tmpRoot)

	// Step 4: Create 5 mount point directories and mount the LV to all of them
	numMountPoints := 5
	mountPoints := make([]string, numMountPoints)
	for i := 0; i < numMountPoints; i++ {
		mountPoint := filepath.Join(tmpRoot, fmt.Sprintf("mount-point-%d", i))
		if err := os.MkdirAll(mountPoint, 0755); err != nil {
			t.Fatalf("Failed to create mount point directory %s: %v", mountPoint, err)
		}
		mountPoints[i] = mountPoint

		t.Logf("  Mounting %s to %s", devicePath, mountPoint)
		if err := syscall.Mount(devicePath, mountPoint, "ext4", 0, ""); err != nil {
			t.Fatalf("Failed to mount %s to %s: %v", devicePath, mountPoint, err)
		}
	}

	// Ensure all mounts are unmounted at the end (in case test fails)
	defer func() {
		for _, mountPoint := range mountPoints {
			if err := syscall.Unmount(mountPoint, 0); err != nil {
				t.Logf("Warning: Failed to unmount %s during cleanup: %v", mountPoint, err)
			}
		}
	}()

	// Step 5: Verify all mount points are mounted
	t.Logf("Step 5: Verifying all %d mount points are mounted", numMountPoints)
	foundMountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(foundMountPoints) != numMountPoints {
		t.Fatalf("Expected %d mount points, but found %d", numMountPoints, len(foundMountPoints))
	}
	t.Logf("Confirmed all %d mount points are mounted", numMountPoints)

	// Step 6: Test the cleanup logic - unmount all mount points
	t.Logf("Step 6: Testing cleanup logic to unmount all mount points")
	removedLvNames := []string{lvName}

	// Simulate the cleanup logic from cleanupDirectories
	for _, lvNameToCleanup := range removedLvNames {
		devicePathToCheck := fmt.Sprintf("/dev/%s/%s", snapshotter.lvmVgName, lvNameToCleanup)
		mountPointsToUnmount, err := lvm.FindMountPointByDevice(devicePathToCheck)
		if err != nil {
			t.Fatalf("Failed to find mount points for LV %s: %v", lvNameToCleanup, err)
		}

		t.Logf("Found %d mount points to unmount for LV %s", len(mountPointsToUnmount), lvNameToCleanup)

		unmountedCount := 0
		for _, mountPoint := range mountPointsToUnmount {
			t.Logf("  Attempting to unmount %s", mountPoint)
			if err := snapshotter.unmountLvm(ctx, mountPoint); err != nil {
				t.Errorf("Failed to unmount %s: %v", mountPoint, err)
			} else {
				unmountedCount++
				t.Logf("  Successfully unmounted %s", mountPoint)
			}
		}

		if unmountedCount != numMountPoints {
			t.Errorf("Expected to unmount %d mount points, but only unmounted %d", numMountPoints, unmountedCount)
		}
	}

	// Step 7: Verify all mount points are unmounted
	t.Logf("Step 7: Verifying all mount points are unmounted")
	foundMountPoints, err = lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed after unmount: %v", err)
	}
	if len(foundMountPoints) != 0 {
		t.Errorf("Expected 0 mount points after cleanup, but found %d: %v", len(foundMountPoints), foundMountPoints)
	} else {
		t.Logf("Successfully verified all mount points are unmounted")
	}

	// Step 8: Verify device can be removed (no mount points should allow removal)
	t.Logf("Step 8: Verifying device has no mount points (can be removed)")
	remainingMountPoints, err := lvm.FindMountPointByDevice(devicePath)
	if err != nil {
		t.Fatalf("findMountPointByDevice failed: %v", err)
	}
	if len(remainingMountPoints) > 0 {
		t.Errorf("Device still has %d mount points, cannot be safely removed: %v", len(remainingMountPoints), remainingMountPoints)
	} else {
		t.Logf("Device has no mount points, can be safely removed")
	}

	t.Logf("Test passed: cleanup logic successfully unmounted all %d mount points", numMountPoints)
}
