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

package blkiorun

import (
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConvertBFQToIOWeight(t *testing.T) {
	tests := []struct {
		name      string
		bfqWeight uint16
		expected  uint64
	}{
		{
			name:      "zero weight",
			bfqWeight: 0,
			expected:  0,
		},
		{
			name:      "minimum BFQ weight",
			bfqWeight: 10,
			expected:  1,
		},
		{
			name:      "normal BFQ weight",
			bfqWeight: 100,
			expected:  910,
		},
		{
			name:      "maximum BFQ weight",
			bfqWeight: 1000,
			expected:  10000,
		},
		{
			name:      "mid-range BFQ weight",
			bfqWeight: 500,
			expected:  4950,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertBFQToIOWeight(tt.bfqWeight)
			assert.Equal(t, tt.expected, result, "BFQ weight %d should convert to io.weight %d", tt.bfqWeight, tt.expected)
		})
	}
}

func TestConvertIOWeightToBFQ(t *testing.T) {
	tests := []struct {
		name     string
		ioWeight uint64
		expected uint16
	}{
		{
			name:     "zero weight",
			ioWeight: 0,
			expected: 0,
		},
		{
			name:     "minimum io.weight",
			ioWeight: 1,
			expected: 10,
		},
		{
			name:     "mid-range io.weight",
			ioWeight: 5000,
			expected: 504,
		},
		{
			name:     "maximum io.weight",
			ioWeight: 10000,
			expected: 1000,
		},
		{
			name:     "above maximum",
			ioWeight: 15000,
			expected: 1000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ConvertIOWeightToBFQ(tt.ioWeight)
			assert.Equal(t, tt.expected, result, "io.weight %d should convert to BFQ weight %d", tt.ioWeight, tt.expected)
		})
	}
}

func TestConversionRoundTrip(t *testing.T) {
	// Test that converting from BFQ to IO and back gives approximately the same value
	tests := []uint16{10, 50, 100, 200, 500, 1000}

	for _, bfqWeight := range tests {
		ioWeight := ConvertBFQToIOWeight(bfqWeight)
		converted := ConvertIOWeightToBFQ(ioWeight)

		// Allow for small rounding errors in conversion
		diff := int(bfqWeight) - int(converted)
		if diff < 0 {
			diff = -diff
		}
		assert.LessOrEqual(t, diff, 1, "Round-trip conversion for BFQ weight %d should be within 1", bfqWeight)
	}
}

func TestIsCgroupV2(t *testing.T) {
	result := isCgroupV2()
	// Just verify it doesn't panic and returns a boolean
	t.Logf("cgroups v2 enabled: %v", result)
}

func TestGetCurrentCgroupPath(t *testing.T) {
	path, err := getCurrentCgroupPath()
	if err != nil {
		t.Skipf("Skipping test: cgroups v2 not available: %v", err)
	}

	assert.True(t, strings.HasPrefix(path, "/sys/fs/cgroup"), "Path should start with /sys/fs/cgroup")

	info, err := os.Stat(path)
	if err == nil {
		assert.True(t, info.IsDir(), "Cgroup path should be a directory")
	}
}

func TestReadIOWeight(t *testing.T) {
	if !isCgroupV2() {
		t.Skip("Test requires cgroups v2")
	}

	cgroupPath, err := getCurrentCgroupPath()
	if err != nil {
		t.Skipf("Cannot get cgroup path: %v", err)
	}

	weight, err := readIOWeight(cgroupPath)
	if err != nil {
		t.Logf("readIOWeight failed (this may be expected): %v", err)
		return
	}

	assert.GreaterOrEqual(t, weight, uint16(BFQWeightMin), "Weight should be at least minimum")
	assert.LessOrEqual(t, weight, uint16(BFQWeightMax), "Weight should be at most maximum")
	t.Logf("Current IO weight: %d", weight)
}

func TestWriteAndReadIOWeight(t *testing.T) {
	if !isCgroupV2() {
		t.Skip("Test requires cgroups v2")
	}

	cgroupPath, err := getCurrentCgroupPath()
	if err != nil {
		t.Skipf("Cannot get cgroup path: %v", err)
	}

	// Read original weight
	originalWeight, err := readIOWeight(cgroupPath)
	if err != nil {
		t.Skipf("Cannot read IO weight: %v", err)
	}
	t.Logf("Original IO weight: %d", originalWeight)

	// Try to write a different weight
	testWeight := uint16(200)
	if originalWeight == testWeight {
		testWeight = 300
	}

	err = writeIOWeight(cgroupPath, testWeight)
	if err != nil {
		t.Skipf("Cannot write IO weight (may need permissions): %v", err)
	}

	// Verify weight was set
	currentWeight, err := readIOWeight(cgroupPath)
	if err != nil {
		t.Fatalf("Failed to read IO weight after setting: %v", err)
	}
	t.Logf("Current IO weight after setting: %d", currentWeight)

	// Allow small variance due to conversion
	diff := int(testWeight) - int(currentWeight)
	if diff < 0 {
		diff = -diff
	}
	assert.LessOrEqual(t, diff, 1, "IO weight should be approximately the set value")

	// Restore original weight
	err = writeIOWeight(cgroupPath, originalWeight)
	if err != nil {
		t.Logf("Warning: Failed to restore original IO weight: %v", err)
	}
}

func TestLocalWithConfigReal(t *testing.T) {
	if !isCgroupV2() {
		t.Skip("Test requires cgroups v2")
	}

	// Skip if not initialized (this is expected in unit tests)
	if !IsInitialized() {
		t.Skip("blkiorun not initialized")
	}

	result, err := LocalWithConfig(Config{Weight: 150}, func() (string, error) {
		return "test-result", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "test-result", result)
}

func TestGoWithConfigReal(t *testing.T) {
	if !isCgroupV2() {
		t.Skip("Test requires cgroups v2")
	}

	// Skip if not initialized
	if !IsInitialized() {
		t.Skip("blkiorun not initialized")
	}

	result, err := GoWithConfig(Config{Weight: 150}, func() (string, error) {
		return "test-result", nil
	})

	assert.NoError(t, err)
	assert.Equal(t, "test-result", result)
}

func TestSliceCgroupPath(t *testing.T) {
	tests := []struct {
		name     string
		expected string
	}{
		{
			name:     "simple.slice",
			expected: "/sys/fs/cgroup/simple.slice",
		},
		{
			name:     "containerdio.slice",
			expected: "/sys/fs/cgroup/containerdio.slice",
		},
		{
			name:     "containerd-io.slice",
			expected: "/sys/fs/cgroup/containerd.slice/containerd-io.slice",
		},
		{
			name:     "a-b-c.slice",
			expected: "/sys/fs/cgroup/a.slice/a-b.slice/a-b-c.slice",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := sliceCgroupPath(tt.name)
			assert.Equal(t, tt.expected, result)
		})
	}
}
