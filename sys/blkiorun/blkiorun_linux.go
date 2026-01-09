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
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	systemdDbus "github.com/coreos/go-systemd/v22/dbus"
	"github.com/godbus/dbus/v5"

	"github.com/containerd/log"
)

const (
	// DefaultSliceName is the default systemd slice name
	DefaultSliceName = "containerdio.slice"

	// SystemdTimeout is the timeout for systemd operations
	SystemdTimeout = 5 * time.Second

	// sliceWaitRetries is the number of retries when waiting for slice creation
	sliceWaitRetries = 10

	// sliceWaitInterval is the interval between retries
	sliceWaitInterval = 100 * time.Millisecond

	// BFQWeightMin is the minimum BFQ IO weight
	BFQWeightMin uint16 = 10
	// BFQWeightMax is the maximum BFQ IO weight
	BFQWeightMax uint16 = 1000
	// BFQWeightDefault is the default BFQ IO weight
	BFQWeightDefault uint16 = 100

	// IOWeightMin is the minimum io.weight value
	IOWeightMin uint64 = 1
	// IOWeightMax is the maximum io.weight value
	IOWeightMax uint64 = 10000

	// Conversion factors for BFQ <-> io.weight
	// io.weight = 1 + (bfq - 10) * 9999 / 990
	// bfq = 10 + (io.weight - 1) * 990 / 9999
	conversionNumerator   = 9999
	conversionDenominator = 990
)

var (
	state     *globalState
	stateOnce sync.Once
	counter   uint64

	// BFQ support detection
	bfqSupported     bool
	bfqSupportedOnce sync.Once

	cgroupV2     bool
	cgroupV2Once sync.Once

	// ErrNotInitialized is returned when blkiorun is not initialized
	ErrNotInitialized = errors.New("blkiorun not initialized, call Init first")
)

// globalState stores the IO weight control state
type globalState struct {
	slicePath      string // cgroup path for the slice
	containerdPath string // containerd's cgroup path
	config         Config // runtime config
	initialized    bool
}

// Init initializes block IO weight control for containerd.
// Parameters:
// - cfg: Runtime config containing IO weight (10-1000). Set weight to 0 to disable.
// - slicePath: Path to existing cgroup (optional, uses systemd if empty)
// - sliceName: Systemd slice name (default: "containerdio.slice")
func Init(cfg Config, slicePath, sliceName string) error {
	var initErr error

	stateOnce.Do(func() {
		s := &globalState{}
		defer func() { state = s }()

		if cfg.Weight == 0 {
			log.L.Debug("blkiorun: disabled (weight=0)")
			return
		}

		if cfg.Weight < BFQWeightMin || cfg.Weight > BFQWeightMax {
			log.L.Warnf("blkiorun: weight %d out of range [%d, %d], disabled", cfg.Weight, BFQWeightMin, BFQWeightMax)
			return
		}

		s.config = cfg
		log.L.Infof("blkiorun: weight configured: %d", cfg.Weight)

		if !isCgroupV2() {
			log.L.Warn("blkiorun: cgroups v2 not available")
		}

		// Get containerd's cgroup path
		var err error
		s.containerdPath, err = getCurrentCgroupPath()
		if err != nil {
			initErr = fmt.Errorf("failed to get cgroup path: %w", err)
			return
		}
		log.L.Debugf("blkiorun: containerd cgroup: %s", s.containerdPath)

		var cgroupPath string
		if slicePath != "" {
			cgroupPath = slicePath
			log.L.Debugf("blkiorun: using configured path: %s", cgroupPath)
		} else {
			// Create systemd slice
			if sliceName == "" {
				sliceName = DefaultSliceName
			}
			if !strings.HasSuffix(sliceName, ".slice") {
				sliceName += ".slice"
			}

			ctx, cancel := context.WithTimeout(context.Background(), SystemdTimeout)
			defer cancel()

			if err := createSlice(ctx, sliceName); err != nil {
				log.L.WithError(err).Warnf("blkiorun: failed to create slice %s", sliceName)
				return
			}

			cgroupPath = sliceCgroupPath(sliceName)
			for i := 0; i < sliceWaitRetries; i++ {
				if _, err := os.Stat(cgroupPath); err == nil {
					break
				}
				time.Sleep(sliceWaitInterval)
			}
		}

		// Verify io.weight is available
		if _, err := os.Stat(filepath.Join(cgroupPath, "io.weight")); os.IsNotExist(err) {
			log.L.Warn("blkiorun: io.weight not available")
			return
		}

		// Enable io controller for children
		if err := enableIOController(cgroupPath); err != nil {
			log.L.WithError(err).Warn("blkiorun: failed to enable io controller")
			return
		}

		// Apply default IO weight to slice
		if err := applyConfig(cgroupPath, cfg); err != nil {
			log.L.WithError(err).Warn("blkiorun: failed to apply config")
			return
		}

		s.slicePath = cgroupPath
		s.initialized = true
		log.L.Infof("blkiorun: initialized at %s with weight %d", cgroupPath, cfg.Weight)
	})

	return initErr
}

// IsInitialized returns true if blkiorun is initialized
func IsInitialized() bool {
	return state != nil && state.initialized
}

// Go executes fn in a new goroutine with configured IO weight.
func Go[T any](fn func() (T, error)) (T, error) {
	return GoWithConfig(state.config, fn)
}

// GoWithConfig executes fn in a new goroutine with specified config.
func GoWithConfig[T any](cfg Config, fn func() (T, error)) (T, error) {
	type result struct {
		value T
		err   error
	}

	ch := make(chan result, 1)
	go func() {
		v, err := LocalWithConfig(cfg, fn)
		ch <- result{v, err}
	}()

	res := <-ch
	return res.value, res.err
}

// Local executes fn in current goroutine with configured IO weight.
func Local[T any](fn func() (T, error)) (T, error) {
	return LocalWithConfig(state.config, fn)
}

// LocalWithConfig executes fn in current goroutine with specified config.
func LocalWithConfig[T any](cfg Config, fn func() (T, error)) (T, error) {
	if cfg.Weight == 0 || !IsInitialized() {
		return fn()
	}

	runtime.LockOSThread()
	defer runtime.UnlockOSThread()

	cg, err := createCgroup(cfg)
	if err != nil {
		log.L.WithError(err).Debug("blkiorun: failed to create cgroup")
		return fn()
	}
	defer cg.destroy()

	if err := cg.enter(); err != nil {
		log.L.WithError(err).Debug("blkiorun: failed to enter cgroup")
		return fn()
	}
	defer cg.leave()

	return fn()
}

// cgroup represents a temporary cgroup for IO weight control
type cgroup struct {
	path string
}

func createCgroup(cfg Config) (*cgroup, error) {
	if state == nil || !state.initialized {
		return nil, ErrNotInitialized
	}

	id := atomic.AddUint64(&counter, 1)
	path := filepath.Join(state.slicePath, fmt.Sprintf("blkio-%d-%d", os.Getpid(), id))

	if err := os.Mkdir(path, 0755); err != nil {
		return nil, err
	}

	cg := &cgroup{path: path}
	if err := applyConfig(cg.path, cfg); err != nil {
		cg.destroy()
		return nil, err
	}

	return cg, nil
}

func applyConfig(path string, cfg Config) error {
	if cfg.Weight > 0 {
		return writeIOWeight(path, cfg.Weight)
	}
	return nil
}

// isBFQSupported checks if BFQ IO scheduler is available in the given cgroup path.
func isBFQSupported(cgroupPath string) bool {
	bfqSupportedOnce.Do(func() {
		bfqPath := filepath.Join(cgroupPath, "io.bfq.weight")
		if _, err := os.Stat(bfqPath); err == nil {
			bfqSupported = true
		}
	})
	return bfqSupported
}

// readIOWeight reads the current IO weight from cgroups.
// Returns BFQ weight if available, otherwise io.weight converted to BFQ range.
func readIOWeight(cgroupPath string) (uint16, error) {
	// Try BFQ first
	if isBFQSupported(cgroupPath) {
		data, err := os.ReadFile(filepath.Join(cgroupPath, "io.bfq.weight"))
		if err == nil {
			fields := strings.Fields(string(bytes.TrimSpace(data)))
			if len(fields) > 0 {
				weight, err := strconv.ParseUint(fields[len(fields)-1], 10, 16)
				if err == nil {
					return uint16(weight), nil
				}
			}
		}
	}

	// Fallback to io.weight
	data, err := os.ReadFile(filepath.Join(cgroupPath, "io.weight"))
	if err != nil {
		return BFQWeightDefault, nil // Return default if reading fails
	}

	fields := strings.Fields(string(bytes.TrimSpace(data)))
	if len(fields) > 0 {
		ioWeight, err := strconv.ParseUint(fields[len(fields)-1], 10, 64)
		if err == nil {
			return ConvertIOWeightToBFQ(ioWeight), nil
		}
	}

	return BFQWeightDefault, nil
}

// writeIOWeight writes IO weight to cgroups.
// Uses io.bfq.weight if available, otherwise io.weight with conversion.
func writeIOWeight(cgroupPath string, weight uint16) error {
	// Try BFQ first
	if isBFQSupported(cgroupPath) {
		bfqPath := filepath.Join(cgroupPath, "io.bfq.weight")
		if err := os.WriteFile(bfqPath, []byte(strconv.FormatUint(uint64(weight), 10)), 0644); err == nil {
			return nil
		}
	}

	// Fallback to io.weight with conversion
	ioWeight := ConvertBFQToIOWeight(weight)
	return os.WriteFile(filepath.Join(cgroupPath, "io.weight"), []byte(strconv.FormatUint(ioWeight, 10)), 0644)
}

func (cg *cgroup) enter() error {
	return os.WriteFile(filepath.Join(cg.path, "cgroup.procs"), []byte(strconv.Itoa(syscall.Gettid())), 0644)
}

func (cg *cgroup) leave() error {
	if state == nil {
		return nil
	}
	return os.WriteFile(filepath.Join(state.containerdPath, "cgroup.procs"), []byte(strconv.Itoa(syscall.Gettid())), 0644)
}

func (cg *cgroup) destroy() {
	os.Remove(cg.path)
}

// Helper functions

func isCgroupV2() bool {
	cgroupV2Once.Do(func() {
		cgroupV2 = checkCgroupV2()
	})
	return cgroupV2
}

func checkCgroupV2() bool {
	stat, err := os.Stat("/sys/fs/cgroup/cgroup.controllers")
	return err == nil && !stat.IsDir()
}

func getCurrentCgroupPath() (string, error) {
	data, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return "", err
	}

	scanner := bufio.NewScanner(bytes.NewReader(data))
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), ":", 3)
		if len(parts) == 3 && parts[0] == "0" {
			p := parts[2]
			if p == "" {
				p = "/"
			}
			return filepath.Join("/sys/fs/cgroup", p), nil
		}
	}
	return "", errors.New("cgroup v2 path not found")
}

func enableIOController(path string) error {
	ctrl := filepath.Join(path, "cgroup.subtree_control")
	data, _ := os.ReadFile(ctrl)
	if strings.Contains(string(data), "io") {
		return nil
	}
	return os.WriteFile(ctrl, []byte("+io"), 0644)
}

func createSlice(ctx context.Context, name string) error {
	conn, err := systemdDbus.NewWithContext(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	props := []systemdDbus.Property{
		systemdDbus.PropDescription("Containerd IO Weight Control"),
		{Name: "DefaultDependencies", Value: dbus.MakeVariant(false)},
		{Name: "IOAccounting", Value: dbus.MakeVariant(true)},
	}

	ch := make(chan string, 1)
	_, err = conn.StartTransientUnitContext(ctx, name, "replace", props, ch)
	if err != nil {
		if strings.Contains(err.Error(), "already") || strings.Contains(err.Error(), "loaded") {
			return nil
		}
		return err
	}

	select {
	case <-ch:
	case <-time.After(SystemdTimeout):
	case <-ctx.Done():
		return ctx.Err()
	}
	return nil
}

func sliceCgroupPath(name string) string {
	n := strings.TrimSuffix(name, ".slice")
	parts := strings.Split(n, "-")
	if len(parts) == 1 {
		return filepath.Join("/sys/fs/cgroup", name)
	}
	var pp []string
	for i := 1; i <= len(parts); i++ {
		pp = append(pp, strings.Join(parts[:i], "-")+".slice")
	}
	return filepath.Join("/sys/fs/cgroup", filepath.Join(pp...))
}

// ConvertBFQToIOWeight converts BFQ weight (10-1000) to io.weight (1-10000).
func ConvertBFQToIOWeight(bfqWeight uint16) uint64 {
	if bfqWeight == 0 {
		return 0
	}
	return uint64(IOWeightMin) + (uint64(bfqWeight)-uint64(BFQWeightMin))*conversionNumerator/conversionDenominator
}

// ConvertIOWeightToBFQ converts io.weight (1-10000) back to BFQ weight (10-1000).
func ConvertIOWeightToBFQ(ioWeight uint64) uint16 {
	if ioWeight == 0 {
		return 0
	}
	if ioWeight <= IOWeightMin {
		return BFQWeightMin
	}
	if ioWeight >= IOWeightMax {
		return BFQWeightMax
	}
	return BFQWeightMin + uint16((ioWeight-IOWeightMin)*conversionDenominator/conversionNumerator)
}
