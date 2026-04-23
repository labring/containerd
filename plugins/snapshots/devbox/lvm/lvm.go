//go:build linux

/*
Copyright 2017 The Kubernetes Authors.

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

package lvm

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	apis "github.com/openebs/lvm-localpv/pkg/apis/openebs.io/lvm/v1alpha1"
	"github.com/pkg/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/klog/v2"
)

// LVM-related constants.
const (
	DevPath       = "/dev/"
	DevMapperPath = "/dev/mapper/"

	// MinExtentRoundOffSize represents minimum size (256Mi) to round off the
	// volume group size in case of thin pool provisioning.
	MinExtentRoundOffSize = 268435456

	// BlockCleanerCommand is the command used to clean filesystem signatures on a device.
	BlockCleanerCommand = "wipefs"
)

// LVM command-related constants.
const (
	VGCreate = "vgcreate"
	VGList   = "vgs"

	LVCreate = "lvcreate"
	LVRemove = "lvremove"
	LVExtend = "lvextend"
	LVList   = "lvs"

	PVList = "pvs"
	PVScan = "pvscan"

	YES        = "yes"
	LVThinPool = "thin-pool"
)

var (
	Enums = map[string][]string{
		"lv_permissions":       {"unknown", "writeable", "read-only", "read-only-override"},
		"lv_when_full":         {"error", "queue"},
		"raid_sync_action":     {"idle", "frozen", "resync", "recover", "check", "repair"},
		"lv_health_status":     {"", "partial", "refresh needed", "mismatches exist"},
		"vg_allocation_policy": {"normal", "contiguous", "cling", "anywhere", "inherited"},
		"vg_permissions":       {"writeable", "read-only"},
	}
)

// LogicalVolume specifies attributes of a given LV that exists on the node.
type LogicalVolume struct {
	Name                string
	FullName            string
	UUID                string
	Size                int64
	Path                string
	DMPath              string
	Device              string
	VGName              string
	SegType             string
	Permission          int
	BehaviourWhenFull   int
	HealthStatus        int
	RaidSyncAction      int
	ActiveStatus        string
	Host                string
	PoolName            string
	UsedSizePercent     float64
	MetadataSize        int64
	MetadataUsedPercent float64
	SnapshotUsedPercent float64
}

// PhysicalVolume specifies attributes of a given PV that exists on the node.
type PhysicalVolume struct {
	Name         string
	UUID         string
	Size         resource.Quantity
	DeviceSize   resource.Quantity
	MetadataSize resource.Quantity
	MetadataFree resource.Quantity
	Free         resource.Quantity
	Used         resource.Quantity
	Allocatable  string
	Missing      string
	InUse        string
	VGName       string
}

// ExecError holds process output along with the underlying execution error.
type ExecError struct {
	Output []byte
	Err    error
}

// Error implements the error interface.
func (e *ExecError) Error() string {
	return fmt.Sprintf("%v - %v", string(e.Output), e.Err)
}

func NewExecError(output []byte, err error) error {
	if err == nil {
		return nil
	}
	return &ExecError{
		Output: output,
		Err:    err,
	}
}

// buildLVMCreateArgs returns lvcreate arguments for the volume.
func buildLVMCreateArgs(ctx context.Context, vol *apis.LVMVolume) []string {
	return buildLVMCreateArgsWithThinPool(ctx, vol, lvThinExists(ctx, vol.Spec.VolGroup, vol.Spec.ThinProvision))
}

func buildLVMCreateArgsWithThinPool(ctx context.Context, vol *apis.LVMVolume, thinPoolExists bool) []string {
	var args []string

	volume := vol.Name
	size := vol.Spec.Capacity + "b"
	pool := vol.Spec.ThinProvision

	if len(vol.Spec.Capacity) != 0 {
		if strings.TrimSpace(vol.Spec.ThinProvision) == "" {
			args = append(args, "-L", size)
		} else if !thinPoolExists {
			args = append(args, "-L", getThinPoolSize(ctx, vol.Spec.VolGroup, vol.Spec.Capacity))
		}
	}

	if strings.TrimSpace(vol.Spec.ThinProvision) != "" {
		args = append(args, "-T", vol.Spec.VolGroup+"/"+pool, "-V", size)
	}

	args = append(args, "-n", volume)
	if strings.TrimSpace(vol.Spec.ThinProvision) == "" {
		args = append(args, vol.Spec.VolGroup)
	}
	args = append(args, "-y")

	return args
}

// CreateVolume creates an LVM volume.
func CreateVolume(ctx context.Context, vol *apis.LVMVolume) error {
	args := buildLVMCreateArgs(ctx, vol)
	klog.Infof("creating lvm volume %q with args %v", vol.Name, args)

	output, _, err := RunCommandSplit(ctx, LVCreate, args...)
	if err != nil {
		wrappedErr := errors.Wrapf(NewExecError(output, err), "failed to create lvm volume %q", vol.Name)

		volume := vol.Spec.VolGroup + "/" + vol.Name
		if cleanupErr := DestroyVolume(ctx, vol); cleanupErr != nil {
			klog.Warningf("lvm: failed to cleanup volume %s: %v", volume, cleanupErr)
		} else {
			klog.Infof("lvm: successfully cleaned up failed volume %s", volume)
		}

		return wrappedErr
	}
	return nil
}

// ResizeLVMVolume resizes an LVM volume.
func ResizeLVMVolume(ctx context.Context, vol *apis.LVMVolume, resizeFS bool) error {
	if vol == nil {
		return fmt.Errorf("volume is nil")
	}
	if vol.Spec.Capacity == "" {
		return fmt.Errorf("volume capacity is empty")
	}

	targetSize := vol.Spec.Capacity + "b"
	lvPath := filepath.Join(DevPath, vol.Spec.VolGroup, vol.Name)

	args := []string{"-L", targetSize}
	if resizeFS {
		args = append(args, "-r")
	}
	args = append(args, lvPath)

	klog.Infof("resizing lvm volume %q with args %v", vol.Name, args)

	output, _, err := RunCommandSplit(ctx, LVExtend, args...)
	if err != nil {
		return errors.Wrapf(NewExecError(output, err), "failed to resize lvm volume %q", vol.Name)
	}
	return nil
}

// DestroyVolume removes an LVM volume.
func DestroyVolume(ctx context.Context, vol *apis.LVMVolume) error {
	if vol == nil {
		return fmt.Errorf("volume is nil")
	}
	lvPath := filepath.Join(DevPath, vol.Spec.VolGroup, vol.Name)
	args := []string{"-f", lvPath}

	klog.Infof("destroying lvm volume %q with args %v", vol.Name, args)

	output, _, err := RunCommandSplit(ctx, LVRemove, args...)
	if err != nil {
		return errors.Wrapf(NewExecError(output, err), "failed to destroy lvm volume %q", vol.Name)
	}
	return nil
}

// ForceDestroyVolume force removes an LVM volume.
func ForceDestroyVolume(ctx context.Context, vol *apis.LVMVolume) error {
	if vol == nil {
		return fmt.Errorf("volume is nil")
	}
	lvPath := filepath.Join(DevPath, vol.Spec.VolGroup, vol.Name)
	args := []string{"-f", "-y", lvPath}

	klog.Infof("force destroying lvm volume %q with args %v", vol.Name, args)

	output, _, err := RunCommandSplit(ctx, LVRemove, args...)
	if err != nil {
		return errors.Wrapf(NewExecError(output, err), "failed to force destroy lvm volume %q", vol.Name)
	}
	return nil
}

// ListLVMLogicalVolumeByVG lists logical volumes in a volume group.
// If thinPoolName is non-empty, only volumes belonging to that pool are returned.
func ListLVMLogicalVolumeByVG(ctx context.Context, vgName, thinPoolName string) ([]LogicalVolume, error) {
	args := []string{
		"--reportformat", "json",
		"--units", "b",
		"--nosuffix",
		"-o", strings.Join([]string{
			LVName,
			LVFullName,
			LVUUID,
			LVPath,
			LVDmPath,
			LVActive,
			LVSize,
			LVMetadataSize,
			LVSegtype,
			LVHost,
			LVPool,
			LVPermissions,
			LVWhenFull,
			LVHealthStatus,
			RaidSyncAction,
			LVDataPercent,
			LVMetadataPercent,
			LVSnapPercent,
			VGName,
		}, ","),
		vgName,
	}

	output, _, err := RunCommandSplit(ctx, LVList, args...)
	if err != nil {
		return nil, errors.Wrap(NewExecError(output, err), "failed to list logical volumes")
	}

	type lvReport struct {
		Report []struct {
			LV []map[string]string `json:"lv"`
		} `json:"report"`
	}

	var report lvReport
	if err := json.Unmarshal(output, &report); err != nil {
		return nil, errors.Wrap(err, "failed to decode lvs json output")
	}

	var result []LogicalVolume
	for _, rep := range report.Report {
		for _, item := range rep.LV {
			lv, err := parseLogicalVolume(item)
			if err != nil {
				klog.Warningf("failed to parse LV %q, skipping: %v", item[LVName], err)
				continue
			}
			if thinPoolName != "" && lv.PoolName != thinPoolName && lv.Name != thinPoolName {
				continue
			}
			deviceName, err := getLvDeviceName(lv.Path)
			if err != nil {
				klog.Warningf("failed to get device name for LV %s, skipping: %v", lv.Name, err)
				continue
			}
			lv.Device = deviceName
			result = append(result, lv)
		}
	}

	return result, nil
}

func lvThinExists(ctx context.Context, vgName, thinPoolName string) bool {
	if strings.TrimSpace(thinPoolName) == "" {
		return false
	}

	output, _, err := RunCommandSplit(ctx, LVList, vgName+"/"+thinPoolName, "--noheadings", "-o", LVName)
	if err != nil {
		klog.Warningf("failed to check thin pool %q in vg %q: %v", thinPoolName, vgName, err)
		return false
	}

	return strings.TrimSpace(string(output)) == thinPoolName
}

func getThinPoolSize(ctx context.Context, vgName, requested string) string {
	reqBytes, err := strconv.ParseInt(requested, 10, 64)
	if err != nil || reqBytes <= 0 {
		return requested + "b"
	}

	rounded := reqBytes
	if rounded < MinExtentRoundOffSize {
		rounded = MinExtentRoundOffSize
	}
	return strconv.FormatInt(rounded, 10) + "b"
}

func parseLogicalVolume(item map[string]string) (LogicalVolume, error) {
	var lv LogicalVolume
	var err error

	lv.Name = item[LVName]
	lv.FullName = item[LVFullName]
	lv.UUID = item[LVUUID]
	lv.Path = item[LVPath]
	lv.DMPath = item[LVDmPath]
	lv.ActiveStatus = item[LVActive]
	lv.SegType = item[LVSegtype]
	lv.Host = item[LVHost]
	lv.PoolName = item[LVPool]
	lv.VGName = item[VGName]

	if lv.Path != "" {
		lv.Device = lv.Path
	} else if lv.DMPath != "" {
		lv.Device = lv.DMPath
	}

	if lv.Size, err = parseInt64Field(item[LVSize]); err != nil {
		return lv, errors.Wrap(err, "failed to parse lv size")
	}
	if lv.MetadataSize, err = parseInt64Field(item[LVMetadataSize]); err != nil {
		return lv, errors.Wrap(err, "failed to parse lv metadata size")
	}
	if lv.UsedSizePercent, err = parseFloat64Field(item[LVDataPercent]); err != nil {
		return lv, errors.Wrap(err, "failed to parse lv data percent")
	}
	if lv.MetadataUsedPercent, err = parseFloat64Field(item[LVMetadataPercent]); err != nil {
		return lv, errors.Wrap(err, "failed to parse lv metadata percent")
	}
	if lv.SnapshotUsedPercent, err = parseFloat64Field(item[LVSnapPercent]); err != nil {
		return lv, errors.Wrap(err, "failed to parse lv snapshot percent")
	}

	lv.Permission = enumIndex("lv_permissions", item[LVPermissions])
	lv.BehaviourWhenFull = enumIndex("lv_when_full", item[LVWhenFull])
	lv.HealthStatus = enumIndex("lv_health_status", item[LVHealthStatus])
	lv.RaidSyncAction = enumIndex("raid_sync_action", item[RaidSyncAction])

	return lv, nil
}

func enumIndex(name, value string) int {
	values, ok := Enums[name]
	if !ok {
		return -1
	}
	for i, v := range values {
		if v == value {
			return i
		}
	}
	return -1
}

func parseInt64Field(v string) (int64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	return strconv.ParseInt(v, 10, 64)
}

func parseFloat64Field(v string) (float64, error) {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0, nil
	}
	return strconv.ParseFloat(v, 64)
}

func getLvDeviceName(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("invalid device path: %s", path)
	}

	dmPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		klog.Errorf("failed to resolve device mapper from lv path %v: %v", path, err)
		return "", err
	}

	_, file := filepath.Split(dmPath)
	if file == "" {
		return "", fmt.Errorf("invalid device path: %s", dmPath)
	}
	return file, nil
}

// RunCommandSplit is a wrapper function to run a command with timeout and
// receive its STDERR and STDOUT streams separately.
func RunCommandSplit(ctx context.Context, command string, args ...string) ([]byte, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, CommandTimeout)
	defer cancel()

	var cmdStdout bytes.Buffer
	var cmdStderr bytes.Buffer

	cmd := exec.CommandContext(ctx, command, args...)
	cmd.Stdout = &cmdStdout
	cmd.Stderr = &cmdStderr
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}
	cmd.Cancel = func() error {
		klog.Warningf("lvm: command %s %v timed out, sending SIGTERM", command, args)
		if cmd.Process != nil {
			pgid := cmd.Process.Pid
			return syscall.Kill(-pgid, syscall.SIGTERM)
		}
		return nil
	}
	cmd.WaitDelay = CommandGraceTimeout

	err := cmd.Run()
	output := cmdStdout.Bytes()
	errorOutput := cmdStderr.Bytes()

	if len(errorOutput) > 0 {
		klog.Warningf("lvm: said into stderr: %s", errorOutput)
	}

	if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) {
		if cmd.Process != nil {
			pgid := cmd.Process.Pid
			_ = syscall.Kill(-pgid, syscall.SIGKILL)
		}
	}

	return output, errorOutput, err
}

// RunCommand is a small helper kept for parity with the original helper set.
func RunCommand(ctx context.Context, name string, args ...string) ([]byte, error) {
	out, _, err := RunCommandSplit(ctx, name, args...)
	return out, err
}
