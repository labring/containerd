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
	"strconv"
	"strings"
	"syscall"

	cp "github.com/otiai10/copy"

	"github.com/containerd/containerd/v2/core/mount"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/plugins/snapshots/devbox/lvm"
	"github.com/containerd/containerd/v2/plugins/snapshots/devbox/storage"
	"github.com/containerd/containerd/v2/plugins/snapshots/overlay/overlayutils"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	apis "github.com/openebs/lvm-localpv/pkg/apis/openebs.io/lvm/v1alpha1"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// upperdirKey is a key of an optional label to each snapshot.
// This optional label of a snapshot contains the location of "upperdir" where
// the change set between this snapshot and its parent is stored.
const upperdirKey = "containerd.io/snapshot/overlay.upperdir"

const newLayerLimitKey = "containerd.io/snapshot/devbox-storage-limit"
const devboxContentIDKey = "containerd.io/snapshot/devbox-content-id"
const privateImageKey = "containerd.io/snapshot/devbox-init"
const removeContentIDKey = "containerd.io/snapshot/devbox-remove-content-id"
const unmountLvm = "containerd.io/snapshot/devbox-unmount-lvm"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	AsyncRemove   bool
	UpperdirLabel bool
	ms            MetaStore
	lvmVgName     string
	ThinPoolName  string
	mountOptions  []string
}

// Opt is an option to configure the overlay snapshotter
type Opt func(config *SnapshotterConfig) error

// AsynchronousRemove defers removal of filesystem content until
// the Cleanup method is called. Removals will make the snapshot
// referred to by the key unavailable and make the key immediately
// available for re-use.
func AsynchronousRemove(config *SnapshotterConfig) error {
	config.AsyncRemove = true
	return nil
}

// WithUpperdirLabel adds as an optional label
// "containerd.io/snapshot/overlay.upperdir". This stores the location
// of the upperdir that contains the changeset between the labelled
// snapshot and its parent.
func WithUpperdirLabel(config *SnapshotterConfig) error {
	config.UpperdirLabel = true
	return nil
}

// WithLvmVgName sets the name of the LVM volume group to use.
func WithLvmVgName(name string) Opt {
	return func(config *SnapshotterConfig) error {
		config.lvmVgName = name
		return nil
	}
}

// WithThinPoolName sets the thin pool name to use.
func WithThinPoolName(name string) Opt {
	return func(config *SnapshotterConfig) error {
		config.ThinPoolName = name
		return nil
	}
}

// WithMountOptions defines the default mount options used for the overlay mount.
// NOTE: Options are not applied to bind mounts.
func WithMountOptions(options []string) Opt {
	return func(config *SnapshotterConfig) error {
		config.mountOptions = append(config.mountOptions, options...)
		return nil
	}
}

type MetaStore interface {
	TransactionContext(ctx context.Context, writable bool) (context.Context, storage.Transactor, error)
	WithTransaction(ctx context.Context, writable bool, fn storage.TransactionCallback) error
	Close() error
}

// WithMetaStore allows the MetaStore to be created outside the snapshotter
// and passed in.
func WithMetaStore(ms MetaStore) Opt {
	return func(config *SnapshotterConfig) error {
		config.ms = ms
		return nil
	}
}

type Snapshotter struct {
	root          string
	ms            MetaStore
	asyncRemove   bool
	upperdirLabel bool
	lvmVgName     string
	ThinPoolName  string
	UseThinPool   bool
	options       []string
}

type devboxLVMPlan struct {
	reuseExisting  bool
	resizeExisting bool
	createNew      bool
	contentID      string
	useLimit       string
	existingLVName string
}

func planDevboxLVM(
	contentID, useLimit, existingLVName string,
	contentIDProvided, storageLimitProvided bool,
) devboxLVMPlan {
	plan := devboxLVMPlan{
		contentID:      strings.TrimSpace(contentID),
		useLimit:       strings.TrimSpace(useLimit),
		existingLVName: strings.TrimSpace(existingLVName),
	}

	if !contentIDProvided || plan.contentID == "" {
		return plan
	}
	if plan.existingLVName != "" {
		plan.reuseExisting = true
		plan.resizeExisting = storageLimitProvided && plan.useLimit != ""
		return plan
	}
	if storageLimitProvided && plan.useLimit != "" {
		plan.createNew = true
	}
	return plan
}

// NewSnapshotter returns a Snapshotter which uses overlayfs. The overlayfs
// diffs are stored under the provided root. A metadata file is stored under
// the root.
func NewSnapshotter(root string, opts ...Opt) (snapshots.Snapshotter, error) {
	var config SnapshotterConfig
	for _, opt := range opts {
		if err := opt(&config); err != nil {
			return nil, err
		}
	}

	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	supportsDType, err := fs.SupportsDType(root)
	if err != nil {
		return nil, err
	}
	if !supportsDType {
		return nil, fmt.Errorf("%s does not support d_type. If the backing filesystem is xfs, please reformat with ftype=1 to enable d_type support", root)
	}
	if config.ms == nil {
		config.ms, err = storage.NewMetaStore(filepath.Join(root, "metadata.db"))
		if err != nil {
			return nil, err
		}
	}

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0o700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if !hasOption(config.mountOptions, "userxattr", false) {
		userxattr, err := overlayutils.NeedsUserXAttr(root)
		if err != nil {
			logrus.WithError(err).Warnf("cannot detect whether \"userxattr\" option needs to be used, assuming to be %v", userxattr)
		}
		if userxattr {
			config.mountOptions = append(config.mountOptions, "userxattr")
		}
	}

	if !hasOption(config.mountOptions, "index", false) && supportsIndex() {
		config.mountOptions = append(config.mountOptions, "index=off")
	}

	return &Snapshotter{
		root:          root,
		ms:            config.ms,
		asyncRemove:   config.AsyncRemove,
		upperdirLabel: config.UpperdirLabel,
		lvmVgName:     config.lvmVgName,
		ThinPoolName:  config.ThinPoolName,
		options:       config.mountOptions,
	}, nil
}

func hasOption(options []string, key string, hasValue bool) bool {
	for _, option := range options {
		if hasValue {
			if strings.HasPrefix(option, key) && len(option) > len(key) && option[len(key)] == '=' {
				return true
			}
		} else if option == key {
			return true
		}
	}
	return false
}

// Stat returns the info for an active or committed snapshot by name or key.
func (o *Snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
	var id string
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, _, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return info, err
	}

	if o.upperdirLabel {
		if info.Labels == nil {
			info.Labels = make(map[string]string)
		}
		info.Labels[upperdirKey] = o.upperPath(id)
	}
	return info, nil
}

func (o *Snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		if value, ok := info.Labels[unmountLvm]; ok && value == "true" {
			mountPath, err := storage.SetUnmountedWithKey(ctx, info.Name)
			if err != nil {
				return fmt.Errorf("failed to set devbox content status to unmounted: %w", err)
			}
			return o.unmountLvm(ctx, mountPath)
		}

		if value, ok := info.Labels[removeContentIDKey]; ok {
			return storage.SetDevboxContentStatusRemoved(ctx, value)
		}

		newInfo, err = storage.UpdateInfo(ctx, info, fieldpaths...)
		if err != nil {
			return err
		}

		if o.upperdirLabel {
			id, _, _, err := storage.GetInfo(ctx, newInfo.Name)
			if err != nil {
				return err
			}
			if newInfo.Labels == nil {
				newInfo.Labels = make(map[string]string)
			}
			newInfo.Labels[upperdirKey] = o.upperPath(id)
		}
		return nil
	})
	return newInfo, err
}

// Usage returns the resources taken by the snapshot identified by key.
func (o *Snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
	var (
		usage snapshots.Usage
		info  snapshots.Info
		id    string
	)
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		id, info, usage, err = storage.GetInfo(ctx, key)
		return err
	}); err != nil {
		return usage, err
	}

	if info.Kind == snapshots.KindActive {
		upperPath := o.upperPath(id)
		du, err := fs.DiskUsage(ctx, upperPath)
		if err != nil {
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

func (o *Snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	log.G(ctx).Debugf("Prepare called with key=%s parent=%s", key, parent)
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *Snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key.
func (o *Snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var s storage.Snapshot
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		contentID, _, err := storage.GetSnapshotDevboxInfo(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get devbox content ID for snapshot %s: %w", key, err)
		}
		if contentID != "" {
			lvName, err := storage.GetDevboxLvName(ctx, contentID, key)
			if err != nil {
				return fmt.Errorf("failed to get devbox logical volume name for content ID %s: %w", contentID, err)
			}
			if lvName == "" {
				return fmt.Errorf("logical volume name for content ID %s is empty", contentID)
			}
		}

		s, err = storage.GetSnapshot(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to get active mount: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return o.mounts(s), nil
}

func (o *Snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		id, _, _, err := storage.GetInfo(ctx, key)
		if err != nil {
			return err
		}

		usage, err := fs.DiskUsage(ctx, o.upperPath(id))
		if err != nil {
			return err
		}

		if _, err = storage.CommitActive(ctx, key, name, snapshots.Usage(usage), opts...); err != nil {
			return fmt.Errorf("failed to commit snapshot %s: %w", key, err)
		}
		return nil
	})
}

// Remove abandons the snapshot identified by key.
func (o *Snapshotter) RemoveDir(ctx context.Context, dir string) {
	isMounted, err := isMountPoint(dir)
	if err != nil {
		log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to check if path is a mount point")
		return
	}
	if isMounted {
		if err1 := o.unmountLvm(ctx, dir); err1 != nil {
			log.G(ctx).WithError(err1).WithField("path", dir).Warn("failed to unmount directory")
			return
		}
		if err1 := os.Remove(dir); err1 != nil {
			log.G(ctx).WithError(err1).WithField("path", dir).Warn("failed to remove directory")
			return
		}
	} else {
		if err1 := os.RemoveAll(dir); err1 != nil {
			log.G(ctx).WithError(err1).WithField("path", dir).Warn("failed to remove directory")
			return
		}
	}
}

func (o *Snapshotter) Remove(ctx context.Context, key string) (err error) {
	var (
		removals        []string
		removedContents []storage.RemovedDevboxContent
	)

	log.G(ctx).WithFields(logrus.Fields{
		"key":         key,
		"snapshotter": "devbox",
	}).Warnf("[DEVBOX-REMOVE-TRACE] ========== Remove function called ==========")
	defer func() {
		if err == nil {
			for _, dir := range removals {
				o.RemoveDir(ctx, dir)
			}
			for _, content := range removedContents {
				err := o.removeLv(ctx, content.LVName)
				if err != nil {
					log.G(ctx).WithError(err).WithFields(logrus.Fields{
						"lvName":    content.LVName,
						"contentID": content.ContentID,
					}).Warn("Remove: failed to destroy LVM logical volume")
					if !isLVNotFoundError(err) {
						continue
					}
				}
				if cleanupErr := o.deleteDevboxContent(ctx, content.ContentID); cleanupErr != nil {
					log.G(ctx).WithError(cleanupErr).WithFields(logrus.Fields{
						"lvName":    content.LVName,
						"contentID": content.ContentID,
					}).Warn("Remove: failed to delete devbox content metadata")
					continue
				}
				log.G(ctx).WithFields(logrus.Fields{
					"lvName":    content.LVName,
					"contentID": content.ContentID,
				}).Info("Remove: devbox content cleanup completed")
			}
		}
	}()

	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		mountPath, err := storage.RemoveDevbox(ctx, key)
		log.G(ctx).WithFields(logrus.Fields{
			"key":       key,
			"mountPath": mountPath,
			"error":     err,
		}).Warnf("[DEVBOX-REMOVE-TRACE] RemoveDevbox returned")
		if err != nil && err != errdefs.ErrNotFound {
			return fmt.Errorf("failed to remove devbox content for snapshot %s: %w", key, err)
		}
		if mountPath != "" {
			log.G(ctx).WithFields(logrus.Fields{
				"key":       key,
				"mountPath": mountPath,
			}).Warnf("[DEVBOX-REMOVE-TRACE] mountPath is NOT empty, calling unmountLvm")
			if err = o.unmountLvm(ctx, mountPath); err != nil {
				log.G(ctx).WithError(err).WithField("path", mountPath).Warn("failed to unmount directory")
			}
		} else {
			log.G(ctx).WithField("key", key).Warnf("[DEVBOX-REMOVE-TRACE] mountPath is EMPTY! unmountLvm will NOT be called")
		}
		_, _, err = storage.Remove(ctx, key)
		if err != nil {
			return fmt.Errorf("failed to remove snapshot %s: %w", key, err)
		}

		if !o.asyncRemove {
			removals, err = o.getCleanupDirectories(ctx)
			if err != nil {
				return fmt.Errorf("unable to get directories for removal: %w", err)
			}
			removedContents, err = o.getCleanupRemovedContents(ctx)
			if err != nil {
				return fmt.Errorf("failed to get removable devbox contents for snapshot %s: %w", key, err)
			}
		}
		return nil
	})
}

// Walk the snapshots.
func (o *Snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	return o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
		if o.upperdirLabel {
			return storage.WalkInfo(ctx, func(ctx context.Context, info snapshots.Info) error {
				id, _, _, err := storage.GetInfo(ctx, info.Name)
				if err != nil {
					return err
				}
				if info.Labels == nil {
					info.Labels = make(map[string]string)
				}
				info.Labels[upperdirKey] = o.upperPath(id)
				return fn(ctx, info)
			}, fs...)
		}
		return storage.WalkInfo(ctx, fn, fs...)
	})
}

// Cleanup cleans up disk resources from removed or abandoned snapshots.
func (o *Snapshotter) Cleanup(ctx context.Context) error {
	log.G(ctx).Info("Cleanup called")
	cleanup, removedContents, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	for _, dir := range cleanup {
		o.RemoveDir(ctx, dir)
	}

	for _, content := range removedContents {
		if err := o.removeLv(ctx, content.LVName); err != nil {
			log.G(ctx).WithError(err).WithFields(logrus.Fields{
				"lvName":    content.LVName,
				"contentID": content.ContentID,
			}).Warn("Cleanup: failed to destroy LVM logical volume")
			if !isLVNotFoundError(err) {
				continue
			}
		}
		if err := o.deleteDevboxContent(ctx, content.ContentID); err != nil {
			log.G(ctx).WithError(err).WithFields(logrus.Fields{
				"lvName":    content.LVName,
				"contentID": content.ContentID,
			}).Warn("Cleanup: failed to delete devbox content metadata")
			continue
		}
		log.G(ctx).WithFields(logrus.Fields{
			"lvName":    content.LVName,
			"contentID": content.ContentID,
		}).Info("Cleanup: devbox content cleanup completed")
	}

	return nil
}

func (o *Snapshotter) cleanupDirectories(ctx context.Context) (_ []string, _ []storage.RemovedDevboxContent, err error) {
	var (
		cleanupDirs     []string
		removedContents []storage.RemovedDevboxContent
	)
	if err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		cleanupDirs, err = o.getCleanupDirectories(ctx)
		if err != nil {
			return err
		}
		removedContents, err = o.getCleanupRemovedContents(ctx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, nil, err
	}

	// Unmount any mounted LVs
	for _, content := range removedContents {
		devicePath := o.devicePath(content.LVName)
		mountPoints, err := findMountPointByDevice(devicePath)
		if err != nil {
			log.G(ctx).WithError(err).WithField("lvName", content.LVName).WithField("devicePath", devicePath).
				Warn("Cleanup: failed to find mount point for LV, continuing")
			continue
		}
		for _, mountPoint := range mountPoints {
			if err := o.unmountLvm(ctx, mountPoint); err != nil {
				log.G(ctx).WithError(err).WithField("lvName", content.LVName).WithField("mountPoint", mountPoint).
					Warn("Cleanup: failed to unmount LV, will retry on next cleanup")
				// Continue to try to unmount other mount points
			} else {
				log.G(ctx).Infof("Cleanup: successfully unmounted LV %s from %s", content.LVName, mountPoint)
			}
		}
	}

	return cleanupDirs, removedContents, nil
}

func (o *Snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
	ids, err := storage.IDMap(ctx)
	if err != nil {
		return nil, err
	}

	snapshotDir := filepath.Join(o.root, "snapshots")
	fd, err := os.Open(snapshotDir)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	dirs, err := fd.Readdirnames(0)
	if err != nil {
		return nil, err
	}

	cleanup := []string{}
	for _, d := range dirs {
		if _, ok := ids[d]; ok {
			continue
		}
		cleanup = append(cleanup, filepath.Join(snapshotDir, d))
	}

	return cleanup, nil
}

func (o *Snapshotter) getCleanupRemovedContents(ctx context.Context) ([]storage.RemovedDevboxContent, error) {
	return storage.GetRemovedDevboxContents(ctx)
}

func (o *Snapshotter) devicePath(lvName string) string {
	return fmt.Sprintf("/dev/%s/%s", o.lvmVgName, lvName)
}

func (o *Snapshotter) deleteDevboxContent(ctx context.Context, contentID string) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		return storage.DeleteDevboxContent(ctx, contentID)
	})
}

func isLVNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	errMsg := err.Error()
	return strings.Contains(errMsg, "not found in volume group") ||
		strings.Contains(errMsg, "Failed to find logical volume")
}

func (o *Snapshotter) resizeLVMVolume(ctx context.Context, lvName, useLimit string) error {
	capacity, err := parseUseLimit(useLimit)
	if err != nil {
		return fmt.Errorf("failed to parse use limit %s: %w", useLimit, err)
	}

	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      capacity,
			VolGroup:      o.lvmVgName,
			ThinProvision: o.ThinPoolName,
		},
	}

	return lvm.ResizeLVMVolume(ctx, vol, true)
}

// readProcMounts reads and parses /proc/mounts file
// Returns a slice of mount entries, where each entry is a slice of fields from /proc/mounts
func readProcMounts() ([][]string, error) {
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return nil, fmt.Errorf("failed to read /proc/mounts: %w", err)
	}

	var mounts [][]string
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		if len(line) == 0 {
			continue
		}

		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}

		mounts = append(mounts, fields)
	}

	return mounts, nil
}

// findMountPointByDevice finds the mount point for a given device path by reading /proc/mounts
// Returns the mount point path if found, empty string if not mounted, and error on failure
func findMountPointByDevice(devicePath string) ([]string, error) {
	mounts, err := readProcMounts()
	if err != nil {
		return nil, err
	}

	var mountPoints []string
	for _, fields := range mounts {
		if len(fields) < 2 {
			continue
		}

		mountDevice := fields[0]
		mountPoint := fields[1]

		// Check if the device matches (handle both direct path and symlink resolution)
		if mountDevice == devicePath {
			mountPoints = append(mountPoints, mountPoint)
			continue
		}

		// Resolve both paths and compare
		resolvedDevicePath, err1 := filepath.EvalSymlinks(devicePath)
		resolvedMountDevice, err2 := filepath.EvalSymlinks(mountDevice)

		// If both resolve successfully, compare resolved paths
		if err1 == nil && err2 == nil {
			if resolvedDevicePath == resolvedMountDevice {
				mountPoints = append(mountPoints, mountPoint)
				continue
			}
		}

		// Also check if one resolves to the other
		if err1 == nil && resolvedDevicePath == mountDevice {
			mountPoints = append(mountPoints, mountPoint)
			continue
		}
		if err2 == nil && resolvedMountDevice == devicePath {
			mountPoints = append(mountPoints, mountPoint)
			continue
		}
	}

	return mountPoints, nil
}

func isMountPoint(dir string) (bool, error) {
	mounts, err := readProcMounts()
	if err != nil {
		return false, err
	}

	// check if the directory is in the mount list
	for _, fields := range mounts {
		mountPoint := fields[1]
		if mountPoint == dir {
			return true, nil
		}
	}

	return false, nil
}

func (o *Snapshotter) mkfs(lvName string) error {
	devicePath := fmt.Sprintf("/dev/%s/%s", o.lvmVgName, lvName)
	if _, err := os.Stat(devicePath); os.IsNotExist(err) {
		return fmt.Errorf("LVM logical volume %s does not exist: %w", devicePath, err)
	}

	cmd := exec.Command("mkfs.ext4", devicePath)
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("failed to create filesystem on %s: %w, output: %s", devicePath, err, string(output))
	}
	return nil
}

func (o *Snapshotter) mountLvm(ctx context.Context, lvName string, path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0o755); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", path, err)
		}
	} else if err != nil {
		return fmt.Errorf("failed to stat path %s: %w", path, err)
	}
	devicePath := fmt.Sprintf("/dev/%s/%s", o.lvmVgName, lvName)
	err = syscall.Mount(devicePath, path, "ext4", 0, "")
	if err != nil {
		return fmt.Errorf("failed to mount LVM logical volume %s to %s: %w", devicePath, path, err)
	}
	return nil
}

func (o *Snapshotter) unmountLvm(ctx context.Context, path string) error {
	isMounted, err := isMountPoint(path)
	if err != nil {
		return fmt.Errorf("failed to check if path %s is a mount point: %w", path, err)
	}
	if !isMounted {
		log.G(ctx).Infof("Path %s is not mounted, skipping unmount", path)
		return nil
	}
	err = syscall.Unmount(path, 0)
	if err != nil {
		return fmt.Errorf("failed to unmount path %s: %w", path, err)
	}
	return nil
}

func (o *Snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		s                       storage.Snapshot
		td, path, npath, lvName string
	)

	defer func() {
		if err != nil {
			if td != "" {
				o.RemoveDir(ctx, td)
			}
			if path != "" {
				o.RemoveDir(ctx, path)
			}
		}
	}()

	base := snapshots.Info{}
	for _, opt := range opts {
		if err = opt(&base); err != nil {
			return nil, fmt.Errorf("failed to apply snapshot option: %w", err)
		}
	}

	for label, value := range base.Labels {
		log.G(ctx).WithFields(logrus.Fields{"label": label, "value": value}).Debug("Snapshot label")
	}

	contentID, idOk := base.Labels[devboxContentIDKey]
	useLimit, limitOk := base.Labels[newLayerLimitKey]
	_, privateImageOk := base.Labels[privateImageKey]
	if err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) (err error) {
		snapshotDir := filepath.Join(o.root, "snapshots")

		directParent := parent
		if privateImageOk {
			directParent, err = storage.GetParentID(ctx, parent)
			if err != nil {
				return fmt.Errorf("failed to get parent ID for private image: %w", err)
			}
		}

		s, err = storage.CreateSnapshot(ctx, kind, key, directParent, opts...)
		if err != nil {
			return fmt.Errorf("failed to create snapshot: %w", err)
		}

		npath = filepath.Join(snapshotDir, s.ID)

		var plan devboxLVMPlan
		if idOk {
			var notExistErr error
			lvName, notExistErr = storage.GetDevboxLvName(ctx, contentID, "")
			if notExistErr != nil && notExistErr != errdefs.ErrNotFound {
				return fmt.Errorf("failed to get LVM logical volume name for key %s: %w", contentID, notExistErr)
			}
			plan = planDevboxLVM(contentID, useLimit, lvName, idOk, limitOk)
		}

		if plan.reuseExisting {
			var isMounted bool
			if isMounted, err = isMountPoint(npath); err != nil {
				return fmt.Errorf("failed to check if path is a mount point: %w", err)
			} else if isMounted {
				log.G(ctx).Infof("Path %s is already mounted, skipping mount", npath)
			} else {
				if plan.resizeExisting {
					if err = o.resizeLVMVolume(ctx, plan.existingLVName, plan.useLimit); err != nil {
						return fmt.Errorf("failed to resize LVM logical volume %s: %w", plan.existingLVName, err)
					}
				}

				if err = storage.SetDevboxContent(ctx, key, plan.contentID, plan.existingLVName, npath); err != nil {
					return fmt.Errorf("failed to set devbox content: %w", err)
				}

				if err = o.mountLvm(ctx, plan.existingLVName, npath); err != nil {
					return fmt.Errorf("failed to mount LVM logical volume %s: %w", plan.existingLVName, err)
				}
				path = npath
			}
			return nil
		}

		if plan.createNew {
			td, lvName, err = o.prepareLvmDirectory(ctx, snapshotDir, plan.contentID, plan.useLimit)
			defer func() {
				if err != nil {
					mountPath, err := storage.RemoveDevbox(ctx, key)
					if err != nil {
						log.G(ctx).WithError(err).Warnf("failed to remove devbox content for key %s", contentID)
					}
					if mountPath != "" {
						if err := o.unmountLvm(ctx, mountPath); err != nil {
							log.G(ctx).WithError(err).WithField("path", mountPath).Warn("failed to unmount directory")
						}
					}
				}
			}()

			if err != nil {
				return fmt.Errorf("failed to prepare LVM directory for snapshot: %w", err)
			}

			if privateImageOk {
				var parentID string
				parentID, err = storage.GetID(ctx, parent)
				if err != nil {
					return fmt.Errorf("failed to get parent ID for private image: %w", err)
				}
				parentUpperdir := o.upperPath(parentID)
				opt := cp.Options{
					OnSymlink: func(src string) cp.SymlinkAction {
						return cp.Shallow
					},
					PreserveTimes: true,
					PreserveOwner: true,
				}
				if err = cp.Copy(parentUpperdir, filepath.Join(td, "fs"), opt); err != nil {
					return fmt.Errorf("failed to copy parent upperdir to new snapshot upperdir: %w, from %s to %s", err, parentUpperdir, td)
				}
			}

			if err = storage.SetDevboxContent(ctx, key, contentID, lvName, npath); err != nil {
				return fmt.Errorf("failed to set devbox content: %w", err)
			}
		} else {
			td, err = o.prepareDirectory(ctx, snapshotDir, kind)
		}

		if err != nil {
			return fmt.Errorf("failed to create prepare snapshot dir: %w", err)
		}

		if len(s.ParentIDs) > 0 {
			st, err := os.Stat(o.upperPath(s.ParentIDs[0]))
			if err != nil {
				return fmt.Errorf("failed to stat parent: %w", err)
			}

			stat := st.Sys().(*syscall.Stat_t)
			if err = os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
				return fmt.Errorf("failed to chown: %w", err)
			}
		}

		if plan.createNew {
			err = o.unmountLvm(ctx, td)
			if err != nil {
				return fmt.Errorf("failed to unmount LVM logical volume %s: %w", lvName, err)
			}
			if err = os.Rename(td, npath); err != nil {
				return fmt.Errorf("failed to rename: %w", err)
			}
			path = npath
			err = o.mountLvm(ctx, lvName, path)
			if err != nil {
				return fmt.Errorf("failed to mount LVM logical volume %s: %w", lvName, err)
			}
		} else {
			if err = os.Rename(td, npath); err != nil {
				return fmt.Errorf("failed to rename: %w", err)
			}
			path = npath
		}
		td = ""

		return nil
	}); err != nil {
		return nil, err
	}

	return o.mounts(s), nil
}

func (o *Snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0o755); err != nil {
		return td, err
	}

	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0o711); err != nil {
			return td, err
		}
	}

	return td, nil
}

func parseUseLimit(useLimit string) (string, error) {
	if useLimit == "" {
		return "", fmt.Errorf("use limit cannot be empty")
	}
	multipliers := 1
	if strings.HasSuffix(useLimit, "Gi") {
		multipliers = 1024 * 1024 * 1024
		useLimit = strings.TrimSuffix(useLimit, "Gi")
	} else if strings.HasSuffix(useLimit, "Mi") {
		multipliers = 1024 * 1024
		useLimit = strings.TrimSuffix(useLimit, "Mi")
	} else if strings.HasSuffix(useLimit, "Ki") {
		multipliers = 1024
		useLimit = strings.TrimSuffix(useLimit, "Ki")
	} else if strings.HasSuffix(useLimit, "B") {
		useLimit = strings.TrimSuffix(useLimit, "B")
	} else {
		return "", fmt.Errorf("invalid use limit format: %s", useLimit)
	}

	capacity, err := strconv.Atoi(useLimit)
	if err != nil {
		return "", fmt.Errorf("failed to parse use limit %s: %w", useLimit, err)
	}
	if capacity <= 0 {
		return "", fmt.Errorf("use limit must be greater than 0: %s", useLimit)
	}
	capacity *= multipliers
	return strconv.Itoa(capacity), nil
}

func (o *Snapshotter) removeLv(ctx context.Context, lvName string) error {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			VolGroup: o.lvmVgName,
		},
	}
	return lvm.DestroyVolume(ctx, vol)
}

// forceRemoveLv force destroys the lvm volume
func (o *Snapshotter) forceRemoveLv(ctx context.Context, lvName string) error {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			VolGroup: o.lvmVgName,
		},
	}
	return lvm.ForceDestroyVolume(ctx, vol)
}

func (o *Snapshotter) prepareLvmDirectory(ctx context.Context, snapshotDir string, contentKey string, useLimit string) (string, string, error) {
	lvName := "devbox-" + contentKey

	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", lvName, fmt.Errorf("failed to create temp dir: %w", err)
	}

	capacity, err := parseUseLimit(useLimit)
	if err != nil {
		return td, lvName, fmt.Errorf("failed to parse use limit %s: %w", useLimit, err)
	}

	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity:      capacity,
			VolGroup:      o.lvmVgName,
			ThinProvision: o.ThinPoolName,
		},
	}
	// Track mount status for cleanup
	// Track mount status for cleanup
	mounted := false

	// Defer cleanup: unmount and force remove LV if any step fails
	defer func() {
		if err != nil {
			if mounted {
				// Unmount first if mounted
				if unmountErr := o.unmountLvm(ctx, td); unmountErr != nil {
					log.G(ctx).WithError(unmountErr).WithField("lvName", lvName).Warn("failed to unmount LVM logical volume during cleanup")
				}
			}
			// Force remove the LV
			if removeErr := o.forceRemoveLv(ctx, lvName); removeErr != nil {
				log.G(ctx).WithError(removeErr).WithField("lvName", lvName).Warn("failed to force destroy LVM logical volume during cleanup")
			}
		}
	}()

	log.G(ctx).Debug("Creating LVM volume:", lvName, "with capacity:", capacity, "in volume group:", o.lvmVgName)
	err = lvm.CreateVolume(ctx, vol)
	if err != nil {
		return td, lvName, fmt.Errorf("failed to create LVM logical volume %s: %w", lvName, err)
	}
	if err = o.mkfs(lvName); err != nil {
		return td, lvName, fmt.Errorf("failed to create filesystem on LVM logical volume %s: %w", lvName, err)
	}

	mounted = true
	if err = o.mountLvm(ctx, lvName, td); err != nil {
		return td, lvName, fmt.Errorf("failed to mount LVM logical volume %s: %w", lvName, err)
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0o755); err != nil {
		return td, lvName, fmt.Errorf("failed to create fs directory: %w", err)
	}
	if err := os.Mkdir(filepath.Join(td, "work"), 0o711); err != nil {
		return td, lvName, fmt.Errorf("failed to create work directory: %w", err)
	}

	return td, lvName, nil
}

func (o *Snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	if len(s.ParentIDs) == 0 {
		roFlag := "rw"
		if s.Kind == snapshots.KindView {
			roFlag = "ro"
		}

		return []mount.Mount{
			{
				Source: o.upperPath(s.ID),
				Type:   "bind",
				Options: []string{
					roFlag,
					"rbind",
				},
			},
		}
	}

	options := append([]string(nil), o.options...)
	if s.Kind == snapshots.KindActive {
		options = append(options,
			fmt.Sprintf("workdir=%s", o.workPath(s.ID)),
			fmt.Sprintf("upperdir=%s", o.upperPath(s.ID)),
		)
	} else if len(s.ParentIDs) == 1 {
		return []mount.Mount{
			{
				Source: o.upperPath(s.ParentIDs[0]),
				Type:   "bind",
				Options: []string{
					"ro",
					"rbind",
				},
			},
		}
	}

	parentPaths := make([]string, len(s.ParentIDs))
	for i := range s.ParentIDs {
		parentPaths[i] = o.upperPath(s.ParentIDs[i])
	}

	options = append(options, fmt.Sprintf("lowerdir=%s", strings.Join(parentPaths, ":")))
	return []mount.Mount{
		{
			Type:    "overlay",
			Source:  "overlay",
			Options: options,
		},
	}
}

func (o *Snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *Snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter.
func (o *Snapshotter) Close() error {
	return o.ms.Close()
}

// supportsIndex checks whether the "index=off" option is supported by the kernel.
func supportsIndex() bool {
	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		return true
	}
	return false
}
