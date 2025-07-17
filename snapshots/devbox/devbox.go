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

	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/snapshots"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"

	apis "github.com/openebs/lvm-localpv/pkg/apis/openebs.io/lvm/v1alpha1"
	"github.com/sirupsen/logrus"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/containerd/containerd/snapshots/devbox/lvm"
	"github.com/containerd/containerd/snapshots/devbox/storage"
)

// upperdirKey is a key of an optional label to each snapshot.
// This optional label of a snapshot contains the location of "upperdir" where
// the change set between this snapshot and its parent is stored.
const upperdirKey = "containerd.io/snapshot/overlay.upperdir"

const newLayerLimitKey = "containerd.io/snapshot/devbox-storage-limit"
const devboxContentIDKey = "containerd.io/snapshot/devbox-content-id"
const privateImageKey = "containerd.io/snapshot/devbox-init"
const removeContentIDKey = "containerd.io/snapshot/devbox-remove-content-id"

// SnapshotterConfig is used to configure the overlay snapshotter instance
type SnapshotterConfig struct {
	AsyncRemove   bool
	UpperdirLabel bool
	ms            MetaStore
	lvmVgName     string // modified by sealos
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

// modified by sealos
// WithLvmVgName sets the name of the LVM volume group to use for the overlay
func WithLvmVgName(name string) Opt {
	return func(config *SnapshotterConfig) error {
		config.lvmVgName = name
		return nil
	}
}

// end modified by sealos

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

type snapshotter struct {
	root          string
	ms            MetaStore
	asyncRemove   bool
	upperdirLabel bool
	lvmVgName     string // modified by sealos
	options       []string
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

	if err := os.MkdirAll(root, 0700); err != nil {
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

	if err := os.Mkdir(filepath.Join(root, "snapshots"), 0700); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if !hasOption(config.mountOptions, "userxattr", false) {
		// figure out whether "userxattr" option is recognized by the kernel && needed
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

	return &snapshotter{
		root:          root,
		ms:            config.ms,
		asyncRemove:   config.AsyncRemove,
		upperdirLabel: config.UpperdirLabel,
		lvmVgName:     config.lvmVgName, // modified by sealos
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

// Stat returns the info for an active or committed snapshot by name or
// key.
//
// Should be used for parent resolution, existence checks and to discern
// the kind of snapshot.
func (o *snapshotter) Stat(ctx context.Context, key string) (info snapshots.Info, err error) {
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

func (o *snapshotter) Update(ctx context.Context, info snapshots.Info, fieldpaths ...string) (newInfo snapshots.Info, err error) {
	err = o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {

		if value, ok := info.Labels[removeContentIDKey]; ok {
			storage.RemoveDevboxContent(ctx, value)
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
//
// For active snapshots, this will scan the usage of the overlay "diff" (aka
// "upper") directory and may take some time.
//
// For committed snapshots, the value is returned from the metadata database.
func (o *snapshotter) Usage(ctx context.Context, key string) (_ snapshots.Usage, err error) {
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
			// TODO(stevvooe): Consider not reporting an error in this case.
			return snapshots.Usage{}, err
		}
		usage = snapshots.Usage(du)
	}
	return usage, nil
}

func (o *snapshotter) Prepare(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	log.G(ctx).Debug("Prepare called with key:", key, "parent:", parent, "opts:", opts)
	return o.createSnapshot(ctx, snapshots.KindActive, key, parent, opts)
}

func (o *snapshotter) View(ctx context.Context, key, parent string, opts ...snapshots.Opt) ([]mount.Mount, error) {
	return o.createSnapshot(ctx, snapshots.KindView, key, parent, opts)
}

// Mounts returns the mounts for the transaction identified by key. Can be
// called on an read-write or readonly transaction.
//
// This can be used to recover mounts after calling View or Prepare.
func (o *snapshotter) Mounts(ctx context.Context, key string) (_ []mount.Mount, err error) {
	var s storage.Snapshot
	if err := o.ms.WithTransaction(ctx, false, func(ctx context.Context) error {
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

func (o *snapshotter) Commit(ctx context.Context, name, key string, opts ...snapshots.Opt) error {
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// grab the existing id
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

// Remove abandons the snapshot identified by key. The snapshot will
// immediately become unavailable and unrecoverable. Disk space will
// be freed up on the next call to `Cleanup`.
func (o *snapshotter) Remove(ctx context.Context, key string) (err error) {
	var (
		removals       []string
		removedLvNames []string
	)

	log.G(ctx).Infof("Remove called with key: %s", key)
	// Remove directories after the transaction is closed, failures must not
	// return error since the transaction is committed with the removal
	// key no longer available.
	defer func() {
		if err == nil {
			for _, dir := range removals {
				// modified by sealos
				if err1 := o.unmountLvm(ctx, dir); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", dir).Warn("failed to unmount directory")
				}
				// end modified by sealos
				if err1 := os.RemoveAll(dir); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", dir).Warn("failed to remove directory")
				}
			}
			for _, lvName := range removedLvNames {
				err := o.removeLv(lvName)
				if err != nil {
					log.G(ctx).WithError(err).WithField("lvName", lvName).Warn("failed to destroy LVM logical volume")
					continue
				}
				log.G(ctx).Infof("LVM logical volume %s removed successfully", lvName)
			}
		}
	}()
	return o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		// modified by sealos
		var mountPath string
		mountPath, err = storage.RemoveDevboxContent(ctx, key)
		log.G(ctx).Infof("Removed devbox content for key: %s, mount path: %s", key, mountPath)
		if err != nil && err != errdefs.ErrNotFound {
			return fmt.Errorf("failed to remove devbox content for snapshot %s: %w", key, err)
		}
		if mountPath != "" {
			if err = o.unmountLvm(ctx, mountPath); err != nil {
				log.G(ctx).WithError(err).WithField("path", mountPath).Warn("failed to unmount directory")
			}
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
			removedLvNames, err = o.getCleanupLvNames(ctx)
			if err != nil {
				return fmt.Errorf("failed to get LVM logical volume names for snapshot %s: %w", key, err)
			}
		}
		return nil
	})
}

// Walk the snapshots.
func (o *snapshotter) Walk(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
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

// Cleanup cleans up disk resources from removed or abandoned snapshots
func (o *snapshotter) Cleanup(ctx context.Context) error {
	log.G(ctx).Infof("Cleanup called")
	cleanup, cleanupLv, err := o.cleanupDirectories(ctx)
	if err != nil {
		return err
	}

	for _, dir := range cleanup {
		// modified by sealos
		if err := o.unmountLvm(ctx, dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to unmount directory")
		}
		// end modified by sealos
		if err := os.RemoveAll(dir); err != nil {
			log.G(ctx).WithError(err).WithField("path", dir).Warn("failed to remove directory")
		}
	}

	for _, lvName := range cleanupLv {
		err := o.removeLv(lvName)
		if err != nil {
			log.G(ctx).WithError(err).WithField("lvName", lvName).Warn("failed to destroy LVM logical volume")
			continue
		}
		log.G(ctx).Infof("LVM logical volume %s removed successfully", lvName)
	}

	return nil
}

func (o *snapshotter) cleanupDirectories(ctx context.Context) (_ []string, _ []string, err error) {
	var (
		cleanupDirs    []string
		removedLvNames []string
	)
	// Get a write transaction to ensure no other write transaction can be entered
	// while the cleanup is scanning.
	if err := o.ms.WithTransaction(ctx, true, func(ctx context.Context) error {
		cleanupDirs, err = o.getCleanupDirectories(ctx)
		if err != nil {
			return err
		}
		removedLvNames, err = o.getCleanupLvNames(ctx)
		return err
	}); err != nil {
		return nil, nil, err
	}
	return cleanupDirs, removedLvNames, nil
}

func (o *snapshotter) getCleanupDirectories(ctx context.Context) ([]string, error) {
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

// modified by sealos
func (o *snapshotter) getCleanupLvNames(ctx context.Context) ([]string, error) {
	nameMap, err := storage.GetDevboxLvNames(ctx)
	if err != nil {
		return nil, err
	}

	// lvs := o.vgo.ListLVs()
	lvs, err := lvm.ListLVMLogicalVolumeByVG(o.lvmVgName)
	if err != nil {
		return nil, fmt.Errorf("failed to list LVM logical volumes: %w", err)
	}

	cleanup := []string{}
	for _, d := range lvs {
		if _, ok := nameMap[d.Name]; ok {
			continue
		}
		// Check if the name start with devbox
		if strings.HasPrefix(d.Name, "devbox") {
			cleanup = append(cleanup, d.Name)
		}
	}

	return cleanup, nil
}

func isMountPoint(dir string) (bool, error) {
	// 读取 /proc/mounts 文件
	data, err := os.ReadFile("/proc/mounts")
	if err != nil {
		return false, err
	}

	// 检查目录是否在挂载列表中
	mounts := strings.Split(string(data), "\n")
	for _, mount := range mounts {
		if len(mount) == 0 {
			continue
		}

		fields := strings.Fields(mount)
		if len(fields) < 2 {
			continue
		}

		mountPoint := fields[1]
		if mountPoint == dir {
			return true, nil
		}
	}

	return false, nil
}

func (o *snapshotter) mkfs(lvName string) error {
	devicePath := fmt.Sprintf("/dev/%s/%s", o.lvmVgName, lvName)
	// Check if the device exists
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

func (o *snapshotter) mountLvm(ctx context.Context, lvName string, path string) error {
	_, err := os.Stat(path)
	if os.IsNotExist(err) {
		if err := os.MkdirAll(path, 0755); err != nil {
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

func (o *snapshotter) unmountLvm(ctx context.Context, path string) error {
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

// end modified by sealos

func (o *snapshotter) createSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts []snapshots.Opt) (_ []mount.Mount, err error) {
	var (
		s                       storage.Snapshot
		td, path, npath, lvName string
	)

	defer func() {
		if err != nil {
			if td != "" {
				if err1 := o.unmountLvm(ctx, td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to unmount temp snapshot directory")
				}
				if err1 := os.RemoveAll(td); err1 != nil {
					log.G(ctx).WithError(err1).Warn("failed to cleanup temp snapshot directory")
				}
			}
			if path != "" {
				if err1 := o.unmountLvm(ctx, path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Warn("failed to unmount snapshot directory")
				}
				if err1 := os.RemoveAll(path); err1 != nil {
					log.G(ctx).WithError(err1).WithField("path", path).Error("failed to reclaim snapshot directory, directory may need removal")
					err = fmt.Errorf("failed to remove path: %v: %w", err1, err)
				}
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

	contentId, idOk := base.Labels[devboxContentIDKey]
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

		log.G(ctx).Debug("Created snapshot:", s.ID)
		npath = filepath.Join(snapshotDir, s.ID) // use npath instead of path to avoid removing the directory before create
		log.G(ctx).Debug("Snapshot directory path:", npath)

		if idOk && limitOk {
			var notExistErr error
			lvName, notExistErr = storage.GetDevboxLvName(ctx, contentId)
			log.G(ctx).Debug("LVM logical volume name for content ID:", contentId, "is", lvName)
			if notExistErr == nil && lvName != "" {
				// mount point for the snapshot
				log.G(ctx).Debug("LVM logical volume name found for content ID:", contentId, "is", lvName)
				var isMounted bool
				if isMounted, err = isMountPoint(npath); err != nil {
					return fmt.Errorf("failed to check if path is a mount point: %w", err)
				} else if isMounted {
					log.G(ctx).Infof("Path %s is already mounted, skipping mount", npath)
				} else {
					// mount the LVM logical volume
					if err = o.mountLvm(ctx, lvName, npath); err != nil {
						return fmt.Errorf("failed to mount LVM logical volume %s: %w", lvName, err)
					}
				}
				// reuse of old lv, no need to prepare a new directory
				return nil
			} else if notExistErr != errdefs.ErrNotFound {
				return fmt.Errorf("failed to get LVM logical volume name for key %s: %w", contentId, notExistErr)
			}

			// remove devbox metadata if new lv is created
			defer func() {
				if err != nil {
					mountPath, err := storage.RemoveDevboxContent(ctx, key)
					if err != nil {
						log.G(ctx).WithError(err).Warnf("failed to remove devbox content for key %s", contentId)
					}
					if mountPath != "" {
						if err := o.unmountLvm(ctx, mountPath); err != nil {
							log.G(ctx).WithError(err).WithField("path", mountPath).Warn("failed to unmount directory")
						}
					}
				}
			}()
			td, lvName, err = o.prepareLvmDirectory(ctx, snapshotDir, contentId, useLimit)
			if err != nil {
				return fmt.Errorf("failed to prepare LVM directory for snapshot: %w", err)
			}

			if privateImageOk {
				var parentID string
				parentID, err = storage.GetID(ctx, parent)
				if err != nil {
					return fmt.Errorf("failed to get parent ID for private image: %w", err)
				}
				parent_upperdir := o.upperPath(parentID)
				// copy all contents from parent upperdir to new snapshot upperdir
				// TODO: maybe move instead of copy?
				opt := cp.Options{
					OnSymlink: func(src string) cp.SymlinkAction {
						return cp.Shallow
					},
					PreserveTimes: true,
					PreserveOwner: true,
				}
				if err = cp.Copy(parent_upperdir, filepath.Join(td, "fs"), opt); err != nil {
					return fmt.Errorf("failed to copy parent upperdir to new snapshot upperdir: %w, from %s to %s", err, parent_upperdir, td)
				}
				log.G(ctx).Debug("Copied parent upperdir to new snapshot upperdir:", td)
			}

			log.G(ctx).Debug("Prepared LVM directory for snapshot:", td, "with logical volume name:", lvName)
			storage.SetDevboxContent(ctx, key, contentId, lvName, npath)
			if err != nil {
				return fmt.Errorf("failed to prepare LVM directory for snapshot: %w", err)
			}
		} else {
			td, err = o.prepareDirectory(ctx, snapshotDir, kind)
			log.G(ctx).Debug("Created temporary directory for snapshot:", td)
		}

		if err != nil {
			return fmt.Errorf("failed to create prepare snapshot dir: %w", err)
		}

		if len(s.ParentIDs) > 0 {
			var st os.FileInfo
			st, err = os.Stat(o.upperPath(s.ParentIDs[0]))
			if err != nil {
				return fmt.Errorf("failed to stat parent: %w", err)
			}

			stat := st.Sys().(*syscall.Stat_t)
			if err = os.Lchown(filepath.Join(td, "fs"), int(stat.Uid), int(stat.Gid)); err != nil {
				return fmt.Errorf("failed to chown: %w", err)
			}
		}

		if idOk && limitOk {
			err = o.unmountLvm(ctx, td)
			if err != nil {
				return fmt.Errorf("failed to unmount LVM logical volume %s: %w", lvName, err)
			}
			log.G(ctx).Debug("Unmounted LVM logical volume:", lvName, "from temporary directory:", td)
			if err = os.MkdirAll(npath, 0755); err != nil {
				return fmt.Errorf("failed to create snapshot directory: %w", err)
			}
			path = npath
			log.G(ctx).Debug("Created snapshot directory:", path)
			err = o.mountLvm(ctx, lvName, path)
			if err != nil {
				return fmt.Errorf("failed to mount LVM logical volume %s: %w", lvName, err)
			}
			log.G(ctx).Debug("Mounted LVM logical volume:", lvName, "to snapshot directory:", path)
		} else {
			if err = os.Rename(td, npath); err != nil {
				return fmt.Errorf("failed to rename: %w", err)
			}
			path = npath
			log.G(ctx).Debug("Renamed temporary directory to snapshot directory:", path)
		}
		td = ""

		return nil
	}); err != nil {
		return nil, err
	}

	return o.mounts(s), nil
}

func (o *snapshotter) prepareDirectory(ctx context.Context, snapshotDir string, kind snapshots.Kind) (string, error) {
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, err
	}

	if kind == snapshots.KindActive {
		if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
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

func (o *snapshotter) removeLv(lvName string) error {
	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			VolGroup: o.lvmVgName,
		},
	}
	return lvm.DestroyVolume(vol)
}

func (o *snapshotter) prepareLvmDirectory(ctx context.Context, snapshotDir string, contentKey string, useLimit string) (string, string, error) {
	lvName := "devbox-" + contentKey
	td, err := os.MkdirTemp(snapshotDir, "new-")
	if err != nil {
		return "", "", fmt.Errorf("failed to create temp dir: %w", err)
	}

	capacity, err := parseUseLimit(useLimit)
	if err != nil {
		return td, "", fmt.Errorf("failed to parse use limit %s: %w", useLimit, err)
	}

	vol := &apis.LVMVolume{
		ObjectMeta: metav1.ObjectMeta{
			Name: lvName,
		},
		Spec: apis.VolumeInfo{
			Capacity: capacity,
			VolGroup: o.lvmVgName,
		},
	}
	log.G(ctx).Debug("Creating LVM volume:", lvName, "with capacity:", capacity, "in volume group:", o.lvmVgName)
	err = lvm.CreateVolume(vol)
	if err != nil {
		return td, "", fmt.Errorf("failed to create LVM logical volume %s: %w", lvName, err)
	}

	err = o.mkfs(lvName)
	if err != nil {
		// If mkfs fails, we should remove the LVM logical volume
		if err1 := o.removeLv(lvName); err1 != nil {
			log.G(ctx).WithError(err1).WithField("lvName", lvName).Warn("failed to destroy LVM logical volume after mkfs failure")
		}
		return td, "", fmt.Errorf("failed to create filesystem on LVM logical volume %s: %w", lvName, err)
	}
	err = o.mountLvm(ctx, lvName, td)
	if err != nil {
		// If mount fails, we should remove the LVM logical volume
		if err1 := o.removeLv(lvName); err1 != nil {
			log.G(ctx).WithError(err1).WithField("lvName", lvName).Warn("failed to destroy LVM logical volume after mount failure")
		}
		return td, "", fmt.Errorf("failed to mount LVM logical volume %s: %w", lvName, err)
	}
	if err := os.Mkdir(filepath.Join(td, "fs"), 0755); err != nil {
		return td, "", fmt.Errorf("failed to create fs directory: %w", err)
	}
	if err := os.Mkdir(filepath.Join(td, "work"), 0711); err != nil {
		return td, "", fmt.Errorf("failed to create work directory: %w", err)
	}

	return td, lvName, nil
}

func (o *snapshotter) mounts(s storage.Snapshot) []mount.Mount {
	if len(s.ParentIDs) == 0 {
		// if we only have one layer/no parents then just return a bind mount as overlay
		// will not work
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

	options := o.options
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

func (o *snapshotter) upperPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "fs")
}

func (o *snapshotter) workPath(id string) string {
	return filepath.Join(o.root, "snapshots", id, "work")
}

// Close closes the snapshotter
func (o *snapshotter) Close() error {
	return o.ms.Close()
}

// supportsIndex checks whether the "index=off" option is supported by the kernel.
func supportsIndex() bool {
	if _, err := os.Stat("/sys/module/overlay/parameters/index"); err == nil {
		return true
	}
	return false
}
