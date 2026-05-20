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

package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/containerd/containerd/v2/core/metadata/boltutil"
	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/containerd/v2/pkg/filters"
	"github.com/containerd/errdefs"
	"github.com/containerd/log"
	bolt "go.etcd.io/bbolt"
)

var (
	bucketKeyStorageVersion = []byte("v1")
	bucketKeySnapshot       = []byte("snapshots")
	bucketKeyParents        = []byte("parents")

	bucketKeyID     = []byte("id")
	bucketKeyParent = []byte("parent")
	bucketKeyKind   = []byte("kind")
	bucketKeyInodes = []byte("inodes")
	bucketKeySize   = []byte("size")

	// ErrNoTransaction is returned when an operation is attempted with
	// a context which is not inside of a transaction.
	ErrNoTransaction = errors.New("no transaction in context")

	DevboxKeyContentID = []byte("content_id")

	DevboxStoragePathBucket = []byte("devbox_storage_path")
	DevboxKeyPath           = []byte("path")
	DevboxKeyLvName         = []byte("lv_name")
	DevboxKeyStatus         = []byte("status")
	DevboxKeySnapshotKey    = []byte("snapshot_key")

	DevboxStatusActive  = []byte("active")
	DevboxStatusRemoved = []byte("removed")
)

type RemovedDevboxContent struct {
	ContentID string
	LVName    string
}

// parentKey returns a composite key of the parent and child identifiers. The
// parts of the key are separated by a zero byte.
func parentKey(parent, child uint64) []byte {
	b := make([]byte, binary.Size([]uint64{parent, child})+1)
	i := binary.PutUvarint(b, parent)
	j := binary.PutUvarint(b[i+1:], child)
	return b[0 : i+j+1]
}

// parentPrefixKey returns the parent part of the composite key with the
// zero byte separator.
func parentPrefixKey(parent uint64) []byte {
	b := make([]byte, binary.Size(parent)+1)
	i := binary.PutUvarint(b, parent)
	return b[0 : i+1]
}

// getParentPrefix returns the first part of the composite key which
// represents the parent identifier.
func getParentPrefix(b []byte) uint64 {
	parent, _ := binary.Uvarint(b)
	return parent
}

// GetInfo returns the snapshot Info directly from the metadata. Requires a
// context with a storage transaction.
func GetInfo(ctx context.Context, key string) (string, snapshots.Info, snapshots.Usage, error) {
	var (
		id uint64
		su snapshots.Usage
		si = snapshots.Info{
			Name: key,
		}
	)
	err := withSnapshotBucket(ctx, key, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		getUsage(bkt, &su)
		return readSnapshot(bkt, &id, &si)
	})
	if err != nil {
		return "", snapshots.Info{}, snapshots.Usage{}, err
	}

	return fmt.Sprintf("%d", id), si, su, nil
}

// UpdateInfo updates an existing snapshot info's data.
func UpdateInfo(ctx context.Context, info snapshots.Info, fieldpaths ...string) (snapshots.Info, error) {
	updated := snapshots.Info{
		Name: info.Name,
	}
	err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(info.Name))
		if sbkt == nil {
			return fmt.Errorf("snapshot does not exist: %w", errdefs.ErrNotFound)
		}
		if err := readSnapshot(sbkt, nil, &updated); err != nil {
			return err
		}

		if len(fieldpaths) > 0 {
			for _, path := range fieldpaths {
				if strings.HasPrefix(path, "labels.") {
					if updated.Labels == nil {
						updated.Labels = map[string]string{}
					}

					key := strings.TrimPrefix(path, "labels.")
					updated.Labels[key] = info.Labels[key]
					continue
				}

				switch path {
				case "labels":
					updated.Labels = info.Labels
				default:
					return fmt.Errorf("cannot update %q field on snapshot %q: %w", path, info.Name, errdefs.ErrInvalidArgument)
				}
			}
		} else {
			updated.Labels = info.Labels
		}
		updated.Updated = time.Now().UTC()
		if err := boltutil.WriteTimestamps(sbkt, updated.Created, updated.Updated); err != nil {
			return err
		}

		return boltutil.WriteLabels(sbkt, updated.Labels)
	})
	if err != nil {
		return snapshots.Info{}, err
	}
	return updated, nil
}

// WalkInfo iterates through all metadata Info for the stored snapshots and
// calls the provided function for each. Requires a context with a storage
// transaction.
func WalkInfo(ctx context.Context, fn snapshots.WalkFunc, fs ...string) error {
	filter, err := filters.ParseAll(fs...)
	if err != nil {
		return err
	}
	return withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		return bkt.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			var (
				sbkt = bkt.Bucket(k)
				si   = snapshots.Info{
					Name: string(k),
				}
			)
			if err := readSnapshot(sbkt, nil, &si); err != nil {
				return err
			}
			if !filter.Match(adaptSnapshot(si)) {
				return nil
			}

			return fn(ctx, si)
		})
	})
}

// GetSnapshot returns the metadata for the active or view snapshot transaction
// referenced by the given key. Requires a context with a storage transaction.
func GetSnapshot(ctx context.Context, key string) (s Snapshot, err error) {
	err = withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot does not exist: %w", errdefs.ErrNotFound)
		}

		s.ID = fmt.Sprintf("%d", readID(sbkt))
		s.Kind = readKind(sbkt)

		if s.Kind != snapshots.KindActive && s.Kind != snapshots.KindView {
			return fmt.Errorf("requested snapshot %v not active or view: %w", key, errdefs.ErrFailedPrecondition)
		}

		if parentKey := sbkt.Get(bucketKeyParent); len(parentKey) > 0 {
			spbkt := bkt.Bucket(parentKey)
			if spbkt == nil {
				return fmt.Errorf("parent does not exist: %w", errdefs.ErrNotFound)
			}

			s.ParentIDs, err = parents(bkt, spbkt, readID(spbkt))
			if err != nil {
				return fmt.Errorf("failed to get parent chain: %w", err)
			}
		}
		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}

	return
}

// CreateSnapshot inserts a record for an active or view snapshot with the provided parent.
func CreateSnapshot(ctx context.Context, kind snapshots.Kind, key, parent string, opts ...snapshots.Opt) (s Snapshot, err error) {
	switch kind {
	case snapshots.KindActive, snapshots.KindView:
	default:
		return Snapshot{}, fmt.Errorf("snapshot type %v invalid; only snapshots of type Active or View can be created: %w", kind, errdefs.ErrInvalidArgument)
	}
	var base snapshots.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return Snapshot{}, err
		}
	}

	err = createBucketIfNotExists(ctx, func(ctx context.Context, _ *bolt.Bucket, bkt, pbkt *bolt.Bucket) error {
		var spbkt *bolt.Bucket
		if parent != "" {
			spbkt = bkt.Bucket([]byte(parent))
			if spbkt == nil {
				return fmt.Errorf("missing parent %q bucket: %w", parent, errdefs.ErrNotFound)
			}

			if readKind(spbkt) != snapshots.KindCommitted {
				return fmt.Errorf("parent %q is not committed snapshot: %w", parent, errdefs.ErrInvalidArgument)
			}
		}
		sbkt, err := bkt.CreateBucket([]byte(key))
		if err != nil {
			if errors.Is(err, bolt.ErrBucketExists) {
				return fmt.Errorf("snapshot %q already exists: %w", key, errdefs.ErrAlreadyExists)
			}
			return err
		}

		id, err := sequenceNext(bkt)
		if err != nil {
			return err
		}

		now := time.Now().UTC()
		si := snapshots.Info{
			Name:    key,
			Parent:  parent,
			Kind:    kind,
			Labels:  base.Labels,
			Created: now,
			Updated: now,
		}

		if err := putSnapshot(sbkt, id, si); err != nil {
			return err
		}

		s = Snapshot{
			Kind: kind,
			ID:   fmt.Sprintf("%d", id),
		}

		if spbkt != nil {
			parentID := readID(spbkt)
			if err := pbkt.Put(parentKey(parentID, id), []byte(key)); err != nil {
				return err
			}
			s.ParentIDs, err = parents(bkt, spbkt, parentID)
			if err != nil {
				return err
			}
		}

		return nil
	})
	if err != nil {
		return Snapshot{}, err
	}

	return s, nil
}

// CommitActive commits an active snapshot to a committed snapshot.
func CommitActive(ctx context.Context, key, name string, usage snapshots.Usage, opts ...snapshots.Opt) (string, error) {
	var base snapshots.Info
	for _, opt := range opts {
		if err := opt(&base); err != nil {
			return "", err
		}
	}

	var id uint64
	err := createBucketIfNotExists(ctx, func(ctx context.Context, version, bkt, pbkt *bolt.Bucket) error {
		if cbkt := bkt.Bucket([]byte(name)); cbkt != nil {
			return fmt.Errorf("snapshot %q already exists: %w", name, errdefs.ErrAlreadyExists)
		}

		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
		}

		if readKind(sbkt) != snapshots.KindActive {
			return fmt.Errorf("snapshot %q is not active: %w", key, errdefs.ErrFailedPrecondition)
		}

		id = readID(sbkt)
		contentID := sbkt.Get(DevboxKeyContentID)
		si := snapshots.Info{
			Name:    name,
			Parent:  string(sbkt.Get(bucketKeyParent)),
			Kind:    snapshots.KindCommitted,
			Labels:  base.Labels,
			Created: readTimestamp(sbkt, bucketKeyCreated),
			Updated: time.Now().UTC(),
		}
		if si.Created.IsZero() {
			si.Created = si.Updated
		}

		cbkt, err := bkt.CreateBucket([]byte(name))
		if err != nil {
			return err
		}
		if err := putSnapshot(cbkt, id, si); err != nil {
			return err
		}
		if err := putUsage(cbkt, usage); err != nil {
			return err
		}
		if len(contentID) > 0 {
			if err := cbkt.Put(DevboxKeyContentID, contentID); err != nil {
				return err
			}
			root := version.Bucket(DevboxStoragePathBucket)
			if root != nil {
				if contentBkt := root.Bucket(contentID); contentBkt != nil {
					snapshotKey := contentBkt.Get(DevboxKeySnapshotKey)
					if len(snapshotKey) == 0 || string(snapshotKey) == key {
						if err := contentBkt.Put(DevboxKeySnapshotKey, []byte(name)); err != nil {
							return err
						}
					}
				}
			}
		}

		if err := bkt.DeleteBucket([]byte(key)); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%d", id), nil
}

// Remove removes a snapshot from metadata.
func Remove(ctx context.Context, key string) (string, snapshots.Kind, error) {
	var (
		id   uint64
		kind snapshots.Kind
	)
	err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
		}

		id = readID(sbkt)
		kind = readKind(sbkt)

		c := pbkt.Cursor()
		prefix := parentPrefixKey(id)
		if k, _ := c.Seek(prefix); k != nil && strings.HasPrefix(string(k), string(prefix)) {
			return fmt.Errorf("cannot remove snapshot with child: %w", errdefs.ErrFailedPrecondition)
		}

		if parent := sbkt.Get(bucketKeyParent); len(parent) > 0 {
			parentBkt := bkt.Bucket(parent)
			if parentBkt != nil {
				parentID := readID(parentBkt)
				if err := pbkt.Delete(parentKey(parentID, id)); err != nil {
					return err
				}
			}
		}

		return bkt.DeleteBucket([]byte(key))
	})
	if err != nil {
		return "", 0, err
	}

	return fmt.Sprintf("%d", id), kind, nil
}

// IDMap returns a map of snapshot IDs currently present in metadata.
func IDMap(ctx context.Context) (map[string]struct{}, error) {
	ids := map[string]struct{}{}
	err := withBucket(ctx, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		return bkt.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			sbkt := bkt.Bucket(k)
			if sbkt == nil {
				return nil
			}
			ids[fmt.Sprintf("%d", readID(sbkt))] = struct{}{}
			return nil
		})
	})
	return ids, err
}

// GetParentID returns the parent snapshot key for the provided snapshot key.
func GetParentID(ctx context.Context, key string) (string, error) {
	var parent string
	err := withSnapshotBucket(ctx, key, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		parent = string(bkt.Get(bucketKeyParent))
		return nil
	})
	if err != nil {
		return "", err
	}
	return parent, nil
}

// GetID returns the internal numeric ID string for the provided snapshot key.
func GetID(ctx context.Context, key string) (string, error) {
	var id uint64
	err := withSnapshotBucket(ctx, key, func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
		id = readID(bkt)
		return nil
	})
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%d", id), nil
}

// GetSnapshotDevboxInfo returns the devbox content ID and mount path for a snapshot key.
func GetSnapshotDevboxInfo(ctx context.Context, key string) (string, string, error) {
	var (
		contentID string
		mountPath string
	)
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
		}
		contentID = string(sbkt.Get(DevboxKeyContentID))
		if contentID == "" {
			return nil
		}
		if root == nil {
			return nil
		}
		cbkt := root.Bucket([]byte(contentID))
		if cbkt == nil {
			return nil
		}
		mountPath = string(cbkt.Get(DevboxKeyPath))
		return nil
	})
	if err != nil {
		return "", "", err
	}
	return contentID, mountPath, nil
}

// SetDevboxContent associates a snapshot key with a devbox content record.
func SetDevboxContent(ctx context.Context, key, contentID, lvName, mountPath string) error {
	return withCreatedDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
		}
		if err := sbkt.Put(DevboxKeyContentID, []byte(contentID)); err != nil {
			return err
		}

		cbkt, err := root.CreateBucketIfNotExists([]byte(contentID))
		if err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyPath, []byte(mountPath)); err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyLvName, []byte(lvName)); err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyStatus, DevboxStatusActive); err != nil {
			return err
		}
		return cbkt.Put(DevboxKeySnapshotKey, []byte(key))
	})
}

// RemoveDevbox removes the snapshot association for a devbox content record and
// returns the mount path if one was recorded. Removed content metadata is kept
// until the LV cleanup path deletes the volume successfully.
func RemoveDevbox(ctx context.Context, key string) (string, error) {
	var mountPath string

	log.G(ctx).WithField("key", key).Warnf("[REMOVE-DEVBOX-TRACE] RemoveDevbox called with key")
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			log.G(ctx).WithField("key", key).Warnf("[REMOVE-DEVBOX-TRACE] devbox snapshot bucket for key %s does not exist", key)
			return errdefs.ErrNotFound
		}
		contentID := sbkt.Get(DevboxKeyContentID)
		if len(contentID) == 0 {
			return nil
		}

		if root == nil {
			return nil
		}
		cbkt := root.Bucket(contentID)
		if cbkt == nil {
			return nil
		}
		snapshotKey := string(cbkt.Get(DevboxKeySnapshotKey))
		if snapshotKey == key {
			mountPath = string(cbkt.Get(DevboxKeyPath))
		}

		log.G(ctx).WithFields(log.Fields{
			"key":             key,
			"contentID":       string(contentID),
			"snapshotKey":     snapshotKey,
			"mountPath":       mountPath,
			"mountPath_empty": mountPath == "",
		}).Warnf("[REMOVE-DEVBOX-TRACE] Retrieved fields from snapshot bucket")
		if snapshotKey != "" && snapshotKey == key {
			return cbkt.Put(DevboxKeySnapshotKey, []byte(""))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return mountPath, nil
}

// GetDevboxLvName returns the LV name for a content ID. If snapshotKey is
// provided, it is used to resolve the content ID from the snapshot record.
func GetDevboxLvName(ctx context.Context, contentID, snapshotKey string) (string, error) {
	var lvName string
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		if root != nil && contentID != "" {
			if cbkt := root.Bucket([]byte(contentID)); cbkt != nil {
				lvName = string(cbkt.Get(DevboxKeyLvName))
				if lvName != "" {
					return nil
				}
			}
		}

		if snapshotKey != "" {
			sbkt := bkt.Bucket([]byte(snapshotKey))
			if sbkt == nil {
				return fmt.Errorf("snapshot %q does not exist: %w", snapshotKey, errdefs.ErrNotFound)
			}
			if cid := sbkt.Get(DevboxKeyContentID); len(cid) > 0 && root != nil {
				if cbkt := root.Bucket(cid); cbkt != nil {
					lvName = string(cbkt.Get(DevboxKeyLvName))
				}
			}
		}

		if lvName == "" {
			return errdefs.ErrNotFound
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return lvName, nil
}

// GetRemovedDevboxContents returns removed devbox contents that are no longer
// attached to any snapshot and are ready for LV cleanup.
func GetRemovedDevboxContents(ctx context.Context) ([]RemovedDevboxContent, error) {
	var contents []RemovedDevboxContent
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			cbkt := root.Bucket(k)
			if cbkt == nil {
				return nil
			}
			if status := cbkt.Get(DevboxKeyStatus); string(status) != string(DevboxStatusRemoved) {
				return nil
			}
			if snapshotKey := cbkt.Get(DevboxKeySnapshotKey); len(snapshotKey) > 0 {
				return nil
			}
			lvName := string(cbkt.Get(DevboxKeyLvName))
			if lvName == "" {
				return nil
			}
			contents = append(contents, RemovedDevboxContent{
				ContentID: string(k),
				LVName:    lvName,
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return contents, nil
}

// GetDevboxLvNames returns all devbox LV names keyed by LV name.
func GetDevboxLvNames(ctx context.Context) (map[string]struct{}, error) {
	names := map[string]struct{}{}
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		if root == nil {
			return nil
		}
		return root.ForEach(func(k, v []byte) error {
			if v != nil {
				return nil
			}
			cbkt := root.Bucket(k)
			if cbkt == nil {
				return nil
			}
			lvName := string(cbkt.Get(DevboxKeyLvName))
			if lvName != "" {
				names[lvName] = struct{}{}
			}
			return nil
		})
	})
	return names, err
}

func DeleteDevboxContent(ctx context.Context, contentID string) error {
	return withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		if root == nil {
			return errdefs.ErrNotFound
		}
		return root.DeleteBucket([]byte(contentID))
	})
}

// SetUnmountedWithKey clears the snapshot association for a devbox content and
// returns the recorded mount path.
func SetUnmountedWithKey(ctx context.Context, key string) (string, error) {
	var mountPath string
	err := withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		sbkt := bkt.Bucket([]byte(key))
		if sbkt == nil {
			return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
		}
		contentID := sbkt.Get(DevboxKeyContentID)
		if len(contentID) == 0 {
			return errdefs.ErrNotFound
		}

		if root == nil {
			return errdefs.ErrNotFound
		}
		cbkt := root.Bucket(contentID)
		if cbkt == nil {
			return errdefs.ErrNotFound
		}
		if snapshotKey := cbkt.Get(DevboxKeySnapshotKey); len(snapshotKey) > 0 && string(snapshotKey) == key {
			mountPath = string(cbkt.Get(DevboxKeyPath))
			return cbkt.Put(DevboxKeySnapshotKey, []byte(""))
		}
		return nil
	})
	if err != nil {
		return "", err
	}
	return mountPath, nil
}

// SetDevboxContentStatusRemoved marks a devbox content record as removed.
func SetDevboxContentStatusRemoved(ctx context.Context, contentID string) error {
	return withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
		if root == nil {
			return errdefs.ErrNotFound
		}
		cbkt := root.Bucket([]byte(contentID))
		if cbkt == nil {
			return errdefs.ErrNotFound
		}
		return cbkt.Put(DevboxKeyStatus, DevboxStatusRemoved)
	})
}

func withBucket(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok || tx == nil {
		return ErrNoTransaction
	}
	version := tx.Bucket(bucketKeyStorageVersion)
	if version == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	return fn(ctx, version.Bucket(bucketKeySnapshot), version.Bucket(bucketKeyParents))
}

func createBucketIfNotExists(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok || tx == nil {
		return ErrNoTransaction
	}

	version, err := tx.CreateBucketIfNotExists(bucketKeyStorageVersion)
	if err != nil {
		return fmt.Errorf("failed to create version bucket: %w", err)
	}
	bkt, err := version.CreateBucketIfNotExists(bucketKeySnapshot)
	if err != nil {
		return fmt.Errorf("failed to create snapshots bucket: %w", err)
	}
	pbkt, err := version.CreateBucketIfNotExists(bucketKeyParents)
	if err != nil {
		return fmt.Errorf("failed to create parents bucket: %w", err)
	}
	return fn(ctx, version, bkt, pbkt)
}

func withSnapshotBucket(ctx context.Context, key string, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok || tx == nil {
		return ErrNoTransaction
	}
	version := tx.Bucket(bucketKeyStorageVersion)
	if version == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	bkt := version.Bucket(bucketKeySnapshot)
	if bkt == nil {
		return fmt.Errorf("snapshots bucket does not exist: %w", errdefs.ErrNotFound)
	}
	sbkt := bkt.Bucket([]byte(key))
	if sbkt == nil {
		return fmt.Errorf("snapshot %q does not exist: %w", key, errdefs.ErrNotFound)
	}
	return fn(ctx, sbkt, version.Bucket(bucketKeyParents))
}

func withDevboxStorageBucket(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok || tx == nil {
		return ErrNoTransaction
	}
	version := tx.Bucket(bucketKeyStorageVersion)
	if version == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	bkt := version.Bucket(bucketKeySnapshot)
	if bkt == nil {
		return fmt.Errorf("snapshots bucket does not exist: %w", errdefs.ErrNotFound)
	}
	return fn(ctx, bkt, version.Bucket(DevboxStoragePathBucket))
}

func withCreatedDevboxStorageBucket(ctx context.Context, fn func(context.Context, *bolt.Bucket, *bolt.Bucket) error) error {
	tx, ok := ctx.Value(transactionKey{}).(*bolt.Tx)
	if !ok || tx == nil {
		return ErrNoTransaction
	}
	version := tx.Bucket(bucketKeyStorageVersion)
	if version == nil {
		return fmt.Errorf("bucket does not exist: %w", errdefs.ErrNotFound)
	}
	bkt := version.Bucket(bucketKeySnapshot)
	if bkt == nil {
		return fmt.Errorf("snapshots bucket does not exist: %w", errdefs.ErrNotFound)
	}
	root, err := version.CreateBucketIfNotExists(DevboxStoragePathBucket)
	if err != nil {
		return err
	}
	return fn(ctx, bkt, root)
}

func sequenceNext(bkt *bolt.Bucket) (uint64, error) {
	return bkt.NextSequence()
}

func putSnapshot(bkt *bolt.Bucket, id uint64, si snapshots.Info) error {
	if err := putID(bkt, id); err != nil {
		return err
	}
	if err := putKind(bkt, si.Kind); err != nil {
		return err
	}
	if si.Parent != "" {
		if err := bkt.Put(bucketKeyParent, []byte(si.Parent)); err != nil {
			return err
		}
	}
	if err := boltutil.WriteTimestamps(bkt, si.Created, si.Updated); err != nil {
		return err
	}
	return boltutil.WriteLabels(bkt, si.Labels)
}

func readSnapshot(bkt *bolt.Bucket, id *uint64, si *snapshots.Info) error {
	if id != nil {
		*id = readID(bkt)
	}
	si.Parent = string(bkt.Get(bucketKeyParent))
	si.Kind = readKind(bkt)
	si.Created = readTimestamp(bkt, bucketKeyCreated)
	si.Updated = readTimestamp(bkt, bucketKeyUpdated)

	labels, err := boltutil.ReadLabels(bkt)
	if err != nil {
		return err
	}
	si.Labels = labels
	return nil
}

func putID(bkt *bolt.Bucket, id uint64) error {
	idEncoded, err := encodeID(id)
	if err != nil {
		return err
	}
	return bkt.Put(bucketKeyID, idEncoded)
}

func readID(bkt *bolt.Bucket) uint64 {
	id, _ := binary.Uvarint(bkt.Get(bucketKeyID))
	return id
}

func putKind(bkt *bolt.Bucket, kind snapshots.Kind) error {
	return bkt.Put(bucketKeyKind, []byte{byte(kind)})
}

func readKind(bkt *bolt.Bucket) snapshots.Kind {
	v := bkt.Get(bucketKeyKind)
	if len(v) != 1 {
		return 0
	}
	return snapshots.Kind(v[0])
}

func putUsage(bkt *bolt.Bucket, usage snapshots.Usage) error {
	for _, entry := range []struct {
		key   []byte
		value int64
	}{
		{bucketKeyInodes, usage.Inodes},
		{bucketKeySize, usage.Size},
	} {
		encoded, err := encodeSize(entry.value)
		if err != nil {
			return err
		}
		if err := bkt.Put(entry.key, encoded); err != nil {
			return err
		}
	}
	return nil
}

func getUsage(bkt *bolt.Bucket, usage *snapshots.Usage) {
	usage.Inodes, _ = binary.Varint(bkt.Get(bucketKeyInodes))
	usage.Size, _ = binary.Varint(bkt.Get(bucketKeySize))
}

func encodeID(id uint64) ([]byte, error) {
	var buf [binary.MaxVarintLen64]byte
	encoded := buf[:binary.PutUvarint(buf[:], id)]
	if len(encoded) == 0 {
		return nil, fmt.Errorf("failed encoding id = %v", id)
	}
	return encoded, nil
}

func encodeSize(size int64) ([]byte, error) {
	var buf [binary.MaxVarintLen64]byte
	encoded := buf[:binary.PutVarint(buf[:], size)]
	if len(encoded) == 0 {
		return nil, fmt.Errorf("failed encoding size = %v", size)
	}
	return encoded, nil
}

var (
	bucketKeyCreated = []byte("created")
	bucketKeyUpdated = []byte("updated")
)

func readTimestamp(bkt *bolt.Bucket, key []byte) time.Time {
	v := bkt.Get(key)
	if len(v) == 0 {
		return time.Time{}
	}
	t, err := time.Parse(time.RFC3339Nano, string(v))
	if err != nil {
		return time.Time{}
	}
	return t
}

func parents(bkt, sbkt *bolt.Bucket, parentID uint64) ([]string, error) {
	var out []string
	for sbkt != nil {
		out = append(out, fmt.Sprintf("%d", parentID))
		parentKey := sbkt.Get(bucketKeyParent)
		if len(parentKey) == 0 {
			break
		}
		sbkt = bkt.Bucket(parentKey)
		if sbkt == nil {
			return nil, fmt.Errorf("parent snapshot %q does not exist: %w", string(parentKey), errdefs.ErrNotFound)
		}
		parentID = readID(sbkt)
	}
	return out, nil
}

type snapshotAdapter snapshots.Info

func adaptSnapshot(info snapshots.Info) snapshotAdapter {
	return snapshotAdapter(info)
}

func (s snapshotAdapter) Field(path []string) (string, bool) {
	if len(path) == 0 {
		return "", false
	}

	switch path[0] {
	case "name":
		return s.Name, true
	case "parent":
		return s.Parent, true
	case "kind":
		return fmt.Sprintf("%d", s.Kind), true
	case "labels":
		if len(path) != 2 || s.Labels == nil {
			return "", false
		}
		v, ok := s.Labels[path[1]]
		return v, ok
	default:
		return "", false
	}
}
