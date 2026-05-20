//go:build linux

package storage

import (
	"context"
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/containerd/containerd/v2/core/snapshots"
	"github.com/containerd/errdefs"
	bolt "go.etcd.io/bbolt"
)

func newTestMetaStore(t *testing.T) *MetaStore {
	t.Helper()

	ms, err := NewMetaStore(filepath.Join(t.TempDir(), "metadata.db"))
	if err != nil {
		t.Fatalf("NewMetaStore() error = %v", err)
	}
	t.Cleanup(func() {
		if err := ms.Close(); err != nil {
			t.Fatalf("MetaStore.Close() error = %v", err)
		}
	})
	return ms
}

func withTestTransaction(t *testing.T, ms *MetaStore, writable bool, fn func(context.Context) error) {
	t.Helper()

	if err := ms.WithTransaction(context.Background(), writable, fn); err != nil {
		t.Fatalf("transaction error = %v", err)
	}
}

func createActiveSnapshotWithContent(t *testing.T, ms *MetaStore, key, contentID, lvName, mountPath string) {
	t.Helper()

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, key, ""); err != nil {
			return err
		}
		return SetDevboxContent(ctx, key, contentID, lvName, mountPath)
	})
}

func readContentRecord(t *testing.T, ms *MetaStore, contentID string) (status, snapshotKey, mountPath string, err error) {
	t.Helper()

	err = ms.WithTransaction(context.Background(), false, func(ctx context.Context) error {
		return withDevboxStorageBucket(ctx, func(ctx context.Context, bkt, root *bolt.Bucket) error {
			if root == nil {
				return errdefs.ErrNotFound
			}
			cbkt := root.Bucket([]byte(contentID))
			if cbkt == nil {
				return errdefs.ErrNotFound
			}
			status = string(cbkt.Get(DevboxKeyStatus))
			snapshotKey = string(cbkt.Get(DevboxKeySnapshotKey))
			mountPath = string(cbkt.Get(DevboxKeyPath))
			return nil
		})
	})
	return
}

func legacyUvarint(v uint64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(buf[:], v)
	return buf[:n]
}

func legacyVarint(v int64) []byte {
	var buf [binary.MaxVarintLen64]byte
	n := binary.PutVarint(buf[:], v)
	return buf[:n]
}

func TestReadLegacyVarintSnapshotMetadata(t *testing.T) {
	ms := newTestMetaStore(t)

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, "active-key", ""); err != nil {
			return err
		}
		_, err := CommitActive(ctx, "active-key", "committed-key", snapshots.Usage{
			Inodes: 7,
			Size:   12345,
		})
		return err
	})

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		return withSnapshotBucket(ctx, "committed-key", func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
			if err := bkt.Put(bucketKeyID, legacyUvarint(1)); err != nil {
				return err
			}
			if err := bkt.Put(bucketKeyKind, []byte{byte(snapshots.KindCommitted)}); err != nil {
				return err
			}
			if err := bkt.Put(bucketKeyInodes, legacyVarint(7)); err != nil {
				return err
			}
			return bkt.Put(bucketKeySize, legacyVarint(12345))
		})
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		id, info, usage, err := GetInfo(ctx, "committed-key")
		if err != nil {
			return err
		}
		if id != "1" {
			t.Fatalf("id = %q, want %q", id, "1")
		}
		if info.Kind != snapshots.KindCommitted {
			t.Fatalf("kind = %v, want %v", info.Kind, snapshots.KindCommitted)
		}
		if usage.Inodes != 7 || usage.Size != 12345 {
			t.Fatalf("usage = %+v, want inodes=7 size=12345", usage)
		}
		return nil
	})
}

func TestWritesLegacySnapshotMetadataEncoding(t *testing.T) {
	ms := newTestMetaStore(t)

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, "active-key", ""); err != nil {
			return err
		}
		_, err := CommitActive(ctx, "active-key", "committed-key", snapshots.Usage{
			Inodes: 7,
			Size:   12345,
		})
		return err
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		return withSnapshotBucket(ctx, "committed-key", func(ctx context.Context, bkt, pbkt *bolt.Bucket) error {
			id := bkt.Get(bucketKeyID)
			if got, want := id, legacyUvarint(1); string(got) != string(want) {
				t.Fatalf("id encoding = %v, want %v", got, want)
			}
			kind := bkt.Get(bucketKeyKind)
			if got, want := kind, []byte{byte(snapshots.KindCommitted)}; string(got) != string(want) {
				t.Fatalf("kind encoding = %v, want %v", got, want)
			}
			inodes := bkt.Get(bucketKeyInodes)
			if got, want := inodes, legacyVarint(7); string(got) != string(want) {
				t.Fatalf("inodes encoding = %v, want %v", got, want)
			}
			size := bkt.Get(bucketKeySize)
			if got, want := size, legacyVarint(12345); string(got) != string(want) {
				t.Fatalf("size encoding = %v, want %v", got, want)
			}
			return nil
		})
	})
}

func TestReadLegacyDevboxContentRootBucket(t *testing.T) {
	ms := newTestMetaStore(t)

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, "active-key", ""); err != nil {
			return err
		}
		tx := ctx.Value(transactionKey{}).(*bolt.Tx)
		version := tx.Bucket(bucketKeyStorageVersion)
		root, err := version.CreateBucketIfNotExists(DevboxStoragePathBucket)
		if err != nil {
			return err
		}
		sbkt := version.Bucket(bucketKeySnapshot).Bucket([]byte("active-key"))
		if err := sbkt.Put(DevboxKeyContentID, []byte("content-1")); err != nil {
			return err
		}
		cbkt, err := root.CreateBucketIfNotExists([]byte("content-1"))
		if err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyLvName, []byte("devbox-content-1")); err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyStatus, DevboxStatusActive); err != nil {
			return err
		}
		if err := cbkt.Put(DevboxKeyPath, []byte("/snapshots/1")); err != nil {
			return err
		}
		return cbkt.Put(DevboxKeySnapshotKey, []byte("active-key"))
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		contentID, mountPath, err := GetSnapshotDevboxInfo(ctx, "active-key")
		if err != nil {
			return err
		}
		if contentID != "content-1" {
			t.Fatalf("contentID = %q, want %q", contentID, "content-1")
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}
		lvName, err := GetDevboxLvName(ctx, "content-1", "")
		if err != nil {
			return err
		}
		if lvName != "devbox-content-1" {
			t.Fatalf("lvName = %q, want %q", lvName, "devbox-content-1")
		}
		lvs, err := GetDevboxLvNames(ctx)
		if err != nil {
			return err
		}
		if _, ok := lvs["devbox-content-1"]; !ok {
			t.Fatalf("expected legacy LV to be listed, got %#v", lvs)
		}
		return nil
	})

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := SetUnmountedWithKey(ctx, "active-key")
		if err != nil {
			return err
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}
		return nil
	})
}

func TestSetUnmountedWithKeyKeepsContentReferenced(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := SetUnmountedWithKey(ctx, "active-key")
		if err != nil {
			return err
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}
		return nil
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		lvName, err := GetDevboxLvName(ctx, "content-1", "")
		if err != nil {
			return err
		}
		if lvName != "devbox-content-1" {
			t.Fatalf("lvName = %q, want %q", lvName, "devbox-content-1")
		}

		lvs, err := GetDevboxLvNames(ctx)
		if err != nil {
			return err
		}
		if _, ok := lvs["devbox-content-1"]; !ok {
			t.Fatalf("expected LV to remain referenced, got %#v", lvs)
		}
		return nil
	})

	status, snapshotKey, mountPath, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "" {
		t.Fatalf("snapshotKey = %q, want empty", snapshotKey)
	}
	if mountPath != "/snapshots/1" {
		t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
	}
}

func TestMarkRemovedStaysReferencedUntilSnapshotRemoval(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		return SetDevboxContentStatusRemoved(ctx, "content-1")
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		lvs, err := GetDevboxLvNames(ctx)
		if err != nil {
			return err
		}
		if _, ok := lvs["devbox-content-1"]; !ok {
			t.Fatalf("expected removed content to stay referenced before snapshot removal, got %#v", lvs)
		}
		return nil
	})

	status, snapshotKey, _, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusRemoved) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusRemoved)
	}
	if snapshotKey != "active-key" {
		t.Fatalf("snapshotKey = %q, want %q", snapshotKey, "active-key")
	}
}

func TestRemoveDevboxKeepsActiveContentAfterSnapshotRemoval(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := RemoveDevbox(ctx, "active-key")
		if err != nil {
			return err
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}
		_, _, err = Remove(ctx, "active-key")
		return err
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		lvName, err := GetDevboxLvName(ctx, "content-1", "")
		if err != nil {
			return err
		}
		if lvName != "devbox-content-1" {
			t.Fatalf("lvName = %q, want %q", lvName, "devbox-content-1")
		}
		return nil
	})

	status, snapshotKey, _, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "" {
		t.Fatalf("snapshotKey = %q, want empty", snapshotKey)
	}
}

func TestRemoveDevboxDeletesRemovedContentAfterSnapshotRemoval(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if err := SetDevboxContentStatusRemoved(ctx, "content-1"); err != nil {
			return err
		}
		mountPath, err := RemoveDevbox(ctx, "active-key")
		if err != nil {
			return err
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}
		_, _, err = Remove(ctx, "active-key")
		return err
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		lvName, err := GetDevboxLvName(ctx, "content-1", "")
		if err != nil {
			return err
		}
		if lvName != "devbox-content-1" {
			t.Fatalf("lvName = %q, want %q", lvName, "devbox-content-1")
		}
		removed, err := GetRemovedDevboxContents(ctx)
		if err != nil {
			return err
		}
		if len(removed) != 1 {
			t.Fatalf("removed content count = %d, want 1", len(removed))
		}
		if removed[0].ContentID != "content-1" || removed[0].LVName != "devbox-content-1" {
			t.Fatalf("removed content = %#v, want content-1/devbox-content-1", removed[0])
		}
		return nil
	})

	status, snapshotKey, _, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusRemoved) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusRemoved)
	}
	if snapshotKey != "" {
		t.Fatalf("snapshotKey = %q, want empty", snapshotKey)
	}
}

func TestRemoveDevboxDoesNotReturnMountPathAfterContentReassociation(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "old-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, "new-key", ""); err != nil {
			return err
		}
		return SetDevboxContent(ctx, "new-key", "content-1", "devbox-content-1", "/snapshots/2")
	})

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := RemoveDevbox(ctx, "old-key")
		if err != nil {
			return err
		}
		if mountPath != "" {
			t.Fatalf("mountPath = %q, want empty", mountPath)
		}
		return nil
	})

	status, snapshotKey, mountPath, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "new-key" {
		t.Fatalf("snapshotKey = %q, want %q", snapshotKey, "new-key")
	}
	if mountPath != "/snapshots/2" {
		t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/2")
	}
}

func TestSetUnmountedWithKeyDoesNotReturnMountPathAfterContentReassociation(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "old-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		if _, err := CreateSnapshot(ctx, snapshots.KindActive, "new-key", ""); err != nil {
			return err
		}
		return SetDevboxContent(ctx, "new-key", "content-1", "devbox-content-1", "/snapshots/2")
	})

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := SetUnmountedWithKey(ctx, "old-key")
		if err != nil {
			return err
		}
		if mountPath != "" {
			t.Fatalf("mountPath = %q, want empty", mountPath)
		}
		return nil
	})

	status, snapshotKey, mountPath, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "new-key" {
		t.Fatalf("snapshotKey = %q, want %q", snapshotKey, "new-key")
	}
	if mountPath != "/snapshots/2" {
		t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/2")
	}
}

func TestRemoveDevboxDoesNotReturnMountPathAfterSetUnmounted(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		_, err := SetUnmountedWithKey(ctx, "active-key")
		return err
	})

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		mountPath, err := RemoveDevbox(ctx, "active-key")
		if err != nil {
			return err
		}
		if mountPath != "" {
			t.Fatalf("mountPath = %q, want empty", mountPath)
		}
		return nil
	})

	status, snapshotKey, mountPath, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "" {
		t.Fatalf("snapshotKey = %q, want empty", snapshotKey)
	}
	if mountPath != "/snapshots/1" {
		t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
	}
}

func TestCommitActiveSyncsDevboxContentBinding(t *testing.T) {
	ms := newTestMetaStore(t)
	createActiveSnapshotWithContent(t, ms, "active-key", "content-1", "devbox-content-1", "/snapshots/1")

	withTestTransaction(t, ms, true, func(ctx context.Context) error {
		_, err := CommitActive(ctx, "active-key", "committed-key", snapshots.Usage{})
		return err
	})

	withTestTransaction(t, ms, false, func(ctx context.Context) error {
		contentID, mountPath, err := GetSnapshotDevboxInfo(ctx, "committed-key")
		if err != nil {
			return err
		}
		if contentID != "content-1" {
			t.Fatalf("contentID = %q, want %q", contentID, "content-1")
		}
		if mountPath != "/snapshots/1" {
			t.Fatalf("mountPath = %q, want %q", mountPath, "/snapshots/1")
		}

		if _, _, _, err := GetInfo(ctx, "active-key"); !errdefs.IsNotFound(err) {
			t.Fatalf("GetInfo(active-key) error = %v, want not found", err)
		}
		return nil
	})

	status, snapshotKey, _, err := readContentRecord(t, ms, "content-1")
	if err != nil {
		t.Fatalf("readContentRecord() error = %v", err)
	}
	if status != string(DevboxStatusActive) {
		t.Fatalf("status = %q, want %q", status, DevboxStatusActive)
	}
	if snapshotKey != "committed-key" {
		t.Fatalf("snapshotKey = %q, want %q", snapshotKey, "committed-key")
	}
}
