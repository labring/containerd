//go:build !windows

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

package overlayfs

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/containerd/continuity/fs"
)

func TestDiffDirChangesDoesNotFollowBaseSymlink(t *testing.T) {
	root := t.TempDir()
	baseDir := filepath.Join(root, "base")
	diffDir := filepath.Join(root, "diff")

	for _, dir := range []string{
		filepath.Join(baseDir, "tmp"),
		filepath.Join(diffDir, "tmp"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}

	baseLink := filepath.Join(baseDir, "tmp", "node_modules")
	// Following this base-side symlink fails deterministically. DiffDirChanges
	// should compare the symlink inode, not resolve its target.
	if err := os.Symlink(baseLink, baseLink); err != nil {
		t.Fatal(err)
	}

	diffLink := filepath.Join(diffDir, "tmp", "node_modules")
	if err := os.Symlink("/home/devbox/node_modules", diffLink); err != nil {
		t.Fatal(err)
	}

	var found bool
	err := fs.DiffDirChanges(context.Background(), baseDir, diffDir, fs.DiffSourceOverlayFS,
		func(kind fs.ChangeKind, path string, f os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			if path != "/tmp/node_modules" {
				return nil
			}
			found = true
			if kind != fs.ChangeKindModify {
				t.Fatalf("expected %s to be a modify, got %s", path, kind)
			}
			if f == nil {
				t.Fatalf("expected file info for %s", path)
			}
			if f.Mode()&os.ModeSymlink == 0 {
				t.Fatalf("expected %s to be reported as a symlink, got mode %s", path, f.Mode())
			}
			return nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected change for /tmp/node_modules")
	}
}
