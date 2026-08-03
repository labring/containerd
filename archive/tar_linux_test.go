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

package archive

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/containerd/containerd/archive/tartest"
	"github.com/containerd/containerd/mount"
	"github.com/containerd/containerd/pkg/testutil"
	"github.com/containerd/containerd/snapshots/overlay/overlayutils"
	"github.com/containerd/continuity/fs"
	"github.com/containerd/continuity/fs/fstest"
	"github.com/containerd/log/logtest"
)

func TestOverlayApply(t *testing.T) {
	testutil.RequiresRoot(t)

	base := t.TempDir()

	if err := overlayutils.Supported(base); err != nil {
		t.Skipf("skipping because overlay is not supported %v", err)
	}
	fstest.FSSuite(t, overlayDiffApplier{
		tmp:  base,
		diff: WriteDiff,
		t:    t,
	})
}

func TestOverlayApplyNoParents(t *testing.T) {
	testutil.RequiresRoot(t)

	base := t.TempDir()

	if err := overlayutils.Supported(base); err != nil {
		t.Skipf("skipping because overlay is not supported %v", err)
	}
	fstest.FSSuite(t, overlayDiffApplier{
		tmp: base,
		diff: func(ctx context.Context, w io.Writer, a, b string, _ ...WriteDiffOpt) error {
			cw := NewChangeWriter(w, b)
			cw.addedDirs = nil
			err := fs.Changes(ctx, a, b, cw.HandleChange)
			if err != nil {
				return fmt.Errorf("failed to create diff tar stream: %w", err)
			}
			return cw.Close()
		},
		t: t,
	})
}

func TestOverlayApplyReplacesWhiteoutParent(t *testing.T) {
	testutil.RequiresRoot(t)

	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	tc := tartest.TarContext{}.WithUIDGID(os.Getuid(), os.Getgid())

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.Dir("home", 0755),
		tc.Dir("home/devbox", 0755),
		tc.File("home/devbox/.wh..vscode-server", []byte{}, 0644),
	)), WithConvertWhiteout(OverlayConvertWhiteout)); err != nil {
		t.Fatal(err)
	}

	parent := filepath.Join(root, "home/devbox/.vscode-server")
	fi, err := os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !isOverlayWhiteout(fi) {
		t.Fatalf("expected %q to be an overlay whiteout, got mode %v", parent, fi.Mode())
	}

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.File("home/devbox/.vscode-server/cli", []byte("ok"), 0644),
	)), WithConvertWhiteout(OverlayConvertWhiteout)); err != nil {
		t.Fatal(err)
	}

	fi, err = os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected %q to be replaced by a directory, got mode %v", parent, fi.Mode())
	}

	b, err := os.ReadFile(filepath.Join(parent, "cli"))
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "ok" {
		t.Fatalf("unexpected cli content %q", string(b))
	}
}

func TestOverlayApplyReplacesWhiteoutAncestor(t *testing.T) {
	testutil.RequiresRoot(t)

	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	tc := tartest.TarContext{}.WithUIDGID(os.Getuid(), os.Getgid())

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.Dir("home", 0755),
		tc.Dir("home/devbox", 0755),
		tc.File("home/devbox/.wh..vscode-server", []byte{}, 0644),
	)), WithConvertWhiteout(OverlayConvertWhiteout)); err != nil {
		t.Fatal(err)
	}

	parent := filepath.Join(root, "home/devbox/.vscode-server")
	fi, err := os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !isOverlayWhiteout(fi) {
		t.Fatalf("expected %q to be an overlay whiteout, got mode %v", parent, fi.Mode())
	}

	deepFile := filepath.Join(parent, "cli/server/bin")
	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.File("home/devbox/.vscode-server/cli/server/bin", []byte("ok"), 0644),
	)), WithConvertWhiteout(OverlayConvertWhiteout)); err != nil {
		t.Fatal(err)
	}

	fi, err = os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected %q to be replaced by a directory, got mode %v", parent, fi.Mode())
	}

	b, err := os.ReadFile(deepFile)
	if err != nil {
		t.Fatal(err)
	}
	if string(b) != "ok" {
		t.Fatalf("unexpected deep file content %q", string(b))
	}
}

func TestOverlayApplyKeepsNonWhiteoutAncestorENOTDIR(t *testing.T) {
	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	tc := tartest.TarContext{}.WithUIDGID(os.Getuid(), os.Getgid())

	parent := filepath.Join(root, "home/devbox/.vscode-server")
	if err := os.MkdirAll(filepath.Dir(parent), 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(parent, []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.File("home/devbox/.vscode-server/cli/server/bin", []byte("ok"), 0644),
	)), WithConvertWhiteout(OverlayConvertWhiteout)); !errors.Is(err, syscall.ENOTDIR) {
		t.Fatalf("expected ENOTDIR, got %v", err)
	}

	fi, err := os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.Mode().IsRegular() {
		t.Fatalf("expected %q to remain a regular file, got mode %v", parent, fi.Mode())
	}
}

func TestApplySkipsMtimeForRemovedDirectory(t *testing.T) {
	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	tc := tartest.TarContext{}.WithUIDGID(os.Getuid(), os.Getgid())

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.Dir("home", 0755),
		tc.Dir("home/devbox", 0755),
		tc.Dir("home/devbox/.vscode-server", 0755),
		tc.Dir("home/devbox/.vscode-server/cli", 0755),
		tc.File("home/devbox/.wh..vscode-server", []byte{}, 0644),
	))); err != nil {
		t.Fatal(err)
	}

	if _, err := os.Lstat(filepath.Join(root, "home/devbox/.vscode-server")); !os.IsNotExist(err) {
		t.Fatalf("expected .vscode-server to be removed, got %v", err)
	}
}

func TestApplySkipsMtimeForReplacedDirectory(t *testing.T) {
	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	tc := tartest.TarContext{}.WithUIDGID(os.Getuid(), os.Getgid())
	dirTime := time.Unix(100, 0).UTC()
	fileTime := time.Unix(200, 0).UTC()

	if _, err := Apply(ctx, root, tartest.TarFromWriterTo(tartest.TarAll(
		tc.WithModTime(dirTime).Dir("home/devbox/.vscode-server", 0755),
		tc.WithModTime(fileTime).File("home/devbox/.vscode-server", []byte("ok"), 0644),
	))); err != nil {
		t.Fatal(err)
	}

	path := filepath.Join(root, "home/devbox/.vscode-server")
	fi, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.IsDir() {
		t.Fatalf("expected %q to be a regular file", path)
	}
	if got := fi.ModTime(); got.Unix() != fileTime.Unix() {
		t.Fatalf("expected file mtime %v, got %v", fileTime, got)
	}
}

func TestMkparentSkipsLowerParentENOTDIR(t *testing.T) {
	ctx := logtest.WithT(context.Background(), t)
	root := t.TempDir()
	lower := t.TempDir()

	for _, dir := range []string{
		filepath.Join(root, "home/devbox"),
		filepath.Join(lower, "home/devbox"),
	} {
		if err := os.MkdirAll(dir, 0755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(lower, "home/devbox/.vscode-server"), []byte("not a directory"), 0644); err != nil {
		t.Fatal(err)
	}

	parent := filepath.Join(root, "home/devbox/.vscode-server/cli")
	if err := mkparent(ctx, parent, root, []string{lower}); err != nil {
		t.Fatal(err)
	}

	fi, err := os.Lstat(parent)
	if err != nil {
		t.Fatal(err)
	}
	if !fi.IsDir() {
		t.Fatalf("expected %q to be created as directory, got mode %v", parent, fi.Mode())
	}
}

type overlayDiffApplier struct {
	tmp  string
	diff func(context.Context, io.Writer, string, string, ...WriteDiffOpt) error
	t    *testing.T
}

type overlayContext struct {
	merged  string
	lowers  []string
	mounted bool
}

type contextKey struct{}

func (d overlayDiffApplier) TestContext(ctx context.Context) (context.Context, func(), error) {
	merged, err := os.MkdirTemp(d.tmp, "merged")
	if err != nil {
		return ctx, nil, fmt.Errorf("failed to make merged dir: %w", err)
	}

	oc := &overlayContext{
		merged: merged,
	}

	ctx = logtest.WithT(ctx, d.t)

	return context.WithValue(ctx, contextKey{}, oc), func() {
		if oc.mounted {
			mount.Unmount(oc.merged, 0)
		}
	}, nil
}

func (d overlayDiffApplier) Apply(ctx context.Context, a fstest.Applier) (string, func(), error) {
	oc := ctx.Value(contextKey{}).(*overlayContext)

	applyCopy, err := os.MkdirTemp(d.tmp, "apply-copy-")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	defer os.RemoveAll(applyCopy)

	base := oc.merged
	if len(oc.lowers) == 1 {
		base = oc.lowers[0]
	}

	if err = fs.CopyDir(applyCopy, base); err != nil {
		return "", nil, fmt.Errorf("failed to copy base: %w", err)
	}

	if err := a.Apply(applyCopy); err != nil {
		return "", nil, fmt.Errorf("failed to apply changes to copy of base: %w", err)
	}

	buf := bytes.NewBuffer(nil)

	if err := d.diff(ctx, buf, base, applyCopy); err != nil {
		return "", nil, fmt.Errorf("failed to create diff: %w", err)
	}

	if oc.mounted {
		if err := mount.Unmount(oc.merged, 0); err != nil {
			return "", nil, fmt.Errorf("failed to unmount: %w", err)
		}
		oc.mounted = false
	}

	next, err := os.MkdirTemp(d.tmp, "lower-")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp dir: %w", err)
	}

	if _, err = Apply(ctx, next, buf, WithConvertWhiteout(OverlayConvertWhiteout), WithParents(oc.lowers)); err != nil {
		return "", nil, fmt.Errorf("failed to apply tar stream: %w", err)
	}

	oc.lowers = append([]string{next}, oc.lowers...)

	if len(oc.lowers) == 1 {
		return oc.lowers[0], nil, nil
	}

	m := mount.Mount{
		Type:   "overlay",
		Source: "overlay",
		Options: []string{
			fmt.Sprintf("lowerdir=%s", strings.Join(oc.lowers, ":")),
		},
	}

	if err := m.Mount(oc.merged); err != nil {
		return "", nil, fmt.Errorf("failed to mount: %v: %w", m, err)
	}
	oc.mounted = true

	return oc.merged, nil, nil
}
