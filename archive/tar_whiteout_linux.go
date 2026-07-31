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
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

func isOverlayWhiteout(fi os.FileInfo) bool {
	if fi.Mode()&os.ModeCharDevice == 0 {
		return false
	}

	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}

	rdev := uint64(st.Rdev)
	return unix.Major(rdev) == 0 && unix.Minor(rdev) == 0
}
