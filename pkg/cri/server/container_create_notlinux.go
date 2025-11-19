//go:build !linux

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

package server

import (
	"github.com/containerd/containerd/snapshots"
	runtime "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// devboxSnapshotterOpts is a no-op stub on non-Linux platforms. The real
// implementation is platform-specific and provided in
// container_create_linux.go. This stub exists so Windows and other non-Linux
// builds that reference devboxSnapshotterOpts compile successfully.
func devboxSnapshotterOpts(snapshotterName string, config *runtime.PodSandboxConfig) (snapshots.Opt, error) {
	return nil, nil
}
