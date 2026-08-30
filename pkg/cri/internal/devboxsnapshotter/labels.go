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

package devboxsnapshotter

const (
	StargzSnapshotter                  = "stargz"
	SealosDevboxContentIDAnnotation    = "devbox.sealos.io/content-id"
	SealosDevboxStorageLimitAnnotation = "devbox.sealos.io/storage-limit"
)

// LabelsFromAnnotations keeps only the annotations used by the devbox-backed
// writable-layer flow. containerd.WithNewSnapshot will translate these labels
// to the snapshotter-specific containerd.io/snapshot/devbox-* keys when needed.
func LabelsFromAnnotations(annotations map[string]string) map[string]string {
	if len(annotations) == 0 {
		return nil
	}

	labels := make(map[string]string)
	if contentID := annotations[SealosDevboxContentIDAnnotation]; contentID != "" {
		labels[SealosDevboxContentIDAnnotation] = contentID
	}
	if storageLimit := annotations[SealosDevboxStorageLimitAnnotation]; storageLimit != "" {
		labels[SealosDevboxStorageLimitAnnotation] = storageLimit
	}
	return labels
}
