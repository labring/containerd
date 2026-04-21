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
	DevboxSnapshotter                  = "devbox"
	StargzSnapshotter                  = "stargz"
	SealosDevboxContentIDAnnotation    = "devbox.sealos.io/content-id"
	SealosDevboxStorageLimitAnnotation = "devbox.sealos.io/storage-limit"
)

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
	if len(labels) == 0 {
		return nil
	}

	return labels
}

func IsWritableSnapshotter(name string) bool {
	switch name {
	case DevboxSnapshotter, StargzSnapshotter:
		return true
	default:
		return false
	}
}
