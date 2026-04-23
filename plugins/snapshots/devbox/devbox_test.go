//go:build linux

package devbox

import "testing"

func TestPlanDevboxLVM(t *testing.T) {
	tests := []struct {
		name                 string
		contentID            string
		useLimit             string
		existingLVName       string
		contentIDProvided    bool
		storageLimitProvided bool
		wantReuseExisting    bool
		wantResizeExisting   bool
		wantCreateNew        bool
	}{
		{
			name:                 "reuse existing lv without storage limit",
			contentID:            "content-1",
			existingLVName:       "devbox-content-1",
			contentIDProvided:    true,
			storageLimitProvided: false,
			wantReuseExisting:    true,
		},
		{
			name:                 "reuse and resize existing lv when storage limit present",
			contentID:            "content-1",
			useLimit:             "20Gi",
			existingLVName:       "devbox-content-1",
			contentIDProvided:    true,
			storageLimitProvided: true,
			wantReuseExisting:    true,
			wantResizeExisting:   true,
		},
		{
			name:                 "create new lv when content id and storage limit are present",
			contentID:            "content-1",
			useLimit:             "20Gi",
			contentIDProvided:    true,
			storageLimitProvided: true,
			wantCreateNew:        true,
		},
		{
			name:              "fall back when content id exists but no lv or storage limit",
			contentID:         "content-1",
			contentIDProvided: true,
		},
		{
			name:                 "ignore storage limit without content id",
			useLimit:             "20Gi",
			storageLimitProvided: true,
		},
		{
			name:                 "trim whitespace before planning",
			contentID:            "  content-1  ",
			existingLVName:       "  devbox-content-1  ",
			contentIDProvided:    true,
			storageLimitProvided: false,
			wantReuseExisting:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planDevboxLVM(
				tt.contentID,
				tt.useLimit,
				tt.existingLVName,
				tt.contentIDProvided,
				tt.storageLimitProvided,
			)

			if plan.reuseExisting != tt.wantReuseExisting {
				t.Fatalf("reuseExisting = %v, want %v", plan.reuseExisting, tt.wantReuseExisting)
			}
			if plan.resizeExisting != tt.wantResizeExisting {
				t.Fatalf("resizeExisting = %v, want %v", plan.resizeExisting, tt.wantResizeExisting)
			}
			if plan.createNew != tt.wantCreateNew {
				t.Fatalf("createNew = %v, want %v", plan.createNew, tt.wantCreateNew)
			}
		})
	}
}
