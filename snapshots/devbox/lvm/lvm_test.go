/*
Copyright 2017 The Kubernetes Authors.

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

package lvm

import (
	"fmt"
	"strings"
	"testing"
)

// TestRunCommandSplitErrorInfo tests the RunCommandSplit function
func TestRunCommandSplitErrorInfo(t *testing.T) {
	// test1: multiline stderr
	t.Run("multiline stderr", func(t *testing.T) {
		_, stderr, err := RunCommandSplit("sh", "-c", "echo 'line1' >&2 && echo 'line2' >&2")

		fmt.Println("error info:", err.Error())
		fmt.Println("stderr:", string(stderr))
		
		// check if stderr contains newline
		if !strings.Contains(string(stderr), "\n") {
			t.Error("stderr should contain newline")
		}
		
		// check if newline is replaced with " | "
		if err != nil && !strings.Contains(err.Error(), " | ") {
			t.Error("newline should be replaced with ' | '")
		}
	})

	// test2: single line stderr
	t.Run("single line stderr", func(t *testing.T) {
		_, stderr, err := RunCommandSplit("sh", "-c", "echo 'single error' >&2")

		fmt.Println("error info:", err.Error())
		fmt.Println("stderr:", string(stderr))
	})
}