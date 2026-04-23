/*
 * Copyright 2024-2025 KusionStack Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package synccontrols

import "testing"

func TestGetTargetsPrefix(t *testing.T) {
	tests := []struct {
		name           string
		override       string
		controllerName string
		expected       string
	}{
		{
			name:           "default naming",
			override:       "",
			controllerName: "myset",
			expected:       "myset-",
		},
		{
			name:           "custom prefix",
			override:       "custom-",
			controllerName: "myset",
			expected:       "custom-",
		},
		{
			name:           "custom prefix without dash",
			override:       "custom",
			controllerName: "myset",
			expected:       "custom",
		},
		{
			name:           "empty override uses default",
			override:       "",
			controllerName: "test-controller",
			expected:       "test-controller-",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := GetTargetsPrefix(tt.override, tt.controllerName)
			if result != tt.expected {
				t.Errorf("GetTargetsPrefix() = %v, want %v", result, tt.expected)
			}
		})
	}
}
