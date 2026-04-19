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

package subresources

import (
	"testing"

	"github.com/onsi/gomega"
)

func TestTemplateHash(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name        string
		obj         interface{}
		expectError bool
	}{
		{
			name: "simple object",
			obj: map[string]interface{}{
				"key": "value",
			},
			expectError: false,
		},
		{
			name: "nested object",
			obj: map[string]interface{}{
				"metadata": map[string]interface{}{
					"name": "test",
				},
				"spec": map[string]interface{}{
					"replicas": 3,
				},
			},
			expectError: false,
		},
		{
			name: "empty object",
			obj:  map[string]interface{}{},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := TemplateHash(tt.obj)
			if tt.expectError {
				g.Expect(err).To(gomega.HaveOccurred())
			} else {
				g.Expect(err).ToNot(gomega.HaveOccurred())
				g.Expect(result).ToNot(gomega.BeEmpty())
			}
		})
	}
}

func TestTemplateHash_Deterministic(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	obj := map[string]interface{}{
		"key": "value",
		"nested": map[string]interface{}{
			"inner": "data",
		},
	}

	hash1, err1 := TemplateHash(obj)
	hash2, err2 := TemplateHash(obj)

	g.Expect(err1).ToNot(gomega.HaveOccurred())
	g.Expect(err2).ToNot(gomega.HaveOccurred())
	g.Expect(hash1).To(gomega.Equal(hash2))
}

func TestTemplateHash_DifferentObjects(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	obj1 := map[string]interface{}{"key": "value1"}
	obj2 := map[string]interface{}{"key": "value2"}

	hash1, err1 := TemplateHash(obj1)
	hash2, err2 := TemplateHash(obj2)

	g.Expect(err1).ToNot(gomega.HaveOccurred())
	g.Expect(err2).ToNot(gomega.HaveOccurred())
	g.Expect(hash1).ToNot(gomega.Equal(hash2))
}

func TestObjectKeyString(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	tests := []struct {
		name     string
		obj      interface {
			GetNamespace() string
			GetName() string
		}
		expected string
	}{
		{
			name: "with namespace",
			obj: &mockObject{
				namespace: "default",
				name:      "my-resource",
			},
			expected: "default/my-resource",
		},
		{
			name: "without namespace",
			obj: &mockObject{
				namespace: "",
				name:      "my-resource",
			},
			expected: "my-resource",
		},
		{
			name: "empty namespace",
			obj: &mockObject{
				namespace: "",
				name:      "test",
			},
			expected: "test",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ObjectKeyString(tt.obj)
			g.Expect(result).To(gomega.Equal(tt.expected))
		})
	}
}

// mockObject is a mock for testing ObjectKeyString
type mockObject struct {
	namespace string
	name      string
}

func (m *mockObject) GetNamespace() string { return m.namespace }
func (m *mockObject) GetName() string      { return m.name }