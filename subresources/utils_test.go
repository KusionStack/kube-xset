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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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
			name:        "empty object",
			obj:         map[string]interface{}{},
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
		name string
		obj  interface {
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

func TestNameTruncator_Truncate(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()

	tests := []struct {
		name     string
		input    string
		expected int // check length, not exact value due to hash
	}{
		{
			name:     "short name unchanged",
			input:    "short-name",
			expected: 10,
		},
		{
			name:     "exactly max length unchanged",
			input:    "a123456789b123456789c123456789d123456789e123456789f123456789g12", // 63 chars
			expected: 63,
		},
		{
			name:     "long name truncated",
			input:    "this-is-a-very-long-resource-name-that-exceeds-kubernetes-limit-of-63-characters",
			expected: 63,
		},
		{
			name:     "very long name truncated",
			input:    "this-is-an-extremely-long-resource-name-that-is-way-longer-than-any-reasonable-name-should-ever-be",
			expected: 63,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := truncator.Truncate(tt.input)
			g.Expect(len(result)).To(gomega.Equal(tt.expected))
		})
	}
}

func TestNameTruncator_TruncateWithMax(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()

	// Custom max length
	result := truncator.TruncateWithMax("short", 10)
	g.Expect(result).To(gomega.Equal("short"))

	result = truncator.TruncateWithMax("this-is-longer-than-ten", 10)
	g.Expect(len(result)).To(gomega.Equal(10))
}

func TestNameTruncator_TruncateLabelValue(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()

	// Short label value unchanged
	result := truncator.TruncateLabelValue("short-value")
	g.Expect(result).To(gomega.Equal("short-value"))

	// Long label value truncated to 63
	longValue := "this-is-a-very-long-label-value-that-exceeds-kubernetes-limit-of-63-characters-for-labels"
	result = truncator.TruncateLabelValue(longValue)
	g.Expect(len(result)).To(gomega.Equal(63))
}

func TestNameTruncator_HashUniqueness(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()

	// Two different long names should produce different truncated results
	name1 := "this-is-a-long-resource-name-with-suffix-a"
	name2 := "this-is-a-long-resource-name-with-suffix-b"

	result1 := truncator.Truncate(name1)
	result2 := truncator.Truncate(name2)

	g.Expect(result1).ToNot(gomega.Equal(result2))
}

func TestNameTruncator_Deterministic(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()

	// Same input should produce same output
	name := "this-is-a-long-resource-name-for-determinism-test"

	result1 := truncator.Truncate(name)
	result2 := truncator.Truncate(name)

	g.Expect(result1).To(gomega.Equal(result2))
}

func TestLabelManager_SetLabel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()
	lm := NewLabelManager(truncator)

	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
	}

	// Set label on object with no labels
	lm.SetLabel(obj, "test-key", "test-value")
	g.Expect(obj.Labels["test-key"]).To(gomega.Equal("test-value"))

	// Set label with long value
	longValue := "this-is-a-very-long-label-value-that-exceeds-kubernetes-limit-of-63-characters"
	lm.SetLabel(obj, "long-key", longValue)
	g.Expect(len(obj.Labels["long-key"])).To(gomega.Equal(63))
}

func TestLabelManager_SetLabelWithTrackedOriginal(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()
	lm := NewLabelManager(truncator)

	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
	}

	// Short value - no annotation needed
	lm.SetLabelWithTrackedOriginal(obj, "short-key", "short-value")
	g.Expect(obj.Labels["short-key"]).To(gomega.Equal("short-value"))
	g.Expect(obj.Annotations).To(gomega.BeEmpty())

	// Long value - annotation tracks original
	longValue := "this-is-a-very-long-label-value-that-exceeds-kubernetes-limit-of-63-characters"
	lm.SetLabelWithTrackedOriginal(obj, "long-key", longValue)
	g.Expect(len(obj.Labels["long-key"])).To(gomega.Equal(63))
	g.Expect(obj.Annotations["long-key.original"]).To(gomega.Equal(longValue))
}

func TestLabelManager_SetOperatingLabel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()
	lm := NewLabelManager(truncator)

	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
	}

	// Set operating label
	lm.SetOperatingLabel(obj, "app.kusionstack.io/operating", "revision-123", "timestamp-value")

	// Check the label key contains truncated ID
	expectedKey := "app.kusionstack.io/operating/revision-123"
	g.Expect(obj.Labels[expectedKey]).To(gomega.Equal("timestamp-value"))
}

func TestLabelManager_SetRevisionLabel(t *testing.T) {
	g := gomega.NewGomegaWithT(t)
	truncator := NewNameTruncator()
	lm := NewLabelManager(truncator)

	obj := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-pod",
		},
	}

	// Set revision label
	lm.SetRevisionLabel(obj, "app.kusionstack.io/revision", "id-123", "revision-abc")

	expectedKey := "app.kusionstack.io/revision/id-123"
	g.Expect(obj.Labels[expectedKey]).To(gomega.Equal("revision-abc"))
}