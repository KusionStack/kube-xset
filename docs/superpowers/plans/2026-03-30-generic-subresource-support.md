# Generic SubResource Support Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add generic subresource support to kube-xset with name truncation and label value handling.

**Architecture:** Define a generic `SubResourceAdapter` interface that PVC, Service, and other subresource types implement. Create `NameTruncator` and `LabelManager` utilities for handling Kubernetes naming limits. Maintain backward compatibility with existing `SubResourcePvcAdapter`.

**Tech Stack:** Go, controller-runtime, k8s.io/apimachinery

---

## File Structure

```
kube-xset/
├── api/
│   ├── xset_controller_types.go    # MODIFY: Add SubResourceAdapterGetter interface
│   └── subresource_types.go         # CREATE: SubResourceAdapter, SubResourceTemplate
├── subresources/
│   ├── types.go                     # CREATE: NameTruncator, LabelManager
│   ├── types_test.go                # CREATE: Unit tests
│   ├── utils.go                     # CREATE: Shared hash utilities
│   ├── getter.go                    # MODIFY: Add GetSubResourceAdapters()
│   ├── pvc_control.go               # KEEP: No changes for backward compatibility
│   ├── pvc_adapter.go               # CREATE: PvcSubResourceAdapter
│   └── service_adapter.go           # CREATE: ServiceSubResourceAdapter
└── CLAUDE.md                        # MODIFY: Add subresource documentation
```

---

### Task 1: Add SubResourceAdapter Interface

**Files:**
- Create: `api/subresource_types.go`

- [ ] **Step 1: Create api/subresource_types.go with interfaces**

```go
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

package api

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SubResourceAdapter is a generic interface for XSet subresources.
// Each subresource type (PVC, Service, ConfigMap, etc.) implements this interface.
type SubResourceAdapter interface {
	// Meta returns the GroupVersionKind for this subresource type
	Meta() schema.GroupVersionKind

	// GetTemplates returns subresource templates from XSet spec
	GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)

	// BuildResource creates a subresource instance from template for a specific target
	BuildResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, target client.Object, targetID string) (client.Object, error)

	// RetainWhenXSetDeleted returns true if subresource should be retained when XSet is deleted
	RetainWhenXSetDeleted(xset XSetObject) bool

	// RetainWhenXSetScaled returns true if subresource should be retained when XSet is scaled in
	RetainWhenXSetScaled(xset XSetObject) bool

	// AttachToTarget attaches subresources to target (e.g., mount PVC volumes to Pod)
	AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error

	// GetAttachedResourceNames returns names of subresources attached to target
	GetAttachedResourceNames(target client.Object) ([]string, error)
}

// SubResourceTemplate represents a parsed template with name and hash
type SubResourceTemplate struct {
	// Name is the template name (e.g., "data", "logs")
	Name string
	// Hash is the hash of template spec for change detection
	Hash string
	// Template is the parsed template object
	Template client.Object
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./api/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add api/subresource_types.go
git commit -m "feat(api): add SubResourceAdapter interface for generic subresource support

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 2: Add SubResourceAdapterGetter Interface

**Files:**
- Modify: `api/xset_controller_types.go`

- [ ] **Step 1: Add SubResourceAdapterGetter interface to xset_controller_types.go**

Read the current file and add the interface after the existing optional interfaces comment section. Find the section around line 44-49 that lists optional interfaces and add the new interface there.

```go
// SubResourceAdapterGetter is used to get subresource adapters.
// Implement this to enable generic subresource management.
type SubResourceAdapterGetter interface {
	GetSubResourceAdapters() []SubResourceAdapter
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./api/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add api/xset_controller_types.go
git commit -m "feat(api): add SubResourceAdapterGetter interface

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 3: Add NameTruncator Utility

**Files:**
- Create: `subresources/types.go`

- [ ] **Step 1: Create subresources/types.go with NameTruncator**

```go
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
	"fmt"
	"hash/fnv"

	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/validation"
)

// NameTruncator handles resource name truncation within Kubernetes limits.
// It truncates names exceeding the limit and appends a hash suffix for uniqueness.
type NameTruncator struct {
	// MaxNameLength is the maximum allowed length for resource names
	MaxNameLength int
}

// NewNameTruncator creates a NameTruncator with default DNS label max length (63).
func NewNameTruncator() *NameTruncator {
	return &NameTruncator{
		MaxNameLength: validation.DNS1035LabelMaxLength, // 63
	}
}

// Truncate truncates name if it exceeds MaxNameLength, appending hash suffix for uniqueness.
func (t *NameTruncator) Truncate(name string) string {
	return t.TruncateWithMax(name, t.MaxNameLength)
}

// TruncateWithMax truncates name to the specified max length with hash suffix.
func (t *NameTruncator) TruncateWithMax(name string, maxLen int) string {
	if len(name) <= maxLen {
		return name
	}

	hash := computeHash(name)
	hashSuffix := fmt.Sprintf("-%s", hash)
	truncatedLen := maxLen - len(hashSuffix)

	if truncatedLen <= 0 {
		// Name too short even for hash, return hash only
		return hashSuffix[1:]
	}

	return name[:truncatedLen] + hashSuffix
}

// TruncateLabelValue truncates label value to Kubernetes max (63 chars).
func (t *NameTruncator) TruncateLabelValue(value string) string {
	return t.TruncateWithMax(value, validation.LabelValueMaxLength)
}

// computeHash generates a 6-character hash from the input string.
func computeHash(s string) string {
	h := fnv.New32a()
	h.Write([]byte(s))
	return rand.SafeEncodeString(fmt.Sprint(h.Sum32()))[:6]
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/types.go
git commit -m "feat(subresources): add NameTruncator for resource name truncation

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 4: Add NameTruncator Tests

**Files:**
- Create: `subresources/types_test.go`

- [ ] **Step 1: Write the failing tests**

```go
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
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go test ./subresources/... -v -run TestNameTruncator`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add subresources/types_test.go
git commit -m "test(subresources): add NameTruncator unit tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 5: Add LabelManager Utility

**Files:**
- Modify: `subresources/types.go`

- [ ] **Step 1: Add LabelManager to types.go**

Add the following code to the end of `subresources/types.go`:

```go

import (
	"fmt"

	"sigs.k8s.io/controller-runtime/pkg/client"
)

// LabelManager handles setting labels with automatic value truncation.
type LabelManager struct {
	truncator *NameTruncator
}

// NewLabelManager creates a LabelManager with the given truncator.
func NewLabelManager(truncator *NameTruncator) *LabelManager {
	return &LabelManager{
		truncator: truncator,
	}
}

// SetLabel sets a label, truncating value if needed with hash suffix.
func (lm *LabelManager) SetLabel(obj client.Object, key, value string) {
	if obj.GetLabels() == nil {
		obj.SetLabels(make(map[string]string))
	}

	truncatedValue := lm.truncator.TruncateLabelValue(value)
	obj.GetLabels()[key] = truncatedValue
}

// SetLabelWithTrackedOriginal sets a label and tracks the original value in an annotation if truncated.
func (lm *LabelManager) SetLabelWithTrackedOriginal(obj client.Object, key, value string) {
	truncatedValue := lm.truncator.TruncateLabelValue(value)

	if obj.GetLabels() == nil {
		obj.SetLabels(make(map[string]string))
	}
	obj.GetLabels()[key] = truncatedValue

	// Track original value in annotation if truncated
	if truncatedValue != value {
		if obj.GetAnnotations() == nil {
			obj.SetAnnotations(make(map[string]string))
		}
		obj.GetAnnotations()[key+".original"] = value
	}
}

// SetOperatingLabel sets an operating label with ID in the key.
// Format: <prefix>/<id> = timestamp
func (lm *LabelManager) SetOperatingLabel(obj client.Object, prefix, id, value string) {
	truncatedID := lm.truncator.TruncateLabelValue(id)
	labelKey := fmt.Sprintf("%s/%s", prefix, truncatedID)

	if obj.GetLabels() == nil {
		obj.SetLabels(make(map[string]string))
	}
	obj.GetLabels()[labelKey] = value
}

// SetRevisionLabel sets revision info as a label value.
// Key: <prefix>/<id>, Value: revisionName
func (lm *LabelManager) SetRevisionLabel(obj client.Object, prefix, id, revisionName string) {
	truncatedID := lm.truncator.TruncateLabelValue(id)
	truncatedRevision := lm.truncator.TruncateLabelValue(revisionName)

	labelKey := fmt.Sprintf("%s/%s", prefix, truncatedID)

	if obj.GetLabels() == nil {
		obj.SetLabels(make(map[string]string))
	}
	obj.GetLabels()[labelKey] = truncatedRevision

	if truncatedRevision != revisionName {
		if obj.GetAnnotations() == nil {
			obj.SetAnnotations(make(map[string]string))
		}
		obj.GetAnnotations()[labelKey+".original-revision"] = revisionName
	}
}

// GetLabel retrieves a label value from an object.
func (lm *LabelManager) GetLabel(obj client.Object, key string) (string, bool) {
	if obj.GetLabels() == nil {
		return "", false
	}
	val, ok := obj.GetLabels()[key]
	return val, ok
}
```

Note: You'll need to update the imports at the top of the file to add `"fmt"` and `"sigs.k8s.io/controller-runtime/pkg/client"`.

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/types.go
git commit -m "feat(subresources): add LabelManager for label value handling

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 6: Add LabelManager Tests

**Files:**
- Modify: `subresources/types_test.go`

- [ ] **Step 1: Add LabelManager tests to types_test.go**

Add the following tests to the end of `subresources/types_test.go`:

```go

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
```

Also add the required imports at the top:
```go
import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	// ... existing imports
)
```

- [ ] **Step 2: Run tests to verify they pass**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go test ./subresources/... -v -run TestLabelManager`
Expected: All tests pass

- [ ] **Step 3: Commit**

```bash
git add subresources/types_test.go
git commit -m "test(subresources): add LabelManager unit tests

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 7: Add Shared Utilities

**Files:**
- Create: `subresources/utils.go`

- [ ] **Step 1: Create subresources/utils.go with shared utilities**

```go
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
	"encoding/json"
	"fmt"
	"hash/fnv"

	"k8s.io/apimachinery/pkg/util/rand"
)

// TemplateHash computes a hash of the given template object for change detection.
func TemplateHash(obj interface{}) (string, error) {
	bytes, err := json.Marshal(obj)
	if err != nil {
		return "", fmt.Errorf("failed to marshal template: %w", err)
	}

	h := fnv.New32()
	if _, err = h.Write(bytes); err != nil {
		return "", fmt.Errorf("failed to compute hash: %w", err)
	}

	return rand.SafeEncodeString(fmt.Sprint(h.Sum32())), nil
}

// ObjectKeyString returns a string representation of namespace/name for logging.
func ObjectKeyString(obj interface {
	GetNamespace() string
	GetName() string
}) string {
	if obj.GetNamespace() == "" {
		return obj.GetName()
	}
	return obj.GetNamespace() + "/" + obj.GetName()
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/utils.go
git commit -m "feat(subresources): add shared utilities for template hashing

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 8: Add PVC Adapter

**Files:**
- Create: `subresources/pvc_adapter.go`

- [ ] **Step 1: Create subresources/pvc_adapter.go**

```go
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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// PvcSubResourceAdapter implements SubResourceAdapter for PVC.
// It also bridges to the legacy SubResourcePvcAdapter for backward compatibility.
type PvcSubResourceAdapter struct {
	xsetController api.XSetController
	labelAnnoMgr   api.XSetLabelAnnotationManager
	truncator      *NameTruncator
	labelManager   *LabelManager
}

// NewPvcSubResourceAdapter creates a new PVC adapter.
func NewPvcSubResourceAdapter(xsetController api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) *PvcSubResourceAdapter {
	truncator := NewNameTruncator()
	return &PvcSubResourceAdapter{
		xsetController: xsetController,
		labelAnnoMgr:   labelAnnoMgr,
		truncator:      truncator,
		labelManager:   NewLabelManager(truncator),
	}
}

// Meta returns the GVK for PVC.
func (p *PvcSubResourceAdapter) Meta() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
}

// GetTemplates returns PVC templates from the XSet.
func (p *PvcSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	// Use old interface for backward compatibility
	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil, nil
	}

	templates := pvcAdapter.GetXSetPvcTemplate(xset)
	var result []api.SubResourceTemplate
	for i := range templates {
		hash, err := TemplateHash(&templates[i])
		if err != nil {
			return nil, fmt.Errorf("failed to compute PVC template hash: %w", err)
		}
		result = append(result, api.SubResourceTemplate{
			Name:     templates[i].Name,
			Hash:     hash,
			Template: &templates[i],
		})
	}
	return result, nil
}

// BuildResource creates a PVC from a template.
func (p *PvcSubResourceAdapter) BuildResource(
	ctx context.Context,
	xset api.XSetObject,
	template api.SubResourceTemplate,
	target client.Object,
	targetID string,
) (client.Object, error) {
	pvc, ok := template.Template.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil, fmt.Errorf("expected PersistentVolumeClaim, got %T", template.Template)
	}

	pvc = pvc.DeepCopy()

	// Generate name: xsetname-templatename-targetid
	baseName := fmt.Sprintf("%s-%s-%s", xset.GetName(), template.Name, targetID)
	pvc.Name = p.truncator.Truncate(baseName)
	pvc.Namespace = xset.GetNamespace()

	// Set owner reference
	xsetMeta := p.xsetController.XSetMeta()
	pvc.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(xset, xsetMeta.GroupVersionKind()),
	}

	// Set labels
	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.ControlledByXSetLabel), "true")
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.XInstanceIdLabelKey), targetID)
	p.labelManager.SetLabelWithTrackedOriginal(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey), template.Name)
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateHashLabelKey), template.Hash)

	return pvc, nil
}

// RetainWhenXSetDeleted returns whether PVCs should be retained when XSet is deleted.
func (p *PvcSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
	if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
		return pvcAdapter.RetainPvcWhenXSetDeleted(xset)
	}
	return false
}

// RetainWhenXSetScaled returns whether PVCs should be retained when XSet is scaled in.
func (p *PvcSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
	if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
		return pvcAdapter.RetainPvcWhenXSetScaled(xset)
	}
	return false
}

// AttachToTarget attaches PVCs to the target by setting volumes.
func (p *PvcSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	if len(resources) == 0 {
		return nil
	}

	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil
	}

	// Build volumes from PVCs
	var volumes []corev1.Volume
	for _, res := range resources {
		pvc, ok := res.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		templateName := pvc.Labels[p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey)]
		volumes = append(volumes, corev1.Volume{
			Name: templateName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
				},
			},
		})
	}

	// Merge with existing volumes
	existingVolumes := pvcAdapter.GetXSpecVolumes(target)
	volumeMap := make(map[string]corev1.Volume)
	for _, v := range existingVolumes {
		volumeMap[v.Name] = v
	}
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	var mergedVolumes []corev1.Volume
	for _, v := range volumeMap {
		mergedVolumes = append(mergedVolumes, v)
	}

	pvcAdapter.SetXSpecVolumes(target, mergedVolumes)
	return nil
}

// GetAttachedResourceNames returns the names of PVCs attached to the target.
func (p *PvcSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil, nil
	}

	volumes := pvcAdapter.GetXSpecVolumes(target)
	var names []string
	for _, v := range volumes {
		if v.PersistentVolumeClaim != nil {
			names = append(names, v.PersistentVolumeClaim.ClaimName)
		}
	}
	return names, nil
}

var _ api.SubResourceAdapter = &PvcSubResourceAdapter{}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/pvc_adapter.go
git commit -m "feat(subresources): add PvcSubResourceAdapter implementing SubResourceAdapter

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 9: Add Service Adapter Example

**Files:**
- Create: `subresources/service_adapter.go`

- [ ] **Step 1: Create subresources/service_adapter.go**

```go
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
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// ServiceSubResourceAdapter implements SubResourceAdapter for Service.
// This is an example adapter for creating Services per target.
type ServiceSubResourceAdapter struct {
	truncator    *NameTruncator
	labelManager *LabelManager
}

// NewServiceSubResourceAdapter creates a new Service adapter.
func NewServiceSubResourceAdapter() *ServiceSubResourceAdapter {
	truncator := NewNameTruncator()
	return &ServiceSubResourceAdapter{
		truncator:    truncator,
		labelManager: NewLabelManager(truncator),
	}
}

// Meta returns the GVK for Service.
func (s *ServiceSubResourceAdapter) Meta() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("Service")
}

// GetTemplates returns Service templates from the XSet.
// This implementation returns nil - XSetController implementations should
// override this by storing templates in annotations or a custom field.
func (s *ServiceSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	// XSetController implementations should provide templates via:
	// 1. A custom annotation with JSON-encoded templates
	// 2. A new field in their XSetSpec
	// Example:
	//   annotations := xset.GetAnnotations()
	//   if templatesJson, ok := annotations["xset.kusionstack.io/service-templates"]; ok {
	//       // parse and return templates
	//   }
	return nil, nil
}

// BuildResource creates a Service from a template.
func (s *ServiceSubResourceAdapter) BuildResource(
	ctx context.Context,
	xset api.XSetObject,
	template api.SubResourceTemplate,
	target client.Object,
	targetID string,
) (client.Object, error) {
	svc, ok := template.Template.(*corev1.Service)
	if !ok {
		return nil, fmt.Errorf("expected Service, got %T", template.Template)
	}

	svc = svc.DeepCopy()

	// Generate name: xsetname-templatename-targetid
	baseName := fmt.Sprintf("%s-%s-%s", xset.GetName(), template.Name, targetID)
	svc.Name = s.truncator.Truncate(baseName)
	svc.Namespace = xset.GetNamespace()

	// Set owner reference - requires XSetController, but for this example we skip
	// Real implementations would get GVK from xsetController.XSetMeta()
	svc.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         xset.GetObjectKind().GroupVersionKind().GroupVersion().String(),
			Kind:               xset.GetObjectKind().GroupVersionKind().Kind,
			Name:               xset.GetName(),
			UID:                xset.GetUID(),
			Controller:         ptrTo(true),
			BlockOwnerDeletion: ptrTo(true),
		},
	}

	// Set labels for selection
	if svc.Labels == nil {
		svc.Labels = make(map[string]string)
	}
	s.labelManager.SetLabel(svc, "app.kubernetes.io/instance", targetID)
	s.labelManager.SetLabel(svc, "app.kubernetes.io/managed-by", "kube-xset")

	return svc, nil
}

// RetainWhenXSetDeleted returns false - Services are deleted with XSet by default.
func (s *ServiceSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
	return false
}

// RetainWhenXSetScaled returns false - Services are deleted when scaled in.
func (s *ServiceSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
	return false
}

// AttachToTarget returns nil - Services are independent, no attachment needed.
func (s *ServiceSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	return nil
}

// GetAttachedResourceNames returns nil - Services are independent.
func (s *ServiceSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
	return nil, nil
}

// ptrTo returns a pointer to the given value.
func ptrTo[T any](v T) *T {
	return &v
}

var _ api.SubResourceAdapter = &ServiceSubResourceAdapter{}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/service_adapter.go
git commit -m "feat(subresources): add ServiceSubResourceAdapter example

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 10: Update getter.go

**Files:**
- Modify: `subresources/getter.go`

- [ ] **Step 1: Add GetSubResourceAdapters function to getter.go**

Add the following function to `subresources/getter.go`:

```go
// GetSubResourceAdapters returns subresource adapters if the controller implements SubResourceAdapterGetter.
func GetSubResourceAdapters(control api.XSetController) (adapters []api.SubResourceAdapter, enabled bool) {
	getter, ok := control.(api.SubResourceAdapterGetter)
	if !ok {
		return nil, false
	}
	return getter.GetSubResourceAdapters(), true
}
```

- [ ] **Step 2: Verify the file compiles**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go build ./subresources/...`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add subresources/getter.go
git commit -m "feat(subresources): add GetSubResourceAdapters getter function

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 11: Update CLAUDE.md Documentation

**Files:**
- Modify: `CLAUDE.md`

- [ ] **Step 1: Add SubResourceAdapter documentation to CLAUDE.md**

Add a new section after the "SubResourcePvcAdapter" section in CLAUDE.md:

```markdown
### SubResourceAdapterGetter (for generic subresource management)

```go
func (g *SubResourceAdapterGetter) GetSubResourceAdapters() []xsetapi.SubResourceAdapter {
    return []xsetapi.SubResourceAdapter{
        subresources.NewPvcSubResourceAdapter(xsetController, labelAnnoMgr),
        // Add other adapters as needed
    }
}
```

### SubResourceAdapter Interface

The `SubResourceAdapter` interface provides a generic way to manage subresources:

```go
type SubResourceAdapter interface {
    Meta() schema.GroupVersionKind
    GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)
    BuildResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, target client.Object, targetID string) (client.Object, error)
    RetainWhenXSetDeleted(xset XSetObject) bool
    RetainWhenXSetScaled(xset XSetObject) bool
    AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error
    GetAttachedResourceNames(target client.Object) ([]string, error)
}
```

### Name Truncation

Resources names are automatically truncated to 63 characters with a hash suffix for uniqueness:

```go
truncator := subresources.NewNameTruncator()
name := truncator.Truncate("very-long-resource-name-exceeding-63-characters-limit")
// Result: "very-long-resource-name-exceeding-63-charact-abc123"
```

### Label Value Handling

Label values are automatically truncated with original value tracking:

```go
lm := subresources.NewLabelManager(truncator)
lm.SetLabel(obj, "key", "very-long-label-value")
lm.SetLabelWithTrackedOriginal(obj, "key", "original-value-tracked-in-annotation")
```
```

- [ ] **Step 2: Commit**

```bash
git add CLAUDE.md
git commit -m "docs: add SubResourceAdapter documentation

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

### Task 12: Run All Tests

- [ ] **Step 1: Run all tests**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go test ./... -v`
Expected: All tests pass

- [ ] **Step 2: Run go vet**

Run: `cd /Users/ana/projects/aicloud/kube-xset && go vet ./...`
Expected: No issues

- [ ] **Step 3: Final commit if any fixes needed**

If any issues were found and fixed:

```bash
git add -A
git commit -m "fix: address test and vet issues

Co-Authored-By: Claude Opus 4.6 <noreply@anthropic.com>"
```

---

## Self-Review Checklist

**Spec Coverage:**
- [x] SubResourceAdapter interface - Task 1
- [x] SubResourceAdapterGetter interface - Task 2
- [x] NameTruncator - Task 3, Task 4
- [x] LabelManager - Task 5, Task 6
- [x] Shared utilities - Task 7
- [x] PVC adapter - Task 8
- [x] Service adapter example - Task 9
- [x] Getter function - Task 10
- [x] Documentation - Task 11

**Placeholder Scan:**
- No TBD, TODO, or placeholder comments
- All code is complete
- All test cases are complete

**Type Consistency:**
- SubResourceAdapter interface matches all adapter implementations
- SubResourceTemplate struct used consistently
- Function signatures match interface definitions