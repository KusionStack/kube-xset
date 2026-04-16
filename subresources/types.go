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
	"sigs.k8s.io/controller-runtime/pkg/client"
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