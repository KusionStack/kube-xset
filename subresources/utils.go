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

	h := fnv.New32a()
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