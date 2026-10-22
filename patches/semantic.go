/*
 * Copyright 2024-2026 KusionStack Authors.
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

package patches

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
)

const SemanticPatchType = "SemanticPatch"

// Schema records the collection semantics needed to produce stable paths.
// A path absent from MapLists is treated as an atomic/unkeyed list. Paths in
// AtomicPaths are emitted as a single operation even when they contain maps.
type Schema struct {
	MapLists    map[string][]string
	AtomicPaths []string
}

// PatchDocument is an order-independent, idempotent collection of Set/Delete
// operations. Operations in one document must not overlap.
type PatchDocument struct {
	Type       string      `json:"type"`
	Operations []Operation `json:"operations"`
}

type Operation struct {
	Path   string          `json:"path"`
	Value  json.RawMessage `json:"value,omitempty"`
	Remove bool            `json:"remove,omitempty"`
}

// BuildPatchPair builds forward and reverse semantic patches. Applying the
// forward patch to oldTemplate yields newTemplate; applying reverse restores
// oldTemplate without touching independent keyed-list items.
func BuildPatchPair(oldTemplate, newTemplate []byte, schema Schema) (runtime.RawExtension, runtime.RawExtension, []string, error) {
	oldValue, err := decodeJSON(oldTemplate)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, fmt.Errorf("decode old template: %w", err)
	}
	newValue, err := decodeJSON(newTemplate)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, fmt.Errorf("decode new template: %w", err)
	}

	forwardOps, err := diffValue("", oldValue, newValue, schema)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, err
	}
	reverseOps, err := diffValue("", newValue, oldValue, schema)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, err
	}
	if len(forwardOps) == 0 {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, nil
	}

	forward, err := encodePatch(forwardOps)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, err
	}
	reverse, err := encodePatch(reverseOps)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, err
	}
	fields := make([]string, 0, len(forwardOps))
	for i := range forwardOps {
		fields = append(fields, forwardOps[i].Path)
	}
	fields, err = CanonicalFieldSet(fields)
	if err != nil {
		return runtime.RawExtension{}, runtime.RawExtension{}, nil, err
	}
	return forward, reverse, fields, nil
}

// Apply applies one semantic patch to a JSON document and returns canonical
// JSON. Re-applying the same patch is safe.
func Apply(template []byte, patch runtime.RawExtension) ([]byte, error) {
	value, err := decodeJSON(template)
	if err != nil {
		return nil, fmt.Errorf("decode template: %w", err)
	}
	document, err := decodePatch(patch)
	if err != nil {
		return nil, err
	}
	for i := range document.Operations {
		operation := document.Operations[i]
		path, err := ParseFieldPath(operation.Path)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i, err)
		}
		var operationValue interface{}
		if !operation.Remove {
			operationValue, err = decodeJSON(operation.Value)
			if err != nil {
				return nil, fmt.Errorf("decode value for %q: %w", operation.Path, err)
			}
		}
		if err := applyAt(value, path.elements, operation.Remove, operationValue); err != nil {
			return nil, fmt.Errorf("apply %q: %w", operation.Path, err)
		}
	}
	return json.Marshal(value)
}

func encodePatch(operations []Operation) (runtime.RawExtension, error) {
	sort.Slice(operations, func(i, j int) bool { return operations[i].Path < operations[j].Path })
	document := PatchDocument{Type: SemanticPatchType, Operations: operations}
	raw, err := json.Marshal(document)
	if err != nil {
		return runtime.RawExtension{}, fmt.Errorf("encode semantic patch: %w", err)
	}
	return runtime.RawExtension{Raw: raw}, nil
}

func decodePatch(patch runtime.RawExtension) (*PatchDocument, error) {
	raw := patch.Raw
	if len(raw) == 0 && patch.Object != nil {
		var err error
		raw, err = json.Marshal(patch.Object)
		if err != nil {
			return nil, fmt.Errorf("encode patch object: %w", err)
		}
	}
	if len(raw) == 0 {
		return nil, fmt.Errorf("empty semantic patch")
	}
	document := &PatchDocument{}
	if err := json.Unmarshal(raw, document); err != nil {
		return nil, fmt.Errorf("decode semantic patch: %w", err)
	}
	if document.Type != SemanticPatchType {
		return nil, fmt.Errorf("unsupported patch type %q", document.Type)
	}
	seen := make(map[string]struct{}, len(document.Operations))
	for i := range document.Operations {
		operation := &document.Operations[i]
		path, err := ParseFieldPath(operation.Path)
		if err != nil {
			return nil, fmt.Errorf("operation %d: %w", i, err)
		}
		if path.String() != operation.Path {
			return nil, fmt.Errorf("operation path %q is not canonical; use %q", operation.Path, path.String())
		}
		if _, exists := seen[operation.Path]; exists {
			return nil, fmt.Errorf("duplicate operation path %q", operation.Path)
		}
		seen[operation.Path] = struct{}{}
		if !operation.Remove && len(operation.Value) == 0 {
			return nil, fmt.Errorf("set operation %q has no value", operation.Path)
		}
	}
	return document, nil
}

func decodeJSON(raw []byte) (interface{}, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value interface{}
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	if decoder.More() {
		return nil, fmt.Errorf("multiple JSON values")
	}
	return value, nil
}

func diffValue(path string, oldValue, newValue interface{}, schema Schema) ([]Operation, error) {
	if reflect.DeepEqual(oldValue, newValue) {
		return nil, nil
	}
	if path != "" && schema.isAtomic(path) {
		return []Operation{setOperation(path, newValue)}, nil
	}

	oldMap, oldIsMap := oldValue.(map[string]interface{})
	newMap, newIsMap := newValue.(map[string]interface{})
	if oldIsMap && newIsMap {
		keys := make(map[string]struct{}, len(oldMap)+len(newMap))
		for key := range oldMap {
			keys[key] = struct{}{}
		}
		for key := range newMap {
			keys[key] = struct{}{}
		}
		ordered := make([]string, 0, len(keys))
		for key := range keys {
			ordered = append(ordered, key)
		}
		sort.Strings(ordered)
		var operations []Operation
		for _, key := range ordered {
			childPath := joinField(path, key)
			oldChild, oldExists := oldMap[key]
			newChild, newExists := newMap[key]
			switch {
			case oldExists && !newExists:
				operations = append(operations, Operation{Path: childPath, Remove: true})
			case !oldExists && newExists:
				operations = append(operations, setOperation(childPath, newChild))
			default:
				childOperations, err := diffValue(childPath, oldChild, newChild, schema)
				if err != nil {
					return nil, err
				}
				operations = append(operations, childOperations...)
			}
		}
		return operations, nil
	}

	oldList, oldIsList := oldValue.([]interface{})
	newList, newIsList := newValue.([]interface{})
	if oldIsList && newIsList {
		keys := schema.MapLists[path]
		if len(keys) == 0 {
			return []Operation{setOperation(path, newValue)}, nil
		}
		return diffMapList(path, oldList, newList, keys, schema)
	}

	if path == "" {
		return nil, fmt.Errorf("template root must remain an object")
	}
	return []Operation{setOperation(path, newValue)}, nil
}

func diffMapList(path string, oldList, newList []interface{}, keys []string, schema Schema) ([]Operation, error) {
	oldItems, err := indexMapList(path, oldList, keys)
	if err != nil {
		return nil, err
	}
	newItems, err := indexMapList(path, newList, keys)
	if err != nil {
		return nil, err
	}
	identities := make(map[string]struct{}, len(oldItems)+len(newItems))
	for identity := range oldItems {
		identities[identity] = struct{}{}
	}
	for identity := range newItems {
		identities[identity] = struct{}{}
	}
	ordered := make([]string, 0, len(identities))
	for identity := range identities {
		ordered = append(ordered, identity)
	}
	sort.Strings(ordered)

	var operations []Operation
	for _, identity := range ordered {
		oldItem, oldExists := oldItems[identity]
		newItem, newExists := newItems[identity]
		itemPath := path + selectorPath(keys, chooseItem(oldItem, newItem))
		switch {
		case oldExists && !newExists:
			operations = append(operations, Operation{Path: itemPath, Remove: true})
		case !oldExists && newExists:
			operations = append(operations, setOperation(itemPath, newItem))
		default:
			childOperations, err := diffValue(itemPath, oldItem, newItem, schema)
			if err != nil {
				return nil, err
			}
			operations = append(operations, childOperations...)
		}
	}
	return operations, nil
}

func indexMapList(path string, list []interface{}, keys []string) (map[string]map[string]interface{}, error) {
	items := make(map[string]map[string]interface{}, len(list))
	for i := range list {
		item, ok := list[i].(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("map-list %q item %d is not an object", path, i)
		}
		identityParts := make([]string, 0, len(keys))
		for _, key := range keys {
			value, exists := item[key]
			if !exists {
				return nil, fmt.Errorf("map-list %q item %d has no key %q", path, i, key)
			}
			encoded, err := json.Marshal(value)
			if err != nil {
				return nil, fmt.Errorf("encode map-list key %q: %w", key, err)
			}
			identityParts = append(identityParts, key+"="+string(encoded))
		}
		identity := strings.Join(identityParts, ",")
		if _, exists := items[identity]; exists {
			return nil, fmt.Errorf("map-list %q repeats key %q", path, identity)
		}
		items[identity] = item
	}
	return items, nil
}

func selectorPath(keys []string, item map[string]interface{}) string {
	ordered := append([]string(nil), keys...)
	sort.Strings(ordered)
	parts := make([]string, 0, len(ordered))
	for _, key := range ordered {
		value := fmt.Sprint(item[key])
		parts = append(parts, key+"="+strconv.Quote(value))
	}
	return "[" + strings.Join(parts, ",") + "]"
}

func chooseItem(left, right map[string]interface{}) map[string]interface{} {
	if left != nil {
		return left
	}
	return right
}

func setOperation(path string, value interface{}) Operation {
	raw, _ := json.Marshal(value)
	return Operation{Path: path, Value: raw}
}

func joinField(parent, field string) string {
	if parent == "" {
		return "." + field
	}
	return parent + "." + field
}

func (s Schema) isAtomic(path string) bool {
	for _, candidate := range s.AtomicPaths {
		if candidate == path {
			return true
		}
	}
	return false
}

func applyAt(root interface{}, elements []pathElement, remove bool, value interface{}) error {
	if len(elements) == 0 {
		return fmt.Errorf("empty path")
	}
	object, ok := root.(map[string]interface{})
	if !ok {
		return fmt.Errorf("root is %T, want object", root)
	}
	return applyObject(object, elements, remove, value)
}

func applyObject(object map[string]interface{}, elements []pathElement, remove bool, value interface{}) error {
	element := elements[0]
	last := len(elements) == 1
	if len(element.selectors) == 0 {
		if last {
			if remove {
				delete(object, element.field)
			} else {
				object[element.field] = deepCopyJSON(value)
			}
			return nil
		}
		child, exists := object[element.field]
		if !exists || child == nil {
			if remove {
				return nil
			}
			child = map[string]interface{}{}
			object[element.field] = child
		}
		childObject, ok := child.(map[string]interface{})
		if !ok {
			return fmt.Errorf("field %q is %T, want object", element.field, child)
		}
		return applyObject(childObject, elements[1:], remove, value)
	}

	listValue, exists := object[element.field]
	if !exists || listValue == nil {
		if remove {
			return nil
		}
		listValue = []interface{}{}
	}
	list, ok := listValue.([]interface{})
	if !ok {
		return fmt.Errorf("field %q is %T, want list", element.field, listValue)
	}
	index := findSelectedItem(list, element.selectors)
	if last {
		if remove {
			if index >= 0 {
				list = append(list[:index], list[index+1:]...)
			}
		} else if index >= 0 {
			list[index] = deepCopyJSON(value)
		} else {
			list = append(list, deepCopyJSON(value))
		}
		object[element.field] = list
		return nil
	}

	var item map[string]interface{}
	if index >= 0 {
		item, ok = list[index].(map[string]interface{})
		if !ok {
			return fmt.Errorf("selected item in %q is %T, want object", element.field, list[index])
		}
	} else {
		if remove {
			return nil
		}
		item = make(map[string]interface{}, len(element.selectors))
		for _, selector := range element.selectors {
			item[selector.key] = selector.value
		}
		list = append(list, item)
		object[element.field] = list
	}
	return applyObject(item, elements[1:], remove, value)
}

func findSelectedItem(list []interface{}, selectors []selector) int {
	for i := range list {
		item, ok := list[i].(map[string]interface{})
		if !ok {
			continue
		}
		matched := true
		for _, selector := range selectors {
			if fmt.Sprint(item[selector.key]) != selector.value {
				matched = false
				break
			}
		}
		if matched {
			return i
		}
	}
	return -1
}

func deepCopyJSON(value interface{}) interface{} {
	raw, _ := json.Marshal(value)
	copy, _ := decodeJSON(raw)
	return copy
}
