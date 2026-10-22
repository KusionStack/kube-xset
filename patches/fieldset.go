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

// Package patches contains workload-neutral, side-effect-free helpers for the
// concurrent patch state model.
package patches

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode"
)

// FieldPath is a canonical path into a target object's template. Collection
// schema must be resolved by the workload adapter before constructing it:
// atomic maps and unkeyed lists are represented by their collection root,
// while map-lists use their complete key selector.
type FieldPath struct {
	elements []pathElement
}

type pathElement struct {
	field     string
	selectors []selector
}

type selector struct {
	key   string
	value string
}

// ParseFieldPath parses and canonicalizes a schema-aware field path.
//
// A numeric or otherwise unkeyed array subscript is deliberately collapsed to
// the array field itself. For example, both .spec.items[0].value and
// .spec.items[1].name become .spec.items and therefore conflict.
func ParseFieldPath(raw string) (FieldPath, error) {
	if raw == "" || raw[0] != '.' {
		return FieldPath{}, fmt.Errorf("field path %q must start with '.'", raw)
	}

	var result FieldPath
	for i := 1; i < len(raw); {
		start := i
		for i < len(raw) && raw[i] != '.' && raw[i] != '[' {
			i++
		}
		if start == i {
			return FieldPath{}, fmt.Errorf("field path %q contains an empty field", raw)
		}

		element := pathElement{field: raw[start:i]}
		if strings.TrimSpace(element.field) != element.field {
			return FieldPath{}, fmt.Errorf("field path %q contains whitespace in field %q", raw, element.field)
		}
		result.elements = append(result.elements, element)

		for i < len(raw) && raw[i] == '[' {
			end, err := selectorEnd(raw, i)
			if err != nil {
				return FieldPath{}, err
			}
			content := strings.TrimSpace(raw[i+1 : end])
			if !strings.Contains(content, "=") {
				// The collection is addressed by position (or by an unknown
				// non-keyed syntax), so the whole collection is the conflict root.
				return normalizeFieldPath(result, raw)
			}

			parsed, err := parseSelectors(content)
			if err != nil {
				return FieldPath{}, fmt.Errorf("invalid selector in field path %q: %w", raw, err)
			}
			last := &result.elements[len(result.elements)-1]
			last.selectors = append(last.selectors, parsed...)
			i = end + 1
		}

		if i == len(raw) {
			break
		}
		if raw[i] != '.' {
			return FieldPath{}, fmt.Errorf("field path %q has unexpected character %q", raw, raw[i])
		}
		i++
		if i == len(raw) {
			return FieldPath{}, fmt.Errorf("field path %q must not end with '.'", raw)
		}
	}

	if len(result.elements) == 0 {
		return FieldPath{}, fmt.Errorf("field path %q must contain at least one field", raw)
	}
	return normalizeFieldPath(result, raw)
}

func normalizeFieldPath(result FieldPath, raw string) (FieldPath, error) {
	for i := range result.elements {
		selectors := result.elements[i].selectors
		sort.Slice(selectors, func(a, b int) bool {
			if selectors[a].key == selectors[b].key {
				return selectors[a].value < selectors[b].value
			}
			return selectors[a].key < selectors[b].key
		})
		for j := 1; j < len(selectors); j++ {
			if selectors[j-1].key == selectors[j].key {
				return FieldPath{}, fmt.Errorf("field path %q repeats selector key %q", raw, selectors[j].key)
			}
		}
	}
	return result, nil
}

func selectorEnd(path string, start int) (int, error) {
	inQuote := false
	escaped := false
	for i := start + 1; i < len(path); i++ {
		switch {
		case escaped:
			escaped = false
		case path[i] == '\\' && inQuote:
			escaped = true
		case path[i] == '"':
			inQuote = !inQuote
		case path[i] == ']' && !inQuote:
			return i, nil
		}
	}
	return 0, fmt.Errorf("field path %q has an unterminated selector", path)
}

func parseSelectors(raw string) ([]selector, error) {
	parts, err := splitSelectorParts(raw)
	if err != nil {
		return nil, err
	}
	selectors := make([]selector, 0, len(parts))
	for _, part := range parts {
		pair := strings.SplitN(part, "=", 2)
		if len(pair) != 2 {
			return nil, fmt.Errorf("selector %q must be key=value", part)
		}
		key := strings.TrimSpace(pair[0])
		if key == "" {
			return nil, fmt.Errorf("selector key must not be empty")
		}
		for _, r := range key {
			if !(unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' || r == '-' || r == '.') {
				return nil, fmt.Errorf("selector key %q contains an unsupported character", key)
			}
		}

		valueText := strings.TrimSpace(pair[1])
		if valueText == "" {
			return nil, fmt.Errorf("selector %q has an empty value", key)
		}
		value := valueText
		if valueText[0] == '"' {
			unquoted, err := strconv.Unquote(valueText)
			if err != nil {
				return nil, fmt.Errorf("selector %q has invalid quoted value: %w", key, err)
			}
			value = unquoted
		}
		selectors = append(selectors, selector{key: key, value: value})
	}
	return selectors, nil
}

func splitSelectorParts(raw string) ([]string, error) {
	var parts []string
	start := 0
	inQuote := false
	escaped := false
	for i := 0; i < len(raw); i++ {
		switch {
		case escaped:
			escaped = false
		case raw[i] == '\\' && inQuote:
			escaped = true
		case raw[i] == '"':
			inQuote = !inQuote
		case raw[i] == ',' && !inQuote:
			parts = append(parts, strings.TrimSpace(raw[start:i]))
			start = i + 1
		}
	}
	if inQuote {
		return nil, fmt.Errorf("unterminated quoted selector value")
	}
	parts = append(parts, strings.TrimSpace(raw[start:]))
	for _, part := range parts {
		if part == "" {
			return nil, fmt.Errorf("selector must not contain an empty component")
		}
	}
	return parts, nil
}

// String returns the canonical representation of a field path.
func (p FieldPath) String() string {
	var b strings.Builder
	for _, element := range p.elements {
		b.WriteByte('.')
		b.WriteString(element.field)
		if len(element.selectors) == 0 {
			continue
		}
		b.WriteByte('[')
		for i, item := range element.selectors {
			if i > 0 {
				b.WriteByte(',')
			}
			b.WriteString(item.key)
			b.WriteByte('=')
			b.WriteString(strconv.Quote(item.value))
		}
		b.WriteByte(']')
	}
	return b.String()
}

// Conflicts reports whether two paths overlap. Parent-child paths overlap;
// keyed list items are independent only when both paths name different keys.
func (p FieldPath) Conflicts(other FieldPath) bool {
	limit := len(p.elements)
	if len(other.elements) < limit {
		limit = len(other.elements)
	}
	for i := 0; i < limit; i++ {
		left, right := p.elements[i], other.elements[i]
		if left.field != right.field {
			return false
		}
		if selectorsEqual(left.selectors, right.selectors) {
			continue
		}
		if len(left.selectors) == 0 || len(right.selectors) == 0 {
			// A collection root overlaps every keyed item below it.
			return true
		}
		return false
	}
	// All compared elements are equal, so one path is the other or its parent.
	return true
}

func selectorsEqual(left, right []selector) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}
	return true
}

// CanonicalFieldSet canonicalizes, sorts, and deduplicates field paths.
func CanonicalFieldSet(paths []string) ([]string, error) {
	canonical := make([]string, 0, len(paths))
	seen := make(map[string]struct{}, len(paths))
	for _, raw := range paths {
		path, err := ParseFieldPath(raw)
		if err != nil {
			return nil, err
		}
		value := path.String()
		if _, exists := seen[value]; exists {
			continue
		}
		seen[value] = struct{}{}
		canonical = append(canonical, value)
	}
	sort.Strings(canonical)
	return canonical, nil
}

// FieldSetConflict identifies one overlapping pair of canonical paths.
type FieldSetConflict struct {
	Left  string
	Right string
}

// FirstFieldSetConflict returns the first deterministic overlap between two
// field sets. The boolean is false when they are independent.
func FirstFieldSetConflict(left, right []string) (FieldSetConflict, bool, error) {
	canonicalLeft, err := CanonicalFieldSet(left)
	if err != nil {
		return FieldSetConflict{}, false, err
	}
	canonicalRight, err := CanonicalFieldSet(right)
	if err != nil {
		return FieldSetConflict{}, false, err
	}

	for _, leftRaw := range canonicalLeft {
		leftPath, _ := ParseFieldPath(leftRaw)
		for _, rightRaw := range canonicalRight {
			rightPath, _ := ParseFieldPath(rightRaw)
			if leftPath.Conflicts(rightPath) {
				return FieldSetConflict{Left: leftRaw, Right: rightRaw}, true, nil
			}
		}
	}
	return FieldSetConflict{}, false, nil
}
