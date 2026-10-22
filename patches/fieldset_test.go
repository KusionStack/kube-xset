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
	"reflect"
	"testing"
)

func TestParseFieldPathCanonicalizesSelectors(t *testing.T) {
	path, err := ParseFieldPath(`.spec.listeners[port="8080",name="http"].protocol`)
	if err != nil {
		t.Fatalf("ParseFieldPath() error = %v", err)
	}
	want := `.spec.listeners[name="http",port="8080"].protocol`
	if got := path.String(); got != want {
		t.Fatalf("ParseFieldPath().String() = %q, want %q", got, want)
	}
}

func TestCanonicalFieldSetCollapsesArrayIndices(t *testing.T) {
	got, err := CanonicalFieldSet([]string{
		`.spec.containers[1].image`,
		`.spec.containers[0].env`,
		`.spec.runtime.artifact`,
		`.spec.runtime.artifact`,
	})
	if err != nil {
		t.Fatalf("CanonicalFieldSet() error = %v", err)
	}
	want := []string{`.spec.containers`, `.spec.runtime.artifact`}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("CanonicalFieldSet() = %#v, want %#v", got, want)
	}
}

func TestFieldPathConflicts(t *testing.T) {
	tests := []struct {
		name  string
		left  string
		right string
		want  bool
	}{
		{name: "same leaf", left: `.spec.runtime.artifact`, right: `.spec.runtime.artifact`, want: true},
		{name: "parent child", left: `.spec.runtime`, right: `.spec.runtime.artifact`, want: true},
		{name: "child parent", left: `.spec.runtime.artifact`, right: `.spec.runtime`, want: true},
		{name: "different fields", left: `.spec.runtime.artifact`, right: `.spec.runtime.templateId`, want: false},
		{name: "unkeyed list indices", left: `.spec.env[0].value`, right: `.spec.env[1].name`, want: true},
		{name: "same keyed item parent child", left: `.spec.env[name="LOG_LEVEL"]`, right: `.spec.env[name="LOG_LEVEL"].valueFrom`, want: true},
		{name: "different keyed items", left: `.spec.env[name="LOG_LEVEL"].value`, right: `.spec.env[name="PORT"].value`, want: false},
		{name: "keyed list root and item", left: `.spec.env`, right: `.spec.env[name="PORT"].value`, want: true},
		{name: "granular map keys", left: `.metadata.labels.team`, right: `.metadata.labels.zone`, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left, err := ParseFieldPath(tt.left)
			if err != nil {
				t.Fatalf("parse left: %v", err)
			}
			right, err := ParseFieldPath(tt.right)
			if err != nil {
				t.Fatalf("parse right: %v", err)
			}
			if got := left.Conflicts(right); got != tt.want {
				t.Fatalf("Conflicts() = %v, want %v (left=%s right=%s)", got, tt.want, left.String(), right.String())
			}
		})
	}
}

func TestFirstFieldSetConflictIsDeterministic(t *testing.T) {
	got, conflict, err := FirstFieldSetConflict(
		[]string{`.spec.runtime.templateId`, `.spec.runtime.artifact`},
		[]string{`.spec.runtime.artifact.registry`, `.spec.sidecars[name="metrics"].image`},
	)
	if err != nil {
		t.Fatalf("FirstFieldSetConflict() error = %v", err)
	}
	if !conflict {
		t.Fatal("FirstFieldSetConflict() conflict = false, want true")
	}
	want := FieldSetConflict{Left: `.spec.runtime.artifact`, Right: `.spec.runtime.artifact.registry`}
	if got != want {
		t.Fatalf("FirstFieldSetConflict() = %#v, want %#v", got, want)
	}
}

func TestParseFieldPathRejectsInvalidSelectors(t *testing.T) {
	for _, raw := range []string{
		`.`,
		`spec.runtime.artifact`,
		`.spec.env[name="unterminated].value`,
		`.spec.env[name="a",name="b"].value`,
		`.spec..runtime`,
	} {
		t.Run(raw, func(t *testing.T) {
			if _, err := ParseFieldPath(raw); err == nil {
				t.Fatalf("ParseFieldPath(%q) error = nil, want non-nil", raw)
			}
		})
	}
}
