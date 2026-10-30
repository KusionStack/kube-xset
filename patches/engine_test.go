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
	"encoding/json"
	"reflect"
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
)

func TestPatchIDForSourceIsSafeForPatchCommandLabels(t *testing.T) {
	id := PatchIDForSource("资源版本/10.with spaces")
	if len(id) > 55 {
		t.Fatalf("PatchIDForSource() length = %d, want <= 55: %q", len(id), id)
	}
	if !regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`).MatchString(id) {
		t.Fatalf("PatchIDForSource() = %q, want DNS label name", id)
	}
	if id != PatchIDForSource("资源版本/10.with spaces") {
		t.Fatal("PatchIDForSource() is not deterministic")
	}
}

var aseTemplateSchema = Schema{
	MapLists: map[string][]string{
		`.spec.runtime.env`: {"name"},
	},
	GranularMaps: []string{`.metadata.labels`, `.metadata.annotations`},
}

func TestBuildPatchPairRoundTripWithKeyedList(t *testing.T) {
	oldTemplate := jsonBytes(t, map[string]interface{}{
		"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": "demo"}},
		"spec": map[string]interface{}{
			"runtime": map[string]interface{}{
				"artifact": "runtime:v1",
				"env": []interface{}{
					map[string]interface{}{"name": "LOG_LEVEL", "value": "info"},
					map[string]interface{}{"name": "PORT", "value": "8080"},
				},
			},
		},
	})
	newTemplate := jsonBytes(t, map[string]interface{}{
		"metadata": map[string]interface{}{"labels": map[string]interface{}{"app": "demo"}},
		"spec": map[string]interface{}{
			"runtime": map[string]interface{}{
				"artifact": "runtime:v2",
				"env": []interface{}{
					map[string]interface{}{"name": "LOG_LEVEL", "value": "debug"},
					map[string]interface{}{"name": "PORT", "value": "8080"},
				},
			},
		},
	})

	forward, reverse, fieldSet, err := BuildPatchPair(oldTemplate, newTemplate, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	wantFields := []string{`.spec.runtime.artifact`, `.spec.runtime.env[name="LOG_LEVEL"].value`}
	if !reflect.DeepEqual(fieldSet, wantFields) {
		t.Fatalf("fieldSet = %#v, want %#v", fieldSet, wantFields)
	}

	applied, err := Apply(oldTemplate, forward)
	if err != nil {
		t.Fatalf("Apply(forward) error = %v", err)
	}
	assertJSONEqual(t, applied, newTemplate)

	restored, err := Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, oldTemplate)
}

func TestBuildPatchPairDoesNotReplaceOtherKeyedListItems(t *testing.T) {
	base := jsonBytes(t, map[string]interface{}{
		"spec": map[string]interface{}{"runtime": map[string]interface{}{"env": []interface{}{
			map[string]interface{}{"name": "LOG_LEVEL", "value": "info"},
			map[string]interface{}{"name": "PORT", "value": "8080"},
		}}},
	})
	changed := jsonBytes(t, map[string]interface{}{
		"spec": map[string]interface{}{"runtime": map[string]interface{}{"env": []interface{}{
			map[string]interface{}{"name": "LOG_LEVEL", "value": "debug"},
			map[string]interface{}{"name": "PORT", "value": "8080"},
		}}},
	})
	forward, _, _, err := BuildPatchPair(base, changed, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}

	baseWithConcurrentChange := jsonBytes(t, map[string]interface{}{
		"spec": map[string]interface{}{"runtime": map[string]interface{}{"env": []interface{}{
			map[string]interface{}{"name": "LOG_LEVEL", "value": "info"},
			map[string]interface{}{"name": "PORT", "value": "9090"},
		}}},
	})
	got, err := Apply(baseWithConcurrentChange, forward)
	if err != nil {
		t.Fatalf("Apply() error = %v", err)
	}
	want := jsonBytes(t, map[string]interface{}{
		"spec": map[string]interface{}{"runtime": map[string]interface{}{"env": []interface{}{
			map[string]interface{}{"name": "LOG_LEVEL", "value": "debug"},
			map[string]interface{}{"name": "PORT", "value": "9090"},
		}}},
	})
	assertJSONEqual(t, got, want)
}

func TestBuildPatchPairRoundTripsGranularMetadataMapKeys(t *testing.T) {
	oldTemplate := []byte(`{
		"metadata": {
			"labels": {"app": "demo", "app.kubernetes.io/name": "runtime"},
			"annotations": {"example.com/config": "v1", "quote.example/a]b\\\"c": "old"}
		}
	}`)
	newTemplate := []byte(`{
		"metadata": {
			"labels": {"app": "demo", "app.kubernetes.io/name": "runtime-v2", "app.kubernetes.io/version": "v2"},
			"annotations": {"example.com/config": "v2", "quote.example/a]b\\\"c": "new"}
		}
	}`)

	forward, reverse, fields, err := BuildPatchPair(oldTemplate, newTemplate, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	wantFields := []string{
		`.metadata.annotations["example.com/config"]`,
		`.metadata.annotations["quote.example/a]b\\\"c"]`,
		`.metadata.labels["app.kubernetes.io/name"]`,
		`.metadata.labels["app.kubernetes.io/version"]`,
	}
	if !reflect.DeepEqual(fields, wantFields) {
		t.Fatalf("fieldSet = %#v, want %#v", fields, wantFields)
	}

	applied, err := Apply(oldTemplate, forward)
	if err != nil {
		t.Fatalf("Apply(forward) error = %v", err)
	}
	assertJSONEqual(t, applied, newTemplate)
	restored, err := Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, oldTemplate)
}

func TestGranularMetadataPatchPreservesConcurrentKeys(t *testing.T) {
	base := []byte(`{"metadata":{"labels":{"app":"demo"},"annotations":{"owner":"platform"}}}`)
	changed := []byte(`{"metadata":{"labels":{"app":"demo","app.kubernetes.io/version":"v2"},"annotations":{"owner":"platform"}}}`)
	forward, reverse, _, err := BuildPatchPair(base, changed, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}

	concurrent := []byte(`{"metadata":{"labels":{"app":"demo","team.example.com/name":"serving"},"annotations":{"owner":"runtime","trace.example/id":"123"}}}`)
	applied, err := Apply(concurrent, forward)
	if err != nil {
		t.Fatalf("Apply(forward) error = %v", err)
	}
	wantApplied := []byte(`{"metadata":{"labels":{"app":"demo","team.example.com/name":"serving","app.kubernetes.io/version":"v2"},"annotations":{"owner":"runtime","trace.example/id":"123"}}}`)
	assertJSONEqual(t, applied, wantApplied)

	restored, err := Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, concurrent)
}

func TestGranularMapMissingCollectionRoundTripAndConcurrentSibling(t *testing.T) {
	base := []byte(`{"metadata":{"labels":{"app":"demo"}}}`)
	changed := []byte(`{"metadata":{"labels":{"app":"demo"},"annotations":{"trace.example/id":"123"}}}`)
	forward, reverse, fields, err := BuildPatchPair(base, changed, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	if want := []string{`.metadata.annotations["trace.example/id"]`}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("fieldSet = %#v, want %#v", fields, want)
	}

	applied, err := Apply(base, forward)
	if err != nil {
		t.Fatalf("Apply(forward) error = %v", err)
	}
	assertJSONEqual(t, applied, changed)
	restored, err := Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, base)

	concurrent := []byte(`{"metadata":{"labels":{"app":"demo"},"annotations":{"owner.example/name":"runtime"}}}`)
	applied, err = Apply(concurrent, forward)
	if err != nil {
		t.Fatalf("Apply(concurrent forward) error = %v", err)
	}
	restored, err = Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(concurrent reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, concurrent)
}

func TestGranularMapRemoveLastKeyPrunesOnlyEmptyCollection(t *testing.T) {
	base := []byte(`{"metadata":{"annotations":{"trace.example/id":"123"}}}`)
	changed := []byte(`{"metadata":{}}`)
	forward, reverse, fields, err := BuildPatchPair(base, changed, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	if want := []string{`.metadata.annotations["trace.example/id"]`}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("fieldSet = %#v, want %#v", fields, want)
	}
	applied, err := Apply(base, forward)
	if err != nil {
		t.Fatalf("Apply(forward) error = %v", err)
	}
	assertJSONEqual(t, applied, changed)
	restored, err := Apply(applied, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, base)

	concurrent := []byte(`{"metadata":{"annotations":{"trace.example/id":"123","owner.example/name":"runtime"}}}`)
	applied, err = Apply(concurrent, forward)
	if err != nil {
		t.Fatalf("Apply(concurrent forward) error = %v", err)
	}
	wantConcurrent := []byte(`{"metadata":{"annotations":{"owner.example/name":"runtime"}}}`)
	assertJSONEqual(t, applied, wantConcurrent)
}

func TestSemanticPatchRejectsAmbiguousOrUnsafeOperations(t *testing.T) {
	tests := []struct {
		name      string
		document  PatchDocument
		wantError string
	}{
		{
			name: "remove with value",
			document: PatchDocument{Type: SemanticPatchType, Operations: []Operation{{
				Path: `.metadata.labels.team`, Remove: true, Value: json.RawMessage(`"runtime"`),
			}}},
			wantError: "remove operation",
		},
		{
			name: "set with prune",
			document: PatchDocument{Type: SemanticPatchType, Operations: []Operation{{
				Path: `.metadata.labels.team`, Value: json.RawMessage(`"runtime"`), PruneEmptyPath: `.metadata.labels`,
			}}},
			wantError: "must not prune",
		},
		{
			name: "prune unrelated path",
			document: PatchDocument{Type: SemanticPatchType, Operations: []Operation{{
				Path: `.metadata.labels.team`, Remove: true, PruneEmptyPath: `.metadata.annotations`,
			}}},
			wantError: "must be a parent",
		},
		{
			name: "overlapping operations",
			document: PatchDocument{Type: SemanticPatchType, Operations: []Operation{
				{Path: `.metadata.labels`, Value: json.RawMessage(`{"team":"runtime"}`)},
				{Path: `.metadata.labels.team`, Value: json.RawMessage(`"runtime"`)},
			}},
			wantError: "overlap",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Apply([]byte(`{"metadata":{"labels":{},"annotations":{}}}`), rawPatch(t, tt.document))
			if err == nil || !strings.Contains(err.Error(), tt.wantError) {
				t.Fatalf("Apply() error = %v, want containing %q", err, tt.wantError)
			}
		})
	}
}

func TestBuildPatchPairRejectsTrailingJSONAndInvalidSchema(t *testing.T) {
	if _, _, _, err := BuildPatchPair([]byte(`{} {}`), []byte(`{}`), Schema{}); err == nil {
		t.Fatal("BuildPatchPair(trailing JSON) error = nil, want rejection")
	}

	tests := []struct {
		name   string
		schema Schema
	}{
		{name: "non canonical granular map", schema: Schema{GranularMaps: []string{`.metadata.labels["team"]`}}},
		{name: "invalid atomic path", schema: Schema{AtomicPaths: []string{`metadata.labels`}}},
		{name: "empty map-list keys", schema: Schema{MapLists: map[string][]string{`.spec.env`: {}}}},
		{name: "duplicate map-list key", schema: Schema{MapLists: map[string][]string{`.spec.env`: {"name", "name"}}}},
		{name: "unsupported map-list key", schema: Schema{MapLists: map[string][]string{`.spec.env`: {"bad/key"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, _, _, err := BuildPatchPair([]byte(`{}`), []byte(`{}`), tt.schema); err == nil {
				t.Fatal("BuildPatchPair() error = nil, want schema rejection")
			}
		})
	}
}

func TestBuildPatchPairKeyedListReorderIsNoOpAndDuplicateKeysFail(t *testing.T) {
	base := []byte(`{"spec":{"runtime":{"env":[{"name":"A","value":"1"},{"name":"B","value":"2"}]}}}`)
	reordered := []byte(`{"spec":{"runtime":{"env":[{"name":"B","value":"2"},{"name":"A","value":"1"}]}}}`)
	forward, reverse, fields, err := BuildPatchPair(base, reordered, aseTemplateSchema)
	if err != nil {
		t.Fatalf("BuildPatchPair(reorder) error = %v", err)
	}
	if len(forward.Raw) != 0 || len(reverse.Raw) != 0 || len(fields) != 0 {
		t.Fatalf("reorder produced patch: forward=%s reverse=%s fields=%v", forward.Raw, reverse.Raw, fields)
	}

	duplicate := []byte(`{"spec":{"runtime":{"env":[{"name":"A","value":"1"},{"name":"A","value":"2"}]}}}`)
	if _, _, _, err := BuildPatchPair(base, duplicate, aseTemplateSchema); err == nil || !strings.Contains(err.Error(), "repeats key") {
		t.Fatalf("BuildPatchPair(duplicate) error = %v, want duplicate-key rejection", err)
	}
}

func TestCaptureUpdateAllowsDifferentMetadataKeysAndRejectsSameOrParent(t *testing.T) {
	r0 := []byte(`{"metadata":{"labels":{"app":"demo"},"annotations":{"owner":"platform"}}}`)
	r1 := []byte(`{"metadata":{"labels":{"app":"demo","app.kubernetes.io/version":"v2"},"annotations":{"owner":"platform"}}}`)
	r2 := []byte(`{"metadata":{"labels":{"app":"demo","app.kubernetes.io/version":"v2"},"annotations":{"owner":"platform","trace.example/id":"123"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	first, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 3, Schema: aseTemplateSchema})
	if err != nil {
		t.Fatalf("first CaptureUpdate() error = %v", err)
	}
	second, err := CaptureUpdate(r1, r2, first.Plan, first.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 3, Schema: aseTemplateSchema})
	if err != nil {
		t.Fatalf("different-key CaptureUpdate() error = %v", err)
	}
	if len(second.Plan.ActivePatches) != 2 {
		t.Fatalf("len(activePatches) = %d, want 2", len(second.Plan.ActivePatches))
	}

	sameKey := []byte(`{"metadata":{"labels":{"app":"demo","app.kubernetes.io/version":"v3"},"annotations":{"owner":"platform"}}}`)
	if _, err := CaptureUpdate(r1, sameKey, first.Plan, first.Plan, CaptureOptions{SourceResourceVersion: "12", Replicas: 3, Schema: aseTemplateSchema}); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("same-key CaptureUpdate() error = %v, want conflict", err)
	}

	removeParent := []byte(`{"metadata":{"annotations":{"owner":"platform"}}}`)
	if _, err := CaptureUpdate(r1, removeParent, first.Plan, first.Plan, CaptureOptions{SourceResourceVersion: "13", Replicas: 3, Schema: aseTemplateSchema}); err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("parent removal CaptureUpdate() error = %v, want conflict", err)
	}
}

func FuzzGranularMapKeyPatchRoundTrip(f *testing.F) {
	for _, key := range []string{"team", "app.kubernetes.io/name", `a]b\"c`, "space key", "", "中文.example/键", "a=b"} {
		f.Add(key)
	}
	f.Fuzz(func(t *testing.T, key string) {
		oldTemplate := jsonBytes(t, map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{}}})
		newTemplate := jsonBytes(t, map[string]interface{}{"metadata": map[string]interface{}{"labels": map[string]interface{}{key: "v2"}}})
		forward, reverse, fields, err := BuildPatchPair(oldTemplate, newTemplate, aseTemplateSchema)
		if err != nil {
			t.Fatalf("BuildPatchPair(%q) error = %v", key, err)
		}
		if len(fields) != 1 {
			t.Fatalf("len(fieldSet) = %d, want 1: %#v", len(fields), fields)
		}
		applied, err := Apply(oldTemplate, forward)
		if err != nil {
			t.Fatalf("Apply(forward, %q) error = %v", key, err)
		}
		assertJSONEqual(t, applied, newTemplate)
		restored, err := Apply(applied, reverse)
		if err != nil {
			t.Fatalf("Apply(reverse, %q) error = %v", key, err)
		}
		assertJSONEqual(t, restored, oldTemplate)
	})
}

func FuzzGranularMapIndependentPatchesCommute(f *testing.F) {
	for _, pair := range [][2]string{
		{"team", "version"},
		{"app.kubernetes.io/name", "app.kubernetes.io/version"},
		{`a]b\"c`, "space key"},
		{"中文.example/键", "a=b"},
	} {
		f.Add(pair[0], pair[1])
	}
	f.Fuzz(func(t *testing.T, firstKey, secondKey string) {
		// BuildPatchPair consumes valid JSON. encoding/json replaces invalid
		// UTF-8 in Go strings with U+FFFD before the engine sees it, so two
		// different invalid byte sequences are not distinct JSON map keys.
		if firstKey == secondKey || !utf8.ValidString(firstKey) || !utf8.ValidString(secondKey) {
			t.Skip()
		}
		base := jsonBytes(t, map[string]interface{}{
			"metadata": map[string]interface{}{"labels": map[string]interface{}{}},
		})
		firstTemplate := jsonBytes(t, map[string]interface{}{
			"metadata": map[string]interface{}{"labels": map[string]interface{}{firstKey: "first"}},
		})
		secondTemplate := jsonBytes(t, map[string]interface{}{
			"metadata": map[string]interface{}{"labels": map[string]interface{}{secondKey: "second"}},
		})
		combined := jsonBytes(t, map[string]interface{}{
			"metadata": map[string]interface{}{"labels": map[string]interface{}{firstKey: "first", secondKey: "second"}},
		})

		firstPatch, firstReverse, firstFields, err := BuildPatchPair(base, firstTemplate, aseTemplateSchema)
		if err != nil {
			t.Fatalf("BuildPatchPair(first %q) error = %v", firstKey, err)
		}
		secondPatch, _, secondFields, err := BuildPatchPair(base, secondTemplate, aseTemplateSchema)
		if err != nil {
			t.Fatalf("BuildPatchPair(second %q) error = %v", secondKey, err)
		}
		if _, conflict, conflictErr := FirstFieldSetConflict(firstFields, secondFields); conflictErr != nil || conflict {
			t.Fatalf("different keys conflict: first=%q second=%q fields=%v/%v err=%v", firstKey, secondKey, firstFields, secondFields, conflictErr)
		}

		firstThenSecond, err := Apply(base, firstPatch)
		if err != nil {
			t.Fatalf("Apply(first) error = %v", err)
		}
		firstThenSecond, err = Apply(firstThenSecond, secondPatch)
		if err != nil {
			t.Fatalf("Apply(first, second) error = %v", err)
		}
		assertJSONEqual(t, firstThenSecond, combined)

		secondThenFirst, err := Apply(base, secondPatch)
		if err != nil {
			t.Fatalf("Apply(second) error = %v", err)
		}
		secondThenFirst, err = Apply(secondThenFirst, firstPatch)
		if err != nil {
			t.Fatalf("Apply(second, first) error = %v", err)
		}
		assertJSONEqual(t, secondThenFirst, combined)

		withoutFirst, err := Apply(firstThenSecond, firstReverse)
		if err != nil {
			t.Fatalf("Apply(first reverse) error = %v", err)
		}
		assertJSONEqual(t, withoutFirst, secondTemplate)
	})
}

func TestBuildPatchPairCollapsesUnkeyedListToCollectionRoot(t *testing.T) {
	oldTemplate := []byte(`{"spec":{"sidecars":[{"name":"a","image":"v1"},{"name":"b","image":"v1"}]}}`)
	newTemplate := []byte(`{"spec":{"sidecars":[{"name":"a","image":"v2"},{"name":"b","image":"v1"}]}}`)
	_, _, fields, err := BuildPatchPair(oldTemplate, newTemplate, Schema{})
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	if want := []string{`.spec.sidecars`}; !reflect.DeepEqual(fields, want) {
		t.Fatalf("fieldSet = %#v, want %#v", fields, want)
	}
}

func TestBuildPatchPairRepresentsMissingValueInReversePatch(t *testing.T) {
	oldTemplate := []byte(`{"spec":{"runtime":{}}}`)
	newTemplate := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	_, reverse, _, err := BuildPatchPair(oldTemplate, newTemplate, Schema{})
	if err != nil {
		t.Fatalf("BuildPatchPair() error = %v", err)
	}
	restored, err := Apply(newTemplate, reverse)
	if err != nil {
		t.Fatalf("Apply(reverse) error = %v", err)
	}
	assertJSONEqual(t, restored, oldTemplate)
}

func TestCaptureUpdateCreatesTwoIndependentPatchesBeforeReconcile(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t1"}}}`)
	r2 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	first, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{
		SourceResourceVersion: "10",
		Replicas:              4,
		Schema:                aseTemplateSchema,
	})
	if err != nil {
		t.Fatalf("first CaptureUpdate() error = %v", err)
	}
	second, err := CaptureUpdate(r1, r2, first.Plan, first.Plan, CaptureOptions{
		SourceResourceVersion: "11",
		Replicas:              4,
		Schema:                aseTemplateSchema,
	})
	if err != nil {
		t.Fatalf("second CaptureUpdate() error = %v", err)
	}
	if len(second.Plan.ActivePatches) != 2 {
		t.Fatalf("len(activePatches) = %d, want 2", len(second.Plan.ActivePatches))
	}
	if second.Plan.ActivePatches[0].SourceResourceVersion != "10" || second.Plan.ActivePatches[1].SourceResourceVersion != "11" {
		t.Fatalf("unexpected capture order: %#v", second.Plan.ActivePatches)
	}
	if _, conflict, err := FirstFieldSetConflict(second.Plan.ActivePatches[0].FieldSet, second.Plan.ActivePatches[1].FieldSet); err != nil || conflict {
		t.Fatalf("captured patches conflict = %v, error = %v", conflict, err)
	}
}

func TestCaptureUpdateRejectsEnablingModeWithTemplateChange(t *testing.T) {
	oldTemplate := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	newTemplate := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	oldPlan := &api.RevisionPlan{}
	newPlan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	_, err := CaptureUpdate(oldTemplate, newTemplate, oldPlan, newPlan, CaptureOptions{
		SourceResourceVersion: "10",
		Replicas:              3,
	})
	if err == nil || !strings.Contains(err.Error(), "enable") {
		t.Fatalf("CaptureUpdate() error = %v, want enabling-mode rejection", err)
	}
}

func TestCaptureUpdateAllowsDisablingModeAfterJournalIsEmpty(t *testing.T) {
	template := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	oldPlan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	newPlan := &api.RevisionPlan{}

	result, err := CaptureUpdate(template, template, oldPlan, newPlan, CaptureOptions{
		SourceResourceVersion: "10",
		Replicas:              3,
	})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}
	if result.Plan == nil || result.Plan.Mode != "" || len(result.Plan.ActivePatches) != 0 {
		t.Fatalf("result plan = %#v, want disabled empty plan", result.Plan)
	}
}

func TestCaptureUpdateRejectsDisablingModeWithActivePatch(t *testing.T) {
	oldTemplate := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	template := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(oldTemplate, template, plan, plan, CaptureOptions{
		SourceResourceVersion: "10",
		Replicas:              3,
	})
	if err != nil {
		t.Fatalf("initial CaptureUpdate() error = %v", err)
	}

	_, err = CaptureUpdate(template, template, captured.Plan, &api.RevisionPlan{}, CaptureOptions{
		SourceResourceVersion: "11",
		Replicas:              3,
	})
	if err == nil || !strings.Contains(err.Error(), "active patches") {
		t.Fatalf("CaptureUpdate() error = %v, want active-patches rejection", err)
	}
}

func TestCaptureUpdateRejectsDisablingModeWithTemplateChange(t *testing.T) {
	oldTemplate := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	newTemplate := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	oldPlan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	_, err := CaptureUpdate(oldTemplate, newTemplate, oldPlan, &api.RevisionPlan{}, CaptureOptions{
		SourceResourceVersion: "10",
		Replicas:              3,
	})
	if err == nil || !strings.Contains(err.Error(), "disable") {
		t.Fatalf("CaptureUpdate() error = %v, want disabling-mode rejection", err)
	}
}

func TestCaptureUpdateReinvocationUpsertsBySourceResourceVersion(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	first, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 3})
	if err != nil {
		t.Fatalf("first CaptureUpdate() error = %v", err)
	}

	// IfNeeded can invoke the webhook again with the original oldObject while
	// currentObject already contains this webhook's first mutation.
	second, err := CaptureUpdate(r0, r1, plan, first.Plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 3})
	if err != nil {
		t.Fatalf("reinvoked CaptureUpdate() error = %v", err)
	}
	if len(second.Plan.ActivePatches) != 1 {
		t.Fatalf("len(activePatches) = %d, want 1", len(second.Plan.ActivePatches))
	}
	if second.Plan.ActivePatches[0].ID != first.Plan.ActivePatches[0].ID {
		t.Fatalf("reinvocation changed patch identity: %q -> %q", first.Plan.ActivePatches[0].ID, second.Plan.ActivePatches[0].ID)
	}
}

func TestCaptureUpdateRejectsSystemOwnedJournalMutation(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 2})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}

	mutated := deepCopyPlan(captured.Plan)
	mutated.ActivePatches[0].FieldSet = []string{`.spec.runtime`}
	_, err = CaptureUpdate(r1, r1, captured.Plan, mutated, CaptureOptions{SourceResourceVersion: "11", Replicas: 2})
	if err == nil || !strings.Contains(err.Error(), "system-owned") {
		t.Fatalf("CaptureUpdate() error = %v, want system-owned rejection", err)
	}
}

func TestCaptureUpdateRemovedEntryWithdrawsPatchAndReversesTemplate(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t1"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 3})
	if err != nil {
		t.Fatalf("initial CaptureUpdate() error = %v", err)
	}

	deleteRequest := deepCopyPlan(captured.Plan)
	deleteRequest.ActivePatches = nil
	result, err := CaptureUpdate(r1, r1, captured.Plan, deleteRequest, CaptureOptions{SourceResourceVersion: "11", Replicas: 3})
	if err != nil {
		t.Fatalf("delete CaptureUpdate() error = %v", err)
	}
	assertJSONEqual(t, result.Template, r0)
	if !result.Canceled || result.PatchID != captured.Plan.ActivePatches[0].ID {
		t.Fatalf("delete result canceled=%v patchID=%q, want true and %q", result.Canceled, result.PatchID, captured.Plan.ActivePatches[0].ID)
	}
	if len(result.Plan.ActivePatches) != 1 {
		t.Fatalf("active patches = %#v, want deleted entry retained while withdrawing", result.Plan.ActivePatches)
	}
	patch := result.Plan.ActivePatches[0]
	if patch.Rollout.TargetState != api.PatchTargetStateAbsent {
		t.Fatalf("targetState = %q, want Absent", patch.Rollout.TargetState)
	}
	if patch.Rollout.ByPartition == nil || patch.Rollout.ByPartition.Partition == nil || *patch.Rollout.ByPartition.Partition != 0 {
		t.Fatalf("withdrawal partition = %#v, want 0", patch.Rollout.ByPartition)
	}
}

func TestCaptureUpdateRemovedEntryAcceptsAlreadyReversedTemplate(t *testing.T) {
	r0 := []byte(`{"spec":{"value":"old"}}`)
	r1 := []byte(`{"spec":{"value":"new"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 1})
	if err != nil {
		t.Fatalf("initial CaptureUpdate() error = %v", err)
	}
	deleteRequest := deepCopyPlan(captured.Plan)
	deleteRequest.ActivePatches = nil

	result, err := CaptureUpdate(r1, r0, captured.Plan, deleteRequest, CaptureOptions{SourceResourceVersion: "11", Replicas: 1})
	if err != nil {
		t.Fatalf("delete with reversed template error = %v", err)
	}
	assertJSONEqual(t, result.Template, r0)
}

func TestCaptureUpdateRemovedEntriesWithdrawAtomically(t *testing.T) {
	r0 := []byte(`{"spec":{"a":"a0","b":"b0","keep":"same"}}`)
	r1 := []byte(`{"spec":{"a":"a1","b":"b0","keep":"same"}}`)
	r2 := []byte(`{"spec":{"a":"a1","b":"b1","keep":"same"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	first, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 2})
	if err != nil {
		t.Fatalf("first CaptureUpdate() error = %v", err)
	}
	second, err := CaptureUpdate(r1, r2, first.Plan, first.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 2})
	if err != nil {
		t.Fatalf("second CaptureUpdate() error = %v", err)
	}
	deleteRequest := deepCopyPlan(second.Plan)
	deleteRequest.ActivePatches = nil

	result, err := CaptureUpdate(r2, r2, second.Plan, deleteRequest, CaptureOptions{SourceResourceVersion: "12", Replicas: 2})
	if err != nil {
		t.Fatalf("batch delete CaptureUpdate() error = %v", err)
	}
	assertJSONEqual(t, result.Template, r0)
	if len(result.Plan.ActivePatches) != 2 {
		t.Fatalf("active patches = %#v, want both withdrawing entries", result.Plan.ActivePatches)
	}
	for i := range result.Plan.ActivePatches {
		if result.Plan.ActivePatches[i].Rollout.TargetState != api.PatchTargetStateAbsent {
			t.Fatalf("patch %q targetState = %q, want Absent", result.Plan.ActivePatches[i].ID, result.Plan.ActivePatches[i].Rollout.TargetState)
		}
	}
}

func TestCaptureUpdateRemovedEntryRejectsUnrelatedTemplateChange(t *testing.T) {
	r0 := []byte(`{"spec":{"value":"old","other":"same"}}`)
	r1 := []byte(`{"spec":{"value":"new","other":"same"}}`)
	unrelated := []byte(`{"spec":{"value":"new","other":"changed"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 1})
	if err != nil {
		t.Fatalf("initial CaptureUpdate() error = %v", err)
	}
	deleteRequest := deepCopyPlan(captured.Plan)
	deleteRequest.ActivePatches = nil

	_, err = CaptureUpdate(r1, unrelated, captured.Plan, deleteRequest, CaptureOptions{SourceResourceVersion: "11", Replicas: 1})
	if err == nil || !strings.Contains(err.Error(), "unrelated template") {
		t.Fatalf("delete with unrelated template error = %v, want rejection", err)
	}
}

func TestCaptureUpdateRemovingAbsentEntryIsIdempotent(t *testing.T) {
	r0 := []byte(`{"spec":{"value":"old"}}`)
	r1 := []byte(`{"spec":{"value":"new"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 1})
	if err != nil {
		t.Fatalf("initial CaptureUpdate() error = %v", err)
	}
	withdrawing := deepCopyPlan(captured.Plan)
	markWithdrawing(&withdrawing.ActivePatches[0])
	deleteRequest := deepCopyPlan(withdrawing)
	deleteRequest.ActivePatches = nil

	result, err := CaptureUpdate(r0, r0, withdrawing, deleteRequest, CaptureOptions{SourceResourceVersion: "11", Replicas: 1})
	if err != nil {
		t.Fatalf("idempotent delete CaptureUpdate() error = %v", err)
	}
	assertJSONEqual(t, result.Template, r0)
	if result.Canceled || result.PatchID != "" {
		t.Fatalf("idempotent delete was classified as a new cancellation: %#v", result)
	}
	if len(result.Plan.ActivePatches) != 1 || result.Plan.ActivePatches[0].Rollout.TargetState != api.PatchTargetStateAbsent {
		t.Fatalf("withdrawing entry was not retained: %#v", result.Plan.ActivePatches)
	}
}

func TestCaptureUpdateAllowsRolloutOnlyMutation(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 4})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}

	changed := deepCopyPlan(captured.Plan)
	partition := int32(2)
	changed.ActivePatches[0].Rollout.ByPartition.Partition = &partition
	result, err := CaptureUpdate(r1, r1, captured.Plan, changed, CaptureOptions{SourceResourceVersion: "11", Replicas: 4})
	if err != nil {
		t.Fatalf("rollout-only CaptureUpdate() error = %v", err)
	}
	if got := *result.Plan.ActivePatches[0].Rollout.ByPartition.Partition; got != 2 {
		t.Fatalf("partition = %d, want 2", got)
	}
	if len(result.Plan.ActivePatches) != 1 {
		t.Fatalf("rollout-only update created a patch: %#v", result.Plan.ActivePatches)
	}
}

func TestCaptureUpdateExactReverseCancelsPatch(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t1"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 4})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}

	canceled, err := CaptureUpdate(r1, r0, captured.Plan, captured.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 4})
	if err != nil {
		t.Fatalf("cancel CaptureUpdate() error = %v", err)
	}
	if len(canceled.Plan.ActivePatches) != 1 {
		t.Fatalf("cancel created or removed journal entry prematurely: %#v", canceled.Plan.ActivePatches)
	}
	rollout := canceled.Plan.ActivePatches[0].Rollout
	if rollout.TargetState != api.PatchTargetStateAbsent {
		t.Fatalf("targetState = %q, want Absent", rollout.TargetState)
	}
	if rollout.ByPartition == nil || rollout.ByPartition.Partition == nil || *rollout.ByPartition.Partition != 0 {
		t.Fatalf("cancel partition = %#v, want 0", rollout.ByPartition)
	}
}

func TestCaptureUpdateExactReverseUnpausesWithdrawal(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 2})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}
	captured.Plan.ActivePatches[0].Rollout.Paused = true
	canceled, err := CaptureUpdate(r1, r0, captured.Plan, captured.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 2})
	if err != nil {
		t.Fatalf("reverse CaptureUpdate() error = %v", err)
	}
	if canceled.Plan.ActivePatches[0].Rollout.Paused {
		t.Fatal("withdrawn patch remained paused")
	}
}

func TestCaptureUpdateRejectsInvalidAttemptsToReuseOwnedFields(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t1"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 4})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}

	tests := []struct {
		name     string
		template string
	}{
		{name: "third value", template: `{"spec":{"runtime":{"artifact":"v3","templateId":"t1"}}}`},
		{name: "cancel plus unrelated field", template: `{"spec":{"runtime":{"artifact":"v1","templateId":"t2"}}}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := CaptureUpdate(r1, []byte(tt.template), captured.Plan, captured.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 4})
			if err == nil || !strings.Contains(err.Error(), "conflict") {
				t.Fatalf("CaptureUpdate() error = %v, want conflict", err)
			}
		})
	}
}

func TestCaptureUpdateRejectsPartialReverseOfMultiFieldPatch(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t1"}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","templateId":"t2"}}}`)
	partialReverse := []byte(`{"spec":{"runtime":{"artifact":"v1","templateId":"t2"}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	captured, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{SourceResourceVersion: "10", Replicas: 4})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}
	_, err = CaptureUpdate(r1, partialReverse, captured.Plan, captured.Plan, CaptureOptions{SourceResourceVersion: "11", Replicas: 4})
	if err == nil || !strings.Contains(err.Error(), "conflict") {
		t.Fatalf("partial reverse error = %v, want conflict", err)
	}
}

func TestCaptureUpdateScaleOutHonorsNewInstancePolicy(t *testing.T) {
	partition := int32(2)
	basePatch := api.PatchRevision{
		ID:                    "p1",
		SourceResourceVersion: "10",
		Patch:                 rawPatch(t, PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: `.spec.value`, Value: json.RawMessage(`"v2"`)}}}),
		ReversePatch:          rawPatch(t, PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: `.spec.value`, Value: json.RawMessage(`"v1"`)}}}),
		FieldSet:              []string{`.spec.value`},
		Rollout: api.PatchRolloutStrategy{
			TargetState: api.PatchTargetStateApplied,
			Type:        api.PatchRolloutStrategyTypeByPartition,
			ByPartition: &api.PatchByPartition{Partition: &partition},
		},
	}
	template := []byte(`{"spec":{"value":"v2"}}`)

	for _, tt := range []struct {
		name   string
		policy api.PatchNewInstancePolicy
		want   int32
	}{
		{name: "apply keeps partition", policy: api.PatchNewInstancePolicyApply, want: 2},
		{name: "do not apply increases partition", policy: api.PatchNewInstancePolicyDoNotApply, want: 4},
	} {
		t.Run(tt.name, func(t *testing.T) {
			patch := basePatch
			patch.Rollout.NewInstancePolicy = tt.policy
			plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{patch}}
			result, err := CaptureUpdate(template, template, plan, plan, CaptureOptions{
				SourceResourceVersion: "11",
				OldReplicas:           4,
				Replicas:              6,
			})
			if err != nil {
				t.Fatalf("CaptureUpdate() error = %v", err)
			}
			if got := *result.Plan.ActivePatches[0].Rollout.ByPartition.Partition; got != tt.want {
				t.Fatalf("partition = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCaptureUpdateScaleOutWithTemplateChangeStillHonorsExistingPolicies(t *testing.T) {
	partition := int32(2)
	basePatch := api.PatchRevision{
		ID:                    "p1",
		SourceResourceVersion: "10",
		Patch:                 rawPatch(t, PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: `.spec.value`, Value: json.RawMessage(`"v2"`)}}}),
		ReversePatch:          rawPatch(t, PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: `.spec.value`, Value: json.RawMessage(`"v1"`)}}}),
		FieldSet:              []string{`.spec.value`},
		Rollout: api.PatchRolloutStrategy{
			TargetState:       api.PatchTargetStateApplied,
			NewInstancePolicy: api.PatchNewInstancePolicyDoNotApply,
			Type:              api.PatchRolloutStrategyTypeByPartition,
			ByPartition:       &api.PatchByPartition{Partition: &partition},
		},
	}
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{basePatch}}
	oldTemplate := []byte(`{"spec":{"value":"v2","other":"old"}}`)
	newTemplate := []byte(`{"spec":{"value":"v2","other":"new"}}`)

	result, err := CaptureUpdate(oldTemplate, newTemplate, plan, plan, CaptureOptions{
		SourceResourceVersion: "11",
		OldReplicas:           4,
		Replicas:              6,
	})
	if err != nil {
		t.Fatalf("CaptureUpdate() error = %v", err)
	}
	if len(result.Plan.ActivePatches) != 2 {
		t.Fatalf("active patches = %d, want 2", len(result.Plan.ActivePatches))
	}
	if got := *result.Plan.ActivePatches[0].Rollout.ByPartition.Partition; got != 4 {
		t.Fatalf("existing DoNotApply partition = %d, want 4", got)
	}
	if got := *result.Plan.ActivePatches[1].Rollout.ByPartition.Partition; got != 6 {
		t.Fatalf("new patch initial partition = %d, want 6", got)
	}
}

func jsonBytes(t *testing.T, value interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	return data
}

func rawPatch(t *testing.T, value PatchDocument) runtime.RawExtension {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatalf("json.Marshal(patch) error = %v", err)
	}
	return runtime.RawExtension{Raw: data}
}

func assertJSONEqual(t *testing.T, got, want []byte) {
	t.Helper()
	var gotValue, wantValue interface{}
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got: %v", err)
	}
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want: %v", err)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON mismatch:\n got: %s\nwant: %s", got, want)
	}
}

func deepCopyPlan(in *api.RevisionPlan) *api.RevisionPlan {
	if in == nil {
		return nil
	}
	data, _ := json.Marshal(in)
	out := &api.RevisionPlan{}
	_ = json.Unmarshal(data, out)
	return out
}
