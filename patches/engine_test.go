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
	AtomicPaths: []string{`.metadata.labels`, `.metadata.annotations`},
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
