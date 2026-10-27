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
	"fmt"
	"reflect"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
)

func TestDefaultRollout(t *testing.T) {
	input := api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition}
	got := DefaultRollout(input)
	if got.TargetState != api.PatchTargetStateApplied {
		t.Fatalf("TargetState = %q, want %q", got.TargetState, api.PatchTargetStateApplied)
	}
	if got.NewInstancePolicy != api.PatchNewInstancePolicyApply {
		t.Fatalf("NewInstancePolicy = %q, want %q", got.NewInstancePolicy, api.PatchNewInstancePolicyApply)
	}
	if input.TargetState != "" || input.NewInstancePolicy != "" {
		t.Fatal("DefaultRollout() mutated its input")
	}
}

func TestDesiredPatchIDsExcludesAbsentAndSorts(t *testing.T) {
	plan := &api.RevisionPlan{ActivePatches: []api.PatchRevision{
		{ID: "p2"},
		{ID: "p3", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateAbsent}},
		{ID: "p1", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateApplied}},
	}}
	want := []string{"p1", "p2"}
	if got := DesiredPatchIDs(plan); !reflect.DeepEqual(got, want) {
		t.Fatalf("DesiredPatchIDs() = %#v, want %#v", got, want)
	}
}

func TestJournalFieldSetConflictsIncludesAbsentOwner(t *testing.T) {
	journal := []api.PatchRevision{
		{ID: "p1", FieldSet: []string{`.spec.runtime.artifact`}},
		{
			ID:       "p2",
			FieldSet: []string{`.spec.runtime.env[name="LOG_LEVEL"].value`},
			Rollout:  api.PatchRolloutStrategy{TargetState: api.PatchTargetStateAbsent},
		},
	}

	got, err := JournalFieldSetConflicts([]string{`.spec.runtime.env[name="LOG_LEVEL"]`}, journal)
	if err != nil {
		t.Fatalf("JournalFieldSetConflicts() error = %v", err)
	}
	want := []string{"p2"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("JournalFieldSetConflicts() = %#v, want %#v", got, want)
	}
}

func TestCompactCompletedRevisionPlanRemovesWithdrawnPatch(t *testing.T) {
	zero := int32(0)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "withdrawn", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateAbsent, Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &zero}}},
		{ID: "progressing", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateApplied, Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &zero}}},
	}}
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{
		{ID: "withdrawn", UpdatedReplicas: 0},
		{ID: "progressing", UpdatedReplicas: 1, AvailableReplicas: 1},
	}}

	got, removed := CompactCompletedRevisionPlan(plan, status, 3)
	if !reflect.DeepEqual(removed, []string{"withdrawn"}) {
		t.Fatalf("removed = %#v, want [withdrawn]", removed)
	}
	if len(got.ActivePatches) != 1 || got.ActivePatches[0].ID != "progressing" {
		t.Fatalf("active patches = %#v, want only progressing", got.ActivePatches)
	}
	if len(plan.ActivePatches) != 2 {
		t.Fatal("CompactCompletedRevisionPlan() mutated its input")
	}
}

func TestCompactCompletedRevisionPlanKeepsWithdrawingPatchStillInUse(t *testing.T) {
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateAbsent}},
	}}
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{{ID: "p1", UpdatedReplicas: 1}}}

	got, removed := CompactCompletedRevisionPlan(plan, status, 3)
	if len(removed) != 0 || len(got.ActivePatches) != 1 {
		t.Fatalf("got patches=%#v removed=%#v, want no compaction", got.ActivePatches, removed)
	}
}

func TestCompactCompletedRevisionPlanRequiresConsistentWithdrawnStatus(t *testing.T) {
	plan := planWithPatches(withdrawnPatch("p1"))
	for _, status := range []api.PatchRevisionStatus{
		{ID: "p1", SelectedReplicas: 1},
		{ID: "p1", ReadyReplicas: 1},
		{ID: "p1", AvailableReplicas: 1},
	} {
		compacted, removed := CompactCompletedRevisionPlan(plan, &api.MultiPatchRevisionStatus{
			PatchStatuses: []api.PatchRevisionStatus{status},
		}, 3)
		if len(removed) != 0 || len(compacted.ActivePatches) != 1 {
			t.Fatalf("status %#v compacted withdrawn patch: removed=%v plan=%#v", status, removed, compacted)
		}
	}
}

func TestCompactCompletedRevisionPlanIgnoresAmbiguousDuplicateStatus(t *testing.T) {
	plan := planWithPatches(withdrawnPatch("p1"))
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{
		{ID: "p1", UpdatedReplicas: 1},
		{ID: "p1", UpdatedReplicas: 0},
	}}
	compacted, removed := CompactCompletedRevisionPlan(plan, status, 3)
	if len(removed) != 0 || len(compacted.ActivePatches) != 1 {
		t.Fatalf("duplicate status compacted patch: removed=%v plan=%#v", removed, compacted)
	}
}

func TestCompactCompletedRevisionPlanBarrier(t *testing.T) {
	zero := int32(0)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &zero}}},
		{ID: "p2", Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}}},
	}}
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{
		{ID: "p1", SelectedReplicas: 3, UpdatedReplicas: 3, ReadyReplicas: 3, AvailableReplicas: 3},
		{ID: "p2", SelectedReplicas: 3, UpdatedReplicas: 3, ReadyReplicas: 3, AvailableReplicas: 3},
	}}

	got, removed := CompactCompletedRevisionPlan(plan, status, 3)
	if len(got.ActivePatches) != 0 || !reflect.DeepEqual(removed, []string{"p1", "p2"}) {
		t.Fatalf("got patches=%#v removed=%#v, want empty barrier compaction", got.ActivePatches, removed)
	}
}

func TestCompactCompletedRevisionPlanDoesNotPartiallyCompactAppliedPatches(t *testing.T) {
	zero, one := int32(0), int32(1)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "complete", Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &zero}}},
		{ID: "partial", Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &one}}},
	}}
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{
		{ID: "complete", SelectedReplicas: 3, UpdatedReplicas: 3, ReadyReplicas: 3, AvailableReplicas: 3},
		{ID: "partial", SelectedReplicas: 2, UpdatedReplicas: 2, ReadyReplicas: 2, AvailableReplicas: 2},
	}}

	got, removed := CompactCompletedRevisionPlan(plan, status, 3)
	if len(removed) != 0 || len(got.ActivePatches) != 2 {
		t.Fatalf("got patches=%#v removed=%#v, want no partial applied compaction", got.ActivePatches, removed)
	}
}

func TestCompactRevisionPlansForCaptureNormalizesBothSides(t *testing.T) {
	oldPlan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "done", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateAbsent}},
		{ID: "keep", Rollout: api.PatchRolloutStrategy{TargetState: api.PatchTargetStateApplied}},
	}}
	currentPlan := normalizePlan(oldPlan)
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{{ID: "done"}}}

	oldResult, currentResult, removed := CompactRevisionPlansForCapture(oldPlan, currentPlan, status, 3)
	if !reflect.DeepEqual(removed, []string{"done"}) {
		t.Fatalf("removed = %#v, want [done]", removed)
	}
	for name, plan := range map[string]*api.RevisionPlan{"old": oldResult, "current": currentResult} {
		if len(plan.ActivePatches) != 1 || plan.ActivePatches[0].ID != "keep" {
			t.Fatalf("%s active patches = %#v, want only keep", name, plan.ActivePatches)
		}
	}
}

func TestValidateJournalIdentity(t *testing.T) {
	plan := &api.RevisionPlan{ActivePatches: []api.PatchRevision{
		{ID: "p1", SourceResourceVersion: "10"},
		{ID: "p2", SourceResourceVersion: "10"},
	}}
	if err := ValidateJournalIdentity(plan); err == nil {
		t.Fatal("ValidateJournalIdentity() error = nil, want duplicate sourceResourceVersion error")
	}
}

func TestValidateRevisionPlan(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*api.RevisionPlan)
		wantErr string
	}{
		{name: "valid"},
		{
			name: "overlapping parent and child",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches = append(plan.ActivePatches, validPatch("p2", "11", `.spec.runtime`))
			},
			wantErr: "overlap",
		},
		{
			name: "withdrawing patch still conflicts",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches[0].Rollout.TargetState = api.PatchTargetStateAbsent
				plan.ActivePatches = append(plan.ActivePatches, validPatch("p2", "11", `.spec.runtime.artifact`))
			},
			wantErr: "overlap",
		},
		{
			name: "non canonical array path",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches[0].FieldSet = []string{`.spec.env[0].value`}
			},
			wantErr: "not canonical",
		},
		{
			name: "strategy union violation",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches[0].Rollout.ByLabel = &api.PatchByLabel{}
			},
			wantErr: "requires only byPartition",
		},
		{
			name: "unsupported patch codec",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches[0].Patch.Raw = []byte(`{"type":"Increment","operations":[]}`)
			},
			wantErr: "unsupported patch type",
		},
		{
			name: "payload path outside field set",
			mutate: func(plan *api.RevisionPlan) {
				plan.ActivePatches[0].FieldSet = []string{`.spec.runtime.templateId`}
			},
			wantErr: "does not match fieldSet",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := &api.RevisionPlan{
				Mode:          api.RevisionModeConcurrentPatches,
				ActivePatches: []api.PatchRevision{validPatch("p1", "10", `.spec.runtime.artifact`)},
			}
			if tt.mutate != nil {
				tt.mutate(plan)
			}
			err := ValidateRevisionPlan(plan)
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateRevisionPlan() error = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidateRevisionPlan() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func validPatch(id, source, field string) api.PatchRevision {
	partition := int32(2)
	forward, _ := json.Marshal(PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: field, Value: json.RawMessage(`"new"`)}}})
	reverse, _ := json.Marshal(PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: field, Value: json.RawMessage(`"old"`)}}})
	return api.PatchRevision{
		ID:                    id,
		SourceResourceVersion: source,
		Patch:                 runtime.RawExtension{Raw: forward},
		ReversePatch:          runtime.RawExtension{Raw: reverse},
		FieldSet:              []string{field},
		Rollout: api.PatchRolloutStrategy{
			Type:        api.PatchRolloutStrategyTypeByPartition,
			ByPartition: &api.PatchByPartition{Partition: &partition},
		},
	}
}

func withdrawnPatch(id string) api.PatchRevision {
	patch := validPatch(id, "1", ".spec.value")
	patch.Rollout.TargetState = api.PatchTargetStateAbsent
	zero := int32(0)
	patch.Rollout.ByPartition.Partition = &zero
	return patch
}

func planWithPatches(patches ...api.PatchRevision) *api.RevisionPlan {
	return &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: patches}
}

func TestValidateRevisionPlanRejectsTooManyPatchesAndPausedWithdrawal(t *testing.T) {
	patches := make([]api.PatchRevision, 0, DefaultMaxActivePatches+1)
	for i := 0; i <= DefaultMaxActivePatches; i++ {
		patches = append(patches, validPatch(fmt.Sprintf("p%d", i), fmt.Sprintf("%d", i+1), fmt.Sprintf(".spec.f%d", i)))
	}
	if err := ValidateRevisionPlan(&api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: patches}); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("ValidateRevisionPlan(too many) error = %v, want limit rejection", err)
	}

	paused := validPatch("p1", "1", ".spec.value")
	paused.Rollout.TargetState = api.PatchTargetStateAbsent
	paused.Rollout.Paused = true
	if err := ValidateRevisionPlan(planWithPatches(paused)); err == nil || !strings.Contains(err.Error(), "Absent") {
		t.Fatalf("ValidateRevisionPlan(paused withdrawal) error = %v, want rejection", err)
	}
}
