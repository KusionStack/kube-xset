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

	"kusionstack.io/kube-xset/api"
)

// TestConcurrentPatchLifecycleE2E exercises the complete pure state-machine
// boundary used by admission and reconciliation: two API updates are captured
// before a reconcile, independently rolled out, one partition is rolled back,
// and then that patch is canceled without disturbing the other patch.
func TestConcurrentPatchLifecycleE2E(t *testing.T) {
	r0 := []byte(`{"spec":{"runtime":{"artifact":"v1","env":[{"name":"LOG_LEVEL","value":"info"}]}}}`)
	r1 := []byte(`{"spec":{"runtime":{"artifact":"v2","env":[{"name":"LOG_LEVEL","value":"info"}]}}}`)
	r2 := []byte(`{"spec":{"runtime":{"artifact":"v2","env":[{"name":"LOG_LEVEL","value":"debug"}]}}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	first, err := CaptureUpdate(r0, r1, plan, plan, CaptureOptions{
		SourceResourceVersion: "100", Replicas: 4, Schema: aseTemplateSchema,
	})
	if err != nil {
		t.Fatalf("capture P1: %v", err)
	}
	second, err := CaptureUpdate(r1, r2, first.Plan, first.Plan, CaptureOptions{
		SourceResourceVersion: "101", Replicas: 4, Schema: aseTemplateSchema,
	})
	if err != nil {
		t.Fatalf("capture P2: %v", err)
	}
	p1 := second.Plan.ActivePatches[0].ID
	p2 := second.Plan.ActivePatches[1].ID

	p1Partition, p2Partition := int32(3), int32(2)
	second.Plan.ActivePatches[0].Rollout.ByPartition.Partition = &p1Partition
	second.Plan.ActivePatches[1].Rollout.ByPartition.Partition = &p2Partition
	instances := []InstanceState{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}
	planned, err := PlanInstances(second.Plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("plan independent rollout: %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{
		0: {}, 1: {}, 2: {p2}, 3: {p1, p2},
	})

	// Persist the reached state, then roll P1 forward once and back once. P2
	// membership must remain unchanged throughout.
	for i := range instances {
		instances[i].AppliedPatchIDs = append([]string(nil), planned[i].DesiredPatchIDs...)
	}
	p1Partition = 2
	planned, err = PlanInstances(second.Plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("plan P1 forward: %v", err)
	}
	for i := range instances {
		instances[i].AppliedPatchIDs = append([]string(nil), planned[i].DesiredPatchIDs...)
	}
	p1Partition = 3
	planned, err = PlanInstances(second.Plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("plan P1 rollback: %v", err)
	}
	if got, want := planned[2].DesiredPatchIDs, []string{p2}; !reflect.DeepEqual(got, want) {
		t.Fatalf("P1 rollback disturbed P2: got %#v, want %#v", got, want)
	}

	// The user's exact template reversal cancels P1. It remains in the journal
	// as Withdrawing until every instance has removed it.
	canceledHead := []byte(`{"spec":{"runtime":{"artifact":"v1","env":[{"name":"LOG_LEVEL","value":"debug"}]}}}`)
	canceled, err := CaptureUpdate(r2, canceledHead, second.Plan, second.Plan, CaptureOptions{
		SourceResourceVersion: "102", Replicas: 4, Schema: aseTemplateSchema,
	})
	if err != nil {
		t.Fatalf("cancel P1: %v", err)
	}
	if canceled.Plan.ActivePatches[0].ID != p1 || canceled.Plan.ActivePatches[0].Rollout.TargetState != api.PatchTargetStateAbsent {
		t.Fatalf("P1 was not retained as Withdrawing: %#v", canceled.Plan.ActivePatches[0])
	}
	planned, err = PlanInstances(canceled.Plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("plan P1 withdrawal: %v", err)
	}
	for _, item := range planned {
		if containsString(item.DesiredPatchIDs, p1) {
			t.Fatalf("instance %d still desires canceled P1: %#v", item.ID, item.DesiredPatchIDs)
		}
		if item.ID >= 2 && !containsString(item.DesiredPatchIDs, p2) {
			t.Fatalf("instance %d lost independent P2: %#v", item.ID, item.DesiredPatchIDs)
		}
	}
}
