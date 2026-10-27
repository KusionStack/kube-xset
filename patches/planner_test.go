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
	"strings"
	"testing"

	"kusionstack.io/kube-xset/api"
)

func TestPlanInstancesIndependentPartitionsAndRollback(t *testing.T) {
	p1Partition := int32(3)
	p2Partition := int32(2)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: partitionRollout(&p1Partition)},
		{ID: "p2", Rollout: partitionRollout(&p2Partition)},
	}}
	instances := []InstanceState{{ID: 0}, {ID: 1}, {ID: 2}, {ID: 3}}

	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	want := map[int][]string{0: {}, 1: {}, 2: {"p2"}, 3: {"p1", "p2"}}
	assertDesiredVectors(t, planned, want)

	// P1 previously reached IDs 2 and 3. Increasing its partition must remove
	// only P1 from ID 2 while preserving P2.
	instances[2].AppliedPatchIDs = []string{"p1", "p2"}
	instances[3].AppliedPatchIDs = []string{"p1", "p2"}
	planned, err = PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances(rollback) error = %v", err)
	}
	assertDesiredVectors(t, planned, want)
	if !reflect.DeepEqual(planned[2].CurrentPatchIDs, []string{"p1", "p2"}) || !reflect.DeepEqual(planned[2].DesiredPatchIDs, []string{"p2"}) {
		t.Fatalf("ID 2 rollback = current %#v desired %#v, want [p1 p2] -> [p2]", planned[2].CurrentPatchIDs, planned[2].DesiredPatchIDs)
	}
}

func TestPlanInstancesAcceptsNilAndEmptyPlan(t *testing.T) {
	instances := []InstanceState{{ID: 2}, {ID: 1}}
	for _, plan := range []*api.RevisionPlan{nil, {}} {
		planned, err := PlanInstances(plan, instances, PlanOptions{BaseRevision: "base"})
		if err != nil {
			t.Fatalf("PlanInstances(%#v) error = %v", plan, err)
		}
		if len(planned) != 2 || planned[0].ID != 1 || planned[1].ID != 2 {
			t.Fatalf("PlanInstances(%#v) = %#v, want sorted identities", plan, planned)
		}
		for i := range planned {
			if len(planned[i].DesiredPatchIDs) != 0 {
				t.Fatalf("identity %d desired patches = %v, want none", planned[i].ID, planned[i].DesiredPatchIDs)
			}
		}
	}
}

func TestPlanInstancesRejectsDuplicateIdentityIDs(t *testing.T) {
	_, err := PlanInstances(&api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}, []InstanceState{{ID: 1}, {ID: 1}}, PlanOptions{})
	if err == nil || !strings.Contains(err.Error(), "duplicate instance id") {
		t.Fatalf("PlanInstances() error = %v, want duplicate identity rejection", err)
	}
}

func TestPlanInstancesPartitionUsesOrdinalPositionWhenIDsHaveHoles(t *testing.T) {
	partition := int32(2)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: partitionRollout(&partition)},
	}}
	instances := []InstanceState{{ID: 0}, {ID: 2}, {ID: 5}, {ID: 9}}

	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{
		0: {}, 2: {}, 5: {"p1"}, 9: {"p1"},
	})
}

func TestPlanInstancesByLabelCombinesCommandsIntoOneDesiredVector(t *testing.T) {
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: labelRollout(api.PatchTargetStateApplied, api.PatchNewInstancePolicyDoNotApply)},
		{ID: "p2", Rollout: labelRollout(api.PatchTargetStateApplied, api.PatchNewInstancePolicyDoNotApply)},
	}}
	instances := []InstanceState{{
		ID:              1,
		AppliedPatchIDs: []string{"p2"},
		Labels: map[string]string{
			"aseset.alipay.com/update-p1": CommandApply,
			"aseset.alipay.com/update-p2": CommandRemove,
		},
	}}

	planned, err := PlanInstances(plan, instances, PlanOptions{CommandLabelPrefix: "aseset.alipay.com/update-"})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	if got, want := planned[0].DesiredPatchIDs, []string{"p1"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("desired vector = %#v, want %#v", got, want)
	}
	if len(planned[0].CommandSnapshot) != 2 {
		t.Fatalf("command snapshot = %#v, want both commands", planned[0].CommandSnapshot)
	}
}

func TestPlanInstancesCommandSnapshotDoesNotClearNewerValue(t *testing.T) {
	snapshot := map[string]string{"aseset.alipay.com/update-p1": CommandApply}
	current := map[string]string{"aseset.alipay.com/update-p1": CommandRemove}
	remaining := ClearConsumedCommands(current, snapshot)
	if got := remaining["aseset.alipay.com/update-p1"]; got != CommandRemove {
		t.Fatalf("newer command = %q, want %q", got, CommandRemove)
	}
}

func TestPlanInstancesRejectsUnknownWithdrawingAndLegacyCommands(t *testing.T) {
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "withdrawing", Rollout: labelRollout(api.PatchTargetStateAbsent, api.PatchNewInstancePolicyApply)},
	}}
	tests := []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "unknown patch", labels: map[string]string{"aseset.alipay.com/update-missing": CommandApply}, want: "unknown patch"},
		{name: "withdrawing apply", labels: map[string]string{"aseset.alipay.com/update-withdrawing": CommandApply}, want: "Absent"},
		{name: "legacy command", labels: map[string]string{"aseset.alipay.com/update-included": "true"}, want: "legacy"},
		{name: "invalid value", labels: map[string]string{"aseset.alipay.com/update-withdrawing": "yes"}, want: "unsupported command"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := PlanInstances(plan, []InstanceState{{ID: 1, Labels: tt.labels}}, PlanOptions{
				CommandLabelPrefix: "aseset.alipay.com/update-",
				LegacyCommandLabel: "aseset.alipay.com/update-included",
			})
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("PlanInstances() error = %v, want containing %q", err, tt.want)
			}
		})
	}
}

func TestPlanInstancesNewIdentityPolicy(t *testing.T) {
	partition := int32(2)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "partition-apply", Rollout: api.PatchRolloutStrategy{
			TargetState: api.PatchTargetStateApplied, NewInstancePolicy: api.PatchNewInstancePolicyApply,
			Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &partition},
		}},
		{ID: "label-apply", Rollout: labelRollout(api.PatchTargetStateApplied, api.PatchNewInstancePolicyApply)},
		{ID: "label-wait", Rollout: labelRollout(api.PatchTargetStateApplied, api.PatchNewInstancePolicyDoNotApply)},
		{ID: "absent", Rollout: labelRollout(api.PatchTargetStateAbsent, api.PatchNewInstancePolicyApply)},
	}}
	instances := []InstanceState{{ID: 5, New: true}}
	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	want := []string{"label-apply", "partition-apply"}
	if !reflect.DeepEqual(planned[0].DesiredPatchIDs, want) {
		t.Fatalf("new identity vector = %#v, want %#v", planned[0].DesiredPatchIDs, want)
	}

	// Once identity state is persisted, changing the policy must not rewrite it.
	plan.ActivePatches[1].Rollout.NewInstancePolicy = api.PatchNewInstancePolicyDoNotApply
	planned, err = PlanInstances(plan, []InstanceState{{ID: 1, AppliedPatchIDs: []string{"label-apply"}}}, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances(existing) error = %v", err)
	}
	if !containsString(planned[0].DesiredPatchIDs, "label-apply") {
		t.Fatalf("policy change retroactively removed existing membership: %#v", planned[0].DesiredPatchIDs)
	}
}

func TestPlanInstancesScaleOutPreservesReachedPartitionCohort(t *testing.T) {
	partition := int32(3) // increased from 2 when replicas grew from 4 to 5
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: api.PatchRolloutStrategy{
			TargetState: api.PatchTargetStateApplied, NewInstancePolicy: api.PatchNewInstancePolicyDoNotApply,
			Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &partition},
		}},
	}}
	instances := []InstanceState{
		{ID: 0}, {ID: 1},
		{ID: 2, AppliedPatchIDs: []string{"p1"}},
		{ID: 3, AppliedPatchIDs: []string{"p1"}},
		{ID: 4, New: true},
	}

	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{
		0: {}, 1: {}, 2: {"p1"}, 3: {"p1"}, 4: {},
	})
}

func TestPlanInstancesClampsPartitionAndRebalancesAfterScaleIn(t *testing.T) {
	partition := int32(9)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: partitionRollout(&partition)},
	}}
	instances := []InstanceState{
		{ID: 0},
		{ID: 4, AppliedPatchIDs: []string{"p1"}},
	}

	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances(over-large partition) error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{0: {}, 4: {}})

	// After scale-in, lowering partition to one keeps one of the two live
	// identities selected. The already-reached identity wins, despite ID holes.
	partition = 1
	planned, err = PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances(scale-in rebalance) error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{0: {}, 4: {"p1"}})
}

func TestPlanInstancesHandlesZeroReplicas(t *testing.T) {
	partition := int32(0)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: partitionRollout(&partition)},
	}}
	planned, err := PlanInstances(plan, nil, PlanOptions{BaseRevision: "base"})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	if len(planned) != 0 {
		t.Fatalf("PlanInstances() = %#v, want empty", planned)
	}
}

func TestPlanInstancesAbsentWithdrawsFromEveryInstance(t *testing.T) {
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "p1", Rollout: labelRollout(api.PatchTargetStateAbsent, api.PatchNewInstancePolicyApply)},
		{ID: "p2", Rollout: labelRollout(api.PatchTargetStateApplied, api.PatchNewInstancePolicyDoNotApply)},
	}}
	instances := []InstanceState{
		{ID: 0, AppliedPatchIDs: []string{"p1"}},
		{ID: 1, AppliedPatchIDs: []string{"p1", "p2"}},
		{ID: 2, AppliedPatchIDs: []string{"p2"}},
	}
	planned, err := PlanInstances(plan, instances, PlanOptions{})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{0: {}, 1: {"p2"}, 2: {"p2"}})
}

func TestPlanInstancesPausedPatchPreservesExistingMembershipAndCommands(t *testing.T) {
	partition := int32(0)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		{ID: "partition", Rollout: api.PatchRolloutStrategy{
			TargetState: api.PatchTargetStateApplied, NewInstancePolicy: api.PatchNewInstancePolicyApply,
			Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &partition}, Paused: true,
		}},
		{ID: "label", Rollout: api.PatchRolloutStrategy{
			TargetState: api.PatchTargetStateApplied, NewInstancePolicy: api.PatchNewInstancePolicyApply,
			Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}, Paused: true,
		}},
	}}
	instances := []InstanceState{
		{ID: 0, AppliedPatchIDs: []string{"partition"}, Labels: map[string]string{"patch/label": CommandApply}},
		{ID: 1, AppliedPatchIDs: []string{"label"}, Labels: map[string]string{"patch/label": CommandRemove}},
		{ID: 2, New: true},
	}

	planned, err := PlanInstances(plan, instances, PlanOptions{CommandLabelPrefix: "patch/"})
	if err != nil {
		t.Fatalf("PlanInstances() error = %v", err)
	}
	assertDesiredVectors(t, planned, map[int][]string{
		0: {"partition"}, 1: {"label"}, 2: {"label", "partition"},
	})
	if planned[0].CommandSnapshot != nil || planned[1].CommandSnapshot != nil {
		t.Fatalf("paused commands were consumed: %#v %#v", planned[0].CommandSnapshot, planned[1].CommandSnapshot)
	}
}

func TestStateKeyIsOrderIndependent(t *testing.T) {
	left := StateKey("base-r0", []string{"p2", "p1"})
	right := StateKey("base-r0", []string{"p1", "p2"})
	if left != right {
		t.Fatalf("StateKey() depends on patch order: %q != %q", left, right)
	}
	if left == StateKey("base-r1", []string{"p1", "p2"}) {
		t.Fatal("StateKey() ignored base revision")
	}
}

func partitionRollout(partition *int32) api.PatchRolloutStrategy {
	return api.PatchRolloutStrategy{
		TargetState:       api.PatchTargetStateApplied,
		NewInstancePolicy: api.PatchNewInstancePolicyApply,
		Type:              api.PatchRolloutStrategyTypeByPartition,
		ByPartition:       &api.PatchByPartition{Partition: partition},
	}
}

func labelRollout(target api.PatchTargetState, policy api.PatchNewInstancePolicy) api.PatchRolloutStrategy {
	return api.PatchRolloutStrategy{
		TargetState:       target,
		NewInstancePolicy: policy,
		Type:              api.PatchRolloutStrategyTypeByLabel,
		ByLabel:           &api.PatchByLabel{},
	}
}

func assertDesiredVectors(t *testing.T, planned []InstancePlan, want map[int][]string) {
	t.Helper()
	for _, item := range planned {
		if !reflect.DeepEqual(item.DesiredPatchIDs, want[item.ID]) {
			t.Fatalf("instance %d desired = %#v, want %#v", item.ID, item.DesiredPatchIDs, want[item.ID])
		}
	}
}
