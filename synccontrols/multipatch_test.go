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

package synccontrols

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	jsonpatch "github.com/evanphx/json-patch"
	appsv1 "k8s.io/api/apps/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
	xsetpatches "kusionstack.io/kube-xset/patches"
)

func TestBuildCommandCleanupPatchUsesValuePrecondition(t *testing.T) {
	patchBytes, err := buildCommandCleanupPatch(map[string]string{"aseset.alipay.com/update-p1": "apply"})
	if err != nil {
		t.Fatalf("buildCommandCleanupPatch() error = %v", err)
	}
	patch, err := jsonpatch.DecodePatch(patchBytes)
	if err != nil {
		t.Fatalf("DecodePatch() error = %v", err)
	}
	original := []byte(`{"metadata":{"labels":{"aseset.alipay.com/update-p1":"apply","keep":"yes"}}}`)
	cleaned, err := patch.Apply(original)
	if err != nil {
		t.Fatalf("Apply(original) error = %v", err)
	}
	if strings.Contains(string(cleaned), "update-p1") || !strings.Contains(string(cleaned), "keep") {
		t.Fatalf("cleaned object = %s", cleaned)
	}

	newer := []byte(`{"metadata":{"labels":{"aseset.alipay.com/update-p1":"remove"}}}`)
	if _, err := patch.Apply(newer); err == nil {
		t.Fatal("cleanup patch deleted a newer command value")
	}
}

func TestBuildMultiPatchExecutionStateResolvesHistoricalAndDesiredVectors(t *testing.T) {
	r0 := multiPatchTestRevision("r0", 1, `{"spec":{"a":"0","b":"0"}}`)
	r1 := multiPatchTestRevision("r1", 2, `{"spec":{"a":"1","b":"0"}}`)
	r2 := multiPatchTestRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	p1Partition, p2Partition := int32(2), int32(1)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, p1Partition),
		multiPatchTestPatch(t, "p2", `.spec.b`, `"2"`, `"0"`, p2Partition),
	}}
	identities := []multiPatchIdentity{
		{ID: 0, RevisionName: "r0"},
		{ID: 1, RevisionName: "r0"},
		{ID: 2, RevisionName: "r1"},
	}

	state, err := buildMultiPatchExecutionState(plan, nil, r0, r2, []*appsv1.ControllerRevision{r0, r1, r2}, identities, multiPatchTestRevisionBuilder, "patch/", "legacy")
	if err != nil {
		t.Fatalf("buildMultiPatchExecutionState() error = %v", err)
	}
	if state.Status.BaseRevision != "r0" || state.Status.HeadRevision != "r2" {
		t.Fatalf("status revisions = %#v, want base r0 head r2", state.Status)
	}
	if got := state.CurrentPatchIDs[2]; !reflect.DeepEqual(got, []string{"p1"}) {
		t.Fatalf("identity 2 current patches = %#v, want [p1]", got)
	}
	if got := state.InstancePlans[1].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p2"}) {
		t.Fatalf("identity 1 desired patches = %#v, want [p2]", got)
	}
	if got := state.InstancePlans[2].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
		t.Fatalf("identity 2 desired patches = %#v, want [p1 p2]", got)
	}
	if state.DesiredRevisions[1].Name == "r2" || state.DesiredRevisions[1].Name == "r0" {
		t.Fatalf("identity 1 desired revision = %q, want synthetic P2 state", state.DesiredRevisions[1].Name)
	}
	if state.DesiredRevisions[2].Name != "r2" {
		t.Fatalf("identity 2 desired revision = %q, want historical head r2", state.DesiredRevisions[2].Name)
	}
}

func TestBuildMultiPatchExecutionStateRejectsUnknownCurrentRevision(t *testing.T) {
	r0 := multiPatchTestRevision("r0", 1, `{"spec":{"a":"0"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}
	_, err := buildMultiPatchExecutionState(plan, nil, r0, r0, []*appsv1.ControllerRevision{r0}, []multiPatchIdentity{{ID: 0, RevisionName: "lost"}}, multiPatchTestRevisionBuilder, "", "")
	if err == nil || !strings.Contains(err.Error(), "unknown revision") {
		t.Fatalf("buildMultiPatchExecutionState() error = %v, want unknown revision", err)
	}
}

func TestBuildMultiPatchExecutionStateReconstructsBaseFromHead(t *testing.T) {
	r1 := multiPatchTestRevision("r1", 2, `{"spec":{"a":"1","b":"0"}}`)
	r2 := multiPatchTestRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	zero := int32(0)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, zero),
		multiPatchTestPatch(t, "p2", `.spec.b`, `"2"`, `"0"`, zero),
	}}
	staleStatus := &api.MultiPatchRevisionStatus{BaseRevision: "already-garbage-collected"}

	state, err := buildMultiPatchExecutionState(
		plan,
		staleStatus,
		r1,
		r2,
		[]*appsv1.ControllerRevision{r1, r2},
		[]multiPatchIdentity{{ID: 0, RevisionName: "r1"}, {ID: 1, RevisionName: "r2"}},
		multiPatchTestRevisionBuilder,
		"patch/",
		"legacy",
	)
	if err != nil {
		t.Fatalf("buildMultiPatchExecutionState() error = %v", err)
	}
	if state.Status.BaseRevision == "" || state.Status.BaseRevision == staleStatus.BaseRevision {
		t.Fatalf("base revision = %q, want reconstructed base", state.Status.BaseRevision)
	}
	if state.Status.HeadRevision != "r2" {
		t.Fatalf("head revision = %q, want r2", state.Status.HeadRevision)
	}
	if got := state.CurrentPatchIDs[0]; !reflect.DeepEqual(got, []string{"p1"}) {
		t.Fatalf("identity 0 current patches = %#v, want [p1]", got)
	}
}

func TestBuildMultiPatchExecutionStateSurvivesControllerRestart(t *testing.T) {
	r0 := multiPatchTestRevision("r0", 1, `{"spec":{"a":"0","b":"0"}}`)
	r2 := multiPatchTestRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	partition := int32(1)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, partition),
		multiPatchTestPatch(t, "p2", `.spec.b`, `"2"`, `"0"`, partition),
	}}
	identities := []multiPatchIdentity{{ID: 0, RevisionName: "r0"}, {ID: 1, RevisionName: "r2"}}

	first, err := buildMultiPatchExecutionState(plan, nil, r0, r2, []*appsv1.ControllerRevision{r0, r2}, identities, multiPatchTestRevisionBuilder, "patch/", "legacy")
	if err != nil {
		t.Fatalf("first buildMultiPatchExecutionState() error = %v", err)
	}
	persistedStatus := copyMultiPatchStatus(first.Status)

	// A fresh process owns no in-memory catalog. It must derive the same
	// vectors solely from the persisted plan, status, revisions and targets.
	restarted, err := buildMultiPatchExecutionState(plan, persistedStatus, r0, r2, []*appsv1.ControllerRevision{r0, r2}, identities, multiPatchTestRevisionBuilder, "patch/", "legacy")
	if err != nil {
		t.Fatalf("restart buildMultiPatchExecutionState() error = %v", err)
	}
	if !reflect.DeepEqual(restarted.CurrentPatchIDs, first.CurrentPatchIDs) {
		t.Fatalf("current vectors after restart = %#v, want %#v", restarted.CurrentPatchIDs, first.CurrentPatchIDs)
	}
	for id, firstPlan := range first.InstancePlans {
		if got := restarted.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, firstPlan.DesiredPatchIDs) {
			t.Fatalf("identity %d desired patches after restart = %#v, want %#v", id, got, firstPlan.DesiredPatchIDs)
		}
	}
	if restarted.Status.BaseRevision != first.Status.BaseRevision || restarted.Status.HeadRevision != first.Status.HeadRevision {
		t.Fatalf("status revisions after restart = %#v, want %#v", restarted.Status, first.Status)
	}
}

func TestSetMultiPatchConditionsReportsProgressPauseAndFailure(t *testing.T) {
	partition := int32(1)
	patch := multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, partition)
	status := &api.PatchRevisionStatus{ID: "p1", SelectedReplicas: 2, UpdatedReplicas: 1, AvailableReplicas: 1}
	state := &MultiPatchSyncContext{Generation: 7}
	setMultiPatchConditions(&patch, status, state)

	assertPatchCondition(t, status, api.PatchConditionProgressing, metav1.ConditionTrue, "RolloutInProgress", 7)
	assertPatchCondition(t, status, api.PatchConditionAvailable, metav1.ConditionFalse, "SelectedInstancesUnavailable", 7)
	assertPatchCondition(t, status, api.PatchConditionPaused, metav1.ConditionFalse, "RolloutRunning", 7)
	assertPatchCondition(t, status, api.PatchConditionWithdrawing, metav1.ConditionFalse, "TargetStateApplied", 7)
	assertPatchCondition(t, status, api.PatchConditionDegraded, metav1.ConditionFalse, "ReconcileSucceeded", 7)

	state.Suspended = true
	state.SyncError = ErrConcurrentPatchRolloutDisabled
	setMultiPatchConditions(&patch, status, state)
	assertPatchCondition(t, status, api.PatchConditionProgressing, metav1.ConditionFalse, "RolloutPaused", 7)
	assertPatchCondition(t, status, api.PatchConditionPaused, metav1.ConditionTrue, "FeatureGateDisabled", 7)
	assertPatchCondition(t, status, api.PatchConditionDegraded, metav1.ConditionTrue, "FeatureGateDisabled", 7)
}

func TestSetMultiPatchConditionsReportsWithdrawal(t *testing.T) {
	partition := int32(0)
	patch := multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, partition)
	patch.Rollout.TargetState = api.PatchTargetStateAbsent
	status := &api.PatchRevisionStatus{ID: "p1"}
	state := &MultiPatchSyncContext{Generation: 8}
	setMultiPatchConditions(&patch, status, state)

	assertPatchCondition(t, status, api.PatchConditionWithdrawing, metav1.ConditionTrue, "TargetStateAbsent", 8)
	assertPatchCondition(t, status, api.PatchConditionAvailable, metav1.ConditionFalse, "SelectedInstancesUnavailable", 8)
}

func assertPatchCondition(t *testing.T, status *api.PatchRevisionStatus, conditionType string, wantStatus metav1.ConditionStatus, wantReason string, wantGeneration int64) {
	t.Helper()
	condition := apiMeta.FindStatusCondition(status.Conditions, conditionType)
	if condition == nil {
		t.Fatalf("condition %q not found in %#v", conditionType, status.Conditions)
	}
	if condition.Status != wantStatus || condition.Reason != wantReason || condition.ObservedGeneration != wantGeneration {
		t.Fatalf("condition %q = %#v, want status=%s reason=%s generation=%d", conditionType, condition, wantStatus, wantReason, wantGeneration)
	}
}

func multiPatchTestRevision(name string, revision int64, raw string) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: name}, Revision: revision, Data: runtime.RawExtension{Raw: []byte(raw)}}
}

func multiPatchTestPatch(t *testing.T, id, path, value, reverse string, partition int32) api.PatchRevision {
	t.Helper()
	forward, _ := json.Marshal(xsetpatches.PatchDocument{Type: xsetpatches.SemanticPatchType, Operations: []xsetpatches.Operation{{Path: path, Value: json.RawMessage(value)}}})
	backward, _ := json.Marshal(xsetpatches.PatchDocument{Type: xsetpatches.SemanticPatchType, Operations: []xsetpatches.Operation{{Path: path, Value: json.RawMessage(reverse)}}})
	return api.PatchRevision{
		ID: id, SourceResourceVersion: id,
		Patch: runtime.RawExtension{Raw: forward}, ReversePatch: runtime.RawExtension{Raw: backward}, FieldSet: []string{path},
		Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &partition}},
	}
}

func multiPatchTestRevisionBuilder(base *appsv1.ControllerRevision, patchSet []api.PatchRevision, name string) (*appsv1.ControllerRevision, error) {
	raw := append([]byte(nil), base.Data.Raw...)
	var err error
	for i := range patchSet {
		raw, err = xsetpatches.Apply(raw, patchSet[i].Patch)
		if err != nil {
			return nil, err
		}
	}
	result := base.DeepCopy()
	result.Name = name
	result.Data.Raw = raw
	return result, nil
}
