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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"kusionstack.io/kube-xset/api"
	xsetpatches "kusionstack.io/kube-xset/patches"
)

func TestConcurrentReplacementPartitionUsesLogicalReplicas(t *testing.T) {
	for _, tc := range []struct {
		name                string
		origin, replacement int
	}{
		{"larger replacement ID", 2, 3}, {"reclaimed smaller ID", 3, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, ctx := replacementPatchFixture(t, tc.origin, tc.replacement, "r0", "r1")
			history := append([]*appsv1.ControllerRevision(nil), ctx.Revisions...)
			if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int{tc.origin, tc.replacement} {
				if got := ctx.MultiPatch.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1"}) {
					t.Fatalf("physical ID %d desired = %v, want only p1; replacement must not release p2", id, got)
				}
			}
			if len(ctx.MultiPatch.CurrentPatchIDs[tc.origin]) != 0 || !reflect.DeepEqual(ctx.MultiPatch.CurrentPatchIDs[tc.replacement], []string{"p1"}) {
				t.Fatal("physical applied vectors were collapsed")
			}
			// Reconstruction after restart must not require the previous execution state.
			first := ctx.MultiPatch.DesiredRevisions[tc.origin].Name
			ctx.MultiPatch = nil
			ctx.Revisions = history
			if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
				t.Fatal(err)
			}
			if ctx.MultiPatch.DesiredRevisions[tc.origin].Name != first {
				t.Fatal("restart changed replacement target")
			}
		})
	}
}

func TestConcurrentReplacementStatusCountsLogicalReplicas(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r1", "r1")
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	s := replacementPatchStatus(t, ctx, "p1")
	if s.SelectedReplicas != 1 || s.UpdatedReplicas != 1 || s.ReadyReplicas != 1 || s.AvailableReplicas != 1 {
		t.Fatalf("replacement counted twice: %#v", s)
	}
	if replacementPatchStatus(t, ctx, "p2").SelectedReplicas != 0 {
		t.Fatal("held patch selected a replacement peer")
	}
}

func TestConcurrentReplacementBarrierRequiresCompleteAvailableVector(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r1", "r0")
	controller := r.xsetController.(*replacementPatchController)
	for i := range controller.plan.ActivePatches {
		*controller.plan.ActivePatches[i].Rollout.ByPartition.Partition = 0
	}
	// One logical replica: its old member has only P1, its new member only P2.
	// Both are Available, but no physical member has the desired P1+P2 vector.
	ctx.TargetWrappers = ctx.TargetWrappers[2:]
	delete(ctx.OwnedIds, 0)
	delete(ctx.OwnedIds, 1)
	p2 := multiPatchTestRevision("r-p2", 4, `{"spec":{"a":"0","b":"2"}}`)
	ctx.Revisions = append(ctx.Revisions, p2)
	ctx.TargetWrappers[1].Object.GetLabels()[appsv1.ControllerRevisionHashLabelKey] = p2.Name
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	for _, id := range []string{"p1", "p2"} {
		s := replacementPatchStatus(t, ctx, id)
		if s.UpdatedReplicas != 1 || s.AvailableReplicas != 0 {
			t.Fatalf("partial peers passed a full-vector barrier: %#v", s)
		}
	}
	if _, removed := xsetpatches.CompactCompletedRevisionPlan(controller.plan, ctx.MultiPatch.Status, 1); len(removed) != 0 {
		t.Fatal("compacted patches before a complete available replacement existed")
	}
}

func TestConcurrentReplacementWithdrawalWaitsForOldPeer(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r1", "r0")
	controller := r.xsetController.(*replacementPatchController)
	controller.plan.ActivePatches[0].Rollout.TargetState = api.PatchTargetStateAbsent
	// Head excludes the withdrawing patch.
	ctx.UpdatedRevision = multiPatchTestRevision("r-p2", 4, `{"spec":{"a":"0","b":"2"}}`)
	ctx.Revisions = append(ctx.Revisions, ctx.UpdatedRevision)
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	if got := replacementPatchStatus(t, ctx, "p1").UpdatedReplicas; got != 1 {
		t.Fatalf("withdrawing patch still used by old peer = %d, want 1", got)
	}
	// Old context can outlive the physical origin for one reconciliation.
	ctx.TargetWrappers = ctx.TargetWrappers[:len(ctx.TargetWrappers)-2]
	ctx.TargetWrappers = append(ctx.TargetWrappers, replacementWrapper(3, "r0"))
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	if got := replacementPatchStatus(t, ctx, "p1").UpdatedReplicas; got != 0 {
		t.Fatalf("removed origin still counted = %d", got)
	}
}

func TestConcurrentReplacementByLabelPolicyAndCommands(t *testing.T) {
	for _, policy := range []api.PatchNewInstancePolicy{api.PatchNewInstancePolicyApply, api.PatchNewInstancePolicyDoNotApply} {
		t.Run(string(policy), func(t *testing.T) {
			r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
			controller := r.xsetController.(*replacementPatchController)
			for i := range controller.plan.ActivePatches {
				controller.plan.ActivePatches[i].Rollout = api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}, NewInstancePolicy: policy}
			}
			// Sending p2 to the replacement must update the logical pair, preserving p1.
			ctx.TargetWrappers[3].Object.SetLabels(map[string]string{appsv1.ControllerRevisionHashLabelKey: "r1", "test/patch-p2": "apply"})
			ctx.OwnedIds[4] = &api.ContextDetail{ID: 4, Data: map[string]string{testContextKey(api.EnumJustCreateContextDataKey): "true"}}
			if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
				t.Fatal(err)
			}
			for _, id := range []int{2, 3} {
				if got := ctx.MultiPatch.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
					t.Fatalf("peer %d desired = %v", id, got)
				}
			}
			if len(ctx.MultiPatch.InstancePlans[2].CommandSnapshot) != 0 || ctx.MultiPatch.InstancePlans[3].CommandSnapshot["test/patch-p2"] != "apply" {
				t.Fatal("command snapshots must belong to their physical object")
			}
			want := 0
			if policy == api.PatchNewInstancePolicyApply {
				want = 2
			}
			if got := len(ctx.MultiPatch.InstancePlans[4].DesiredPatchIDs); got != want {
				t.Fatalf("genuine scale-out applied %d patches, want %d", got, want)
			}
		})
	}
}

func TestConcurrentReplacementConflictingCommandsFailClosed(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	controller := r.xsetController.(*replacementPatchController)
	controller.plan.ActivePatches[0].Rollout = api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}}
	ctx.TargetWrappers[2].Object.GetLabels()["test/patch-p1"] = "apply"
	ctx.TargetWrappers[3].Object.GetLabels()["test/patch-p1"] = "remove"
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err == nil {
		t.Fatal("conflicting peer commands must not silently choose a winner")
	}
}

func TestConcurrentReplacementPauseAndIndependentRollback(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	history := append([]*appsv1.ControllerRevision(nil), ctx.Revisions...)
	controller := r.xsetController.(*replacementPatchController)
	controller.plan.ActivePatches[0].Rollout.Paused = true
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	if got := ctx.MultiPatch.InstancePlans[2].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1"}) {
		t.Fatalf("pause cancelled an in-flight replacement: %v", got)
	}
	controller.plan.ActivePatches[0].Rollout.Paused = false
	*controller.plan.ActivePatches[0].Rollout.ByPartition.Partition = 3
	*controller.plan.ActivePatches[1].Rollout.ByPartition.Partition = 2
	ctx.Revisions = history
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		if got := ctx.MultiPatch.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p2"}) {
			t.Fatalf("peer %d after independent rollback = %v", id, got)
		}
	}
}

func TestConcurrentReplacementByLabelIntentSurvivesPeerRecreation(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	controller := r.xsetController.(*replacementPatchController)
	for i := range controller.plan.ActivePatches {
		controller.plan.ActivePatches[i].Rollout = api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}}
	}
	// A command sent to the peer caused Replace to persist P1+P2 on the origin.
	// The peer and its command have now been deleted for recreation.
	r.xsetLabelAnnoMgr.Set(ctx.TargetWrappers[2].Object, api.XReplaceByReplaceUpdateLabelKey, "r2")
	ctx.TargetWrappers = ctx.TargetWrappers[:3]
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		if got := ctx.MultiPatch.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
			t.Fatalf("peer %d lost persisted intent: %v", id, got)
		}
	}
}

func TestConcurrentReplacementIntentSurvivesHistoryGC(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	controller := r.xsetController.(*replacementPatchController)
	for i := range controller.plan.ActivePatches {
		controller.plan.ActivePatches[i].Rollout = api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}}
	}
	origin := ctx.TargetWrappers[2]
	r.xsetLabelAnnoMgr.Set(origin.Object, api.XReplaceByReplaceUpdateLabelKey, "collected-revision")
	origin.Object.SetAnnotations(map[string]string{"xset.kusionstack.io/replace-patch-intent": `{"version":1,"ownerUID":"test-uid","revision":"collected-revision","patchIDs":["p1","p2"]}`})
	ctx.TargetWrappers = ctx.TargetWrappers[:3]
	ctx.OwnedIds[3].Data[testContextKey(api.EnumRevisionContextDataKey)] = "collected-revision"
	ctx.Revisions = []*appsv1.ControllerRevision{ctx.CurrentRevision, ctx.UpdatedRevision}
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	for _, id := range []int{2, 3} {
		if got := ctx.MultiPatch.InstancePlans[id].DesiredPatchIDs; !reflect.DeepEqual(got, []string{"p1", "p2"}) {
			t.Fatalf("lost replacement intent after history GC: %d = %v", id, got)
		}
	}
}

func TestConcurrentReplacementPersistsIntentBeforeDeletingPeer(t *testing.T) {
	for _, fail := range []bool{true, false} {
		t.Run(fmt.Sprint("origin write fails=", fail), func(t *testing.T) {
			r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
			origin, peer := ctx.TargetWrappers[2], ctx.TargetWrappers[3]
			if err := r.Client.Get(context.Background(), client.ObjectKeyFromObject(origin.Object), origin.Object); err != nil {
				t.Fatal(err)
			}
			c := &replacementOrderClient{Client: r.Client, origin: origin.GetName(), failOrigin: fail}
			info := &TargetUpdateInfo{TargetWrapper: origin, UpdateRevision: ctx.UpdatedRevision, multiPatchIntent: &replacementPatchIntent{Version: 1, OwnerUID: "test-uid", Revision: "r2", PatchIDs: []string{"p1", "p2"}}}
			err := updateReplaceOriginTarget(context.Background(), c, record.NewFakeRecorder(10), r.xsetLabelAnnoMgr, info, &TargetUpdateInfo{TargetWrapper: peer})
			if fail {
				if err == nil || !reflect.DeepEqual(c.writes, []string{"origin"}) {
					t.Fatalf("deleted peer before recording intent: %v %v", c.writes, err)
				}
				return
			}
			if err != nil || !reflect.DeepEqual(c.writes, []string{"origin", "peer"}) {
				t.Fatalf("write order = %v, %v", c.writes, err)
			}
			stored := &corev1.Pod{}
			if err := c.Get(context.Background(), client.ObjectKeyFromObject(origin.Object), stored); err != nil {
				t.Fatal(err)
			}
			var intent replacementPatchIntent
			if err := json.Unmarshal([]byte(stored.Annotations[replacementPatchIntentAnnotation]), &intent); err != nil {
				t.Fatal(err)
			}
			if intent.Revision != stored.Labels[r.xsetLabelAnnoMgr.Value(api.XReplaceByReplaceUpdateLabelKey)] || !reflect.DeepEqual(intent.PatchIDs, []string{"p1", "p2"}) {
				t.Fatalf("intent and replace label diverged: %#v", intent)
			}
		})
	}
}

type replacementOrderClient struct {
	client.Client
	origin     string
	failOrigin bool
	writes     []string
}

func (c *replacementOrderClient) Patch(ctx context.Context, object client.Object, patch client.Patch, options ...client.PatchOption) error {
	if object.GetName() == c.origin {
		c.writes = append(c.writes, "origin")
		if c.failOrigin {
			return errors.New("simulated origin write failure")
		}
	} else {
		c.writes = append(c.writes, "peer")
	}
	return c.Client.Patch(ctx, object, patch, options...)
}

func TestConcurrentReplacementMultiplePairs(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 3, 0, "r0", "r1")
	controller := r.xsetController.(*replacementPatchController)
	*controller.plan.ActivePatches[0].Rollout.ByPartition.Partition = 1
	ctx.TargetWrappers = append(ctx.TargetWrappers, replacementWrapper(4, "r1"))
	ctx.OwnedIds[1].Data[testContextKey(api.EnumReplaceNewTargetIDContextDataKey)] = "4"
	ctx.OwnedIds[4] = &api.ContextDetail{ID: 4, Data: map[string]string{testContextKey(api.EnumReplaceOriginTargetIDContextDataKey): "1", testContextKey(api.EnumRevisionContextDataKey): "r1"}}
	if err := r.Client.Create(context.Background(), ctx.TargetWrappers[4].Object); err != nil {
		t.Fatal(err)
	}
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	if s := replacementPatchStatus(t, ctx, "p1"); s.SelectedReplicas != 2 || s.UpdatedReplicas != 2 {
		t.Fatalf("multiple replacing pairs: %#v", s)
	}
	if replacementPatchStatus(t, ctx, "p2").SelectedReplicas != 0 {
		t.Fatal("multiple replacements released p2")
	}
}

func TestConcurrentReplacementPendingCommandsAndReadiness(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	controller := r.xsetController.(*replacementPatchController)
	controller.plan.ActivePatches[0].Rollout = api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByLabel, ByLabel: &api.PatchByLabel{}}
	for _, wrapper := range ctx.TargetWrappers[2:] {
		wrapper.Object.GetLabels()["test/patch-p1"] = "apply"
	}
	ctx.TargetWrappers[3].Object.GetLabels()["test/not-ready"] = "true"
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	r.calculateMultiPatchStatus(ctx)
	s := replacementPatchStatus(t, ctx, "p1")
	if s.PendingApplyCommands != 1 || s.UpdatedReplicas != 1 || s.ReadyReplicas != 0 || s.AvailableReplicas != 0 {
		t.Fatalf("physical peers skewed logical readiness/commands: %#v", s)
	}
}

func TestConcurrentReplacementInvalidContextFailsClosed(t *testing.T) {
	for _, value := range []string{"bad", "-1", "3", "1"} {
		t.Run(value, func(t *testing.T) {
			r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
			ctx.OwnedIds[3].Data[testContextKey(api.EnumReplaceOriginTargetIDContextDataKey)] = value
			if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err == nil {
				t.Fatal("invalid replacement relationship was counted as genuine scale-out")
			}
		})
	}
}

func TestReplaceFinishRequiresCorrectAvailableRevision(t *testing.T) {
	controller := &replacementPatchController{}
	manager := api.NewXSetLabelAnnotationManager(nil)
	u := &replaceUpdateTargetUpdater{}
	u.Setup(&UpdateConfig{XsetController: controller, XsetLabelAnnoMgr: manager}, syncControlTestOwner())
	for _, tc := range []struct {
		name, revision string
		deleting       bool
		want           bool
	}{
		{"wrong available revision", "r0", false, false},
		{"correct available revision", "r1", false, true},
		{"correct but being deleted", "r1", true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			peer := replacementWrapper(3, tc.revision)
			if tc.deleting {
				manager.Set(peer.Object, api.XDeletionIndicationLabelKey, "1")
			}
			info := &TargetUpdateInfo{UpdateRevision: multiPatchTestRevision("r1", 1, `{}`), ReplacePairNewTargetInfo: &TargetUpdateInfo{TargetWrapper: peer}}
			got, _, err := u.GetTargetUpdateFinishStatus(context.Background(), info)
			if err != nil || got != tc.want {
				t.Fatalf("finished = %v, %v; want %v", got, err, tc.want)
			}
		})
	}
}

func TestConcurrentManualReplacementFollowsDesiredVector(t *testing.T) {
	r, ctx := replacementPatchFixture(t, 2, 3, "r0", "r1")
	r.updateConfig = &UpdateConfig{XsetLabelAnnoMgr: r.xsetLabelAnnoMgr}
	r.scaleInLifecycleAdapter = &syncControlTestLifecycle{id: "scale-in"}
	origin, peer := ctx.TargetWrappers[2], ctx.TargetWrappers[3]
	r.xsetLabelAnnoMgr.Set(origin.Object, api.XInstanceIdLabelKey, "2")
	r.xsetLabelAnnoMgr.Set(origin.Object, api.XReplaceIndicationLabelKey, "1")
	r.xsetLabelAnnoMgr.Set(peer.Object, api.XReplacePairOriginName, origin.GetName())
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	if got, want := r.getReplaceRevision(origin.Object, ctx), ctx.MultiPatch.DesiredRevisions[2]; got.Name != want.Name {
		t.Fatalf("manual replacement uses %s, want per-identity desired %s", got.Name, want.Name)
	}
	// A manually started replacement can become stale while P2 is advanced.
	controller := r.xsetController.(*replacementPatchController)
	*controller.plan.ActivePatches[1].Rollout.ByPartition.Partition = 2
	ctx.Revisions = ctx.Revisions[:3]
	if err := r.prepareMultiPatch(context.Background(), syncControlTestOwner(), ctx); err != nil {
		t.Fatal(err)
	}
	_, _, _, deletions := r.dealReplaceTargets(context.Background(), ctx.TargetWrappers, ctx.MultiPatch)
	if len(deletions) != 0 {
		t.Fatal("manual replacement deleted the origin with an obsolete Available peer")
	}
}

type replacementPatchController struct{ syncControlTestController }

func (*replacementPatchController) CheckReadyTime(object client.Object) (bool, *metav1.Time) {
	return object.GetLabels()["test/not-ready"] != "true", nil
}

func (*replacementPatchController) CheckAvailable(object client.Object) bool {
	return object.GetLabels()["test/not-ready"] != "true"
}

func (*replacementPatchController) BuildMultiPatchRevision(base *appsv1.ControllerRevision, patches []api.PatchRevision, name string) (*appsv1.ControllerRevision, error) {
	return multiPatchTestRevisionBuilder(base, patches, name)
}

func replacementPatchFixture(t *testing.T, origin, replacement int, oldRevision, newRevision string) (*RealSyncControl, *SyncContext) {
	t.Helper()
	r0 := multiPatchTestRevision("r0", 1, `{"spec":{"a":"0","b":"0"}}`)
	r1 := multiPatchTestRevision("r1", 2, `{"spec":{"a":"1","b":"0"}}`)
	r2 := multiPatchTestRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	controller := &replacementPatchController{syncControlTestController: syncControlTestController{plan: &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, 2), multiPatchTestPatch(t, "p2", `.spec.b`, `"2"`, `"0"`, 3),
	}}}}
	ctx := &SyncContext{CurrentRevision: r0, UpdatedRevision: r2, Revisions: []*appsv1.ControllerRevision{r0, r1, r2}, OwnedIds: map[int]*api.ContextDetail{}, CurrentIDs: sets.NewInt()}
	for _, id := range []int{1, 0} {
		if id == replacement {
			id = 2
		}
		ctx.TargetWrappers = append(ctx.TargetWrappers, replacementWrapper(id, "r0"))
	}
	ctx.TargetWrappers = append(ctx.TargetWrappers, replacementWrapper(origin, oldRevision), replacementWrapper(replacement, newRevision))
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	objects := []client.Object{}
	for _, wrapper := range ctx.TargetWrappers {
		objects = append(objects, wrapper.Object)
		ctx.CurrentIDs.Insert(wrapper.ID)
		ctx.OwnedIds[wrapper.ID] = &api.ContextDetail{ID: wrapper.ID, Data: map[string]string{testContextKey(api.EnumRevisionContextDataKey): wrapper.GetLabels()[appsv1.ControllerRevisionHashLabelKey]}}
	}
	ctx.OwnedIds[origin].Data[testContextKey(api.EnumReplaceNewTargetIDContextDataKey)] = fmt.Sprint(replacement)
	ctx.OwnedIds[replacement].Data[testContextKey(api.EnumReplaceOriginTargetIDContextDataKey)] = fmt.Sprint(origin)
	r := &RealSyncControl{xsetController: controller, xsetLabelAnnoMgr: api.NewXSetLabelAnnotationManager(nil), resourceContextControl: &syncControlTestResourceContexts{}}
	r.Client = fake.NewClientBuilder().WithScheme(scheme).WithObjects(objects...).Build()
	return r, ctx
}

func replacementWrapper(id int, revision string) *TargetWrapper {
	return &TargetWrapper{ID: id, Object: &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("pod-%d", id), Namespace: "default", Labels: map[string]string{appsv1.ControllerRevisionHashLabelKey: revision}}}}
}

func replacementPatchStatus(t *testing.T, ctx *SyncContext, id string) api.PatchRevisionStatus {
	t.Helper()
	for _, status := range ctx.MultiPatch.Status.PatchStatuses {
		if status.ID == id {
			return status
		}
	}
	t.Fatalf("patch %s has no status", id)
	return api.PatchRevisionStatus{}
}
