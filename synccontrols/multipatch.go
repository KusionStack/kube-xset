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
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"kusionstack.io/kube-xset/api"
	"kusionstack.io/kube-xset/features"
	xsetpatches "kusionstack.io/kube-xset/patches"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

var ErrConcurrentPatchRolloutDisabled = errors.New("ConcurrentPatchRollout feature gate is disabled")

type multiPatchIdentity struct {
	ID                 int
	RevisionName       string
	Labels             map[string]string
	New                bool
	Placeholder        bool
	ReplaceOriginID    *int
	IntentRevisionName string
	IntentPatchIDs     []string
}

// MultiPatchSyncContext is the per-reconcile execution model. It translates
// patch vectors into full revisions so the existing update implementations can
// remain unaware of patch composition.
type MultiPatchSyncContext struct {
	Plan             *api.RevisionPlan
	Status           *api.MultiPatchRevisionStatus
	Catalog          *xsetpatches.RevisionCatalog
	InstancePlans    map[int]xsetpatches.InstancePlan
	CurrentPatchIDs  map[int][]string
	DesiredRevisions map[int]*appsv1.ControllerRevision
	// LogicalIDs groups physical replacement peers without hiding their own revisions.
	LogicalIDs map[int]int
	Generation int64
	Suspended  bool
	SyncError  error
}

func (r *RealSyncControl) prepareMultiPatch(ctx context.Context, xsetObject api.XSetObject, syncContext *SyncContext) error {
	operation, supported := r.xsetController.(api.MultiPatchXSetOperation)
	if !supported {
		return nil
	}
	plan := operation.GetRevisionPlan(xsetObject)
	if plan == nil || plan.Mode == "" {
		return nil
	}
	if plan.Mode != api.RevisionModeConcurrentPatches {
		return fmt.Errorf("unsupported revision mode %q", plan.Mode)
	}

	wrappersByID := make(map[int]*TargetWrapper, len(syncContext.TargetWrappers))
	for _, wrapper := range syncContext.TargetWrappers {
		wrappersByID[wrapper.ID] = wrapper
	}
	identities, err := r.multiPatchIdentities(syncContext, wrappersByID, string(xsetObject.GetUID()))
	if err != nil {
		return err
	}

	state, err := buildMultiPatchExecutionState(
		plan,
		operation.GetMultiPatchStatus(xsetObject),
		syncContext.CurrentRevision,
		syncContext.UpdatedRevision,
		syncContext.Revisions,
		identities,
		operation.BuildMultiPatchRevision,
		operation.MultiPatchCommandLabelPrefix(),
		r.xsetLabelAnnoMgr.Value(api.XSetUpdateIndicationLabelKey),
	)
	if err != nil {
		return err
	}
	state.Generation = xsetObject.GetGeneration()
	if !features.DefaultFeatureGate.Enabled(features.ConcurrentPatchRollout) {
		state.Suspended = true
		state.SyncError = ErrConcurrentPatchRolloutDisabled
		syncContext.MultiPatch = state
		return ErrConcurrentPatchRolloutDisabled
	}

	knownRevisions := make(map[string]struct{}, len(syncContext.Revisions))
	for _, revision := range syncContext.Revisions {
		knownRevisions[revision.Name] = struct{}{}
	}
	for _, revision := range state.Catalog.SyntheticRevisions() {
		if _, exists := knownRevisions[revision.Name]; exists {
			continue
		}
		syncContext.Revisions = append(syncContext.Revisions, revision)
		knownRevisions[revision.Name] = struct{}{}
	}

	contextChanged := false
	for id, desiredRevision := range state.DesiredRevisions {
		if wrappersByID[id] != nil {
			continue
		}
		detail := syncContext.OwnedIds[id]
		if !r.resourceContextControl.Contains(detail, api.EnumRevisionContextDataKey, desiredRevision.Name) {
			r.resourceContextControl.Put(detail, api.EnumRevisionContextDataKey, desiredRevision.Name)
			contextChanged = true
		}
	}
	if contextChanged {
		if err := r.resourceContextControl.UpdateToTargetContext(ctx, xsetObject, syncContext.OwnedIds); err != nil {
			return fmt.Errorf("persist new identity patch vectors: %w", err)
		}
	}

	syncContext.MultiPatch = state
	return r.publishPatchStates(ctx, xsetObject, syncContext)
}

func buildMultiPatchExecutionState(
	plan *api.RevisionPlan,
	status *api.MultiPatchRevisionStatus,
	_, updatedRevision *appsv1.ControllerRevision,
	history []*appsv1.ControllerRevision,
	identities []multiPatchIdentity,
	builder xsetpatches.RevisionBuilder,
	commandLabelPrefix, legacyCommandLabel string,
) (*MultiPatchSyncContext, error) {
	if plan == nil || plan.Mode != api.RevisionModeConcurrentPatches {
		return nil, fmt.Errorf("ConcurrentPatches revision plan is required")
	}
	if updatedRevision == nil {
		return nil, fmt.Errorf("updated revision must not be nil")
	}

	resultStatus := copyMultiPatchStatus(status)
	base, err := reconstructMultiPatchBase(updatedRevision, plan, builder)
	if err != nil {
		return nil, err
	}

	catalog, err := xsetpatches.BuildRevisionCatalog(base, history, plan, builder)
	if err != nil {
		return nil, err
	}
	resolvedBase, found := catalog.RevisionFor(nil)
	if !found {
		return nil, fmt.Errorf("base revision not found")
	}
	resultStatus.BaseRevision = resolvedBase.Name
	currentByID := make(map[int][]string, len(identities))
	intendedByID := make(map[int][]string)
	replacementIDs := make(map[int]bool)
	activePatchIDs := make(map[string]bool)
	for _, identity := range identities {
		if identity.ReplaceOriginID != nil {
			replacementIDs[identity.ID], replacementIDs[*identity.ReplaceOriginID] = true, true
		}
	}
	for _, patch := range plan.ActivePatches {
		activePatchIDs[patch.ID] = true
	}
	for _, identity := range identities {
		var currentPatchIDs []string
		if identity.RevisionName != "" {
			var found bool
			currentPatchIDs, found = catalog.PatchIDsForRevision(identity.RevisionName)
			if !found && !(identity.Placeholder && replacementIDs[identity.ID]) {
				return nil, fmt.Errorf("identity %d references unknown revision %q", identity.ID, identity.RevisionName)
			}
		} else if !identity.New {
			return nil, fmt.Errorf("identity %d has no current revision", identity.ID)
		}
		currentByID[identity.ID] = append([]string(nil), currentPatchIDs...)
		if identity.IntentRevisionName != "" {
			ids, found := catalog.PatchIDsForRevision(identity.IntentRevisionName)
			if identity.IntentPatchIDs != nil {
				// Completed patches may have folded into Base since this intent
				// was issued. The vector, unlike a historical revision alias,
				// survives GC and is projected onto the current active journal.
				ids = []string{}
				for _, id := range identity.IntentPatchIDs {
					if activePatchIDs[id] {
						ids = append(ids, id)
					}
				}
				found = true
			}
			// Old controllers stored only a revision label. If that alias was
			// collected, derive a fresh target from actual members + commands;
			// never fall back to the workload's full Head.
			if found {
				intendedByID[identity.ID] = ids
			}
		}
	}
	instanceStates, logicalIDs, err := logicalMultiPatchInstances(identities, currentByID, intendedByID, commandLabelPrefix, legacyCommandLabel)
	if err != nil {
		return nil, err
	}

	planned, err := xsetpatches.PlanInstances(plan, instanceStates, xsetpatches.PlanOptions{
		CommandLabelPrefix: commandLabelPrefix,
		LegacyCommandLabel: legacyCommandLabel,
		BaseRevision:       base.Name,
	})
	if err != nil {
		return nil, err
	}
	logicalPlans := make(map[int]xsetpatches.InstancePlan, len(planned))
	for i := range planned {
		logicalPlans[planned[i].ID] = planned[i]
	}
	instancePlans := make(map[int]xsetpatches.InstancePlan, len(identities))
	desiredRevisions := make(map[int]*appsv1.ControllerRevision, len(identities))
	for _, identity := range identities {
		item := logicalPlans[logicalIDs[identity.ID]]
		item.ID = identity.ID
		item.CurrentPatchIDs = currentByID[identity.ID]
		// A peer must never consume a command observed on the other object.
		snapshot := make(map[string]string)
		for key, value := range item.CommandSnapshot {
			if observed, exists := identity.Labels[key]; exists && observed == value {
				snapshot[key] = value
			}
		}
		item.CommandSnapshot = snapshot
		revision, found := catalog.RevisionFor(item.DesiredPatchIDs)
		if !found {
			return nil, fmt.Errorf("desired revision for identity %d patches %v not found", item.ID, item.DesiredPatchIDs)
		}
		instancePlans[item.ID] = item
		desiredRevisions[item.ID] = revision
	}
	head, found := catalog.RevisionFor(xsetpatches.DesiredPatchIDs(plan))
	if !found {
		return nil, fmt.Errorf("head revision not found")
	}
	resultStatus.HeadRevision = head.Name

	return &MultiPatchSyncContext{
		Plan:             plan,
		Status:           resultStatus,
		Catalog:          catalog,
		InstancePlans:    instancePlans,
		CurrentPatchIDs:  currentByID,
		DesiredRevisions: desiredRevisions,
		LogicalIDs:       logicalIDs,
	}, nil
}

// reconstructMultiPatchBase derives Base from the authoritative Head template
// on every reconcile. That makes reconciliation resilient even if an old
// ControllerRevision has already been garbage-collected: reversing the active
// desired patches is sufficient because their FieldSets are non-overlapping.
func reconstructMultiPatchBase(head *appsv1.ControllerRevision, plan *api.RevisionPlan, builder xsetpatches.RevisionBuilder) (*appsv1.ControllerRevision, error) {
	ids := xsetpatches.DesiredPatchIDs(plan)
	byID := make(map[string]api.PatchRevision, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		byID[plan.ActivePatches[i].ID] = plan.ActivePatches[i]
	}
	reverseSet := make([]api.PatchRevision, 0, len(ids))
	for _, id := range ids {
		patch := byID[id]
		patch.Patch, patch.ReversePatch = patch.ReversePatch, patch.Patch
		reverseSet = append(reverseSet, patch)
	}
	base, err := builder(head, reverseSet, xsetpatches.StateKey(head.Name, append([]string{"base"}, ids...)))
	if err != nil {
		return nil, fmt.Errorf("reconstruct base revision from head: %w", err)
	}
	if base == nil {
		return nil, fmt.Errorf("revision builder returned nil while reconstructing base")
	}
	return base, nil
}

func copyMultiPatchStatus(in *api.MultiPatchRevisionStatus) *api.MultiPatchRevisionStatus {
	if in == nil {
		return &api.MultiPatchRevisionStatus{}
	}
	out := &api.MultiPatchRevisionStatus{BaseRevision: in.BaseRevision, HeadRevision: in.HeadRevision}
	if in.PatchStatuses != nil {
		out.PatchStatuses = make([]api.PatchRevisionStatus, len(in.PatchStatuses))
		copy(out.PatchStatuses, in.PatchStatuses)
		for i := range out.PatchStatuses {
			out.PatchStatuses[i].Conditions = append([]metav1.Condition(nil), in.PatchStatuses[i].Conditions...)
		}
	}
	return out
}

func (r *RealSyncControl) calculateMultiPatchStatus(syncContext *SyncContext) {
	state := syncContext.MultiPatch
	if state == nil {
		return
	}
	statuses := make(map[string]*api.PatchRevisionStatus, len(state.Plan.ActivePatches))
	oldStatuses := make(map[string]api.PatchRevisionStatus, len(state.Status.PatchStatuses))
	for i := range state.Status.PatchStatuses {
		oldStatuses[state.Status.PatchStatuses[i].ID] = state.Status.PatchStatuses[i]
	}
	for i := range state.Plan.ActivePatches {
		patch := &state.Plan.ActivePatches[i]
		status := &api.PatchRevisionStatus{ID: patch.ID}
		status.Conditions = append([]metav1.Condition(nil), oldStatuses[patch.ID].Conditions...)
		if patch.Rollout.Type == api.PatchRolloutStrategyTypeByLabel {
			status.CommandLabelKey = r.xsetController.(api.MultiPatchXSetOperation).MultiPatchCommandLabelPrefix() + patch.ID
		}
		statuses[patch.ID] = status
	}
	for patchID, status := range statuses {
		r.calculateLogicalPatchReplicas(syncContext, patchID, status)
	}
	ids := make([]string, 0, len(statuses))
	for id := range statuses {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	state.Status.PatchStatuses = make([]api.PatchRevisionStatus, 0, len(ids))
	for _, id := range ids {
		status := statuses[id]
		patch := findPatchByID(state.Plan, id)
		setMultiPatchConditions(patch, status, state)
		state.Status.PatchStatuses = append(state.Status.PatchStatuses, *status)
	}
}

func findPatchByID(plan *api.RevisionPlan, id string) *api.PatchRevision {
	if plan == nil {
		return nil
	}
	for i := range plan.ActivePatches {
		if plan.ActivePatches[i].ID == id {
			return &plan.ActivePatches[i]
		}
	}
	return nil
}

func setMultiPatchConditions(patch *api.PatchRevision, status *api.PatchRevisionStatus, state *MultiPatchSyncContext) {
	if patch == nil || status == nil || state == nil {
		return
	}
	pendingCommands := status.PendingApplyCommands + status.PendingRemoveCommands
	complete := status.UpdatedReplicas == status.SelectedReplicas && pendingCommands == 0
	progressing := !patch.Rollout.Paused && !state.Suspended && !complete
	rollout := xsetpatches.DefaultRollout(patch.Rollout)
	available := rollout.TargetState != api.PatchTargetStateAbsent && complete && status.AvailableReplicas == status.SelectedReplicas
	progressReason := conditionReason(progressing, "RolloutInProgress", "RolloutComplete")
	progressMessage := conditionMessage(progressing, "patch rollout is progressing", "patch rollout reached its selected instances")
	if !complete && !progressing {
		progressReason = "RolloutPaused"
		progressMessage = "patch rollout has remaining work but is paused"
	}
	setPatchCondition(status, state.Generation, api.PatchConditionProgressing, progressing, progressReason, progressMessage)
	setPatchCondition(status, state.Generation, api.PatchConditionAvailable, available,
		conditionReason(available, "SelectedInstancesAvailable", "SelectedInstancesUnavailable"),
		conditionMessage(available, "all selected instances are available", "not all selected instances are available"))
	paused := rollout.Paused || state.Suspended
	pausedReason := "RolloutRunning"
	pausedMessage := "patch rollout is not paused"
	if rollout.Paused {
		pausedReason = "RolloutPaused"
		pausedMessage = "patch rollout is paused by its rollout policy"
	} else if state.Suspended {
		pausedReason = "FeatureGateDisabled"
		pausedMessage = ErrConcurrentPatchRolloutDisabled.Error()
	}
	setPatchCondition(status, state.Generation, api.PatchConditionPaused, paused, pausedReason, pausedMessage)
	withdrawing := rollout.TargetState == api.PatchTargetStateAbsent
	setPatchCondition(status, state.Generation, api.PatchConditionWithdrawing, withdrawing,
		conditionReason(withdrawing, "TargetStateAbsent", "TargetStateApplied"),
		conditionMessage(withdrawing, "patch is being removed from instances", "patch target state is applied"))
	degraded := state.SyncError != nil
	degradedReason := "ReconcileSucceeded"
	degradedMessage := "patch reconciliation is healthy"
	if degraded {
		degradedReason = "ReconcileFailed"
		degradedMessage = state.SyncError.Error()
		if errors.Is(state.SyncError, ErrConcurrentPatchRolloutDisabled) {
			degradedReason = "FeatureGateDisabled"
		}
	}
	setPatchCondition(status, state.Generation, api.PatchConditionDegraded, degraded, degradedReason, degradedMessage)
}

func setPatchCondition(status *api.PatchRevisionStatus, generation int64, conditionType string, truth bool, reason, message string) {
	conditionStatus := metav1.ConditionFalse
	if truth {
		conditionStatus = metav1.ConditionTrue
	}
	apiMeta.SetStatusCondition(&status.Conditions, metav1.Condition{
		Type:               conditionType,
		Status:             conditionStatus,
		ObservedGeneration: generation,
		Reason:             reason,
		Message:            message,
	})
}

func conditionReason(truth bool, trueReason, falseReason string) string {
	if truth {
		return trueReason
	}
	return falseReason
}

func conditionMessage(truth bool, trueMessage, falseMessage string) string {
	if truth {
		return trueMessage
	}
	return falseMessage
}

func containsPatchID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

func buildCommandCleanupPatch(snapshot map[string]string) ([]byte, error) {
	keys := make([]string, 0, len(snapshot))
	for key := range snapshot {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	operations := make([]map[string]interface{}, 0, len(keys)*2)
	for _, key := range keys {
		path := "/metadata/labels/" + escapeJSONPointerToken(key)
		operations = append(operations,
			map[string]interface{}{"op": "test", "path": path, "value": snapshot[key]},
			map[string]interface{}{"op": "remove", "path": path},
		)
	}
	return json.Marshal(operations)
}

func escapeJSONPointerToken(value string) string {
	value = strings.ReplaceAll(value, "~", "~0")
	return strings.ReplaceAll(value, "/", "~1")
}

func (r *RealSyncControl) clearConsumedMultiPatchCommands(ctx context.Context, targetInfo *TargetUpdateInfo) (bool, error) {
	if len(targetInfo.CommandSnapshot) == 0 {
		return false, nil
	}
	patch, err := buildCommandCleanupPatch(targetInfo.CommandSnapshot)
	if err != nil {
		return false, err
	}
	if err := r.xControl.PatchTarget(ctx, targetInfo.Object, client.RawPatch(types.JSONPatchType, patch)); err != nil {
		return false, fmt.Errorf("clear consumed multi-patch commands from %s/%s: %w", targetInfo.GetNamespace(), targetInfo.GetName(), err)
	}
	return true, nil
}
