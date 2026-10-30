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
	"fmt"
	"reflect"
	"sort"

	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
)

// DefaultRollout returns a copy with API defaults applied. It does not mutate
// the caller's value.
func DefaultRollout(rollout api.PatchRolloutStrategy) api.PatchRolloutStrategy {
	if rollout.TargetState == "" {
		rollout.TargetState = api.PatchTargetStateApplied
	}
	if rollout.NewInstancePolicy == "" {
		rollout.NewInstancePolicy = api.PatchNewInstancePolicyApply
	}
	return rollout
}

// DesiredPatchIDs returns the canonical, order-independent set of patches that
// belong to the workload Head. Withdrawing (Absent) patches remain in the
// journal but are deliberately excluded.
func DesiredPatchIDs(plan *api.RevisionPlan) []string {
	if plan == nil {
		return nil
	}
	ids := make([]string, 0, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		rollout := DefaultRollout(plan.ActivePatches[i].Rollout)
		if rollout.TargetState == api.PatchTargetStateApplied {
			ids = append(ids, plan.ActivePatches[i].ID)
		}
	}
	sort.Strings(ids)
	return ids
}

// CompactCompletedRevisionPlan removes journal entries that no longer carry
// rollout state. A fully withdrawn patch can be removed independently. Applied
// patches use a barrier: every remaining patch must be selected, updated,
// ready, and available on every desired identity before the whole applied set
// is folded into Head/Base together.
//
// The returned plan is a copy; callers may safely compare it with the input.
func CompactCompletedRevisionPlan(plan *api.RevisionPlan, status *api.MultiPatchRevisionStatus, replicas int32) (*api.RevisionPlan, []string) {
	if plan == nil {
		return nil, nil
	}
	result := normalizePlan(plan)
	if result.Mode != api.RevisionModeConcurrentPatches || status == nil {
		return result, nil
	}
	statusByID := make(map[string]api.PatchRevisionStatus, len(status.PatchStatuses))
	duplicateStatus := false
	for i := range status.PatchStatuses {
		if _, exists := statusByID[status.PatchStatuses[i].ID]; exists {
			duplicateStatus = true
		}
		statusByID[status.PatchStatuses[i].ID] = status.PatchStatuses[i]
	}
	if duplicateStatus {
		return result, nil
	}

	removed := make([]string, 0)
	kept := make([]api.PatchRevision, 0, len(result.ActivePatches))
	for i := range result.ActivePatches {
		patch := result.ActivePatches[i]
		rollout := DefaultRollout(patch.Rollout)
		patchStatus, observed := statusByID[patch.ID]
		if rollout.TargetState == api.PatchTargetStateAbsent && observed &&
			patchStatus.SelectedReplicas == 0 &&
			patchStatus.UpdatedReplicas == 0 &&
			patchStatus.ReadyReplicas == 0 && patchStatus.AvailableReplicas == 0 &&
			patchStatus.PendingApplyCommands == 0 && patchStatus.PendingRemoveCommands == 0 {
			removed = append(removed, patch.ID)
			continue
		}
		kept = append(kept, patch)
	}
	result.ActivePatches = kept

	if len(result.ActivePatches) != 0 && replicas >= 0 && allAppliedPatchesAtBarrier(result, statusByID, replicas) {
		for i := range result.ActivePatches {
			removed = append(removed, result.ActivePatches[i].ID)
		}
		result.ActivePatches = nil
	}
	sort.Strings(removed)
	return result, removed
}

// RemoveRevisionPlanPatches returns a copy without the named journal entries.
// Admission uses it to normalize the request-side plan with the same completed
// entries compacted from oldObject before classifying a new template update.
func RemoveRevisionPlanPatches(plan *api.RevisionPlan, ids []string) *api.RevisionPlan {
	if plan == nil || len(ids) == 0 {
		return normalizePlan(plan)
	}
	removed := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		removed[id] = struct{}{}
	}
	result := normalizePlan(plan)
	kept := make([]api.PatchRevision, 0, len(result.ActivePatches))
	for i := range result.ActivePatches {
		if _, drop := removed[result.ActivePatches[i].ID]; !drop {
			kept = append(kept, result.ActivePatches[i])
		}
	}
	result.ActivePatches = kept
	return result
}

// CompactRevisionPlansForCapture applies the same safe compaction to both
// sides of an admission update. oldObject supplies trustworthy rollout status;
// request-side entries with the completed IDs are discarded before immutable
// journal fields are compared or a new template diff is captured.
func CompactRevisionPlansForCapture(oldPlan, currentPlan *api.RevisionPlan, status *api.MultiPatchRevisionStatus, replicas int32) (*api.RevisionPlan, *api.RevisionPlan, []string) {
	compactedOld, removed := CompactCompletedRevisionPlan(oldPlan, status, replicas)
	if len(removed) == 0 {
		return compactedOld, normalizePlan(currentPlan), nil
	}
	// Admission must classify user-owned rollout changes before discarding a
	// completed journal entry. Otherwise a request that cancels, pauses, or
	// moves the partition at the completion barrier would be accepted after
	// silently deleting the very entry needed to apply that transition.
	if rolloutChangedForRemovedPatches(oldPlan, currentPlan, removed) {
		return normalizePlan(oldPlan), normalizePlan(currentPlan), nil
	}
	return compactedOld, RemoveRevisionPlanPatches(currentPlan, removed), removed
}

func rolloutChangedForRemovedPatches(oldPlan, currentPlan *api.RevisionPlan, removed []string) bool {
	if oldPlan == nil || currentPlan == nil {
		return false
	}
	removedSet := make(map[string]struct{}, len(removed))
	for _, id := range removed {
		removedSet[id] = struct{}{}
	}
	oldRollouts := make(map[string]api.PatchRolloutStrategy, len(removed))
	for i := range oldPlan.ActivePatches {
		patch := &oldPlan.ActivePatches[i]
		if _, ok := removedSet[patch.ID]; ok {
			oldRollouts[patch.ID] = DefaultRollout(patch.Rollout)
		}
	}
	seen := make(map[string]struct{}, len(removed))
	for i := range currentPlan.ActivePatches {
		patch := &currentPlan.ActivePatches[i]
		oldRollout, ok := oldRollouts[patch.ID]
		if !ok {
			continue
		}
		if _, duplicate := seen[patch.ID]; duplicate {
			// Preserve malformed input so normal journal validation can reject it.
			return true
		}
		seen[patch.ID] = struct{}{}
		if !reflect.DeepEqual(oldRollout, DefaultRollout(patch.Rollout)) {
			return true
		}
	}
	return false
}

func allAppliedPatchesAtBarrier(plan *api.RevisionPlan, statusByID map[string]api.PatchRevisionStatus, replicas int32) bool {
	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		rollout := DefaultRollout(patch.Rollout)
		if rollout.TargetState != api.PatchTargetStateApplied || rollout.Paused {
			return false
		}
		if rollout.Type == api.PatchRolloutStrategyTypeByPartition &&
			(rollout.ByPartition == nil || (rollout.ByPartition.Partition != nil && *rollout.ByPartition.Partition != 0)) {
			return false
		}
		patchStatus, observed := statusByID[patch.ID]
		if !observed || patchStatus.SelectedReplicas != replicas || patchStatus.UpdatedReplicas != replicas ||
			patchStatus.ReadyReplicas != replicas || patchStatus.AvailableReplicas != replicas ||
			patchStatus.PendingApplyCommands != 0 || patchStatus.PendingRemoveCommands != 0 {
			return false
		}
	}
	return true
}

// ValidateJournalIdentity verifies the workload-local identity constraints
// shared by admission and reconciliation. Patch content validation remains the
// responsibility of the selected patch codec.
func ValidateJournalIdentity(plan *api.RevisionPlan) error {
	if plan == nil {
		return nil
	}
	seenIDs := make(map[string]struct{}, len(plan.ActivePatches))
	seenSources := make(map[string]string, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		if patch.ID == "" {
			return fmt.Errorf("activePatches[%d].id must not be empty", i)
		}
		if _, exists := seenIDs[patch.ID]; exists {
			return fmt.Errorf("duplicate patch id %q", patch.ID)
		}
		seenIDs[patch.ID] = struct{}{}

		if patch.SourceResourceVersion == "" {
			return fmt.Errorf("patch %q sourceResourceVersion must not be empty", patch.ID)
		}
		if owner, exists := seenSources[patch.SourceResourceVersion]; exists && owner != patch.ID {
			return fmt.Errorf("sourceResourceVersion %q is shared by patches %q and %q", patch.SourceResourceVersion, owner, patch.ID)
		}
		seenSources[patch.SourceResourceVersion] = patch.ID
	}
	return nil
}

// ValidateRevisionPlan verifies the workload-neutral invariants required by
// the multi-patch engine. Workload admission may impose additional policy such
// as who is allowed to change system-owned fields.
func ValidateRevisionPlan(plan *api.RevisionPlan) error {
	if plan == nil {
		return nil
	}
	if plan.Mode == "" {
		if len(plan.ActivePatches) != 0 {
			return fmt.Errorf("activePatches requires revision mode %q", api.RevisionModeConcurrentPatches)
		}
		return nil
	}
	if plan.Mode != api.RevisionModeConcurrentPatches {
		return fmt.Errorf("unsupported revision mode %q", plan.Mode)
	}
	if len(plan.ActivePatches) > DefaultMaxActivePatches {
		return fmt.Errorf("active patch count %d exceeds limit %d", len(plan.ActivePatches), DefaultMaxActivePatches)
	}
	if err := ValidateJournalIdentity(plan); err != nil {
		return err
	}

	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		if len(patch.Patch.Raw) == 0 && patch.Patch.Object == nil {
			return fmt.Errorf("patch %q has an empty patch payload", patch.ID)
		}
		if len(patch.ReversePatch.Raw) == 0 && patch.ReversePatch.Object == nil {
			return fmt.Errorf("patch %q has an empty reversePatch payload", patch.ID)
		}
		if err := validateCanonicalFieldSet(patch.ID, patch.FieldSet); err != nil {
			return err
		}
		if err := validatePatchFieldSet(patch.ID, "patch", patch.Patch, patch.FieldSet); err != nil {
			return err
		}
		if err := validatePatchFieldSet(patch.ID, "reversePatch", patch.ReversePatch, patch.FieldSet); err != nil {
			return err
		}
		if err := validateRollout(patch.ID, patch.Rollout); err != nil {
			return err
		}

		for j := 0; j < i; j++ {
			conflict, overlaps, err := FirstFieldSetConflict(plan.ActivePatches[j].FieldSet, patch.FieldSet)
			if err != nil {
				return fmt.Errorf("compare patches %q and %q: %w", plan.ActivePatches[j].ID, patch.ID, err)
			}
			if overlaps {
				return fmt.Errorf("patches %q and %q overlap at %q and %q", plan.ActivePatches[j].ID, patch.ID, conflict.Left, conflict.Right)
			}
		}
	}
	return nil
}

func validateCanonicalFieldSet(patchID string, fieldSet []string) error {
	if len(fieldSet) == 0 {
		return fmt.Errorf("patch %q fieldSet must not be empty", patchID)
	}
	seen := make(map[string]struct{}, len(fieldSet))
	for _, raw := range fieldSet {
		path, err := ParseFieldPath(raw)
		if err != nil {
			return fmt.Errorf("patch %q has invalid field path %q: %w", patchID, raw, err)
		}
		canonical := path.String()
		if canonical != raw {
			return fmt.Errorf("patch %q field path %q is not canonical; use %q", patchID, raw, canonical)
		}
		if _, exists := seen[canonical]; exists {
			return fmt.Errorf("patch %q repeats field path %q", patchID, canonical)
		}
		seen[canonical] = struct{}{}
	}
	for i := range fieldSet {
		left, _ := ParseFieldPath(fieldSet[i])
		for j := 0; j < i; j++ {
			right, _ := ParseFieldPath(fieldSet[j])
			if left.Conflicts(right) {
				return fmt.Errorf("patch %q contains overlapping field paths %q and %q", patchID, fieldSet[j], fieldSet[i])
			}
		}
	}
	return nil
}

func validatePatchFieldSet(patchID, payloadName string, payload runtime.RawExtension, fieldSet []string) error {
	document, err := decodePatch(payload)
	if err != nil {
		return fmt.Errorf("patch %q has invalid %s: %w", patchID, payloadName, err)
	}
	paths := make([]string, 0, len(document.Operations))
	for i := range document.Operations {
		paths = append(paths, document.Operations[i].Path)
	}
	paths, err = CanonicalFieldSet(paths)
	if err != nil {
		return fmt.Errorf("patch %q has invalid %s paths: %w", patchID, payloadName, err)
	}
	canonicalFields, err := CanonicalFieldSet(fieldSet)
	if err != nil {
		return err
	}
	if len(paths) != len(canonicalFields) {
		return fmt.Errorf("patch %q %s does not match fieldSet: paths %v, fieldSet %v", patchID, payloadName, paths, canonicalFields)
	}
	for i := range paths {
		if paths[i] != canonicalFields[i] {
			return fmt.Errorf("patch %q %s does not match fieldSet: paths %v, fieldSet %v", patchID, payloadName, paths, canonicalFields)
		}
	}
	return nil
}

func validateRollout(patchID string, rollout api.PatchRolloutStrategy) error {
	rollout = DefaultRollout(rollout)
	if rollout.TargetState == api.PatchTargetStateAbsent && rollout.Paused {
		return fmt.Errorf("patch %q cannot be paused while targetState is Absent", patchID)
	}
	if rollout.TargetState != api.PatchTargetStateApplied && rollout.TargetState != api.PatchTargetStateAbsent {
		return fmt.Errorf("patch %q has unsupported targetState %q", patchID, rollout.TargetState)
	}
	if rollout.NewInstancePolicy != api.PatchNewInstancePolicyApply && rollout.NewInstancePolicy != api.PatchNewInstancePolicyDoNotApply {
		return fmt.Errorf("patch %q has unsupported newInstancePolicy %q", patchID, rollout.NewInstancePolicy)
	}

	switch rollout.Type {
	case api.PatchRolloutStrategyTypeByPartition:
		if rollout.ByPartition == nil || rollout.ByLabel != nil {
			return fmt.Errorf("patch %q ByPartition rollout requires only byPartition settings", patchID)
		}
		if rollout.ByPartition.Partition != nil && *rollout.ByPartition.Partition < 0 {
			return fmt.Errorf("patch %q partition must not be negative", patchID)
		}
	case api.PatchRolloutStrategyTypeByLabel:
		if rollout.ByLabel == nil || rollout.ByPartition != nil {
			return fmt.Errorf("patch %q ByLabel rollout requires only byLabel settings", patchID)
		}
	default:
		return fmt.Errorf("patch %q has unsupported rollout type %q", patchID, rollout.Type)
	}
	return nil
}

// JournalFieldSetConflicts returns all journal entries that overlap the
// candidate FieldSet. An Absent patch keeps owning its FieldSet until it has
// been fully withdrawn and removed from ActivePatches.
func JournalFieldSetConflicts(candidate []string, journal []api.PatchRevision) ([]string, error) {
	var owners []string
	for i := range journal {
		_, conflict, err := FirstFieldSetConflict(candidate, journal[i].FieldSet)
		if err != nil {
			return nil, fmt.Errorf("compare candidate with patch %q: %w", journal[i].ID, err)
		}
		if conflict {
			owners = append(owners, journal[i].ID)
		}
	}
	sort.Strings(owners)
	return owners, nil
}
