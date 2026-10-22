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
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"kusionstack.io/kube-xset/api"
)

const DefaultMaxActivePatches = 8

type CaptureOptions struct {
	SourceResourceVersion string
	PatchID               string
	OldReplicas           int32
	Replicas              int32
	Schema                Schema
	MaxActivePatches      int
}

type CaptureResult struct {
	Template []byte
	Plan     *api.RevisionPlan
	PatchID  string
	Canceled bool
}

// CaptureUpdate classifies one workload update. System-owned journal fields
// are reconstructed from oldPlan, while user-owned rollout changes are merged
// from currentPlan. The same sourceResourceVersion is an idempotent upsert key
// for Admission IfNeeded reinvocation.
func CaptureUpdate(oldTemplate, currentTemplate []byte, oldPlan, currentPlan *api.RevisionPlan, options CaptureOptions) (*CaptureResult, error) {
	if options.SourceResourceVersion == "" {
		return nil, fmt.Errorf("sourceResourceVersion must not be empty")
	}
	oldPlan = normalizePlan(oldPlan)
	currentPlan = normalizePlan(currentPlan)
	if err := validateModeTransition(oldPlan, currentPlan); err != nil {
		return nil, err
	}
	if oldPlan.Mode == "" && currentPlan.Mode == api.RevisionModeConcurrentPatches && !jsonEqual(oldTemplate, currentTemplate) {
		return nil, fmt.Errorf("cannot enable %q mode in the same update as a template change", api.RevisionModeConcurrentPatches)
	}

	patchID := options.PatchID
	if patchID == "" {
		patchID = PatchIDForSource(options.SourceResourceVersion)
	}
	journal, err := mergeOwnedJournal(oldPlan, currentPlan, options.SourceResourceVersion, patchID)
	if err != nil {
		return nil, err
	}
	resultPlan := &api.RevisionPlan{Mode: currentPlan.Mode, ActivePatches: journal}

	changedTarget, err := findTargetStateTransition(oldPlan, resultPlan)
	if err != nil {
		return nil, err
	}
	if changedTarget != nil {
		return captureExplicitTargetTransition(oldTemplate, currentTemplate, resultPlan, changedTarget)
	}

	forward, reverse, fieldSet, err := BuildPatchPair(oldTemplate, currentTemplate, options.Schema)
	if err != nil {
		return nil, fmt.Errorf("build template patch: %w", err)
	}
	if len(fieldSet) == 0 {
		applyScaleOutPolicy(resultPlan, options.OldReplicas, options.Replicas)
		if err := ValidateRevisionPlan(resultPlan); err != nil {
			return nil, err
		}
		return &CaptureResult{Template: append([]byte(nil), currentTemplate...), Plan: resultPlan}, nil
	}

	owners, err := JournalFieldSetConflicts(fieldSet, resultPlan.ActivePatches)
	if err != nil {
		return nil, err
	}
	if len(owners) != 0 {
		if len(owners) == 1 {
			owner := findPatch(resultPlan, owners[0])
			if owner != nil && DefaultRollout(owner.Rollout).TargetState == api.PatchTargetStateApplied {
				restored, applyErr := Apply(oldTemplate, owner.ReversePatch)
				if applyErr == nil && jsonEqual(restored, currentTemplate) && sameStringSet(fieldSet, owner.FieldSet) {
					markWithdrawing(owner)
					if err := ValidateRevisionPlan(resultPlan); err != nil {
						return nil, err
					}
					return &CaptureResult{
						Template: append([]byte(nil), currentTemplate...),
						Plan:     resultPlan,
						PatchID:  owner.ID,
						Canceled: true,
					}, nil
				}
			}
		}
		return nil, fmt.Errorf("template field conflict with active patches %v", owners)
	}

	limit := options.MaxActivePatches
	if limit <= 0 {
		limit = DefaultMaxActivePatches
	}
	if len(resultPlan.ActivePatches) >= limit {
		return nil, fmt.Errorf("active patch limit %d reached", limit)
	}
	partition := options.Replicas
	entry := api.PatchRevision{
		ID:                    patchID,
		SourceResourceVersion: options.SourceResourceVersion,
		Patch:                 forward,
		ReversePatch:          reverse,
		FieldSet:              fieldSet,
		Rollout: api.PatchRolloutStrategy{
			TargetState:       api.PatchTargetStateApplied,
			NewInstancePolicy: api.PatchNewInstancePolicyApply,
			Type:              api.PatchRolloutStrategyTypeByPartition,
			ByPartition:       &api.PatchByPartition{Partition: &partition},
		},
	}
	resultPlan.ActivePatches = append(resultPlan.ActivePatches, entry)
	if err := ValidateRevisionPlan(resultPlan); err != nil {
		return nil, err
	}
	return &CaptureResult{
		Template: append([]byte(nil), currentTemplate...),
		Plan:     resultPlan,
		PatchID:  patchID,
	}, nil
}

func normalizePlan(plan *api.RevisionPlan) *api.RevisionPlan {
	if plan == nil {
		return &api.RevisionPlan{}
	}
	raw, _ := json.Marshal(plan)
	copy := &api.RevisionPlan{}
	_ = json.Unmarshal(raw, copy)
	return copy
}

func validateModeTransition(oldPlan, currentPlan *api.RevisionPlan) error {
	if oldPlan.Mode == "" {
		if currentPlan.Mode == "" || currentPlan.Mode == api.RevisionModeConcurrentPatches {
			return nil
		}
		return fmt.Errorf("unsupported revision mode %q", currentPlan.Mode)
	}
	if oldPlan.Mode != api.RevisionModeConcurrentPatches {
		return fmt.Errorf("unsupported old revision mode %q", oldPlan.Mode)
	}
	if currentPlan.Mode != oldPlan.Mode {
		return fmt.Errorf("revision mode %q cannot be disabled or changed", oldPlan.Mode)
	}
	return nil
}

func mergeOwnedJournal(oldPlan, currentPlan *api.RevisionPlan, source, patchID string) ([]api.PatchRevision, error) {
	oldByID := make(map[string]api.PatchRevision, len(oldPlan.ActivePatches))
	for i := range oldPlan.ActivePatches {
		oldByID[oldPlan.ActivePatches[i].ID] = oldPlan.ActivePatches[i]
	}
	currentByID := make(map[string]api.PatchRevision, len(currentPlan.ActivePatches))
	for i := range currentPlan.ActivePatches {
		patch := currentPlan.ActivePatches[i]
		if _, exists := currentByID[patch.ID]; exists {
			return nil, fmt.Errorf("duplicate patch id %q", patch.ID)
		}
		currentByID[patch.ID] = patch
	}

	journal := make([]api.PatchRevision, 0, len(oldPlan.ActivePatches))
	for i := range oldPlan.ActivePatches {
		oldPatch := oldPlan.ActivePatches[i]
		currentPatch, exists := currentByID[oldPatch.ID]
		if !exists {
			return nil, fmt.Errorf("system-owned patch %q cannot be removed", oldPatch.ID)
		}
		if !sameSystemFields(oldPatch, currentPatch) {
			return nil, fmt.Errorf("system-owned fields of patch %q cannot be modified", oldPatch.ID)
		}
		oldPatch.Rollout = currentPatch.Rollout
		journal = append(journal, oldPatch)
	}

	for i := range currentPlan.ActivePatches {
		currentPatch := currentPlan.ActivePatches[i]
		if _, existed := oldByID[currentPatch.ID]; existed {
			continue
		}
		// A reinvoked admission request may already contain the entry inserted
		// by the previous invocation. Drop it and deterministically rebuild it.
		if currentPatch.SourceResourceVersion == source && currentPatch.ID == patchID {
			continue
		}
		return nil, fmt.Errorf("system-owned patch %q cannot be added by the caller", currentPatch.ID)
	}
	return journal, nil
}

func sameSystemFields(left, right api.PatchRevision) bool {
	return left.ID == right.ID &&
		left.SourceResourceVersion == right.SourceResourceVersion &&
		reflect.DeepEqual(left.Patch, right.Patch) &&
		reflect.DeepEqual(left.ReversePatch, right.ReversePatch) &&
		reflect.DeepEqual(left.FieldSet, right.FieldSet)
}

func findTargetStateTransition(oldPlan, currentPlan *api.RevisionPlan) (*api.PatchRevision, error) {
	oldByID := make(map[string]api.PatchRevision, len(oldPlan.ActivePatches))
	for i := range oldPlan.ActivePatches {
		oldByID[oldPlan.ActivePatches[i].ID] = oldPlan.ActivePatches[i]
	}
	var changed *api.PatchRevision
	for i := range currentPlan.ActivePatches {
		patch := &currentPlan.ActivePatches[i]
		oldPatch, exists := oldByID[patch.ID]
		if !exists {
			continue
		}
		oldTarget := DefaultRollout(oldPatch.Rollout).TargetState
		newTarget := DefaultRollout(patch.Rollout).TargetState
		if oldTarget == newTarget {
			continue
		}
		if changed != nil {
			return nil, fmt.Errorf("one update may change targetState for only one patch")
		}
		if oldTarget == api.PatchTargetStateAbsent || newTarget != api.PatchTargetStateAbsent {
			return nil, fmt.Errorf("patch %q cannot transition from %q to %q", patch.ID, oldTarget, newTarget)
		}
		changed = patch
	}
	return changed, nil
}

func captureExplicitTargetTransition(oldTemplate, currentTemplate []byte, plan *api.RevisionPlan, patch *api.PatchRevision) (*CaptureResult, error) {
	expected, err := Apply(oldTemplate, patch.ReversePatch)
	if err != nil {
		return nil, fmt.Errorf("apply reverse patch %q: %w", patch.ID, err)
	}
	if !jsonEqual(currentTemplate, oldTemplate) && !jsonEqual(currentTemplate, expected) {
		return nil, fmt.Errorf("targetState transition for patch %q conflicts with unrelated template changes", patch.ID)
	}
	markWithdrawing(patch)
	if err := ValidateRevisionPlan(plan); err != nil {
		return nil, err
	}
	return &CaptureResult{
		Template: expected,
		Plan:     plan,
		PatchID:  patch.ID,
		Canceled: true,
	}, nil
}

func markWithdrawing(patch *api.PatchRevision) {
	patch.Rollout = DefaultRollout(patch.Rollout)
	patch.Rollout.TargetState = api.PatchTargetStateAbsent
	if patch.Rollout.Type == api.PatchRolloutStrategyTypeByPartition {
		zero := int32(0)
		if patch.Rollout.ByPartition == nil {
			patch.Rollout.ByPartition = &api.PatchByPartition{}
		}
		patch.Rollout.ByPartition.Partition = &zero
	}
}

func applyScaleOutPolicy(plan *api.RevisionPlan, oldReplicas, replicas int32) {
	delta := replicas - oldReplicas
	if delta <= 0 {
		return
	}
	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		rollout := DefaultRollout(patch.Rollout)
		if rollout.TargetState != api.PatchTargetStateApplied ||
			rollout.NewInstancePolicy != api.PatchNewInstancePolicyDoNotApply ||
			rollout.Type != api.PatchRolloutStrategyTypeByPartition || rollout.ByPartition == nil {
			continue
		}
		partition := int32(0)
		if rollout.ByPartition.Partition != nil {
			partition = *rollout.ByPartition.Partition
		}
		partition += delta
		if partition > replicas {
			partition = replicas
		}
		rollout.ByPartition.Partition = &partition
		patch.Rollout = rollout
	}
}

func findPatch(plan *api.RevisionPlan, id string) *api.PatchRevision {
	for i := range plan.ActivePatches {
		if plan.ActivePatches[i].ID == id {
			return &plan.ActivePatches[i]
		}
	}
	return nil
}

func PatchIDForSource(source string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(source) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteByte('-')
		}
	}
	clean := strings.Trim(b.String(), "-")
	if clean == "" {
		clean = "rv"
	}
	sum := sha256.Sum256([]byte(source))
	suffix := hex.EncodeToString(sum[:5])
	if len(clean) > 42 {
		clean = strings.Trim(clean[:42], "-")
	}
	return "p-" + clean + "-" + suffix
}

func jsonEqual(left, right []byte) bool {
	leftValue, leftErr := decodeJSON(left)
	rightValue, rightErr := decodeJSON(right)
	return leftErr == nil && rightErr == nil && reflect.DeepEqual(leftValue, rightValue)
}

func sameStringSet(left, right []string) bool {
	left = append([]string(nil), left...)
	right = append([]string(nil), right...)
	sort.Strings(left)
	sort.Strings(right)
	return reflect.DeepEqual(left, right)
}
