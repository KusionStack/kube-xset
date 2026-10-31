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
	"fmt"
	"sort"
	"strings"

	"kusionstack.io/kube-xset/api"
)

const (
	CommandApply  = "apply"
	CommandRemove = "remove"
)

type InstanceState struct {
	ID              int
	AppliedPatchIDs []string
	Labels          map[string]string
	// New means this identity has just been allocated and has no persisted
	// patch vector yet. NewInstancePolicy is evaluated only in this state.
	New bool
}

type PlanOptions struct {
	CommandLabelPrefix string
	LegacyCommandLabel string
	BaseRevision       string
}

type InstancePlan struct {
	ID              int
	CurrentPatchIDs []string
	DesiredPatchIDs []string
	CommandSnapshot map[string]string
	StateKey        string
}

// PlanInstances computes one complete desired patch vector per identity. All
// commands on one identity are merged before a physical update is performed.
func PlanInstances(plan *api.RevisionPlan, instances []InstanceState, options PlanOptions) ([]InstancePlan, error) {
	if err := ValidateRevisionPlanForPlanning(plan); err != nil {
		return nil, err
	}
	if plan == nil {
		plan = &api.RevisionPlan{}
	}
	seenInstanceIDs := make(map[int]struct{}, len(instances))
	for i := range instances {
		if _, exists := seenInstanceIDs[instances[i].ID]; exists {
			return nil, fmt.Errorf("duplicate instance id %d", instances[i].ID)
		}
		seenInstanceIDs[instances[i].ID] = struct{}{}
	}
	patchByID := make(map[string]*api.PatchRevision, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		patchByID[plan.ActivePatches[i].ID] = &plan.ActivePatches[i]
	}
	partitionMembership := buildPartitionMembership(plan, instances)

	result := make([]InstancePlan, 0, len(instances))
	for i := range instances {
		instance := instances[i]
		current := canonicalIDs(instance.AppliedPatchIDs)
		desiredSet := make(map[string]struct{}, len(current))
		for _, id := range current {
			if _, exists := patchByID[id]; !exists {
				return nil, fmt.Errorf("instance %d contains unknown patch %q", instance.ID, id)
			}
			desiredSet[id] = struct{}{}
		}

		for patchIndex := range plan.ActivePatches {
			patch := &plan.ActivePatches[patchIndex]
			rollout := DefaultRollout(patch.Rollout)
			if rollout.Paused {
				if instance.New {
					if rollout.TargetState == api.PatchTargetStateApplied && rollout.NewInstancePolicy == api.PatchNewInstancePolicyApply {
						desiredSet[patch.ID] = struct{}{}
					} else {
						delete(desiredSet, patch.ID)
					}
				}
				continue
			}
			switch {
			case rollout.TargetState == api.PatchTargetStateAbsent:
				delete(desiredSet, patch.ID)
			case rollout.Type == api.PatchRolloutStrategyTypeByPartition:
				_, selected := partitionMembership[patch.ID][instance.ID]
				if instance.New {
					selected = rollout.NewInstancePolicy == api.PatchNewInstancePolicyApply
				}
				if selected {
					desiredSet[patch.ID] = struct{}{}
				} else {
					delete(desiredSet, patch.ID)
				}
			case rollout.Type == api.PatchRolloutStrategyTypeByLabel && instance.New:
				if rollout.NewInstancePolicy == api.PatchNewInstancePolicyApply {
					desiredSet[patch.ID] = struct{}{}
				} else {
					delete(desiredSet, patch.ID)
				}
			}
		}

		snapshot, err := applyLabelCommands(instance, patchByID, desiredSet, options)
		if err != nil {
			return nil, err
		}
		desired := setIDs(desiredSet)
		result = append(result, InstancePlan{
			ID:              instance.ID,
			CurrentPatchIDs: current,
			DesiredPatchIDs: desired,
			CommandSnapshot: snapshot,
			StateKey:        StateKey(options.BaseRevision, desired),
		})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

// buildPartitionMembership treats partition as a count of identities that
// remain without the patch, rather than as an identity-number threshold. XSet
// identity numbers may contain holes after scale-in, so comparing ID >=
// partition would silently change the requested old/new ratio.
func buildPartitionMembership(plan *api.RevisionPlan, instances []InstanceState) map[string]map[int]struct{} {
	result := make(map[string]map[int]struct{}, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		rollout := DefaultRollout(patch.Rollout)
		if rollout.TargetState != api.PatchTargetStateApplied || rollout.Type != api.PatchRolloutStrategyTypeByPartition {
			continue
		}
		partition := 0
		if rollout.ByPartition != nil && rollout.ByPartition.Partition != nil {
			partition = int(*rollout.ByPartition.Partition)
		}
		if partition < 0 {
			partition = 0
		}
		if partition > len(instances) {
			partition = len(instances)
		}
		desiredCount := len(instances) - partition
		selected := make(map[int]struct{}, desiredCount)
		var reached, unreached []int
		for j := range instances {
			instance := &instances[j]
			if instance.New {
				if rollout.NewInstancePolicy == api.PatchNewInstancePolicyApply {
					selected[instance.ID] = struct{}{}
				}
				continue
			}
			if containsString(instance.AppliedPatchIDs, patch.ID) {
				reached = append(reached, instance.ID)
			} else {
				unreached = append(unreached, instance.ID)
			}
		}
		// Preserve already reached identities first. Within each class, larger
		// stable IDs are selected first to retain legacy partition behavior and
		// make rollback deterministic when identity numbers contain holes.
		sort.Sort(sort.Reverse(sort.IntSlice(reached)))
		sort.Sort(sort.Reverse(sort.IntSlice(unreached)))
		remaining := desiredCount - len(selected)
		for _, candidates := range [][]int{reached, unreached} {
			for _, id := range candidates {
				if remaining <= 0 {
					break
				}
				selected[id] = struct{}{}
				remaining--
			}
		}
		result[patch.ID] = selected
	}
	return result
}

// ValidateRevisionPlanForPlanning accepts nil/empty plans for callers that
// share code with the legacy path, while retaining full validation for an
// enabled multi-patch plan.
func ValidateRevisionPlanForPlanning(plan *api.RevisionPlan) error {
	if plan == nil {
		return nil
	}
	if plan.Mode == "" && len(plan.ActivePatches) == 0 {
		return nil
	}
	if plan.Mode != api.RevisionModeConcurrentPatches {
		return fmt.Errorf("unsupported revision mode %q", plan.Mode)
	}
	seen := make(map[string]struct{}, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		patch := &plan.ActivePatches[i]
		if patch.ID == "" {
			return fmt.Errorf("activePatches[%d].id must not be empty", i)
		}
		if _, exists := seen[patch.ID]; exists {
			return fmt.Errorf("duplicate patch id %q", patch.ID)
		}
		seen[patch.ID] = struct{}{}
		if err := validateRollout(patch.ID, patch.Rollout); err != nil {
			return err
		}
	}
	return nil
}

func applyLabelCommands(instance InstanceState, patchByID map[string]*api.PatchRevision, desiredSet map[string]struct{}, options PlanOptions) (map[string]string, error) {
	if len(instance.Labels) == 0 {
		return nil, nil
	}
	if options.LegacyCommandLabel != "" {
		if _, exists := instance.Labels[options.LegacyCommandLabel]; exists {
			return nil, fmt.Errorf("instance %d uses legacy update command %q in ConcurrentPatches mode", instance.ID, options.LegacyCommandLabel)
		}
	}
	if options.CommandLabelPrefix == "" {
		return nil, nil
	}
	keys := make([]string, 0)
	for key := range instance.Labels {
		if strings.HasPrefix(key, options.CommandLabelPrefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	snapshot := make(map[string]string, len(keys))
	for _, key := range keys {
		patchID := strings.TrimPrefix(key, options.CommandLabelPrefix)
		patch, exists := patchByID[patchID]
		if !exists {
			return nil, fmt.Errorf("instance %d command %q references unknown patch %q", instance.ID, key, patchID)
		}
		rollout := DefaultRollout(patch.Rollout)
		if rollout.Paused {
			continue
		}
		if rollout.Type != api.PatchRolloutStrategyTypeByLabel {
			return nil, fmt.Errorf("instance %d command %q targets non-ByLabel patch %q", instance.ID, key, patchID)
		}
		command := instance.Labels[key]
		switch command {
		case CommandApply:
			if rollout.TargetState == api.PatchTargetStateAbsent {
				// Withdrawal wins over stale ByLabel commands. Snapshot the
				// command so sync can consume it without re-applying the patch.
				delete(desiredSet, patchID)
				break
			}
			desiredSet[patchID] = struct{}{}
		case CommandRemove:
			delete(desiredSet, patchID)
		default:
			return nil, fmt.Errorf("instance %d has unsupported command %q for patch %q", instance.ID, command, patchID)
		}
		snapshot[key] = command
	}
	if len(snapshot) == 0 {
		return nil, nil
	}
	return snapshot, nil
}

// ClearConsumedCommands removes only command labels whose current value still
// matches the snapshot taken when the operation began.
func ClearConsumedCommands(current, snapshot map[string]string) map[string]string {
	result := make(map[string]string, len(current))
	for key, value := range current {
		result[key] = value
	}
	for key, value := range snapshot {
		if result[key] == value {
			delete(result, key)
		}
	}
	return result
}

// StateKey is an execution identity, not a patch identity or ordering digest.
// It is stable for one Base plus one unordered patch set.
func StateKey(baseRevision string, patchIDs []string) string {
	ids := canonicalIDs(patchIDs)
	sum := sha256.Sum256([]byte(baseRevision + "\x00" + strings.Join(ids, "\x00")))
	return "state-" + hex.EncodeToString(sum[:10])
}

func canonicalIDs(ids []string) []string {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		if id != "" {
			set[id] = struct{}{}
		}
	}
	return setIDs(set)
}

func setIDs(set map[string]struct{}) []string {
	ids := make([]string, 0, len(set))
	for id := range set {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
