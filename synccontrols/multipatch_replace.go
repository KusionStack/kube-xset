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
	"fmt"
	"sort"
	"strconv"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	"kusionstack.io/kube-xset/api"
	xsetpatches "kusionstack.io/kube-xset/patches"
)

const replacementPatchIntentAnnotation = "xset.kusionstack.io/replace-patch-intent"

type replacementPatchIntent struct {
	Version  int      `json:"version"`
	OwnerUID string   `json:"ownerUID"`
	Revision string   `json:"revision"`
	PatchIDs []string `json:"patchIDs"`
}

func (r *RealSyncControl) multiPatchIdentities(ctx *SyncContext, wrappers map[int]*TargetWrapper, ownerUID string) ([]multiPatchIdentity, error) {
	identities := make([]multiPatchIdentity, 0, len(ctx.OwnedIds))
	for id, detail := range ctx.OwnedIds {
		identity := multiPatchIdentity{ID: id, Placeholder: true}
		_, identity.New = r.resourceContextControl.Get(detail, api.EnumJustCreateContextDataKey)
		if wrapper := wrappers[id]; wrapper != nil && !wrapper.PlaceHolder {
			identity.Placeholder = false
			identity.Labels = wrapper.GetLabels()
			identity.RevisionName = identity.Labels[appsv1.ControllerRevisionHashLabelKey]
			identity.IntentRevisionName, _ = r.xsetLabelAnnoMgr.Get(wrapper.Object, api.XReplaceByReplaceUpdateLabelKey)
			if raw := wrapper.GetAnnotations()[replacementPatchIntentAnnotation]; raw != "" && identity.IntentRevisionName != "" {
				var intent replacementPatchIntent
				if err := json.Unmarshal([]byte(raw), &intent); err != nil || intent.Version != 1 || intent.OwnerUID != ownerUID || intent.Revision != identity.IntentRevisionName || intent.PatchIDs == nil {
					return nil, fmt.Errorf("invalid replacement patch intent for identity %d", id)
				}
				identity.IntentPatchIDs = intent.PatchIDs
			}
		}
		if identity.RevisionName == "" {
			identity.RevisionName, _ = r.resourceContextControl.Get(detail, api.EnumRevisionContextDataKey)
		}
		if raw, exists := r.resourceContextControl.Get(detail, api.EnumReplaceOriginTargetIDContextDataKey); exists {
			origin, err := strconv.Atoi(raw)
			if err != nil || origin < 0 || origin == id {
				return nil, fmt.Errorf("invalid replacement origin %q for identity %d", raw, id)
			}
			// A replacement is never genuine scale-out, even while its origin is being reclaimed.
			identity.New = false
			if originDetail := ctx.OwnedIds[origin]; originDetail != nil {
				peer, ok := r.resourceContextControl.Get(originDetail, api.EnumReplaceNewTargetIDContextDataKey)
				if !ok || peer != strconv.Itoa(id) {
					return nil, fmt.Errorf("inconsistent replacement pair %d -> %d", origin, id)
				}
				identity.ReplaceOriginID = &origin
			}
		}
		identities = append(identities, identity)
	}
	return identities, nil
}

// Collapse only execution identities, never the physical applied vectors exposed
// by Inspect. All information comes from persisted contexts, revisions and labels.
func logicalMultiPatchInstances(identities []multiPatchIdentity, current, intended map[int][]string, commandPrefix, legacyCommand string) ([]xsetpatches.InstanceState, map[int]int, error) {
	byID := make(map[int]multiPatchIdentity, len(identities))
	for _, identity := range identities {
		if _, exists := byID[identity.ID]; exists {
			return nil, nil, fmt.Errorf("duplicate instance id %d", identity.ID)
		}
		byID[identity.ID] = identity
	}
	logicalIDs := make(map[int]int, len(identities))
	groups := make(map[int][]multiPatchIdentity)
	for _, identity := range identities {
		logicalID := identity.ID
		if identity.ReplaceOriginID != nil {
			if origin, exists := byID[*identity.ReplaceOriginID]; exists {
				if origin.ReplaceOriginID != nil {
					return nil, nil, fmt.Errorf("nested replacement identity %d", identity.ID)
				}
				logicalID = origin.ID
			}
		}
		logicalIDs[identity.ID] = logicalID
		groups[logicalID] = append(groups[logicalID], identity)
	}
	instances := make([]xsetpatches.InstanceState, 0, len(groups))
	for logicalID, members := range groups {
		if len(members) > 2 {
			return nil, nil, fmt.Errorf("multiple replacement peers for identity %d", logicalID)
		}
		representative := byID[logicalID]
		labels := make(map[string]string)
		for _, member := range members {
			// The new member carries the in-flight vector. Once the old object
			// disappears, its stale context must not undo a ByLabel command.
			if member.ID != logicalID && (!member.Placeholder || representative.Placeholder) {
				representative = member
			}
			for key, value := range member.Labels {
				if !(commandPrefix != "" && strings.HasPrefix(key, commandPrefix)) && !(legacyCommand != "" && key == legacyCommand) {
					continue
				}
				if previous, exists := labels[key]; exists && previous != value {
					return nil, nil, fmt.Errorf("conflicting replacement commands for identity %d: %s", logicalID, key)
				}
				labels[key] = value
			}
		}
		vector := current[representative.ID]
		// Replace already persists its last issued target on the origin. Keep
		// this intent if a command sent to the new peer forces its recreation;
		// otherwise deleting that peer would also delete the only command copy.
		if target, exists := intended[logicalID]; exists {
			vector = target
		}
		instances = append(instances, xsetpatches.InstanceState{ID: logicalID, AppliedPatchIDs: vector, Labels: labels, New: representative.New && len(members) == 1})
	}
	sort.Slice(instances, func(i, j int) bool { return instances[i].ID < instances[j].ID })
	return instances, logicalIDs, nil
}

func (r *RealSyncControl) calculateLogicalPatchReplicas(ctx *SyncContext, patchID string, status *api.PatchRevisionStatus) {
	selected, updated, ready, available := sets.NewInt(), sets.NewInt(), sets.NewInt(), sets.NewInt()
	applyCommands, removeCommands := sets.NewInt(), sets.NewInt()
	state := ctx.MultiPatch
	replacing := make(map[int]bool)
	for physicalID, logicalID := range state.LogicalIDs {
		if physicalID != logicalID {
			replacing[logicalID] = true
		}
	}
	for _, wrapper := range ctx.TargetWrappers {
		if wrapper.PlaceHolder || wrapper.ToExclude {
			continue
		}
		planned, exists := state.InstancePlans[wrapper.ID]
		if !exists {
			continue
		}
		logicalID, grouped := state.LogicalIDs[wrapper.ID]
		if !grouped {
			logicalID = wrapper.ID
		}
		if containsPatchID(planned.DesiredPatchIDs, patchID) {
			selected.Insert(logicalID)
		}
		// Any surviving member still using a patch blocks its withdrawal. This
		// also keeps progress truthful while old and new physical states differ.
		if containsPatchID(state.CurrentPatchIDs[wrapper.ID], patchID) {
			updated.Insert(logicalID)
			// Do not assemble an Available full vector from P1 on the old
			// member and P2 on an obsolete new member. That would let the
			// compaction barrier pass without any correct replacement existing.
			desired := state.DesiredRevisions[wrapper.ID]
			reached := !replacing[logicalID] || (desired != nil && IsTargetUpdatedRevision(wrapper.Object, desired.Name))
			if isReady, _ := r.xsetController.CheckReadyTime(wrapper.Object); isReady && reached {
				ready.Insert(logicalID)
			}
			if reached && r.xsetController.CheckAvailable(wrapper.Object) {
				available.Insert(logicalID)
			}
		}
		switch planned.CommandSnapshot[status.CommandLabelKey] {
		case xsetpatches.CommandApply:
			applyCommands.Insert(logicalID)
		case xsetpatches.CommandRemove:
			removeCommands.Insert(logicalID)
		}
	}
	status.SelectedReplicas = int32(selected.Len())
	status.UpdatedReplicas = int32(updated.Len())
	status.ReadyReplicas = int32(ready.Len())
	status.AvailableReplicas = int32(available.Len())
	status.PendingApplyCommands = int32(applyCommands.Len())
	status.PendingRemoveCommands = int32(removeCommands.Len())
}
