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

package api

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// RevisionMode selects the revision state model exposed by a workload adapter.
type RevisionMode string

const (
	RevisionModeConcurrentPatches RevisionMode = "ConcurrentPatches"
)

// RevisionPlan is XSet's workload-neutral representation of a patch journal.
// Workload API types should be converted explicitly instead of aliasing this type.
type RevisionPlan struct {
	Mode          RevisionMode    `json:"mode,omitempty"`
	ActivePatches []PatchRevision `json:"activePatches,omitempty"`
}

// PatchRevision describes one independently rolled out, immutable patch.
type PatchRevision struct {
	ID                    string               `json:"id"`
	SourceResourceVersion string               `json:"sourceResourceVersion"`
	Patch                 runtime.RawExtension `json:"patch"`
	ReversePatch          runtime.RawExtension `json:"reversePatch"`
	FieldSet              []string             `json:"fieldSet"`
	Rollout               PatchRolloutStrategy `json:"rollout"`
}

type PatchRolloutStrategyType string

const (
	PatchRolloutStrategyTypeByPartition PatchRolloutStrategyType = "ByPartition"
	PatchRolloutStrategyTypeByLabel     PatchRolloutStrategyType = "ByLabel"
)

type PatchTargetState string

const (
	PatchTargetStateApplied PatchTargetState = "Applied"
	PatchTargetStateAbsent  PatchTargetState = "Absent"
)

type PatchNewInstancePolicy string

const (
	PatchNewInstancePolicyApply      PatchNewInstancePolicy = "Apply"
	PatchNewInstancePolicyDoNotApply PatchNewInstancePolicy = "DoNotApply"
)

// PatchRolloutStrategy contains the user-mutable part of a patch revision.
type PatchRolloutStrategy struct {
	TargetState       PatchTargetState         `json:"targetState,omitempty"`
	NewInstancePolicy PatchNewInstancePolicy   `json:"newInstancePolicy,omitempty"`
	Type              PatchRolloutStrategyType `json:"type"`
	ByPartition       *PatchByPartition        `json:"byPartition,omitempty"`
	ByLabel           *PatchByLabel            `json:"byLabel,omitempty"`
	Paused            bool                     `json:"paused,omitempty"`
}

type PatchByPartition struct {
	Partition *int32 `json:"partition,omitempty"`
}

type PatchByLabel struct{}

// MultiPatchRevisionStatus reports the base, head, and per-patch progress.
type MultiPatchRevisionStatus struct {
	BaseRevision  string                `json:"baseRevision,omitempty"`
	HeadRevision  string                `json:"headRevision,omitempty"`
	PatchStatuses []PatchRevisionStatus `json:"patchStatuses,omitempty"`
}

type PatchRevisionStatus struct {
	ID                    string             `json:"id"`
	SelectedReplicas      int32              `json:"selectedReplicas,omitempty"`
	UpdatedReplicas       int32              `json:"updatedReplicas,omitempty"`
	ReadyReplicas         int32              `json:"readyReplicas,omitempty"`
	AvailableReplicas     int32              `json:"availableReplicas,omitempty"`
	CommandLabelKey       string             `json:"commandLabelKey,omitempty"`
	PendingApplyCommands  int32              `json:"pendingApplyCommands,omitempty"`
	PendingRemoveCommands int32              `json:"pendingRemoveCommands,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// Per-patch condition types. Conditions are intentionally orthogonal: for
// example a patch can be Withdrawing and Progressing at the same time.
const (
	PatchConditionProgressing = "Progressing"
	PatchConditionAvailable   = "Available"
	PatchConditionPaused      = "Paused"
	PatchConditionWithdrawing = "Withdrawing"
	PatchConditionDegraded    = "Degraded"
)
