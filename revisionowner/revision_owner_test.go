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

package revisionowner

import (
	"testing"

	"kusionstack.io/kube-xset/api"
)

func TestMultiPatchInUseRevisions(t *testing.T) {
	status := &api.MultiPatchRevisionStatus{
		BaseRevision: "base-r1",
		HeadRevision: "head-r3",
	}

	got := multiPatchInUseRevisions(&api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}, status)
	if !got.HasAll("base-r1", "head-r3") || got.Len() != 2 {
		t.Fatalf("in-use revisions = %v, want base and head", got.List())
	}
}

func TestMultiPatchInUseRevisionsDisabled(t *testing.T) {
	status := &api.MultiPatchRevisionStatus{BaseRevision: "base-r1", HeadRevision: "head-r3"}
	for name, plan := range map[string]*api.RevisionPlan{
		"nil":      nil,
		"disabled": {},
	} {
		t.Run(name, func(t *testing.T) {
			if got := multiPatchInUseRevisions(plan, status); got.Len() != 0 {
				t.Fatalf("in-use revisions = %v, want empty", got.List())
			}
		})
	}
}

func TestMultiPatchInUseRevisionsIgnoresEmptyNames(t *testing.T) {
	got := multiPatchInUseRevisions(
		&api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches},
		&api.MultiPatchRevisionStatus{BaseRevision: "base-r1"},
	)
	if !got.Has("base-r1") || got.Len() != 1 {
		t.Fatalf("in-use revisions = %v, want only base-r1", got.List())
	}
}
