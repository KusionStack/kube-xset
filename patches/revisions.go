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
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	"kusionstack.io/kube-xset/api"
)

// RevisionBuilder materializes one workload-specific ControllerRevision from
// the stable base plus an unordered patch set. The returned revision is an
// in-memory execution state and does not need to be persisted.
type RevisionBuilder func(base *appsv1.ControllerRevision, patchSet []api.PatchRevision, name string) (*appsv1.ControllerRevision, error)

// RevisionCatalog maps Base+PatchSet states to full revisions understood by
// the existing XSet update machinery. Existing history is reused whenever its
// payload equals a state; missing combinations receive deterministic virtual
// revision names.
type RevisionCatalog struct {
	byVector  map[string]*appsv1.ControllerRevision
	byName    map[string]*appsv1.ControllerRevision
	patchIDs  map[string][]string
	synthetic []*appsv1.ControllerRevision
}

func BuildRevisionCatalog(base *appsv1.ControllerRevision, history []*appsv1.ControllerRevision, plan *api.RevisionPlan, builder RevisionBuilder) (*RevisionCatalog, error) {
	if base == nil {
		return nil, fmt.Errorf("base revision must not be nil")
	}
	if builder == nil {
		return nil, fmt.Errorf("revision builder must not be nil")
	}
	if err := ValidateRevisionPlanForPlanning(plan); err != nil {
		return nil, err
	}
	if len(plan.ActivePatches) > DefaultMaxActivePatches {
		return nil, fmt.Errorf("active patch count %d exceeds catalog limit %d", len(plan.ActivePatches), DefaultMaxActivePatches)
	}

	patchesByID := make(map[string]api.PatchRevision, len(plan.ActivePatches))
	ids := make([]string, 0, len(plan.ActivePatches))
	for i := range plan.ActivePatches {
		patch := plan.ActivePatches[i]
		patchesByID[patch.ID] = patch
		ids = append(ids, patch.ID)
	}
	sort.Strings(ids)

	catalog := &RevisionCatalog{
		byVector: make(map[string]*appsv1.ControllerRevision, 1<<len(ids)),
		byName:   make(map[string]*appsv1.ControllerRevision, len(history)+(1<<len(ids))),
		patchIDs: make(map[string][]string, len(history)+(1<<len(ids))),
	}
	basePayloadKey, err := revisionPayloadKey(base.Data.Raw)
	if err != nil {
		return nil, fmt.Errorf("identify base revision payload: %w", err)
	}
	for _, revision := range history {
		if revision != nil {
			catalog.byName[revision.Name] = revision
		}
	}

	for mask := 0; mask < 1<<len(ids); mask++ {
		subsetIDs := make([]string, 0, len(ids))
		subset := make([]api.PatchRevision, 0, len(ids))
		for i, id := range ids {
			if mask&(1<<i) == 0 {
				continue
			}
			subsetIDs = append(subsetIDs, id)
			subset = append(subset, patchesByID[id])
		}

		virtualName := StateKey(basePayloadKey, subsetIDs)
		virtual, err := builder(base, subset, virtualName)
		if err != nil {
			return nil, fmt.Errorf("build revision for patches %v: %w", subsetIDs, err)
		}
		if virtual == nil {
			return nil, fmt.Errorf("revision builder returned nil for patches %v", subsetIDs)
		}
		virtual.Name = virtualName
		catalog.synthetic = append(catalog.synthetic, virtual)
		catalog.byName[virtualName] = virtual
		catalog.patchIDs[virtualName] = append([]string(nil), subsetIDs...)

		for _, equivalent := range equivalentRevisions(history, virtual) {
			catalog.patchIDs[equivalent.Name] = append([]string(nil), subsetIDs...)
		}
		chosen := newestEquivalentRevision(history, virtual)
		if chosen == nil {
			chosen = virtual
		}
		key := patchVectorKey(subsetIDs)
		catalog.byVector[key] = chosen
		catalog.byName[chosen.Name] = chosen
		catalog.patchIDs[chosen.Name] = append([]string(nil), subsetIDs...)
	}

	return catalog, nil
}

func revisionPayloadKey(raw []byte) (string, error) {
	value, err := decodeJSON(raw)
	if err != nil {
		return "", err
	}
	canonical, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "payload-" + hex.EncodeToString(sum[:10]), nil
}

func (c *RevisionCatalog) RevisionFor(patchIDs []string) (*appsv1.ControllerRevision, bool) {
	if c == nil {
		return nil, false
	}
	revision, ok := c.byVector[patchVectorKey(patchIDs)]
	return revision, ok
}

func (c *RevisionCatalog) RevisionByName(name string) (*appsv1.ControllerRevision, bool) {
	if c == nil {
		return nil, false
	}
	revision, ok := c.byName[name]
	return revision, ok
}

func (c *RevisionCatalog) PatchIDsForRevision(name string) ([]string, bool) {
	if c == nil {
		return nil, false
	}
	ids, ok := c.patchIDs[name]
	return append([]string(nil), ids...), ok
}

func (c *RevisionCatalog) SyntheticRevisions() []*appsv1.ControllerRevision {
	if c == nil {
		return nil
	}
	return append([]*appsv1.ControllerRevision(nil), c.synthetic...)
}

func equivalentRevisions(history []*appsv1.ControllerRevision, want *appsv1.ControllerRevision) []*appsv1.ControllerRevision {
	result := make([]*appsv1.ControllerRevision, 0)
	for _, candidate := range history {
		if candidate != nil && jsonEqual(candidate.Data.Raw, want.Data.Raw) {
			result = append(result, candidate)
		}
	}
	return result
}

func newestEquivalentRevision(history []*appsv1.ControllerRevision, want *appsv1.ControllerRevision) *appsv1.ControllerRevision {
	var newest *appsv1.ControllerRevision
	for _, candidate := range history {
		if candidate == nil || !jsonEqual(candidate.Data.Raw, want.Data.Raw) {
			continue
		}
		if newest == nil || candidate.Revision > newest.Revision ||
			(candidate.Revision == newest.Revision && candidate.Name > newest.Name) {
			newest = candidate
		}
	}
	return newest
}

func patchVectorKey(ids []string) string {
	return strings.Join(canonicalIDs(ids), "\x00")
}
