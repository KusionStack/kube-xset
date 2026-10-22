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
	"encoding/json"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
)

func TestBuildRevisionCatalogReusesHistoryAndSynthesizesMissingStates(t *testing.T) {
	base := testRevision("r0", 1, `{"spec":{"a":"0","b":"0"}}`)
	r1 := testRevision("r1", 2, `{"spec":{"a":"1","b":"0"}}`)
	r2 := testRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		testPatch(t, "p1", `.spec.a`, `"1"`, `"0"`),
		testPatch(t, "p2", `.spec.b`, `"2"`, `"0"`),
	}}

	catalog, err := BuildRevisionCatalog(base, []*appsv1.ControllerRevision{base, r1, r2}, plan, testRevisionBuilder)
	if err != nil {
		t.Fatalf("BuildRevisionCatalog() error = %v", err)
	}
	assertCatalogRevision(t, catalog, nil, "r0")
	assertCatalogRevision(t, catalog, []string{"p1"}, "r1")
	assertCatalogRevision(t, catalog, []string{"p1", "p2"}, "r2")
	if revision, ok := catalog.RevisionFor([]string{"p2"}); !ok || revision.Name == "r0" || revision.Name == "r1" || revision.Name == "r2" {
		t.Fatalf("RevisionFor([p2]) = %#v, %v, want a synthetic revision", revision, ok)
	}

	if got, ok := catalog.PatchIDsForRevision("r1"); !ok || !reflect.DeepEqual(got, []string{"p1"}) {
		t.Fatalf("PatchIDsForRevision(r1) = %#v, %v, want [p1], true", got, ok)
	}
}

func TestBuildRevisionCatalogPrefersNewestEquivalentHistory(t *testing.T) {
	base := testRevision("r0", 1, `{"spec":{"a":"0"}}`)
	revertedHead := testRevision("r3", 4, `{"spec":{"a":"0"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	catalog, err := BuildRevisionCatalog(base, []*appsv1.ControllerRevision{base, revertedHead}, plan, testRevisionBuilder)
	if err != nil {
		t.Fatalf("BuildRevisionCatalog() error = %v", err)
	}
	assertCatalogRevision(t, catalog, nil, "r3")
}

func TestBuildRevisionCatalogMapsEveryEquivalentHistoricalName(t *testing.T) {
	base := testRevision("r0", 1, `{"spec":{"a":"0"}}`)
	newerBase := testRevision("r0-newer", 2, `{"spec":{"a":"0"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches}

	catalog, err := BuildRevisionCatalog(base, []*appsv1.ControllerRevision{base, newerBase}, plan, testRevisionBuilder)
	if err != nil {
		t.Fatalf("BuildRevisionCatalog() error = %v", err)
	}
	assertCatalogRevision(t, catalog, nil, "r0-newer")
	for _, name := range []string{"r0", "r0-newer"} {
		if ids, ok := catalog.PatchIDsForRevision(name); !ok || len(ids) != 0 {
			t.Fatalf("PatchIDsForRevision(%q) = %#v, %v, want empty vector, true", name, ids, ok)
		}
	}
}

func TestBuildRevisionCatalogSyntheticNamesStableAcrossEquivalentBaseNames(t *testing.T) {
	baseA := testRevision("base-from-head-a", 1, `{"spec":{"a":"0","b":"0"}}`)
	baseB := testRevision("base-from-head-b", 2, `{"spec":{"a":"0","b":"0"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		testPatch(t, "p1", `.spec.a`, `"1"`, `"0"`),
		testPatch(t, "p2", `.spec.b`, `"2"`, `"0"`),
	}}

	first, err := BuildRevisionCatalog(baseA, nil, plan, testRevisionBuilder)
	if err != nil {
		t.Fatal(err)
	}
	second, err := BuildRevisionCatalog(baseB, nil, plan, testRevisionBuilder)
	if err != nil {
		t.Fatal(err)
	}
	firstP2, _ := first.RevisionFor([]string{"p2"})
	secondP2, _ := second.RevisionFor([]string{"p2"})
	if firstP2.Name != secondP2.Name {
		t.Fatalf("equivalent Base+P2 names differ: %q vs %q", firstP2.Name, secondP2.Name)
	}
}

func assertCatalogRevision(t *testing.T, catalog *RevisionCatalog, ids []string, wantName string) {
	t.Helper()
	revision, ok := catalog.RevisionFor(ids)
	if !ok || revision.Name != wantName {
		t.Fatalf("RevisionFor(%v) = %#v, %v, want name %q", ids, revision, ok, wantName)
	}
}

func testRevision(name string, revision int64, raw string) *appsv1.ControllerRevision {
	return &appsv1.ControllerRevision{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Revision:   revision,
		Data:       runtime.RawExtension{Raw: []byte(raw)},
	}
}

func testPatch(t *testing.T, id, path, value, reverse string) api.PatchRevision {
	t.Helper()
	forwardRaw, err := json.Marshal(PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: path, Value: json.RawMessage(value)}}})
	if err != nil {
		t.Fatal(err)
	}
	reverseRaw, err := json.Marshal(PatchDocument{Type: SemanticPatchType, Operations: []Operation{{Path: path, Value: json.RawMessage(reverse)}}})
	if err != nil {
		t.Fatal(err)
	}
	partition := int32(0)
	return api.PatchRevision{
		ID: id, SourceResourceVersion: id,
		Patch: runtime.RawExtension{Raw: forwardRaw}, ReversePatch: runtime.RawExtension{Raw: reverseRaw}, FieldSet: []string{path},
		Rollout: api.PatchRolloutStrategy{Type: api.PatchRolloutStrategyTypeByPartition, ByPartition: &api.PatchByPartition{Partition: &partition}},
	}
}

func testRevisionBuilder(base *appsv1.ControllerRevision, patchSet []api.PatchRevision, name string) (*appsv1.ControllerRevision, error) {
	raw := append([]byte(nil), base.Data.Raw...)
	var err error
	for i := range patchSet {
		raw, err = Apply(raw, patchSet[i].Patch)
		if err != nil {
			return nil, err
		}
	}
	result := base.DeepCopy()
	result.Name = name
	result.Data.Raw = raw
	return result, nil
}
