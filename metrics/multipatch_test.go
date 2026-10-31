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

package metrics

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"kusionstack.io/kube-xset/api"
)

func TestObserveMultiPatchReplacesAndForgetsSeries(t *testing.T) {
	kind, namespace, name := "DemoSet", "default", "metrics-test"
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{{ID: "p1"}, {ID: "p2"}}}
	status := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{{ID: "p1", SelectedReplicas: 2}, {ID: "p2", UpdatedReplicas: 1}}}
	ObserveMultiPatch(kind, namespace, name, plan, status)
	if got := testutil.ToFloat64(multiPatchActive.WithLabelValues(kind, namespace, name)); got != 2 {
		t.Fatalf("active patches = %v, want 2", got)
	}
	if got := testutil.ToFloat64(multiPatchSelected.WithLabelValues(kind, namespace, name, "p1")); got != 2 {
		t.Fatalf("p1 selected replicas = %v, want 2", got)
	}

	ObserveMultiPatch(kind, namespace, name, &api.RevisionPlan{}, nil)
	if got := testutil.ToFloat64(multiPatchActive.WithLabelValues(kind, namespace, name)); got != 0 {
		t.Fatalf("active patches after mode exit = %v, want 0", got)
	}
	if deleted := multiPatchSelected.DeleteLabelValues(kind, namespace, name, "p1"); deleted {
		t.Fatal("p1 metric still existed after journal compaction")
	}

	ForgetMultiPatch(kind, namespace, name)
	if deleted := multiPatchActive.DeleteLabelValues(kind, namespace, name); deleted {
		t.Fatal("workload metric still existed after ForgetMultiPatch")
	}
}
