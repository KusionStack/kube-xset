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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"kusionstack.io/kube-xset/api"
	controllerMetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	multiPatchActive = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kube_xset_multipatch_active_patches",
		Help: "Number of active concurrent patches on an XSet workload.",
	}, []string{"kind", "namespace", "name"})
	multiPatchSelected      = newPatchGauge("selected_replicas", "Number of instances selected by a patch rollout.")
	multiPatchUpdated       = newPatchGauge("updated_replicas", "Number of instances currently carrying a patch.")
	multiPatchReady         = newPatchGauge("ready_replicas", "Number of ready instances currently carrying a patch.")
	multiPatchAvailable     = newPatchGauge("available_replicas", "Number of available instances currently carrying a patch.")
	multiPatchPendingApply  = newPatchGauge("pending_apply_commands", "Number of unconsumed ByLabel apply commands for a patch.")
	multiPatchPendingRemove = newPatchGauge("pending_remove_commands", "Number of unconsumed ByLabel remove commands for a patch.")
	multiPatchCompactions   = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "kube_xset_multipatch_compactions_total",
		Help: "Number of completed patch journal entries compacted by workload kind.",
	}, []string{"kind"})

	metricState = struct {
		sync.Mutex
		patches map[objectKey]map[string]struct{}
	}{patches: make(map[objectKey]map[string]struct{})}
)

type objectKey struct {
	kind      string
	namespace string
	name      string
}

func init() {
	controllerMetrics.Registry.MustRegister(
		multiPatchActive,
		multiPatchSelected,
		multiPatchUpdated,
		multiPatchReady,
		multiPatchAvailable,
		multiPatchPendingApply,
		multiPatchPendingRemove,
		multiPatchCompactions,
	)
}

func newPatchGauge(suffix, help string) *prometheus.GaugeVec {
	return prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "kube_xset_multipatch_" + suffix,
		Help: help,
	}, []string{"kind", "namespace", "name", "patch_id"})
}

// ObserveMultiPatch replaces all metric series for one workload. Replacing,
// rather than only setting current patches, prevents completed patch IDs from
// leaking as stale Prometheus series after journal compaction.
func ObserveMultiPatch(kind, namespace, name string, plan *api.RevisionPlan, status *api.MultiPatchRevisionStatus) {
	key := objectKey{kind: kind, namespace: namespace, name: name}
	current := make(map[string]struct{})
	statusByID := make(map[string]api.PatchRevisionStatus)
	if status != nil {
		for i := range status.PatchStatuses {
			statusByID[status.PatchStatuses[i].ID] = status.PatchStatuses[i]
		}
	}
	active := 0
	if plan != nil && plan.Mode == api.RevisionModeConcurrentPatches {
		active = len(plan.ActivePatches)
		for i := range plan.ActivePatches {
			id := plan.ActivePatches[i].ID
			current[id] = struct{}{}
			setPatchGauges(kind, namespace, name, id, statusByID[id])
		}
	}
	multiPatchActive.WithLabelValues(kind, namespace, name).Set(float64(active))

	metricState.Lock()
	defer metricState.Unlock()
	for id := range metricState.patches[key] {
		if _, exists := current[id]; !exists {
			deletePatchGauges(kind, namespace, name, id)
		}
	}
	if len(current) == 0 {
		delete(metricState.patches, key)
	} else {
		metricState.patches[key] = current
	}
}

func ForgetMultiPatch(kind, namespace, name string) {
	key := objectKey{kind: kind, namespace: namespace, name: name}
	metricState.Lock()
	defer metricState.Unlock()
	for id := range metricState.patches[key] {
		deletePatchGauges(kind, namespace, name, id)
	}
	delete(metricState.patches, key)
	multiPatchActive.DeleteLabelValues(kind, namespace, name)
}

func RecordMultiPatchCompaction(kind string, count int) {
	if count > 0 {
		multiPatchCompactions.WithLabelValues(kind).Add(float64(count))
	}
}

func setPatchGauges(kind, namespace, name, id string, status api.PatchRevisionStatus) {
	labels := []string{kind, namespace, name, id}
	multiPatchSelected.WithLabelValues(labels...).Set(float64(status.SelectedReplicas))
	multiPatchUpdated.WithLabelValues(labels...).Set(float64(status.UpdatedReplicas))
	multiPatchReady.WithLabelValues(labels...).Set(float64(status.ReadyReplicas))
	multiPatchAvailable.WithLabelValues(labels...).Set(float64(status.AvailableReplicas))
	multiPatchPendingApply.WithLabelValues(labels...).Set(float64(status.PendingApplyCommands))
	multiPatchPendingRemove.WithLabelValues(labels...).Set(float64(status.PendingRemoveCommands))
}

func deletePatchGauges(kind, namespace, name, id string) {
	labels := []string{kind, namespace, name, id}
	multiPatchSelected.DeleteLabelValues(labels...)
	multiPatchUpdated.DeleteLabelValues(labels...)
	multiPatchReady.DeleteLabelValues(labels...)
	multiPatchAvailable.DeleteLabelValues(labels...)
	multiPatchPendingApply.DeleteLabelValues(labels...)
	multiPatchPendingRemove.DeleteLabelValues(labels...)
}
