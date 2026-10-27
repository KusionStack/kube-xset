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

package xset

import (
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/client-go/tools/record"
	"kusionstack.io/kube-utils/controller/mixin"

	"kusionstack.io/kube-xset/api"
)

func TestEmitMultiPatchConditionEventsOnlyOnTrueTransition(t *testing.T) {
	recorder := record.NewFakeRecorder(10)
	reconciler := &xSetCommonReconciler{ReconcilerMixin: mixin.ReconcilerMixin{Recorder: recorder}}
	instance := &unstructured.Unstructured{Object: map[string]interface{}{
		"apiVersion": "example.io/v1", "kind": "DemoSet",
		"metadata": map[string]interface{}{"name": "demo", "namespace": "default"},
	}}
	available := metav1.Condition{Type: api.PatchConditionAvailable, Status: metav1.ConditionTrue, Reason: "Ready", Message: "all selected instances are available"}
	newStatus := &api.MultiPatchRevisionStatus{PatchStatuses: []api.PatchRevisionStatus{{ID: "p1", Conditions: []metav1.Condition{available}}}}

	reconciler.emitMultiPatchConditionEvents(instance, nil, newStatus)
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "PatchAvailable") || !strings.Contains(event, "patch p1") {
			t.Fatalf("unexpected event: %s", event)
		}
	case <-time.After(time.Second):
		t.Fatal("expected PatchAvailable event")
	}

	// Re-observing the same True condition must not emit an event on every
	// reconcile, otherwise a stable rollout would flood the Event API.
	reconciler.emitMultiPatchConditionEvents(instance, newStatus, newStatus)
	select {
	case event := <-recorder.Events:
		t.Fatalf("unexpected repeated event: %s", event)
	case <-time.After(100 * time.Millisecond):
	}
}
