/*
 * Copyright 2024-2025 KusionStack Authors.
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

package subresources

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

func TestSubResourceState(t *testing.T) {
	pvc := &corev1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{Name: "test-pvc"},
	}
	gvk := corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")

	// Mock adapter for testing
	adapter := &mockAdapter{gvk: gvk}

	state := SubResourceState{
		Object:  pvc,
		GVK:     gvk,
		Adapter: adapter,
	}

	if state.Object.GetName() != "test-pvc" {
		t.Errorf("expected name test-pvc, got %s", state.Object.GetName())
	}
	if state.GVK.Kind != "PersistentVolumeClaim" {
		t.Errorf("expected Kind PersistentVolumeClaim, got %s", state.GVK.Kind)
	}
}

type mockAdapter struct {
	gvk                       schema.GroupVersionKind
	recreateWhenTargetReplaced bool
}

func (m *mockAdapter) Meta() schema.GroupVersionKind { return m.gvk }
func (m *mockAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	return nil, nil
}
func (m *mockAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool     { return false }
func (m *mockAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool      { return false }
func (m *mockAdapter) RecreateWhenTargetReplaced(xset api.XSetObject) bool { return m.recreateWhenTargetReplaced }
func (m *mockAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	return nil
}

func TestNewRealSubResourceControl(t *testing.T) {
	// Test with no adapters
	control, err := NewRealSubResourceControl(nil, nil, nil, nil, nil)
	if err != nil {
		t.Errorf("expected no error with nil adapters, got %v", err)
	}
	if control != nil {
		t.Error("expected nil control for nil adapters")
	}
}

func TestRecreateWhenTargetReplaced(t *testing.T) {
	gvk := corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")

	t.Run("adapter with RecreateWhenTargetReplaced=true", func(t *testing.T) {
		adapter := &mockAdapter{gvk: gvk, recreateWhenTargetReplaced: true}
		if !adapter.RecreateWhenTargetReplaced(nil) {
			t.Error("expected RecreateWhenTargetReplaced to return true")
		}
	})

	t.Run("adapter with RecreateWhenTargetReplaced=false", func(t *testing.T) {
		adapter := &mockAdapter{gvk: gvk, recreateWhenTargetReplaced: false}
		if adapter.RecreateWhenTargetReplaced(nil) {
			t.Error("expected RecreateWhenTargetReplaced to return false")
		}
	})
}
