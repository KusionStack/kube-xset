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

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/onsi/gomega"
	"kusionstack.io/kube-xset/api"
)

// mockControllerWithoutAdapters is a controller that implements neither interface
type mockControllerWithoutAdapters struct{}

func (m *mockControllerWithoutAdapters) ControllerName() string { return "mock" }
func (m *mockControllerWithoutAdapters) FinalizerName() string  { return "mock/finalizer" }
func (m *mockControllerWithoutAdapters) XSetMeta() metav1.TypeMeta {
	return metav1.TypeMeta{Kind: "MockSet", APIVersion: "v1"}
}
func (m *mockControllerWithoutAdapters) XMeta() metav1.TypeMeta {
	return metav1.TypeMeta{Kind: "Mock", APIVersion: "v1"}
}
func (m *mockControllerWithoutAdapters) NewXSetObject() api.XSetObject { return nil }
func (m *mockControllerWithoutAdapters) NewXObject() client.Object     { return nil }
func (m *mockControllerWithoutAdapters) NewXObjectList() client.ObjectList {
	return &corev1.PodList{}
}
func (m *mockControllerWithoutAdapters) GetXSetSpec(object api.XSetObject) *api.XSetSpec {
	return nil
}
func (m *mockControllerWithoutAdapters) GetXSetPatch(object metav1.Object) ([]byte, error) {
	return nil, nil
}
func (m *mockControllerWithoutAdapters) GetXSetStatus(object api.XSetObject) *api.XSetStatus {
	return nil
}
func (m *mockControllerWithoutAdapters) SetXSetStatus(object api.XSetObject, status *api.XSetStatus) {
}
func (m *mockControllerWithoutAdapters) UpdateScaleStrategy(ctx context.Context, c client.Client, object api.XSetObject, scaleStrategy *api.ScaleStrategy) error {
	return nil
}
func (m *mockControllerWithoutAdapters) GetXSetTemplatePatcher(object metav1.Object) func(client.Object) error {
	return nil
}
func (m *mockControllerWithoutAdapters) GetXObjectFromRevision(revision *appsv1.ControllerRevision) (client.Object, error) {
	return nil, nil
}
func (m *mockControllerWithoutAdapters) CheckScheduled(object client.Object) bool  { return false }
func (m *mockControllerWithoutAdapters) CheckReadyTime(object client.Object) (bool, *metav1.Time) {
	return false, nil
}
func (m *mockControllerWithoutAdapters) CheckAvailable(object client.Object) bool   { return false }
func (m *mockControllerWithoutAdapters) CheckInactive(object client.Object) bool    { return false }
func (m *mockControllerWithoutAdapters) GetXOpsPriority(ctx context.Context, c client.Client, object client.Object) (*api.OpsPriority, error) {
	return nil, nil
}
func (m *mockControllerWithoutAdapters) GetTargetPrefix(xset api.XSetObject) string {
	return ""
}

// mockControllerWithPvcAdapter implements SubResourcePvcAdapter
type mockControllerWithPvcAdapter struct {
	mockControllerWithoutAdapters
}

func (m *mockControllerWithPvcAdapter) RetainPvcWhenXSetDeleted(object api.XSetObject) bool {
	return true
}
func (m *mockControllerWithPvcAdapter) RetainPvcWhenXSetScaled(object api.XSetObject) bool {
	return false
}
func (m *mockControllerWithPvcAdapter) GetXSetPvcTemplate(object api.XSetObject) []corev1.PersistentVolumeClaim {
	return nil
}
func (m *mockControllerWithPvcAdapter) GetXSpecVolumes(object client.Object) []corev1.Volume {
	return nil
}
func (m *mockControllerWithPvcAdapter) GetXVolumeMounts(object client.Object) []corev1.VolumeMount {
	return nil
}
func (m *mockControllerWithPvcAdapter) SetXSpecVolumes(object client.Object, pvcs []corev1.Volume) {
}

// mockSubResourceAdapter is a mock adapter for testing
type mockSubResourceAdapter struct{}

func (m *mockSubResourceAdapter) Meta() schema.GroupVersionKind {
	return schema.GroupVersionKind{Group: "test", Version: "v1", Kind: "MockResource"}
}
func (m *mockSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	return nil, nil
}
func (m *mockSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool   { return false }
func (m *mockSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool    { return false }
func (m *mockSubResourceAdapter) RecreateWhenXSetUpdated(xset api.XSetObject) bool { return false }
func (m *mockSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	return nil
}

// mockControllerWithAdapterGetter implements SubResourceAdapterGetter
type mockControllerWithAdapterGetter struct {
	mockControllerWithoutAdapters
	adapters []api.SubResourceAdapter
}

func (m *mockControllerWithAdapterGetter) GetSubResourceAdapters() []api.SubResourceAdapter {
	return m.adapters
}

// mockControllerWithBothInterfaces implements both SubResourceAdapterGetter and SubResourcePvcAdapter
type mockControllerWithBothInterfaces struct {
	mockControllerWithPvcAdapter
	adapters []api.SubResourceAdapter
}

func (m *mockControllerWithBothInterfaces) GetSubResourceAdapters() []api.SubResourceAdapter {
	return m.adapters
}

func TestBuildAdapters_NoAdapters(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	controller := &mockControllerWithoutAdapters{}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	g.Expect(result).To(gomega.BeNil())
}

func TestBuildAdapters_PvcAdapterAutoBridge(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	controller := &mockControllerWithPvcAdapter{}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	g.Expect(result).ToNot(gomega.BeNil())
	g.Expect(result).To(gomega.HaveLen(1))
	g.Expect(result[0]).To(gomega.BeAssignableToTypeOf(&PvcSubResourceAdapter{}))
}

func TestBuildAdapters_AdapterGetter(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	mockAdapter := &mockSubResourceAdapter{}
	controller := &mockControllerWithAdapterGetter{
		adapters: []api.SubResourceAdapter{mockAdapter},
	}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	g.Expect(result).ToNot(gomega.BeNil())
	g.Expect(result).To(gomega.HaveLen(1))
	g.Expect(result[0]).To(gomega.Equal(mockAdapter))
}

func TestBuildAdapters_AdapterGetterMultipleAdapters(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	mockAdapter1 := &mockSubResourceAdapter{}
	mockAdapter2 := &mockSubResourceAdapter{}
	controller := &mockControllerWithAdapterGetter{
		adapters: []api.SubResourceAdapter{mockAdapter1, mockAdapter2},
	}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	g.Expect(result).ToNot(gomega.BeNil())
	g.Expect(result).To(gomega.HaveLen(2))
}

func TestBuildAdapters_AdapterGetterPriorityOverPvc(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	// Controller implements both interfaces - AdapterGetter should take priority
	mockAdapter := &mockSubResourceAdapter{}
	controller := &mockControllerWithBothInterfaces{
		adapters: []api.SubResourceAdapter{mockAdapter},
	}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	g.Expect(result).ToNot(gomega.BeNil())
	g.Expect(result).To(gomega.HaveLen(1))
	// Should use the custom adapter, not auto-bridged PVC adapter
	g.Expect(result[0]).To(gomega.Equal(mockAdapter))
}

func TestBuildAdapters_EmptyAdapterList(t *testing.T) {
	g := gomega.NewGomegaWithT(t)

	controller := &mockControllerWithAdapterGetter{
		adapters: []api.SubResourceAdapter{},
	}
	labelAnnoMgr := api.NewXSetLabelAnnotationManager(nil)

	result := BuildAdapters(controller, labelAnnoMgr)

	// Empty slice from getter should be returned as-is
	g.Expect(result).ToNot(gomega.BeNil())
	g.Expect(result).To(gomega.BeEmpty())
}