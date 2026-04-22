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

	corev1 "k8s.io/api/core/v1"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/mixin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// FieldIndexOwnerRefUID is the field index for owner reference UID.
const FieldIndexOwnerRefUID = "ownerRefUID"

// PVCGvk is the GroupVersionKind for PersistentVolumeClaim.
var PVCGvk = corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")

// PvcControl interface for PVC lifecycle management.
// This interface is kept for backward compatibility.
type PvcControl interface {
	GetFilteredPvcs(context.Context, api.XSetObject) ([]*corev1.PersistentVolumeClaim, error)
	CreateTargetPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	DeleteTargetPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	DeleteTargetUnusedPvcs(context.Context, api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) error
	OrphanPvc(context.Context, api.XSetObject, *corev1.PersistentVolumeClaim) error
	AdoptPvc(context.Context, api.XSetObject, *corev1.PersistentVolumeClaim) error
	AdoptPvcsLeftByRetainPolicy(context.Context, api.XSetObject) ([]*corev1.PersistentVolumeClaim, error)
	IsTargetPvcTmpChanged(api.XSetObject, client.Object, []*corev1.PersistentVolumeClaim) (bool, error)
	RetainPvcWhenXSetDeleted(xset api.XSetObject) bool
	RetainPvcWhenXSetScaled(xset api.XSetObject) bool
}

// pvcControlWrapper wraps SubResourceControl to implement PvcControl.
type pvcControlWrapper struct {
	subResourceControl SubResourceControl
	pvcAdapter         api.SubResourcePvcAdapter
}

// NewRealPvcControl creates a PvcControl from SubResourceControl.
// Returns nil if the controller does not implement SubResourcePvcAdapter.
func NewRealPvcControl(mixin *mixin.ReconcilerMixin, expectations *expectations.CacheExpectations, xsetLabelAnnoMgr api.XSetLabelAnnotationManager, xsetController api.XSetController) (PvcControl, error) {
	pvcAdapter, ok := GetSubresourcePvcAdapter(xsetController)
	if !ok {
		return nil, nil
	}

	adapters := []api.SubResourceAdapter{
		NewPvcSubResourceAdapter(xsetController, xsetLabelAnnoMgr),
	}

	subResourceControl, err := NewRealSubResourceControl(mixin, adapters, expectations, xsetLabelAnnoMgr, xsetController)
	if err != nil {
		return nil, err
	}

	return &pvcControlWrapper{
		subResourceControl: subResourceControl,
		pvcAdapter:         pvcAdapter,
	}, nil
}

// GetFilteredPvcs delegates to SubResourceControl and converts to PVC slice.
func (w *pvcControlWrapper) GetFilteredPvcs(ctx context.Context, xset api.XSetObject) ([]*corev1.PersistentVolumeClaim, error) {
	resources, err := w.subResourceControl.GetFilteredResources(ctx, xset)
	if err != nil {
		return nil, err
	}

	var pvcs []*corev1.PersistentVolumeClaim
	for _, state := range resources {
		if pvc, ok := state.Object.(*corev1.PersistentVolumeClaim); ok {
			pvcs = append(pvcs, pvc)
		}
	}
	return pvcs, nil
}

// CreateTargetPvcs delegates to SubResourceControl.
func (w *pvcControlWrapper) CreateTargetPvcs(ctx context.Context, xset api.XSetObject, target client.Object, existingPvcs []*corev1.PersistentVolumeClaim) error {
	existing := pvcsToSubResourceStates(existingPvcs)
	return w.subResourceControl.CreateTargetResources(ctx, xset, target, existing)
}

// DeleteTargetPvcs delegates to SubResourceControl.
func (w *pvcControlWrapper) DeleteTargetPvcs(ctx context.Context, xset api.XSetObject, target client.Object, pvcs []*corev1.PersistentVolumeClaim) error {
	existing := pvcsToSubResourceStates(pvcs)
	return w.subResourceControl.DeleteTargetResources(ctx, xset, target, existing, false)
}

// DeleteTargetUnusedPvcs delegates to SubResourceControl.
func (w *pvcControlWrapper) DeleteTargetUnusedPvcs(ctx context.Context, xset api.XSetObject, target client.Object, existingPvcs []*corev1.PersistentVolumeClaim) error {
	existing := pvcsToSubResourceStates(existingPvcs)
	return w.subResourceControl.DeleteTargetUnusedResources(ctx, xset, target, existing)
}

// OrphanPvc delegates to SubResourceControl.
func (w *pvcControlWrapper) OrphanPvc(ctx context.Context, xset api.XSetObject, pvc *corev1.PersistentVolumeClaim) error {
	return w.subResourceControl.OrphanResource(ctx, xset, pvc)
}

// AdoptPvc adopts a single PVC.
func (w *pvcControlWrapper) AdoptPvc(ctx context.Context, xset api.XSetObject, pvc *corev1.PersistentVolumeClaim) error {
	return w.subResourceControl.AdoptSingleResource(ctx, xset, pvc)
}

// AdoptPvcsLeftByRetainPolicy delegates to SubResourceControl.
func (w *pvcControlWrapper) AdoptPvcsLeftByRetainPolicy(ctx context.Context, xset api.XSetObject) ([]*corev1.PersistentVolumeClaim, error) {
	resources, err := w.subResourceControl.AdoptOrphanedResources(ctx, xset)
	if err != nil {
		return nil, err
	}

	var pvcs []*corev1.PersistentVolumeClaim
	for _, state := range resources {
		if pvc, ok := state.Object.(*corev1.PersistentVolumeClaim); ok {
			pvcs = append(pvcs, pvc)
		}
	}
	return pvcs, nil
}

// IsTargetPvcTmpChanged delegates to SubResourceControl.
func (w *pvcControlWrapper) IsTargetPvcTmpChanged(xset api.XSetObject, target client.Object, existingPvcs []*corev1.PersistentVolumeClaim) (bool, error) {
	existing := pvcsToSubResourceStates(existingPvcs)
	return w.subResourceControl.IsTargetTemplateChanged(xset, target, existing, false)
}

// RetainPvcWhenXSetDeleted delegates to PVC adapter.
func (w *pvcControlWrapper) RetainPvcWhenXSetDeleted(xset api.XSetObject) bool {
	return w.pvcAdapter.RetainPvcWhenXSetDeleted(xset)
}

// RetainPvcWhenXSetScaled delegates to PVC adapter.
func (w *pvcControlWrapper) RetainPvcWhenXSetScaled(xset api.XSetObject) bool {
	return w.pvcAdapter.RetainPvcWhenXSetScaled(xset)
}

// pvcsToSubResourceStates converts PVC slice to SubResourceState slice.
func pvcsToSubResourceStates(pvcs []*corev1.PersistentVolumeClaim) []SubResourceState {
	if pvcs == nil {
		return nil
	}
	states := make([]SubResourceState, len(pvcs))
	pvcGVK := corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
	for i, pvc := range pvcs {
		states[i] = SubResourceState{
			Object:  pvc,
			GVK:     pvcGVK,
		}
	}
	return states
}

