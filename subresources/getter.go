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
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// PVCGvk is the GroupVersionKind for PersistentVolumeClaim.
var PVCGvk = corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")

// GetSubresourcePvcAdapter returns the PVC adapter if the controller implements SubResourcePvcAdapter.
func GetSubresourcePvcAdapter(control api.XSetController) (adapter api.SubResourcePvcAdapter, enabled bool) {
	adapter, enabled = control.(api.SubResourcePvcAdapter)
	return adapter, enabled
}

// GetSubResourceAdapters returns subresource adapters if the controller implements SubResourceAdapterGetter.
func GetSubResourceAdapters(control api.XSetController) (adapters []api.SubResourceAdapter, enabled bool) {
	getter, ok := control.(api.SubResourceAdapterGetter)
	if !ok {
		return nil, false
	}
	return getter.GetSubResourceAdapters(), true
}

// BuildAdapters builds the adapter list with auto-bridge for legacy controllers.
// Priority:
// 1. If controller implements SubResourceAdapterGetter, use its adapters
// 2. Else if controller implements SubResourcePvcAdapter, auto-bridge to SubResourceControl
// 3. Else return nil (no subresource management)
func BuildAdapters(controller api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) []api.SubResourceAdapter {
	// Priority 1: Controller provides its own adapters
	if getter, ok := controller.(api.SubResourceAdapterGetter); ok {
		return getter.GetSubResourceAdapters()
	}

	// Priority 2: Auto-bridge legacy SubResourcePvcAdapter
	if _, ok := controller.(api.SubResourcePvcAdapter); ok {
		return []api.SubResourceAdapter{
			NewPvcSubResourceAdapter(controller, labelAnnoMgr),
		}
	}

	// No subresource management
	return nil
}

// PvcSubResourceAdapter implements SubResourceAdapter for PVC.
// It bridges to the legacy SubResourcePvcAdapter for backward compatibility.
type PvcSubResourceAdapter struct {
	xsetController api.XSetController
	labelAnnoMgr   api.XSetLabelAnnotationManager
}

// NewPvcSubResourceAdapter creates a new PVC adapter.
func NewPvcSubResourceAdapter(xsetController api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) *PvcSubResourceAdapter {
	return &PvcSubResourceAdapter{
		xsetController: xsetController,
		labelAnnoMgr:   labelAnnoMgr,
	}
}

// Meta returns the GVK for PVC.
func (p *PvcSubResourceAdapter) Meta() schema.GroupVersionKind {
	return PVCGvk
}

// GetTemplates returns PVC templates from the XSet.
func (p *PvcSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil, nil
	}

	templates := pvcAdapter.GetXSetPvcTemplate(xset)
	var result []api.SubResourceTemplate
	for i := range templates {
		hash, err := TemplateHash(&templates[i])
		if err != nil {
			return nil, fmt.Errorf("failed to compute PVC template hash: %w", err)
		}
		result = append(result, api.SubResourceTemplate{
			Name:     templates[i].Name,
			Hash:     hash,
			Template: &templates[i],
		})
	}
	return result, nil
}

// RetainWhenXSetDeleted returns whether PVCs should be retained when XSet is deleted.
func (p *PvcSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
	if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
		return pvcAdapter.RetainPvcWhenXSetDeleted(xset)
	}
	return false
}

// RetainWhenXSetScaled returns whether PVCs should be retained when XSet is scaled in.
func (p *PvcSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
	if pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter); ok {
		return pvcAdapter.RetainPvcWhenXSetScaled(xset)
	}
	return false
}

// RecreateWhenTargetReplaced returns false by default for backward compatibility.
// PVCs are recreated only when template spec changes (hash mismatch).
func (p *PvcSubResourceAdapter) RecreateWhenTargetReplaced(xset api.XSetObject) bool {
	return false
}

// AttachToTarget attaches PVCs to the target by setting volumes.
func (p *PvcSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	if len(resources) == 0 {
		return nil
	}

	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil
	}

	var volumes []corev1.Volume
	for _, res := range resources {
		pvc, ok := res.(*corev1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		templateName := pvc.Labels[p.labelAnnoMgr.Value(api.SubResourceTemplateLabelKey)]
		volumes = append(volumes, corev1.Volume{
			Name: templateName,
			VolumeSource: corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{
					ClaimName: pvc.Name,
				},
			},
		})
	}

	existingVolumes := pvcAdapter.GetXSpecVolumes(target)
	volumeMap := make(map[string]corev1.Volume, len(existingVolumes)+len(volumes))
	for i := range existingVolumes {
		volumeMap[existingVolumes[i].Name] = existingVolumes[i]
	}
	for i := range volumes {
		volumeMap[volumes[i].Name] = volumes[i]
	}

	// Convert map to slice and sort by name for deterministic order
	mergedVolumes := make([]corev1.Volume, 0, len(volumeMap))
	for _, v := range volumeMap {
		mergedVolumes = append(mergedVolumes, v)
	}
	// Sort by volume name for deterministic ordering
	sort.Slice(mergedVolumes, func(i, j int) bool {
		return mergedVolumes[i].Name < mergedVolumes[j].Name
	})

	pvcAdapter.SetXSpecVolumes(target, mergedVolumes)
	return nil
}

var _ api.SubResourceAdapter = &PvcSubResourceAdapter{}
