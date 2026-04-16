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

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// PvcSubResourceAdapter implements SubResourceAdapter for PVC.
// It bridges to the legacy SubResourcePvcAdapter for backward compatibility.
type PvcSubResourceAdapter struct {
	xsetController api.XSetController
	labelAnnoMgr   api.XSetLabelAnnotationManager
	truncator      *NameTruncator
	labelManager   *LabelManager
}

// NewPvcSubResourceAdapter creates a new PVC adapter.
func NewPvcSubResourceAdapter(xsetController api.XSetController, labelAnnoMgr api.XSetLabelAnnotationManager) *PvcSubResourceAdapter {
	truncator := NewNameTruncator()
	return &PvcSubResourceAdapter{
		xsetController: xsetController,
		labelAnnoMgr:   labelAnnoMgr,
		truncator:      truncator,
		labelManager:   NewLabelManager(truncator),
	}
}

// Meta returns the GVK for PVC.
func (p *PvcSubResourceAdapter) Meta() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim")
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

// BuildResource creates a PVC from a template.
func (p *PvcSubResourceAdapter) BuildResource(
	ctx context.Context,
	xset api.XSetObject,
	template api.SubResourceTemplate,
	target client.Object,
	targetID string,
) (client.Object, error) {
	pvc, ok := template.Template.(*corev1.PersistentVolumeClaim)
	if !ok {
		return nil, fmt.Errorf("expected PersistentVolumeClaim, got %T", template.Template)
	}

	pvc = pvc.DeepCopy()
	pvc.Namespace = xset.GetNamespace()

	xsetMeta := p.xsetController.XSetMeta()
	pvc.OwnerReferences = []metav1.OwnerReference{
		*metav1.NewControllerRef(xset, xsetMeta.GroupVersionKind()),
	}

	if pvc.Labels == nil {
		pvc.Labels = make(map[string]string)
	}
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.ControlledByXSetLabel), "true")
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.XInstanceIdLabelKey), targetID)
	p.labelManager.SetLabelWithTrackedOriginal(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey), template.Name)
	p.labelManager.SetLabel(pvc, p.labelAnnoMgr.Value(api.SubResourcePvcTemplateHashLabelKey), template.Hash)

	return pvc, nil
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

// RecreateWhenXSetUpdated returns false by default for backward compatibility.
// PVCs are recreated only when template spec changes (hash mismatch).
func (p *PvcSubResourceAdapter) RecreateWhenXSetUpdated(xset api.XSetObject) bool {
	// Default to false for backward compatibility with legacy SubResourcePvcAdapter
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
		templateName := pvc.Labels[p.labelAnnoMgr.Value(api.SubResourcePvcTemplateLabelKey)]
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
	volumeMap := make(map[string]corev1.Volume)
	for _, v := range existingVolumes {
		volumeMap[v.Name] = v
	}
	for _, v := range volumes {
		volumeMap[v.Name] = v
	}

	var mergedVolumes []corev1.Volume
	for _, v := range volumeMap {
		mergedVolumes = append(mergedVolumes, v)
	}

	pvcAdapter.SetXSpecVolumes(target, mergedVolumes)
	return nil
}

// GetAttachedResourceNames returns the names of PVCs attached to the target.
func (p *PvcSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
	pvcAdapter, ok := p.xsetController.(api.SubResourcePvcAdapter)
	if !ok {
		return nil, nil
	}

	volumes := pvcAdapter.GetXSpecVolumes(target)
	var names []string
	for _, v := range volumes {
		if v.PersistentVolumeClaim != nil {
			names = append(names, v.PersistentVolumeClaim.ClaimName)
		}
	}
	return names, nil
}

var _ api.SubResourceAdapter = &PvcSubResourceAdapter{}
