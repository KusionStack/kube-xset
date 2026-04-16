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

// ServiceSubResourceAdapter implements SubResourceAdapter for Service.
// This is an example adapter for creating Services per target.
type ServiceSubResourceAdapter struct {
	truncator    *NameTruncator
	labelManager *LabelManager
}

// NewServiceSubResourceAdapter creates a new Service adapter.
func NewServiceSubResourceAdapter() *ServiceSubResourceAdapter {
	truncator := NewNameTruncator()
	return &ServiceSubResourceAdapter{
		truncator:    truncator,
		labelManager: NewLabelManager(truncator),
	}
}

// Meta returns the GVK for Service.
func (s *ServiceSubResourceAdapter) Meta() schema.GroupVersionKind {
	return corev1.SchemeGroupVersion.WithKind("Service")
}

// GetTemplates returns Service templates from the XSet.
// This implementation returns nil - XSetController implementations should
// override this by storing templates in annotations or a custom field.
func (s *ServiceSubResourceAdapter) GetTemplates(xset api.XSetObject) ([]api.SubResourceTemplate, error) {
	// XSetController implementations should provide templates via:
	// 1. A custom annotation with JSON-encoded templates
	// 2. A new field in their XSetSpec
	// Example:
	//   annotations := xset.GetAnnotations()
	//   if templatesJson, ok := annotations["xset.kusionstack.io/service-templates"]; ok {
	//       // parse and return templates
	//   }
	return nil, nil
}

// BuildResource creates a Service from a template.
func (s *ServiceSubResourceAdapter) BuildResource(
	ctx context.Context,
	xset api.XSetObject,
	template api.SubResourceTemplate,
	target client.Object,
	targetID string,
) (client.Object, error) {
	svc, ok := template.Template.(*corev1.Service)
	if !ok {
		return nil, fmt.Errorf("expected Service, got %T", template.Template)
	}

	svc = svc.DeepCopy()

	baseName := fmt.Sprintf("%s-%s-%s", xset.GetName(), template.Name, targetID)
	svc.Name = s.truncator.Truncate(baseName)
	svc.Namespace = xset.GetNamespace()

	svc.OwnerReferences = []metav1.OwnerReference{
		{
			APIVersion:         xset.GetObjectKind().GroupVersionKind().GroupVersion().String(),
			Kind:               xset.GetObjectKind().GroupVersionKind().Kind,
			Name:               xset.GetName(),
			UID:                xset.GetUID(),
			Controller:         ptrTo(true),
			BlockOwnerDeletion: ptrTo(true),
		},
	}

	if svc.Labels == nil {
		svc.Labels = make(map[string]string)
	}
	s.labelManager.SetLabel(svc, "app.kubernetes.io/instance", targetID)
	s.labelManager.SetLabel(svc, "app.kubernetes.io/managed-by", "kube-xset")

	return svc, nil
}

// RetainWhenXSetDeleted returns false - Services are deleted with XSet by default.
func (s *ServiceSubResourceAdapter) RetainWhenXSetDeleted(xset api.XSetObject) bool {
	return false
}

// RetainWhenXSetScaled returns false - Services are deleted when scaled in.
func (s *ServiceSubResourceAdapter) RetainWhenXSetScaled(xset api.XSetObject) bool {
	return false
}

// RecreateWhenXSetUpdated returns false by default for backward compatibility.
func (s *ServiceSubResourceAdapter) RecreateWhenXSetUpdated(xset api.XSetObject) bool {
	return false
}

// AttachToTarget returns nil - Services are independent, no attachment needed.
func (s *ServiceSubResourceAdapter) AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error {
	return nil
}

// GetAttachedResourceNames returns nil - Services are independent.
func (s *ServiceSubResourceAdapter) GetAttachedResourceNames(target client.Object) ([]string, error) {
	return nil, nil
}

// ptrTo returns a pointer to the given value.
func ptrTo[T any](v T) *T {
	return &v
}

var _ api.SubResourceAdapter = &ServiceSubResourceAdapter{}