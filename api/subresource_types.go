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

package api

import (
	"context"

	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// SubResourceAdapter is a generic interface for XSet subresources.
// Each subresource type (PVC, Service, ConfigMap, etc.) implements this interface.
type SubResourceAdapter interface {
	// Meta returns the GroupVersionKind for this subresource type
	Meta() schema.GroupVersionKind

	// GetTemplates returns subresource templates from XSet spec.
	// The controller computes the Hash from each template using TemplateHash().
	GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)

	// RetainWhenXSetDeleted returns true if subresource should be retained when XSet is deleted
	RetainWhenXSetDeleted(xset XSetObject) bool

	// RetainWhenXSetScaled returns true if subresource should be retained when XSet is scaled in
	RetainWhenXSetScaled(xset XSetObject) bool

	// RecreateWhenXSetUpdated returns true if subresource should be recreated
	// when XSet is updating targets. When true, subresources are deleted and
	// recreated during update operations regardless of template spec changes.
	RecreateWhenXSetUpdated(xset XSetObject) bool

	// AttachToTarget attaches subresources to target (e.g., mount PVC volumes to Pod)
	AttachToTarget(ctx context.Context, target client.Object, resources []client.Object) error

	// Optional interfaces:
	//   - SubResourceDecorator
}

// SubResourceDecorator is an optional interface for customizing subresources.
// Implement this interface to customize the resource created from a template.
//
// IMPORTANT: Decorators MUST NOT modify the following fields as they are
// managed by the control code:
//   - Namespace (set by control code to xset.Namespace)
//   - OwnerReferences (set by control code with controller reference to xset)
//   - Labels managed by control code:
//   - ControlledByXSetLabel (or equivalent from XSetLabelAnnotationManager)
//   - XInstanceIdLabelKey (set to targetID)
//   - Template name label (e.g., SubResourcePvcTemplateLabelKey)
//   - Template hash label (e.g., SubResourcePvcTemplateHashLabelKey)
//
// Decorators CAN:
//   - Set Name (overrides GenerateName if set)
//   - Add custom labels and annotations
//   - Customize spec fields
//   - Propagate additional labels from xset
type SubResourceDecorator interface {
	DecorateResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, resource client.Object, target client.Object, targetID string) error
}

// SubResourceTemplate represents a parsed template with name and hash
type SubResourceTemplate struct {
	// Name is the template name (e.g., "data", "logs")
	Name string
	// Hash is the hash of template spec for change detection.
	// This is computed by the controller using TemplateHash(), users do not need to set it.
	Hash string
	// Template is the parsed template object
	Template client.Object
}

