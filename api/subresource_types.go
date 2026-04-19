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

	// GetTemplates returns subresource templates from XSet spec
	GetTemplates(xset XSetObject) ([]SubResourceTemplate, error)

	// BuildResource creates a subresource instance from template for a specific target
	BuildResource(ctx context.Context, xset XSetObject, template SubResourceTemplate, target client.Object, targetID string) (client.Object, error)

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
	//		- SubResourcePrefixGetter
}

// SubResourcePrefixGetter is used to get custom prefix for subresource names.
// If not implemented or returns empty string, defaults to "{xset-name}-{template-name}-".
// Adapter is responsible for truncation if needed.
// The returned prefix should end with "-" if a separator is desired.
type SubResourcePrefixGetter interface {
	GetSubResourcePrefix(xset XSetObject, template SubResourceTemplate) string
}

// SubResourceTemplate represents a parsed template with name and hash
type SubResourceTemplate struct {
	// Name is the template name (e.g., "data", "logs")
	Name string
	// Hash is the hash of template spec for change detection
	Hash string
	// Template is the parsed template object
	Template client.Object
}