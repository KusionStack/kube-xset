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

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	kubeutilclient "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/mixin"
	refmanagerutil "kusionstack.io/kube-utils/controller/refmanager"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

// FieldIndexOwnerRefUID is the field index for owner reference UID.
const FieldIndexOwnerRefUID = "ownerRefUID"

// SubResourceState wraps a subresource with its adapter metadata.
type SubResourceState struct {
	// Object is the actual subresource (PVC, Service, etc.)
	Object client.Object
	// GVK is the GroupVersionKind of the subresource
	GVK schema.GroupVersionKind
	// Adapter is the adapter that manages this subresource type
	Adapter api.SubResourceAdapter
}

// SubResourceControl manages all subresource types through registered adapters.
type SubResourceControl interface {
	// Lifecycle operations (called from sync_control.go)

	// GetFilteredResources lists all subresources owned by the XSet
	GetFilteredResources(ctx context.Context, xset api.XSetObject) ([]SubResourceState, error)

	// AdoptOrphanedResources adopts subresources left by retention policy
	AdoptOrphanedResources(ctx context.Context, xset api.XSetObject) ([]SubResourceState, error)

	// CreateTargetResources creates subresources for a target and attaches them
	CreateTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error

	// DeleteTargetResources deletes subresources for a target.
	// If isReplaceTarget is true, deletes all resources. Otherwise, only deletes resources
	// where the adapter's RetainWhenXSetScaled returns false.
	DeleteTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState, isReplaceTarget bool) error

	// DeleteTargetUnusedResources deletes unused subresources for a target
	DeleteTargetUnusedResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error

	// DeleteTargetRecreateResources deletes subresources for a target that need recreation on update.
	// Only deletes resources for adapters where RecreateWhenXSetUpdated returns true.
	DeleteTargetRecreateResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error

	// OrphanResource removes owner reference from a subresource
	OrphanResource(ctx context.Context, xset api.XSetObject, resource client.Object) error

	// Query operations

	// IsTargetTemplateChanged returns true if any subresource template changed for target,
	// or if the target has resources that need recreation on update (when isUpdatedRevision is false).
	IsTargetTemplateChanged(xset api.XSetObject, target client.Object, existing []SubResourceState, isUpdatedRevision bool) (bool, error)

	// ReclaimSubResourcesOnDeletion handles subresource retention when XSet is being deleted.
	// It fetches all subresources and orphans those marked for retention.
	ReclaimSubResourcesOnDeletion(ctx context.Context, xset api.XSetObject) error

	// Include/Exclude support

	// CheckAllowIncludeExclude checks if target's subresources allow include/exclude
	CheckAllowIncludeExclude(ctx context.Context, xset api.XSetObject, target client.Object, fn CheckAllowFunc) (bool, error)

	// AdoptTargetResources adopts subresources for a target during include operation
	AdoptTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, instanceID string) error

	// OrphanTargetResources orphans all subresources for a target during exclude operation
	OrphanTargetResources(ctx context.Context, xset api.XSetObject, target client.Object) error

	// AdoptSingleResource adopts a single subresource by setting owner reference.
	AdoptSingleResource(ctx context.Context, xset api.XSetObject, resource client.Object) error
}

// CheckAllowFunc is the function type for checking include/exclude permission.
// Defined in synccontrols package: func(obj client.Object, ownerName, ownerKind string, labelMgr api.XSetLabelAnnotationManager) (bool, string)
type CheckAllowFunc func(obj client.Object, ownerName, ownerKind string, labelMgr api.XSetLabelAnnotationManager) (bool, string)

// RealSubResourceControl implements SubResourceControl with multiple adapters.
type RealSubResourceControl struct {
	client         client.Client
	scheme         *runtime.Scheme
	adaptersByGVK  map[schema.GroupVersionKind]api.SubResourceAdapter
	expectations   *expectations.CacheExpectations
	labelAnnoMgr   api.XSetLabelAnnotationManager
	xsetController api.XSetController
}

// NewRealSubResourceControl creates a new SubResourceControl.
// Returns nil if no adapters are provided (subresource management disabled).
func NewRealSubResourceControl(
	mixin *mixin.ReconcilerMixin,
	adapters []api.SubResourceAdapter,
	expectations *expectations.CacheExpectations,
	labelAnnoMgr api.XSetLabelAnnotationManager,
	xsetController api.XSetController,
) (SubResourceControl, error) {
	if len(adapters) == 0 {
		return nil, nil
	}

	// Build GVK index from adapters
	adaptersByGVK := make(map[schema.GroupVersionKind]api.SubResourceAdapter)
	for _, adapter := range adapters {
		gvk := adapter.Meta()
		adaptersByGVK[gvk] = adapter
	}

	// Set up cache indexes for all adapter GVKs
	if err := setUpCacheForAdapters(mixin.Cache, mixin.Scheme, adapters, xsetController); err != nil {
		return nil, err
	}

	return &RealSubResourceControl{
		client:         mixin.Client,
		scheme:         mixin.Scheme,
		adaptersByGVK:  adaptersByGVK,
		expectations:   expectations,
		labelAnnoMgr:   labelAnnoMgr,
		xsetController: xsetController,
	}, nil
}

// setUpCacheForAdapters registers field indexes for all adapter GVKs.
func setUpCacheForAdapters(cache cache.Cache, scheme *runtime.Scheme, adapters []api.SubResourceAdapter, controller api.XSetController) error {
	for _, adapter := range adapters {
		gvk := adapter.Meta()
		obj, err := scheme.New(gvk)
		if err != nil {
			return fmt.Errorf("failed to set up cache for adapter GVK %s: type is not registered in the scheme; ensure this type is added to the controller scheme: %w", gvk, err)
		}
		if err := cache.IndexField(context.TODO(), obj.(client.Object), FieldIndexOwnerRefUID, func(object client.Object) []string {
			ownerRef := metav1.GetControllerOf(object)
			if ownerRef == nil || ownerRef.Kind != controller.XSetMeta().Kind {
				return nil
			}
			return []string{string(ownerRef.UID)}
		}); err != nil {
			return fmt.Errorf("failed to index by field for %s->xset %s: %w", gvk.Kind, FieldIndexOwnerRefUID, err)
		}
	}
	return nil
}

// newListForGVK creates a new list object for the given GVK using the scheme.
func (sc *RealSubResourceControl) newListForGVK(gvk schema.GroupVersionKind) (client.ObjectList, error) {
	listGVK := gvk.GroupVersion().WithKind(gvk.Kind + "List")
	obj, err := sc.scheme.New(listGVK)
	if err != nil {
		return nil, fmt.Errorf("failed to create list for GVK %s: %w", gvk, err)
	}
	return obj.(client.ObjectList), nil
}

// newObjectForGVK creates a new object for the given GVK using the scheme.
func (sc *RealSubResourceControl) newObjectForGVK(gvk schema.GroupVersionKind) (client.Object, error) {
	obj, err := sc.scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create object for GVK %s: %w", gvk, err)
	}
	return obj.(client.Object), nil
}

// GetFilteredResources lists all subresources owned by the XSet.
func (sc *RealSubResourceControl) GetFilteredResources(ctx context.Context, xset api.XSetObject) ([]SubResourceState, error) {
	var resources []SubResourceState

	for _, adapter := range sc.adaptersByGVK {
		gvk := adapter.Meta()
		list, err := sc.newListForGVK(gvk)
		if err != nil {
			continue
		}

		if err := sc.client.List(ctx, list, &client.ListOptions{
			Namespace:     xset.GetNamespace(),
			FieldSelector: fields.OneTermEqualSelector(FieldIndexOwnerRefUID, string(xset.GetUID())),
		}); err != nil {
			return nil, fmt.Errorf("failed to list %s: %w", gvk.Kind, err)
		}

		items := extractListItems(list)
		for _, item := range items {
			if item.GetDeletionTimestamp() == nil {
				resources = append(resources, SubResourceState{
					Object:  item,
					GVK:     gvk,
					Adapter: adapter,
				})
			}
		}
	}

	return resources, nil
}

// extractListItems extracts items from any ObjectList using reflection.
func extractListItems(list client.ObjectList) []client.Object {
	items := make([]client.Object, 0)
	if err := meta.EachListItem(list, func(obj runtime.Object) error {
		items = append(items, obj.(client.Object))
		return nil
	}); err != nil {
		return nil
	}
	return items
}

// AdoptOrphanedResources adopts subresources left by retention policy.
func (sc *RealSubResourceControl) AdoptOrphanedResources(ctx context.Context, xset api.XSetObject) ([]SubResourceState, error) {
	var adopted []SubResourceState

	for _, adapter := range sc.adaptersByGVK {
		if adapter.RetainWhenXSetDeleted(xset) {
			// Find orphaned resources for this adapter
			orphaned, err := sc.findOrphanedResources(ctx, xset, adapter)
			if err != nil {
				return nil, err
			}

			for _, res := range orphaned {
				if err := sc.adoptResource(ctx, xset, res); err != nil {
					return nil, err
				}
				adopted = append(adopted, SubResourceState{
					Object:  res,
					GVK:     adapter.Meta(),
					Adapter: adapter,
				})
			}
		}
	}

	return adopted, nil
}

// findOrphanedResources finds subresources that have the controlled-by label but no owner reference.
func (sc *RealSubResourceControl) findOrphanedResources(ctx context.Context, xset api.XSetObject, adapter api.SubResourceAdapter) ([]client.Object, error) {
	xsetSpec := sc.xsetController.GetXSetSpec(xset)
	ownerSelector := xsetSpec.Selector.DeepCopy()
	if ownerSelector.MatchLabels == nil {
		ownerSelector.MatchLabels = map[string]string{}
	}
	ownerSelector.MatchLabels[sc.labelAnnoMgr.Value(api.ControlledByXSetLabel)] = "true"
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions,
		metav1.LabelSelectorRequirement{
			Key:      sc.labelAnnoMgr.Value(api.XOrphanedIndicationLabelKey),
			Operator: metav1.LabelSelectorOpDoesNotExist,
		},
		metav1.LabelSelectorRequirement{
			Key:      sc.labelAnnoMgr.Value(api.XInstanceIdLabelKey),
			Operator: metav1.LabelSelectorOpExists,
		},
	)

	selector, err := metav1.LabelSelectorAsSelector(ownerSelector)
	if err != nil {
		return nil, err
	}

	gvk := adapter.Meta()
	list, err := sc.newListForGVK(gvk)
	if err != nil {
		return nil, nil
	}

	if err := sc.client.List(ctx, list, &client.ListOptions{
		Namespace:     xset.GetNamespace(),
		LabelSelector: selector,
	}); err != nil {
		return nil, err
	}

	items := extractListItems(list)
	var orphaned []client.Object
	for _, item := range items {
		if len(item.GetOwnerReferences()) == 0 {
			orphaned = append(orphaned, item)
		}
	}
	return orphaned, nil
}

// adoptResource sets the owner reference on a subresource.
func (sc *RealSubResourceControl) adoptResource(ctx context.Context, xset api.XSetObject, res client.Object) error {
	xsetSpec := sc.xsetController.GetXSetSpec(xset)
	if xsetSpec.Selector.MatchLabels == nil {
		return nil
	}

	refWriter := refmanagerutil.NewOwnerRefWriter(sc.client)
	matcher, err := refmanagerutil.LabelSelectorAsMatch(xsetSpec.Selector)
	if err != nil {
		return fmt.Errorf("fail to create labelSelector matcher: %w", err)
	}
	refManager := refmanagerutil.NewObjectControllerRefManager(refWriter, xset, xset.GetObjectKind().GroupVersionKind(), matcher)

	if _, err := refManager.Claim(ctx, res); err != nil {
		return fmt.Errorf("failed to adopt subresource: %w", err)
	}
	return nil
}

// AdoptSingleResource adopts a single subresource by setting owner reference.
func (sc *RealSubResourceControl) AdoptSingleResource(ctx context.Context, xset api.XSetObject, resource client.Object) error {
	return sc.adoptResource(ctx, xset, resource)
}

// classifyResourcesByHash classifies resources into new and old based on template hash.
// Returns two maps: newResources (hash matches current template) and oldResources (hash differs).
func (sc *RealSubResourceControl) classifyResourcesByHash(targetID string, xset api.XSetObject, adapter api.SubResourceAdapter, existing []SubResourceState) (map[string]SubResourceState, map[string]SubResourceState, error) {
	newResources := make(map[string]SubResourceState)
	oldResources := make(map[string]SubResourceState)

	gvk := adapter.Meta()

	// Get current template hashes
	templates, err := adapter.GetTemplates(xset)
	if err != nil {
		return newResources, oldResources, err
	}
	templateHashes := make(map[string]string)
	for _, tmpl := range templates {
		templateHashes[tmpl.Name] = tmpl.Hash
	}

	// Classify existing resources for this adapter
	for _, state := range existing {
		if state.GVK != gvk {
			continue
		}

		// Skip resources being deleted
		if state.Object.GetDeletionTimestamp() != nil {
			continue
		}

		// Only process resources for this target
		resourceID, exist := sc.labelAnnoMgr.Get(state.Object, api.XInstanceIdLabelKey)
		if !exist || resourceID != targetID {
			continue
		}

		// Get template hash and name
		resourceHash, exist := sc.labelAnnoMgr.Get(state.Object, api.SubResourceTemplateHashLabelKey)
		if !exist {
			continue
		}

		templateName, _ := sc.labelAnnoMgr.Get(state.Object, api.SubResourceTemplateLabelKey)

		// Classify by hash comparison
		if currentHash, ok := templateHashes[templateName]; ok && currentHash == resourceHash {
			newResources[templateName] = state
		} else {
			oldResources[templateName] = state
		}
	}

	return newResources, oldResources, nil
}

// filterByTarget returns resources belonging to the given target.
func (sc *RealSubResourceControl) filterByTarget(existing []SubResourceState, target client.Object) []SubResourceState {
	targetID, _ := sc.labelAnnoMgr.Get(target, api.XInstanceIdLabelKey)
	if targetID == "" {
		return nil
	}
	var result []SubResourceState
	for _, state := range existing {
		if resourceID, _ := sc.labelAnnoMgr.Get(state.Object, api.XInstanceIdLabelKey); resourceID == targetID {
			result = append(result, state)
		}
	}
	return result
}

// hasRecreateOnUpdateResources returns true if target has resources that need recreation on update.
func (sc *RealSubResourceControl) hasRecreateOnUpdateResources(xset api.XSetObject, target client.Object, existing []SubResourceState) bool {
	for _, state := range sc.filterByTarget(existing, target) {
		if state.Adapter != nil && state.Adapter.RecreateWhenXSetUpdated(xset) {
			return true
		}
	}
	return false
}

// orphanRetainedResources orphans resources marked for retention on XSet deletion.
func (sc *RealSubResourceControl) orphanRetainedResources(ctx context.Context, xset api.XSetObject, existing []SubResourceState) error {
	for _, state := range existing {
		if state.Adapter != nil && state.Adapter.RetainWhenXSetDeleted(xset) && len(state.Object.GetOwnerReferences()) > 0 {
			if err := sc.OrphanResource(ctx, xset, state.Object); err != nil {
				return err
			}
		}
	}
	return nil
}

// ReclaimSubResourcesOnDeletion handles subresource retention when XSet is being deleted.
// It fetches all subresources and orphans those marked for retention.
func (sc *RealSubResourceControl) ReclaimSubResourcesOnDeletion(ctx context.Context, xset api.XSetObject) error {
	resources, err := sc.GetFilteredResources(ctx, xset)
	if err != nil {
		return err
	}
	return sc.orphanRetainedResources(ctx, xset, resources)
}

// deleteResource deletes a subresource and tracks the expectation.
// It issues a normal delete and lets Kubernetes handle finalizers naturally.
// Resources with finalizers (e.g., PVCs with kubernetes.io/pvc-protection) will
// get a deletion timestamp and be deleted when their finalizers are cleared.
func (sc *RealSubResourceControl) deleteResource(ctx context.Context, xset api.XSetObject, resource client.Object, gvk schema.GroupVersionKind) error {
	if err := sc.client.Delete(ctx, resource); err != nil && !apierrors.IsNotFound(err) {
		return fmt.Errorf("failed to delete %s %s: %w", gvk.Kind, resource.GetName(), err)
	}

	// Track expectation for deletion
	if err := sc.expectations.ExpectDeletion(
		kubeutilclient.ObjectKeyString(xset),
		gvk,
		resource.GetNamespace(),
		resource.GetName(),
	); err != nil {
		return err
	}

	return nil
}

// CreateTargetResources creates subresources for a target and attaches them.
func (sc *RealSubResourceControl) CreateTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error {
	id, exist := sc.labelAnnoMgr.Get(target, api.XInstanceIdLabelKey)
	if !exist {
		return nil
	}

	for _, adapter := range sc.adaptersByGVK {
		if err := sc.createResourcesForAdapter(ctx, xset, target, existing, id, adapter); err != nil {
			return err
		}
	}

	return nil
}

// createResourcesForAdapter creates subresources for a specific adapter.
func (sc *RealSubResourceControl) createResourcesForAdapter(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState, targetID string, adapter api.SubResourceAdapter) error {
	// Get desired templates
	templates, err := adapter.GetTemplates(xset)
	if err != nil {
		return fmt.Errorf("failed to get templates from adapter %s: %w", adapter.Meta().Kind, err)
	}
	if len(templates) == 0 {
		return nil
	}

	// Classify existing resources for this adapter and target
	gvk := adapter.Meta()
	existingByTemplateName := make(map[string]SubResourceState)
	for _, state := range existing {
		if state.GVK == gvk {
			// Only consider resources belonging to this target
			resourceID, _ := sc.labelAnnoMgr.Get(state.Object, api.XInstanceIdLabelKey)
			if resourceID != targetID {
				continue
			}
			templateName, _ := sc.labelAnnoMgr.Get(state.Object, api.SubResourceTemplateLabelKey)
			if templateName != "" {
				existingByTemplateName[templateName] = state
			}
		}
	}

	// Create resources and build list for attachment
	var createdResources []client.Object
	for _, template := range templates {
		// Check if we can reuse existing resource
		if existing, ok := existingByTemplateName[template.Name]; ok {
			existingHash, _ := sc.labelAnnoMgr.Get(existing.Object, api.SubResourceTemplateHashLabelKey)
			if existingHash == template.Hash {
				// Reuse existing — hash matches, no need to recreate
				createdResources = append(createdResources, existing.Object)
				continue
			}
			// Delete old resource before creating new one (hash changed)
			if err := sc.deleteResource(ctx, xset, existing.Object, gvk); err != nil && !apierrors.IsNotFound(err) {
				return fmt.Errorf("failed to delete old %s %s: %w", gvk.Kind, existing.Object.GetName(), err)
			}
		}

		// Create new resource from template
		resource := template.Template.DeepCopyObject().(client.Object)

		// Set namespace
		resource.SetNamespace(xset.GetNamespace())

		// Set owner reference
		xsetMeta := sc.xsetController.XSetMeta()
		resource.SetOwnerReferences([]metav1.OwnerReference{
			*metav1.NewControllerRef(xset, xsetMeta.GroupVersionKind()),
		})

		// Set labels
		labels := resource.GetLabels()
		if labels == nil {
			labels = make(map[string]string)
		}
		labels[sc.labelAnnoMgr.Value(api.ControlledByXSetLabel)] = "true"
		labels[sc.labelAnnoMgr.Value(api.XInstanceIdLabelKey)] = targetID
		labels[sc.labelAnnoMgr.Value(api.SubResourceTemplateLabelKey)] = template.Name
		labels[sc.labelAnnoMgr.Value(api.SubResourceTemplateHashLabelKey)] = template.Hash
		resource.SetLabels(labels)

		// Let adapter decorate the resource (optional)
		if decorator, ok := adapter.(api.SubResourceDecorator); ok {
			if err := decorator.DecorateResource(ctx, xset, template, resource, target, targetID); err != nil {
				return fmt.Errorf("failed to decorate %s from template %s: %w", gvk.Kind, template.Name, err)
			}
		}

		// Set GenerateName if Name is not set
		if resource.GetName() == "" {
			resource.SetGenerateName(fmt.Sprintf("%s-%s-", xset.GetName(), template.Name))
		}

		if err := sc.client.Create(ctx, resource); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Resource already exists — fetch and check state
				existingResource, newObjErr := sc.newObjectForGVK(gvk)
				if newObjErr != nil {
					return fmt.Errorf("failed to create object for GVK %s: %w", gvk, newObjErr)
				}
				if getErr := sc.client.Get(ctx, client.ObjectKey{
					Namespace: resource.GetNamespace(),
					Name:      resource.GetName(),
				}, existingResource); getErr != nil {
					return fmt.Errorf("failed to get existing %s %s: %w", gvk.Kind, resource.GetName(), getErr)
				}
				// If the existing resource is being deleted, wait for it to be gone
				if existingResource.GetDeletionTimestamp() != nil {
					// Return error to requeue — the resource will be gone in the next reconcile
					return fmt.Errorf("%s %s is being deleted, will retry on next reconcile", gvk.Kind, resource.GetName())
				}
				createdResources = append(createdResources, existingResource)
				continue
			}
			return fmt.Errorf("failed to create %s %s: %w", gvk.Kind, resource.GetName(), err)
		}

		// Track expectation
		if err := sc.expectations.ExpectCreation(
			kubeutilclient.ObjectKeyString(xset),
			gvk,
			resource.GetNamespace(),
			resource.GetName(),
		); err != nil {
			return err
		}

		createdResources = append(createdResources, resource)
	}

	// Attach to target (e.g., set volumes on Pod)
	if err := adapter.AttachToTarget(ctx, target, createdResources); err != nil {
		return fmt.Errorf("failed to attach %s to target: %w", gvk.Kind, err)
	}

	return nil
}

// DeleteTargetResources deletes subresources for a target.
// If isReplaceTarget is true, deletes all resources. Otherwise, only deletes resources
// where the adapter's RetainWhenXSetScaled returns false.
func (sc *RealSubResourceControl) DeleteTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState, isReplaceTarget bool) error {
	for _, state := range sc.filterByTarget(existing, target) {
		// For replace targets, delete all resources
		// For scale-in, only delete if adapter doesn't want to retain
		if !isReplaceTarget && state.Adapter != nil && state.Adapter.RetainWhenXSetScaled(xset) {
			continue
		}
		if err := sc.deleteResource(ctx, xset, state.Object, state.GVK); err != nil {
			return fmt.Errorf("failed to delete %s %s: %w", state.GVK.Kind, state.Object.GetName(), err)
		}
	}
	return nil
}

// DeleteTargetUnusedResources deletes unused subresources for a target.
// It classifies resources into new/old by hash and deletes:
// - unclaimed old resources (templates removed from XSet)
// - old resources if not retaining on scale and new version exists
//
// IMPORTANT: This should only be called when the target is being deleted or replaced.
// A template may be removed from the XSet while an existing target still references
// the previously created subresource until that target is recreated or updated.
// Calling this on active targets can delete in-use resources and break workloads.
func (sc *RealSubResourceControl) DeleteTargetUnusedResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error {
	targetID, exist := sc.labelAnnoMgr.Get(target, api.XInstanceIdLabelKey)
	if !exist {
		return nil
	}

	// Process each adapter type
	for _, adapter := range sc.adaptersByGVK {
		if err := sc.deleteUnusedResourcesForAdapter(ctx, xset, target, existing, targetID, adapter); err != nil {
			return err
		}
	}

	return nil
}

// DeleteTargetRecreateResources deletes subresources for a target that need recreation on update.
// Only deletes resources for adapters where RecreateWhenXSetUpdated returns true.
// This is called in the update phase BEFORE the pod is deleted, so that scale-out in the next
// reconcile creates fresh subresources without cache staleness issues.
func (sc *RealSubResourceControl) DeleteTargetRecreateResources(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState) error {
	for _, state := range sc.filterByTarget(existing, target) {
		if state.Adapter == nil || !state.Adapter.RecreateWhenXSetUpdated(xset) {
			continue
		}
		if err := sc.deleteResource(ctx, xset, state.Object, state.GVK); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to delete %s %s for recreation: %w", state.GVK.Kind, state.Object.GetName(), err)
		}
	}
	return nil
}

// deleteUnusedResourcesForAdapter handles unused resource deletion for a specific adapter.
func (sc *RealSubResourceControl) deleteUnusedResourcesForAdapter(ctx context.Context, xset api.XSetObject, target client.Object, existing []SubResourceState, targetID string, adapter api.SubResourceAdapter) error {
	gvk := adapter.Meta()

	// Classify resources by hash for this adapter
	newResources, oldResources, err := sc.classifyResourcesByHash(targetID, xset, adapter, existing)
	if err != nil {
		return fmt.Errorf("failed to classify %s: %w", gvk.Kind, err)
	}

	// Get template names that are in use
	templates, err := adapter.GetTemplates(xset)
	if err != nil {
		return fmt.Errorf("failed to get %s templates: %w", gvk.Kind, err)
	}
	templateNames := make(map[string]bool)
	for _, tmpl := range templates {
		templateNames[tmpl.Name] = true
	}

	// Delete unclaimed old resources (not in templates)
	for templateName, state := range oldResources {
		// If resource template is still in use, keep it
		if templateNames[templateName] {
			continue
		}

		if err := sc.deleteResource(ctx, xset, state.Object, state.GVK); err != nil {
			return fmt.Errorf("failed to delete unclaimed %s %s: %w", gvk.Kind, state.Object.GetName(), err)
		}
	}

	// Delete old resources if not retaining on scale and new version exists
	if !adapter.RetainWhenXSetScaled(xset) {
		for templateName, oldState := range oldResources {
			// Only delete if new version exists
			if _, hasNew := newResources[templateName]; hasNew {
				if err := sc.deleteResource(ctx, xset, oldState.Object, oldState.GVK); err != nil {
					return fmt.Errorf("failed to delete old %s %s: %w", gvk.Kind, oldState.Object.GetName(), err)
				}
			}
		}
	}

	return nil
}

// OrphanResource removes owner reference from a subresource.
// This is used when the XSet is being deleted but resources should be retained.
func (sc *RealSubResourceControl) OrphanResource(ctx context.Context, xset api.XSetObject, resource client.Object) error {
	xsetSpec := sc.xsetController.GetXSetSpec(xset)
	if xsetSpec.Selector.MatchLabels == nil {
		return nil
	}

	if resource.GetLabels() == nil {
		resource.SetLabels(make(map[string]string))
	}
	if resource.GetAnnotations() == nil {
		resource.SetAnnotations(make(map[string]string))
	}

	refWriter := refmanagerutil.NewOwnerRefWriter(sc.client)
	if err := refWriter.Release(ctx, xset, resource); err != nil {
		return fmt.Errorf("failed to orphan resource %s: %w", resource.GetName(), err)
	}

	return nil
}

// IsTargetTemplateChanged returns true if any subresource template changed for target,
// or if the target has resources that need recreation on update (when isUpdatedRevision is false).
func (r *RealSubResourceControl) IsTargetTemplateChanged(xset api.XSetObject, target client.Object, existing []SubResourceState, isUpdatedRevision bool) (bool, error) {
	targetID, exist := r.labelAnnoMgr.Get(target, api.XInstanceIdLabelKey)
	if !exist {
		return false, nil
	}

	for _, adapter := range r.adaptersByGVK {
		changed, err := r.isAdapterTemplateChanged(xset, target, targetID, adapter, existing)
		if err != nil {
			return false, err
		}
		if changed {
			return true, nil
		}
	}

	// Check if any resources need recreation on update (only for non-updated targets)
	if !isUpdatedRevision && r.hasRecreateOnUpdateResources(xset, target, existing) {
		return true, nil
	}

	return false, nil
}

// isAdapterTemplateChanged checks if template changed for a specific adapter.
// It compares template hashes on existing resources against current template hashes.
// Note: This does NOT check for missing resources (cache lag after creation can cause
// false positives). Missing resource detection is handled by RecreateWhenXSetUpdated
// and the update flow's DeleteTargetRecreateResources.
func (r *RealSubResourceControl) isAdapterTemplateChanged(xset api.XSetObject, target client.Object, targetID string, adapter api.SubResourceAdapter, existing []SubResourceState) (bool, error) {
	gvk := adapter.Meta()

	// Get current templates
	templates, err := adapter.GetTemplates(xset)
	if err != nil {
		return false, fmt.Errorf("failed to get templates from adapter %s: %w", gvk.Kind, err)
	}

	// Build map of template name to hash
	templateHashes := make(map[string]string)
	for _, tmpl := range templates {
		templateHashes[tmpl.Name] = tmpl.Hash
	}

	// Check existing resources for this target and adapter.
	// If any existing resource has a hash mismatch or references a removed template,
	// the template has changed.
	for _, state := range existing {
		if state.GVK != gvk {
			continue
		}

		// Skip resources being deleted
		if state.Object.GetDeletionTimestamp() != nil {
			continue
		}

		// Only check resources for this target
		resourceID, exist := r.labelAnnoMgr.Get(state.Object, api.XInstanceIdLabelKey)
		if !exist || resourceID != targetID {
			continue
		}

		// Get template name and hash from the resource
		templateName, exist := r.labelAnnoMgr.Get(state.Object, api.SubResourceTemplateLabelKey)
		if !exist {
			continue
		}

		resourceHash, exist := r.labelAnnoMgr.Get(state.Object, api.SubResourceTemplateHashLabelKey)
		if !exist {
			// No hash means we can't compare, treat as changed
			return true, nil
		}

		// Check if template still exists
		currentHash, templateExists := templateHashes[templateName]
		if !templateExists {
			// Template was removed, this is a change
			return true, nil
		}

		// Compare hashes
		if currentHash != resourceHash {
			return true, nil
		}
	}

	return false, nil
}

// CheckAllowIncludeExclude checks if target's subresources allow include/exclude.
// It finds subresources by owner reference + instance ID label and checks each using the provided CheckAllowFunc.
func (r *RealSubResourceControl) CheckAllowIncludeExclude(ctx context.Context, xset api.XSetObject, target client.Object, fn CheckAllowFunc) (bool, error) {
	xsetGVK := xset.GetObjectKind().GroupVersionKind()
	ownerName := xset.GetName()
	ownerKind := xsetGVK.Kind

	// Get all subresources owned by this XSet
	resources, err := r.GetFilteredResources(ctx, xset)
	if err != nil {
		return false, fmt.Errorf("failed to get subresources: %w", err)
	}

	// Filter to only those belonging to this target
	targetResources := r.filterByTarget(resources, target)

	// Check each subresource
	for _, state := range targetResources {
		if allowed, reason := fn(state.Object, ownerName, ownerKind, r.labelAnnoMgr); !allowed {
			return false, fmt.Errorf("subresource %s/%s does not allow include/exclude: %s", state.Object.GetNamespace(), state.Object.GetName(), reason)
		}
	}

	return true, nil
}

// AdoptTargetResources adopts subresources for a target during include operation.
// It finds orphaned resources by selector + orphaned label and adopts those that belong to this target.
func (sc *RealSubResourceControl) AdoptTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, instanceID string) error {
	for _, adapter := range sc.adaptersByGVK {
		// Find orphaned resources for this adapter
		orphaned, err := sc.findOrphanedResourcesForTarget(ctx, xset, adapter, target)
		if err != nil {
			return fmt.Errorf("failed to find orphaned %s: %w", adapter.Meta().Kind, err)
		}

		for _, res := range orphaned {
			// Update instance ID and remove orphaned label
			sc.labelAnnoMgr.Set(res, api.XInstanceIdLabelKey, instanceID)
			sc.labelAnnoMgr.Delete(res, api.XOrphanedIndicationLabelKey)
			if err := sc.adoptResource(ctx, xset, res); err != nil {
				return err
			}
		}
	}
	return nil
}

// findOrphanedResourcesForTarget finds orphaned subresources that belong to a specific target.
// It uses the PVC adapter (if available) to check which PVCs are mounted to the target.
func (sc *RealSubResourceControl) findOrphanedResourcesForTarget(ctx context.Context, xset api.XSetObject, adapter api.SubResourceAdapter, target client.Object) ([]client.Object, error) {
	xsetSpec := sc.xsetController.GetXSetSpec(xset)
	ownerSelector := xsetSpec.Selector.DeepCopy()
	if ownerSelector.MatchLabels == nil {
		ownerSelector.MatchLabels = map[string]string{}
	}
	ownerSelector.MatchLabels[sc.labelAnnoMgr.Value(api.ControlledByXSetLabel)] = "true"
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{
		Key:      sc.labelAnnoMgr.Value(api.XOrphanedIndicationLabelKey),
		Operator: metav1.LabelSelectorOpExists,
	})

	selector, err := metav1.LabelSelectorAsSelector(ownerSelector)
	if err != nil {
		return nil, err
	}

	gvk := adapter.Meta()
	list, err := sc.newListForGVK(gvk)
	if err != nil {
		return nil, nil
	}

	if err := sc.client.List(ctx, list, &client.ListOptions{
		Namespace:     xset.GetNamespace(),
		LabelSelector: selector,
	}); err != nil {
		return nil, fmt.Errorf("failed to list orphaned %s: %w", gvk.Kind, err)
	}

	items := extractListItems(list)
	var orphaned []client.Object
	for _, item := range items {
		// Skip if has owner reference (not truly orphaned)
		if len(item.GetOwnerReferences()) > 0 {
			continue
		}

		// For PVC adapter, check if this PVC is mounted to the target
		if gvk.Kind == "PersistentVolumeClaim" {
			if pvcAdapter, ok := sc.xsetController.(api.SubResourcePvcAdapter); ok {
				volumes := pvcAdapter.GetXSpecVolumes(target)
				isMounted := false
				for i := range volumes {
					if volumes[i].PersistentVolumeClaim != nil && volumes[i].PersistentVolumeClaim.ClaimName == item.GetName() {
						isMounted = true
						break
					}
				}
				if !isMounted {
					continue
				}
			}
		}

		orphaned = append(orphaned, item)
	}
	return orphaned, nil
}

// OrphanTargetResources orphans all subresources for a target during exclude operation.
// It finds subresources by owner reference + instance ID label and removes owner reference.
func (sc *RealSubResourceControl) OrphanTargetResources(ctx context.Context, xset api.XSetObject, target client.Object) error {
	// Get all subresources owned by this XSet
	resources, err := sc.GetFilteredResources(ctx, xset)
	if err != nil {
		return fmt.Errorf("failed to get subresources: %w", err)
	}

	// Filter to only those belonging to this target
	targetResources := sc.filterByTarget(resources, target)

	// Orphan each subresource
	for _, state := range targetResources {
		sc.labelAnnoMgr.Set(state.Object, api.XOrphanedIndicationLabelKey, "true")
		if err := sc.OrphanResource(ctx, xset, state.Object); err != nil {
			return err
		}
	}

	return nil
}
