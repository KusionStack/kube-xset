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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
// It replaces the legacy PvcControl with a generic interface.
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

	// Build GVK index
	adaptersByGVK := make(map[schema.GroupVersionKind]api.SubResourceAdapter)
	for _, adapter := range adapters {
		adaptersByGVK[adapter.Meta()] = adapter
	}

	// Set up cache indexes for all adapter GVKs
	if err := setUpCacheForAdapters(mixin.Cache, adapters, xsetController); err != nil {
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
func setUpCacheForAdapters(cache cache.Cache, adapters []api.SubResourceAdapter, controller api.XSetController) error {
	for _, adapter := range adapters {
		gvk := adapter.Meta()
		obj := newObjectForGVK(gvk)
		if obj == nil {
			continue // Skip unknown GVKs
		}
		if err := cache.IndexField(context.TODO(), obj, FieldIndexOwnerRefUID, func(object client.Object) []string {
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

// newObjectForGVK creates a new object for the given GVK.
func newObjectForGVK(gvk schema.GroupVersionKind) client.Object {
	switch gvk {
	case corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"):
		return &corev1.PersistentVolumeClaim{}
	case corev1.SchemeGroupVersion.WithKind("Service"):
		return &corev1.Service{}
	default:
		// Fallback: try to create via scheme if available
		return nil
	}
}

// GetFilteredResources lists all subresources owned by the XSet.
func (sc *RealSubResourceControl) GetFilteredResources(ctx context.Context, xset api.XSetObject) ([]SubResourceState, error) {
	var resources []SubResourceState

	for _, adapter := range sc.adaptersByGVK {
		gvk := adapter.Meta()
		list := sc.newListForGVK(gvk)
		if list == nil {
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

// newListForGVK creates a new list object for the given GVK.
func (sc *RealSubResourceControl) newListForGVK(gvk schema.GroupVersionKind) client.ObjectList {
	switch gvk {
	case corev1.SchemeGroupVersion.WithKind("PersistentVolumeClaim"):
		return &corev1.PersistentVolumeClaimList{}
	case corev1.SchemeGroupVersion.WithKind("Service"):
		return &corev1.ServiceList{}
	default:
		return nil
	}
}

// extractListItems extracts items from a list object.
func extractListItems(list client.ObjectList) []client.Object {
	switch l := list.(type) {
	case *corev1.PersistentVolumeClaimList:
		items := make([]client.Object, len(l.Items))
		for i := range l.Items {
			items[i] = &l.Items[i]
		}
		return items
	case *corev1.ServiceList:
		items := make([]client.Object, len(l.Items))
		for i := range l.Items {
			items[i] = &l.Items[i]
		}
		return items
	default:
		return nil
	}
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
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{
		Key:      sc.labelAnnoMgr.Value(api.XOrphanedIndicationLabelKey),
		Operator: metav1.LabelSelectorOpDoesNotExist,
	})
	ownerSelector.MatchExpressions = append(ownerSelector.MatchExpressions, metav1.LabelSelectorRequirement{
		Key:      sc.labelAnnoMgr.Value(api.XInstanceIdLabelKey),
		Operator: metav1.LabelSelectorOpExists,
	})

	selector, err := metav1.LabelSelectorAsSelector(ownerSelector)
	if err != nil {
		return nil, err
	}

	gvk := adapter.Meta()
	list := sc.newListForGVK(gvk)
	if list == nil {
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
		resourceHash, exist := sc.labelAnnoMgr.Get(state.Object, api.SubResourcePvcTemplateHashLabelKey)
		if !exist {
			continue
		}

		templateName, _ := sc.labelAnnoMgr.Get(state.Object, api.SubResourcePvcTemplateLabelKey)

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
func (sc *RealSubResourceControl) deleteResource(ctx context.Context, xset api.XSetObject, resource client.Object, gvk schema.GroupVersionKind) error {
	// Remove finalizers before deleting to ensure immediate removal from etcd.
	// Without this, resources with finalizers (e.g., PVCs with kubernetes.io/pvc-protection)
	// would only get DeletionTimestamp set but remain in etcd until the finalizer controller runs.
	if len(resource.GetFinalizers()) > 0 {
		patch := client.MergeFrom(resource.DeepCopyObject().(client.Object))
		resource.SetFinalizers(nil)
		if err := sc.client.Patch(ctx, resource, patch); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("failed to remove finalizers from %s %s: %w", gvk.Kind, resource.GetName(), err)
		}
	}

	if err := sc.client.Delete(ctx, resource); err != nil {
		return err
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
			templateName, _ := sc.labelAnnoMgr.Get(state.Object, api.SubResourcePvcTemplateLabelKey)
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
			existingHash, _ := sc.labelAnnoMgr.Get(existing.Object, api.SubResourcePvcTemplateHashLabelKey)
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

		// Create new resource
		resource, err := adapter.BuildResource(ctx, xset, template, target, targetID)
		if err != nil {
			return fmt.Errorf("failed to build %s from template %s: %w", gvk.Kind, template.Name, err)
		}

		if err := sc.client.Create(ctx, resource); err != nil {
			if apierrors.IsAlreadyExists(err) {
				// Resource already exists — fetch and check state
				existingResource := newObjectForGVK(gvk)
				if existingResource != nil {
					if getErr := sc.client.Get(ctx, client.ObjectKey{
						Namespace: resource.GetNamespace(),
						Name:      resource.GetName(),
					}, existingResource); getErr != nil {
						return fmt.Errorf("failed to get existing %s %s: %w", gvk.Kind, resource.GetName(), getErr)
					}
					// If the existing resource is being deleted, help it along by removing finalizers
					if existingResource.GetDeletionTimestamp() != nil {
						if len(existingResource.GetFinalizers()) > 0 {
							patch := client.MergeFrom(existingResource.DeepCopyObject().(client.Object))
							existingResource.SetFinalizers(nil)
							if patchErr := sc.client.Patch(ctx, existingResource, patch); patchErr != nil && !apierrors.IsNotFound(patchErr) {
								return fmt.Errorf("failed to remove finalizers from dying %s %s: %w", gvk.Kind, resource.GetName(), patchErr)
							}
						}
						// Return error to requeue — the resource will be gone in the next reconcile
						return fmt.Errorf("%s %s is being deleted, will retry on next reconcile", gvk.Kind, resource.GetName())
					}
					createdResources = append(createdResources, existingResource)
					continue
				}
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
// - unclaimed old resources
// - old resources if not retaining on scale
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

	// Get attached resource names from target
	attachedNames, err := adapter.GetAttachedResourceNames(target)
	if err != nil {
		return fmt.Errorf("failed to get attached %s names: %w", gvk.Kind, err)
	}
	attachedSet := make(map[string]bool)
	for _, name := range attachedNames {
		attachedSet[name] = true
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

	// Delete unclaimed old resources (not mounted and not in templates)
	for templateName, state := range oldResources {
		// If resource is still attached/mounted, keep it
		if attachedSet[templateName] {
			continue
		}

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
		templateName, exist := r.labelAnnoMgr.Get(state.Object, api.SubResourcePvcTemplateLabelKey)
		if !exist {
			continue
		}

		resourceHash, exist := r.labelAnnoMgr.Get(state.Object, api.SubResourcePvcTemplateHashLabelKey)
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
// It iterates through all adapters and checks each attached subresource using the provided CheckAllowFunc.
func (r *RealSubResourceControl) CheckAllowIncludeExclude(ctx context.Context, xset api.XSetObject, target client.Object, fn CheckAllowFunc) (bool, error) {
	xsetGVK := xset.GetObjectKind().GroupVersionKind()
	ownerName := xset.GetName()
	ownerKind := xsetGVK.Kind

	for _, adapter := range r.adaptersByGVK {
		// Get attached resource names from target
		attachedNames, err := adapter.GetAttachedResourceNames(target)
		if err != nil {
			return false, fmt.Errorf("failed to get attached resource names for adapter %s: %w", adapter.Meta().Kind, err)
		}

		// Check each attached resource
		for _, resourceName := range attachedNames {
			// Get the subresource object
			gvk := adapter.Meta()
			subResource := newObjectForGVK(gvk)
			if subResource == nil {
				continue
			}

			if err := r.client.Get(ctx, client.ObjectKey{
				Namespace: target.GetNamespace(),
				Name:      resourceName,
			}, subResource); err != nil {
				// If subresource not found, ignore it (might be filtered by controller-mesh)
				continue
			}

			// Check if this subresource allows include/exclude
			if allowed, reason := fn(subResource, ownerName, ownerKind, r.labelAnnoMgr); !allowed {
				return false, fmt.Errorf("subresource %s/%s does not allow include/exclude: %s", subResource.GetNamespace(), subResource.GetName(), reason)
			}
		}
	}

	return true, nil
}

// AdoptTargetResources adopts subresources for a target during include operation.
// It finds attached resources by name and sets owner reference + instance ID label.
func (sc *RealSubResourceControl) AdoptTargetResources(ctx context.Context, xset api.XSetObject, target client.Object, instanceID string) error {
	for _, adapter := range sc.adaptersByGVK {
		names, err := adapter.GetAttachedResourceNames(target)
		if err != nil {
			return fmt.Errorf("failed to get attached resource names for adapter %s: %w", adapter.Meta().Kind, err)
		}
		gvk := adapter.Meta()
		for _, name := range names {
			resource := newObjectForGVK(gvk)
			if resource == nil {
				continue
			}
			if err := sc.client.Get(ctx, client.ObjectKey{
				Namespace: target.GetNamespace(),
				Name:      name,
			}, resource); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			sc.labelAnnoMgr.Set(resource, api.XInstanceIdLabelKey, instanceID)
			sc.labelAnnoMgr.Delete(resource, api.XOrphanedIndicationLabelKey)
			if err := sc.adoptResource(ctx, xset, resource); err != nil {
				return err
			}
		}
	}
	return nil
}

// OrphanTargetResources orphans all subresources for a target during exclude operation.
// It finds attached resources by name and removes owner reference.
func (sc *RealSubResourceControl) OrphanTargetResources(ctx context.Context, xset api.XSetObject, target client.Object) error {
	for _, adapter := range sc.adaptersByGVK {
		names, err := adapter.GetAttachedResourceNames(target)
		if err != nil {
			return fmt.Errorf("failed to get attached resource names for adapter %s: %w", adapter.Meta().Kind, err)
		}
		gvk := adapter.Meta()
		for _, name := range names {
			resource := newObjectForGVK(gvk)
			if resource == nil {
				continue
			}
			if err := sc.client.Get(ctx, client.ObjectKey{
				Namespace: target.GetNamespace(),
				Name:      name,
			}, resource); err != nil {
				if apierrors.IsNotFound(err) {
					continue
				}
				return err
			}
			sc.labelAnnoMgr.Set(resource, api.XOrphanedIndicationLabelKey, "true")
			if err := sc.OrphanResource(ctx, xset, resource); err != nil {
				return err
			}
		}
	}
	return nil
}