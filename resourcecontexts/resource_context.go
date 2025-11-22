/*
Copyright 2023-2025 The KusionStack Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package resourcecontexts

import (
	"context"
	"fmt"
	"sort"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiservererrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	clientutil "kusionstack.io/kube-utils/client"
	"kusionstack.io/kube-utils/controller/expectations"
	"kusionstack.io/kube-utils/controller/mixin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
	"kusionstack.io/kube-xset/xcontrol"
)

type ResourceContextControl interface {
	AllocateID(ctx context.Context, xsetObject api.XSetObject, currentRevision, updatedRevision string, replicas int, objs []client.Object) (map[int]*api.ContextDetail, error)
	CleanUnusedIDs(ctx context.Context, xsetObject api.XSetObject, objs []client.Object) error
	UpdateToTargetContext(ctx context.Context, xsetObject api.XSetObject, ownedIDs map[int]*api.ContextDetail) error
	ExtractAvailableContexts(diff int, ownedIDs map[int]*api.ContextDetail, targetInstanceIDSet sets.Int) []*api.ContextDetail
	DecideContextRevisionAfterCreate(contextDetail *api.ContextDetail, updatedRevision *appsv1.ControllerRevision, createErr error) bool
	Get(detail *api.ContextDetail, enum api.ResourceContextKeyEnum) (string, bool)
	Contains(detail *api.ContextDetail, enum api.ResourceContextKeyEnum, value string) bool
	Put(detail *api.ContextDetail, enum api.ResourceContextKeyEnum, value string)
	Remove(detail *api.ContextDetail, enum api.ResourceContextKeyEnum)
}

type RealResourceContextControl struct {
	client.Client
	record.EventRecorder
	xsetController         api.XSetController
	resourceContextAdapter api.ResourceContextAdapter
	resourceContextKeys    map[api.ResourceContextKeyEnum]string
	resourceContextGVK     schema.GroupVersionKind
	cacheExpectations      expectations.CacheExpectationsInterface
	xsetLabelManager       api.XSetLabelAnnotationManager
}

func NewRealResourceContextControl(
	mixin *mixin.ReconcilerMixin,
	xsetController api.XSetController,
	resourceContextAdapter api.ResourceContextAdapter,
	resourceContextGVK schema.GroupVersionKind,
	cacheExpectations expectations.CacheExpectationsInterface,
	xsetLabelManager api.XSetLabelAnnotationManager,
) ResourceContextControl {
	resourceContextKeys := resourceContextAdapter.GetContextKeys()
	if resourceContextKeys == nil {
		resourceContextKeys = defaultResourceContextKeys
	}

	return &RealResourceContextControl{
		Client:                 mixin.Client,
		EventRecorder:          mixin.Recorder,
		xsetController:         xsetController,
		resourceContextAdapter: resourceContextAdapter,
		resourceContextKeys:    resourceContextKeys,
		resourceContextGVK:     resourceContextGVK,
		cacheExpectations:      cacheExpectations,
		xsetLabelManager:       xsetLabelManager,
	}
}

func (r *RealResourceContextControl) AllocateID(
	ctx context.Context,
	xsetObject api.XSetObject,
	currentRevision, updatedRevision string,
	replicas int, objs []client.Object,
) (map[int]*api.ContextDetail, error) {
	contextName := getContextName(r.xsetController, xsetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	notFound := false
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !apiservererrors.IsNotFound(err) {
			return nil, fmt.Errorf("fail to find ResourceContext %s/%s for owner %s: %w", xsetObject.GetNamespace(), contextName, xsetObject.GetName(), err)
		}

		notFound = true
		targetContext.SetNamespace(xsetObject.GetNamespace())
		targetContext.SetName(contextName)
	}

	xsetSpec := r.xsetController.GetXSetSpec(xsetObject)
	// store all the IDs crossing Multiple workload
	existingIDs := map[int]*api.ContextDetail{}
	// only store the IDs belonging to this owner
	ownedIDs := map[int]*api.ContextDetail{}
	resourceContextSpec := r.resourceContextAdapter.GetResourceContextSpec(targetContext)
	for i := range resourceContextSpec.Contexts {
		detail := &resourceContextSpec.Contexts[i]
		if r.Contains(detail, api.EnumOwnerContextKey, xsetObject.GetName()) {
			ownedIDs[detail.ID] = detail
			existingIDs[detail.ID] = detail
		} else if xsetSpec.ScaleStrategy.Context != "" {
			// add other collaset targetContexts only if context pool enabled
			existingIDs[detail.ID] = detail
		}
	}

	// get unrecorded model ids
	unRecordedIDs := r.getUnRecordTargetIDs(existingIDs, objs, currentRevision)

	// if owner has enough ID, return
	if len(ownedIDs) >= replicas && len(unRecordedIDs) == 0 {
		return ownedIDs, nil
	}

	ownedIDs = r.doAllocateID(ownedIDs, existingIDs, unRecordedIDs, replicas, xsetObject.GetName(), xsetSpec, currentRevision, updatedRevision)

	if notFound {
		return ownedIDs, r.doCreateTargetContext(ctx, xsetObject, ownedIDs)
	}

	return ownedIDs, r.doUpdateTargetContext(ctx, xsetObject, ownedIDs, targetContext)
}

func (r *RealResourceContextControl) CleanUnusedIDs(ctx context.Context, xsetObject api.XSetObject, objs []client.Object) error {
	contextName := getContextName(r.xsetController, xsetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xsetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !apiservererrors.IsNotFound(err) {
			return fmt.Errorf("fail to find ResourceContext %s/%s for owner %s: %w", xsetObject.GetNamespace(), contextName, xsetObject.GetName(), err)
		}
		return nil
	}

	resourceContextSpec := r.resourceContextAdapter.GetResourceContextSpec(targetContext)
	xsetSpec := r.xsetController.GetXSetSpec(xsetObject)
	ownedIDs := map[int]*api.ContextDetail{}
	currentIDs := map[int]struct{}{}
	var allowDeleteIDs []int
	var needCleanCount int

	for i := range resourceContextSpec.Contexts {
		detail := &resourceContextSpec.Contexts[i]
		if r.Contains(detail, api.EnumOwnerContextKey, xsetObject.GetName()) {
			ownedIDs[detail.ID] = detail
		}
	}
	needCleanCount = len(ownedIDs) - maxInt(int(ptr.Deref(xsetSpec.Replicas, 0)), len(objs))

	if needCleanCount <= 0 {
		return nil
	}

	for i := range objs {
		if id, err := xcontrol.GetInstanceID(r.xsetLabelManager, objs[i]); err == nil {
			currentIDs[id] = struct{}{}
		}
	}

	for i := range ownedIDs {
		id := ownedIDs[i].ID
		if _, exist := currentIDs[id]; exist {
			continue
		}
		allowDeleteIDs = append(allowDeleteIDs, id)
	}

	if len(allowDeleteIDs) == 0 {
		return nil
	}

	if len(allowDeleteIDs) < needCleanCount {
		needCleanCount = len(allowDeleteIDs)
	}

	deletedIDs := map[int]*api.ContextDetail{}
	for i := range needCleanCount {
		id := allowDeleteIDs[i]
		delete(ownedIDs, id)
		deletedIDs[id] = ownedIDs[id]
	}
	r.EventRecorder.Eventf(xsetObject, corev1.EventTypeWarning, "ResourceContextClean", "clean %v unused IDs from ResourceContext %s/%s", deletedIDs, xsetObject.GetNamespace(), contextName)
	return r.doUpdateTargetContext(ctx, xsetObject, ownedIDs, targetContext)
}

func (r *RealResourceContextControl) UpdateToTargetContext(
	ctx context.Context,
	xSetObject api.XSetObject,
	ownedIDs map[int]*api.ContextDetail,
) error {
	contextName := getContextName(r.xsetController, xSetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: xSetObject.GetNamespace(), Name: contextName}, targetContext); err != nil {
		if !apiservererrors.IsNotFound(err) {
			return fmt.Errorf("fail to find ResourceContext %s/%s: %w", xSetObject.GetNamespace(), contextName, err)
		}

		if len(ownedIDs) == 0 {
			return nil
		}

		if err := r.doCreateTargetContext(ctx, xSetObject, ownedIDs); err != nil {
			return fmt.Errorf("fail to create ResourceContext %s/%s after not found: %w", xSetObject.GetNamespace(), contextName, err)
		}
	}

	return r.doUpdateTargetContext(ctx, xSetObject, ownedIDs, targetContext)
}

func (r *RealResourceContextControl) ExtractAvailableContexts(diff int, ownedIDs map[int]*api.ContextDetail, targetInstanceIDSet sets.Int) []*api.ContextDetail {
	var availableContexts []*api.ContextDetail
	if diff <= 0 {
		return availableContexts
	}

	idx := 0
	for id := range ownedIDs {
		if _, inUsed := targetInstanceIDSet[id]; inUsed {
			continue
		}

		availableContexts = append(availableContexts, ownedIDs[id])
		idx++
		if idx == diff {
			break
		}
	}

	return availableContexts
}

// DecideContextRevisionAfterCreate decides revision for 3 target create types: (1) just create, (2) upgrade by recreate, (3) delete and recreate
func (r *RealResourceContextControl) DecideContextRevisionAfterCreate(contextDetail *api.ContextDetail, updatedRevision *appsv1.ControllerRevision, createErr error) bool {
	needUpdateContext := false
	if UnrecoverableCreateError(createErr) {
		// if target is just create or upgrade by recreate, change revisionKey to updatedRevision
		if r.Contains(contextDetail, api.EnumJustCreateContextDataKey, "true") ||
			r.Contains(contextDetail, api.EnumRecreateUpdateContextDataKey, "true") {
			r.Put(contextDetail, api.EnumRevisionContextDataKey, updatedRevision.GetName())
			r.Remove(contextDetail, api.EnumTargetDecorationRevisionKey)
			needUpdateContext = true
		}
		// if target is delete and recreate, never change revisionKey
	} else {
		r.Remove(contextDetail, api.EnumJustCreateContextDataKey)
		r.Remove(contextDetail, api.EnumRecreateUpdateContextDataKey)
		needUpdateContext = true
	}
	return needUpdateContext
}

// UnrecoverableCreateError checks a creation error is uncoverable or not
// A recoverable error can be recovered by retrying create, such as 409/429
// An unrecoverable error can only be recovered by updating the revision
func UnrecoverableCreateError(createErr error) bool {
	return apiservererrors.IsForbidden(createErr) ||
		apiservererrors.IsInvalid(createErr)
}

// DecideContextsRevisionBeforeCreate decides revision for newIDs contexts before create
//  1. if owner update strategy is byLabel, use defaultRevision for all contexts
//  2. if owner update strategy is byPartition, decide revisions by partition and
//     revisions in inCluster contexts
func (r *RealResourceContextControl) DecideContextsRevisionBeforeCreate(
	ownedIDs, newIDs map[int]*api.ContextDetail,
	spec *api.XSetSpec,
	currentRevision, updatedRevision string,
) {
	rollingUpdateStrategy := spec.UpdateStrategy.RollingUpdate
	if rollingUpdateStrategy == nil {
		for i := range newIDs {
			r.Put(newIDs[i], api.EnumRevisionContextDataKey, updatedRevision)
		}
		return
	}

	if rollingUpdateStrategy.ByLabel != nil {
		for i := range newIDs {
			r.Put(newIDs[i], api.EnumRevisionContextDataKey, currentRevision)
		}
		return
	}

	if rollingUpdateStrategy.ByPartition == nil || rollingUpdateStrategy.ByPartition.Partition == nil {
		for i := range newIDs {
			r.Put(newIDs[i], api.EnumRevisionContextDataKey, updatedRevision)
		}
		return
	}

	replicas := ptr.Deref(spec.Replicas, 0)
	partition := ptr.Deref(rollingUpdateStrategy.ByPartition.Partition, 0)
	var updatedReplicas int
	for i := range ownedIDs {
		if _, exist := r.Get(ownedIDs[i], api.EnumReplaceOriginTargetIDContextDataKey); exist {
			continue
		}
		if r.Contains(ownedIDs[i], api.EnumRevisionContextDataKey, updatedRevision) {
			updatedReplicas++
		}
	}
	for i := range newIDs {
		if i < int(replicas-partition)-updatedReplicas {
			r.Put(newIDs[i], api.EnumRevisionContextDataKey, updatedRevision)
		} else {
			r.Put(newIDs[i], api.EnumRevisionContextDataKey, currentRevision)
		}
	}
}

func (r *RealResourceContextControl) Get(detail *api.ContextDetail, enum api.ResourceContextKeyEnum) (string, bool) {
	return detail.Get(r.resourceContextKeys[enum])
}

func (r *RealResourceContextControl) Contains(detail *api.ContextDetail, enum api.ResourceContextKeyEnum, value string) bool {
	return detail.Contains(r.resourceContextKeys[enum], value)
}

func (r *RealResourceContextControl) Put(detail *api.ContextDetail, enum api.ResourceContextKeyEnum, value string) {
	detail.Put(r.resourceContextKeys[enum], value)
}

func (r *RealResourceContextControl) Remove(detail *api.ContextDetail, enum api.ResourceContextKeyEnum) {
	detail.Remove(r.resourceContextKeys[enum])
}

func (r *RealResourceContextControl) doCreateTargetContext(
	ctx context.Context,
	xSetObject api.XSetObject,
	ownerIDs map[int]*api.ContextDetail,
) error {
	contextName := getContextName(r.xsetController, xSetObject)
	targetContext := r.resourceContextAdapter.NewResourceContext()
	targetContext.SetNamespace(xSetObject.GetNamespace())
	targetContext.SetName(contextName)

	spec := &api.ResourceContextSpec{}
	for i := range ownerIDs {
		spec.Contexts = append(spec.Contexts, *ownerIDs[i])
	}
	sort.Sort(ContextDetailsByOrder(spec.Contexts))
	r.resourceContextAdapter.SetResourceContextSpec(spec, targetContext)
	if err := r.Client.Create(ctx, targetContext); err != nil {
		return err
	}
	return r.cacheExpectations.ExpectCreation(clientutil.ObjectKeyString(xSetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName())
}

func (r *RealResourceContextControl) doUpdateTargetContext(
	ctx context.Context,
	xsetObject client.Object,
	ownedIDs map[int]*api.ContextDetail,
	targetContext api.ResourceContextObject,
) error {
	// store all IDs crossing all workload
	existingIDs := map[int]*api.ContextDetail{}

	// add other collaset targetContexts only if context pool enabled
	xsetSpec := r.xsetController.GetXSetSpec(xsetObject)
	resourceContextSpec := r.resourceContextAdapter.GetResourceContextSpec(targetContext)
	ownerContextKey := r.resourceContextKeys[api.EnumOwnerContextKey]
	if xsetSpec.ScaleStrategy.Context != "" {
		for i := range resourceContextSpec.Contexts {
			detail := resourceContextSpec.Contexts[i]
			if detail.Contains(ownerContextKey, xsetObject.GetName()) {
				continue
			}
			existingIDs[detail.ID] = &detail
		}
	}

	for _, contextDetail := range ownedIDs {
		existingIDs[contextDetail.ID] = contextDetail
	}

	// delete TargetContext if it is empty
	if len(existingIDs) == 0 {
		err := r.Client.Delete(ctx, targetContext)
		if err != nil {
			return err
		}
		return r.cacheExpectations.ExpectDeletion(clientutil.ObjectKeyString(xsetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName())
	}

	resourceContextSpec.Contexts = make([]api.ContextDetail, len(existingIDs))
	idx := 0
	for _, contextDetail := range existingIDs {
		resourceContextSpec.Contexts[idx] = *contextDetail
		idx++
	}

	// keep context detail in order by ID
	sort.Sort(ContextDetailsByOrder(resourceContextSpec.Contexts))
	r.resourceContextAdapter.SetResourceContextSpec(resourceContextSpec, targetContext)
	err := r.Client.Update(ctx, targetContext)
	if err != nil {
		return err
	}
	return r.cacheExpectations.ExpectUpdation(clientutil.ObjectKeyString(xsetObject), r.resourceContextGVK, targetContext.GetNamespace(), targetContext.GetName(), targetContext.GetResourceVersion())
}

// getUnRecordTargetIDs get ids which are used by targets but not recorded in ResourceContext
func (r *RealResourceContextControl) getUnRecordTargetIDs(existingIDs map[int]*api.ContextDetail, objs []client.Object, defaultRevision string) map[int]string {
	unRecordIDs := make(map[int]string)
	for i := range objs {
		if objs[i].GetDeletionTimestamp() != nil {
			continue
		}
		// should not create ids for new pod
		if _, exist := objs[i].GetLabels()[r.xsetLabelManager.Value(api.XReplacePairOriginName)]; exist {
			continue
		}
		if id, err := xcontrol.GetInstanceID(r.xsetLabelManager, objs[i]); err == nil {
			if _, exist := existingIDs[id]; !exist {
				unRecordIDs[id] = xcontrol.GetTargetRevision(objs[i], defaultRevision)
			}
		}
	}
	return unRecordIDs
}

func (r *RealResourceContextControl) doAllocateID(
	ownedIDs, existingIDs map[int]*api.ContextDetail,
	unRecordIDs map[int]string,
	replicas int, ownerName string,
	spec *api.XSetSpec,
	currentRevision, updatedRevision string,
) map[int]*api.ContextDetail {
	// first add unRecorded ids into ownedIDs
	r.addUnrecordedIDs(ownedIDs, unRecordIDs, ownerName)

	// find new IDs for owner to fulfill replicas
	newIDs := r.allocateNewIDs(ownedIDs, existingIDs, replicas, ownerName)

	// decide revision for newIDs
	r.DecideContextsRevisionBeforeCreate(ownedIDs, newIDs, spec, currentRevision, updatedRevision)

	for k, v := range newIDs {
		ownedIDs[k] = v
	}
	return ownedIDs
}

// addUnrecordedIDs add unrecorded ids to ownedIDs, using revision from target
func (r *RealResourceContextControl) addUnrecordedIDs(ownedIDs map[int]*api.ContextDetail, unRecordIDs map[int]string, ownerName string) {
	for id, revision := range unRecordIDs {
		detail := &api.ContextDetail{
			ID: id,
			Data: map[string]string{
				r.resourceContextKeys[api.EnumOwnerContextKey]:          ownerName,
				r.resourceContextKeys[api.EnumRevisionContextDataKey]:   revision,
				r.resourceContextKeys[api.EnumJustCreateContextDataKey]: "true",
			},
		}
		ownedIDs[id] = detail
	}
}

// allocateNewIDs fulfill ids for ownedIDs in order to meet replicas
func (r *RealResourceContextControl) allocateNewIDs(ownedIDs, existingIDs map[int]*api.ContextDetail, replicas int, ownerName string) map[int]*api.ContextDetail {
	// use new ids from 0 inorder
	var newIDs []int
	for id := 0; ; id++ {
		if len(newIDs) >= replicas-len(ownedIDs) {
			break
		}
		if _, exist := existingIDs[id]; exist {
			continue
		}
		newIDs = append(newIDs, id)
	}

	// fulfill ownedIDs
	newOwnerIDs := make(map[int]*api.ContextDetail)
	for i := range newIDs {
		detail := &api.ContextDetail{
			ID: newIDs[i],
			// TODO choose just create target' revision according to scaleStrategy
			Data: map[string]string{
				r.resourceContextKeys[api.EnumOwnerContextKey]:          ownerName,
				r.resourceContextKeys[api.EnumJustCreateContextDataKey]: "true",
			},
		}
		newOwnerIDs[newIDs[i]] = detail
	}
	return newOwnerIDs
}

func getContextName(xsetControl api.XSetController, instance api.XSetObject) string {
	spec := xsetControl.GetXSetSpec(instance)
	if spec.ScaleStrategy.Context != "" {
		return spec.ScaleStrategy.Context
	}

	return instance.GetName()
}

type ContextDetailsByOrder []api.ContextDetail

func (s ContextDetailsByOrder) Len() int      { return len(s) }
func (s ContextDetailsByOrder) Swap(i, j int) { s[i], s[j] = s[j], s[i] }

func (s ContextDetailsByOrder) Less(i, j int) bool {
	l, r := s[i], s[j]
	return l.ID < r.ID
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
