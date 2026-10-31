/*
 * Copyright 2024-2026 KusionStack Authors.
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

package synccontrols

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/go-logr/logr"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/tools/record"
	"kusionstack.io/kube-utils/controller/mixin"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
	xsetpatches "kusionstack.io/kube-xset/patches"
)

func TestScaleLegacyCreateFailureRecoversToUpdatedRevision(t *testing.T) {
	replicas := int32(1)
	controller := &syncControlTestController{spec: api.XSetSpec{Replicas: &replicas}}
	owner := syncControlTestOwner()
	oldRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "old"}}
	updatedRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "fixed"}}
	contextDetail := &api.ContextDetail{ID: 0, Data: map[string]string{
		testContextKey(api.EnumRevisionContextDataKey):   oldRevision.Name,
		testContextKey(api.EnumJustCreateContextDataKey): "true",
	}}
	resourceContexts := &syncControlTestResourceContexts{}
	control := &RealSyncControl{
		ReconcilerMixin: mixin.ReconcilerMixin{Recorder: record.NewFakeRecorder(10)},
		xControl: &syncControlTestTargetControl{createErr: apierrors.NewForbidden(
			schema.GroupResource{Resource: "pods"}, "", errors.New("old template is invalid"),
		)},
		xsetController:         controller,
		xsetLabelAnnoMgr:       api.NewXSetLabelAnnotationManager(nil),
		resourceContextControl: resourceContexts,
		xsetGVK:                schema.GroupVersionKind{Group: "apps.kusionstack.io", Version: "v1", Kind: "TestSet"},
		targetGVK:              corev1.SchemeGroupVersion.WithKind("Pod"),
	}
	syncContext := &SyncContext{
		Revisions:       []*appsv1.ControllerRevision{oldRevision, updatedRevision},
		CurrentRevision: oldRevision,
		UpdatedRevision: updatedRevision,
		OwnedIds:        map[int]*api.ContextDetail{0: contextDetail},
		CurrentIDs:      sets.NewInt(),
		activeTargets:   nil,
		replacingMap:    map[string]*TargetWrapper{},
		NewStatus:       &api.XSetStatus{},
	}

	if _, _, err := control.Scale(logr.NewContext(context.Background(), logr.Discard()), owner, syncContext); err == nil {
		t.Fatal("Scale() error = nil, want unrecoverable create error")
	}
	if resourceContexts.decidedRevision != updatedRevision.Name {
		t.Fatalf("DecideContextRevisionAfterCreate() revision = %q, want legacy UpdatedRevision %q", resourceContexts.decidedRevision, updatedRevision.Name)
	}
}

func TestScaleConcurrentPatchCreateFailureRetainsIdentityRevision(t *testing.T) {
	replicas := int32(1)
	controller := &syncControlTestController{spec: api.XSetSpec{Replicas: &replicas}}
	owner := syncControlTestOwner()
	baseRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "base"}}
	headRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "head"}}
	identityRevision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "base-plus-p1"}}
	contextDetail := &api.ContextDetail{ID: 0, Data: map[string]string{
		testContextKey(api.EnumRevisionContextDataKey):   identityRevision.Name,
		testContextKey(api.EnumJustCreateContextDataKey): "true",
	}}
	resourceContexts := &syncControlTestResourceContexts{}
	control := &RealSyncControl{
		ReconcilerMixin: mixin.ReconcilerMixin{Recorder: record.NewFakeRecorder(10)},
		xControl: &syncControlTestTargetControl{createErr: apierrors.NewForbidden(
			schema.GroupResource{Resource: "pods"}, "", errors.New("combination template is invalid"),
		)},
		xsetController:         controller,
		xsetLabelAnnoMgr:       api.NewXSetLabelAnnotationManager(nil),
		resourceContextControl: resourceContexts,
		xsetGVK:                schema.GroupVersionKind{Group: "apps.kusionstack.io", Version: "v1", Kind: "TestSet"},
		targetGVK:              corev1.SchemeGroupVersion.WithKind("Pod"),
	}
	syncContext := &SyncContext{
		Revisions:       []*appsv1.ControllerRevision{baseRevision, headRevision, identityRevision},
		CurrentRevision: baseRevision,
		UpdatedRevision: headRevision,
		OwnedIds:        map[int]*api.ContextDetail{0: contextDetail},
		CurrentIDs:      sets.NewInt(),
		replacingMap:    map[string]*TargetWrapper{},
		MultiPatch:      &MultiPatchSyncContext{},
		NewStatus:       &api.XSetStatus{},
	}

	if _, _, err := control.Scale(logr.NewContext(context.Background(), logr.Discard()), owner, syncContext); err == nil {
		t.Fatal("Scale() error = nil, want unrecoverable create error")
	}
	if resourceContexts.decidedRevision != identityRevision.Name {
		t.Fatalf("DecideContextRevisionAfterCreate() revision = %q, want identity revision %q", resourceContexts.decidedRevision, identityRevision.Name)
	}
}

func TestUpdateConcurrentPatchFinishesLifecycleAfterRevisionReached(t *testing.T) {
	originalFactory := NewInPlaceOnlyUpdaterFunc
	defer func() { NewInPlaceOnlyUpdaterFunc = originalFactory }()

	updater := &syncControlTestUpdater{finishStatus: true}
	NewInPlaceOnlyUpdaterFunc = func() TargetUpdater { return updater }
	replicas := int32(1)
	controller := &syncControlTestController{
		spec: api.XSetSpec{
			Replicas: &replicas,
			UpdateStrategy: api.UpdateStrategy{
				UpdatePolicy: api.XSetInPlaceOnlyTargetUpdateStrategyType,
			},
		},
		plan: &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches},
	}
	manager := api.NewXSetLabelAnnotationManager(nil)
	lifecycle := &syncControlTestLifecycle{id: "test-update", operationType: api.OpsLifecycleTypeUpdate}
	revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "base-plus-p1"}}
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Namespace: "default",
		Name:      "target-0",
		Labels: map[string]string{
			appsv1.ControllerRevisionHashLabelKey:                            revision.Name,
			manager.Value(api.OperatingLabelPrefix) + "/" + lifecycle.id:     "1",
			manager.Value(api.OperationTypeLabelPrefix) + "/" + lifecycle.id: string(api.OpsLifecycleTypeUpdate),
			manager.Value(api.OperateLabelPrefix) + "/" + lifecycle.id:       "1",
		},
	}}
	control := &RealSyncControl{
		ReconcilerMixin:        mixin.ReconcilerMixin{Recorder: record.NewFakeRecorder(10)},
		xsetController:         controller,
		xsetLabelAnnoMgr:       manager,
		resourceContextControl: &syncControlTestResourceContexts{},
		updateLifecycleAdapter: lifecycle,
		updateConfig: &UpdateConfig{
			XsetController:   controller,
			XsetLabelAnnoMgr: manager,
		},
	}
	syncContext := &SyncContext{
		Revisions:       []*appsv1.ControllerRevision{revision},
		CurrentRevision: revision,
		UpdatedRevision: revision,
		TargetWrappers: []*TargetWrapper{{
			Object:            target,
			ID:                0,
			IsDuringUpdateOps: true,
		}},
		OwnedIds: map[int]*api.ContextDetail{0: {ID: 0}},
		MultiPatch: &MultiPatchSyncContext{
			InstancePlans:    map[int]xsetpatches.InstancePlan{0: {ID: 0}},
			DesiredRevisions: map[int]*appsv1.ControllerRevision{0: revision},
		},
		NewStatus: &api.XSetStatus{},
	}

	if _, _, err := control.Update(logr.NewContext(context.Background(), logr.Discard()), syncControlTestOwner(), syncContext); err != nil {
		t.Fatalf("Update() error = %v", err)
	}
	if updater.finishStatusCalls != 1 {
		t.Fatalf("GetTargetUpdateFinishStatus() calls = %d, want 1", updater.finishStatusCalls)
	}
	if updater.finishCalls != 1 || updater.finishedByCancel {
		t.Fatalf("FinishUpdateTarget() calls=%d cancel=%v, want one graceful finish", updater.finishCalls, updater.finishedByCancel)
	}
}

type syncControlTestController struct {
	spec   api.XSetSpec
	status api.XSetStatus
	plan   *api.RevisionPlan
}

func (c *syncControlTestController) ControllerName() string { return "test-xset" }
func (c *syncControlTestController) FinalizerName() string  { return "test-xset/finalizer" }
func (c *syncControlTestController) XSetMeta() metav1.TypeMeta {
	return metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}
}
func (c *syncControlTestController) XMeta() metav1.TypeMeta {
	return metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"}
}
func (c *syncControlTestController) NewXSetObject() api.XSetObject { return &corev1.ConfigMap{} }
func (c *syncControlTestController) NewXObject() client.Object     { return &corev1.Pod{} }
func (c *syncControlTestController) NewXObjectList() client.ObjectList {
	return &corev1.PodList{}
}
func (c *syncControlTestController) GetXSetSpec(api.XSetObject) *api.XSetSpec { return &c.spec }
func (c *syncControlTestController) GetXSetPatch(metav1.Object) ([]byte, error) {
	return nil, nil
}
func (c *syncControlTestController) GetXSetStatus(api.XSetObject) *api.XSetStatus {
	return &c.status
}
func (c *syncControlTestController) SetXSetStatus(_ api.XSetObject, status *api.XSetStatus) {
	c.status = *status
}
func (c *syncControlTestController) UpdateScaleStrategy(context.Context, client.Client, api.XSetObject, *api.ScaleStrategy) error {
	return nil
}
func (c *syncControlTestController) GetXSetTemplatePatcher(metav1.Object) func(client.Object) error {
	return func(client.Object) error { return nil }
}
func (c *syncControlTestController) GetXObjectFromRevision(*appsv1.ControllerRevision) (client.Object, error) {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{}}}, nil
}
func (c *syncControlTestController) CheckScheduled(client.Object) bool { return true }
func (c *syncControlTestController) CheckReadyTime(client.Object) (bool, *metav1.Time) {
	return true, nil
}
func (c *syncControlTestController) CheckAvailable(client.Object) bool { return true }
func (c *syncControlTestController) CheckInactive(client.Object) bool  { return false }
func (c *syncControlTestController) GetXOpsPriority(context.Context, client.Client, client.Object) (*api.OpsPriority, error) {
	return nil, nil
}
func (c *syncControlTestController) GetRevisionPlan(api.XSetObject) *api.RevisionPlan {
	return c.plan
}
func (c *syncControlTestController) SetRevisionPlan(_ api.XSetObject, plan *api.RevisionPlan) {
	c.plan = plan
}
func (c *syncControlTestController) GetMultiPatchStatus(api.XSetObject) *api.MultiPatchRevisionStatus {
	return nil
}
func (c *syncControlTestController) SetMultiPatchStatus(api.XSetObject, *api.MultiPatchRevisionStatus) {
}
func (c *syncControlTestController) BuildMultiPatchRevision(*appsv1.ControllerRevision, []api.PatchRevision, string) (*appsv1.ControllerRevision, error) {
	return nil, nil
}
func (c *syncControlTestController) MultiPatchCommandLabelPrefix() string { return "test/patch-" }

type syncControlTestTargetControl struct{ createErr error }

func (*syncControlTestTargetControl) GetFilteredTargets(context.Context, *metav1.LabelSelector, api.XSetObject) ([]client.Object, []client.Object, error) {
	return nil, nil, nil
}
func (c *syncControlTestTargetControl) CreateTarget(_ context.Context, target client.Object) (client.Object, error) {
	return target, c.createErr
}
func (*syncControlTestTargetControl) DeleteTarget(context.Context, client.Object) error { return nil }
func (*syncControlTestTargetControl) UpdateTarget(context.Context, client.Object) error { return nil }
func (*syncControlTestTargetControl) PatchTarget(context.Context, client.Object, client.Patch) error {
	return nil
}
func (*syncControlTestTargetControl) OrphanTarget(context.Context, api.XSetObject, client.Object) error {
	return nil
}
func (*syncControlTestTargetControl) AdoptTarget(context.Context, api.XSetObject, client.Object) error {
	return nil
}

type syncControlTestResourceContexts struct {
	decidedRevision string
}

func (*syncControlTestResourceContexts) AllocateID(context.Context, api.XSetObject, string, string, int, []client.Object) (map[int]*api.ContextDetail, error) {
	return nil, errors.New("unexpected AllocateID call")
}
func (*syncControlTestResourceContexts) CleanUnusedIDs(context.Context, api.XSetObject, []client.Object) error {
	return nil
}
func (*syncControlTestResourceContexts) UpdateToTargetContext(context.Context, api.XSetObject, map[int]*api.ContextDetail) error {
	return nil
}
func (*syncControlTestResourceContexts) ExtractAvailableContexts(want int, owned map[int]*api.ContextDetail, used sets.Int) []*api.ContextDetail {
	result := make([]*api.ContextDetail, 0, want)
	for id, detail := range owned {
		if used.Has(id) {
			continue
		}
		result = append(result, detail)
		if len(result) == want {
			break
		}
	}
	return result
}
func (c *syncControlTestResourceContexts) DecideContextRevisionAfterCreate(_ *api.ContextDetail, revision *appsv1.ControllerRevision, _ error) bool {
	c.decidedRevision = revision.Name
	return true
}
func (*syncControlTestResourceContexts) Get(detail *api.ContextDetail, key api.ResourceContextKeyEnum) (string, bool) {
	return detail.Get(testContextKey(key))
}
func (*syncControlTestResourceContexts) Contains(detail *api.ContextDetail, key api.ResourceContextKeyEnum, value string) bool {
	return detail.Contains(testContextKey(key), value)
}
func (*syncControlTestResourceContexts) Put(detail *api.ContextDetail, key api.ResourceContextKeyEnum, value string) {
	detail.Put(testContextKey(key), value)
}
func (*syncControlTestResourceContexts) Remove(detail *api.ContextDetail, key api.ResourceContextKeyEnum) {
	detail.Remove(testContextKey(key))
}

func testContextKey(key api.ResourceContextKeyEnum) string { return strconv.Itoa(int(key)) }

type syncControlTestLifecycle struct {
	id            string
	operationType api.OperationType
}

func (a *syncControlTestLifecycle) GetID() string                        { return a.id }
func (a *syncControlTestLifecycle) GetType() api.OperationType           { return a.operationType }
func (*syncControlTestLifecycle) AllowMultiType() bool                   { return false }
func (*syncControlTestLifecycle) WhenBegin(client.Object) (bool, error)  { return false, nil }
func (*syncControlTestLifecycle) WhenFinish(client.Object) (bool, error) { return false, nil }

type syncControlTestUpdater struct {
	finishStatus      bool
	finishStatusCalls int
	finishCalls       int
	finishedByCancel  bool
}

func (*syncControlTestUpdater) Setup(*UpdateConfig, api.XSetObject) {}
func (*syncControlTestUpdater) FulfillTargetUpdatedInfo(context.Context, *appsv1.ControllerRevision, *TargetUpdateInfo) error {
	return nil
}
func (*syncControlTestUpdater) BeginUpdateTarget(context.Context, *SyncContext, chan *TargetUpdateInfo) (bool, error) {
	return false, nil
}
func (*syncControlTestUpdater) FilterAllowOpsTargets(context.Context, []*TargetUpdateInfo, map[int]*api.ContextDetail, *SyncContext, chan *TargetUpdateInfo) (*time.Duration, error) {
	return nil, nil
}
func (*syncControlTestUpdater) UpgradeTarget(context.Context, *TargetUpdateInfo) error { return nil }
func (u *syncControlTestUpdater) GetTargetUpdateFinishStatus(context.Context, *TargetUpdateInfo) (bool, string, error) {
	u.finishStatusCalls++
	return u.finishStatus, "", nil
}
func (u *syncControlTestUpdater) FinishUpdateTarget(_ context.Context, _ *TargetUpdateInfo, cancel bool) error {
	u.finishCalls++
	u.finishedByCancel = cancel
	return nil
}

func syncControlTestOwner() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"},
		ObjectMeta: metav1.ObjectMeta{Namespace: "default", Name: "test-set", UID: "test-uid"},
	}
}
