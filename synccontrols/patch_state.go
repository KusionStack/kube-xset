package synccontrols

import (
	"context"
	"encoding/json"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"kusionstack.io/kube-xset/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func targetPatchState(owner metav1.Object, state *MultiPatchSyncContext, target *TargetWrapper) (*api.TargetPatchState, error) {
	revision := target.GetLabels()[appsv1.ControllerRevisionHashLabelKey]
	applied, ok := state.Catalog.PatchIDsForRevision(revision)
	if !ok {
		return nil, fmt.Errorf("target %s references unknown revision %q", target.GetName(), revision)
	}
	planned, ok := state.InstancePlans[target.ID]
	if !ok || state.DesiredRevisions[target.ID] == nil {
		return nil, fmt.Errorf("target %s has no patch plan", target.GetName())
	}
	return &api.TargetPatchState{
		Version: 1, ObservedSetUID: string(owner.GetUID()), ObservedSetGeneration: owner.GetGeneration(),
		CurrentRevision: revision, DesiredRevision: state.DesiredRevisions[target.ID].Name,
		AppliedPatchIDs: append([]string{}, applied...), DesiredPatchIDs: append([]string{}, planned.DesiredPatchIDs...),
		CommandSnapshot: planned.CommandSnapshot,
	}, nil
}

func publishPatchState(ctx context.Context, c client.Client, target client.Object, state *api.TargetPatchState) (bool, error) {
	raw, err := json.Marshal(state)
	if err != nil {
		return false, err
	}
	if target.GetAnnotations()[api.TargetPatchStateAnnotation] == string(raw) {
		return false, nil
	}
	// resourceVersion and UID prevent attributing an old observation to a newer
	// spec, a new ByLabel command or a replacement with the same resource name.
	patch, err := json.Marshal(map[string]interface{}{"metadata": map[string]interface{}{
		"resourceVersion": target.GetResourceVersion(), "uid": target.GetUID(),
		"annotations": map[string]string{api.TargetPatchStateAnnotation: string(raw)},
	}})
	if err != nil {
		return false, err
	}
	copy := target.DeepCopyObject().(client.Object)
	if err = c.Patch(ctx, copy, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return false, err
	}
	target.SetResourceVersion(copy.GetResourceVersion())
	target.SetAnnotations(copy.GetAnnotations())
	return true, nil
}

func (r *RealSyncControl) publishPatchStates(ctx context.Context, owner api.XSetObject, syncContext *SyncContext) error {
	for _, target := range syncContext.TargetWrappers {
		if target.PlaceHolder || target.ToExclude {
			continue
		}
		// Scale-in can reclaim an identity before its terminating object disappears.
		if _, planned := syncContext.MultiPatch.InstancePlans[target.ID]; !planned {
			continue
		}
		state, err := targetPatchState(owner, syncContext.MultiPatch, target)
		if err != nil {
			return err
		}
		object := target.Object.DeepCopyObject().(client.Object)
		if _, err = publishPatchState(ctx, r.Client, object, state); err != nil {
			return err
		}
		target.Object = object
	}
	return nil
}
