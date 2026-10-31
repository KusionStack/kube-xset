package synccontrols

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"kusionstack.io/kube-xset/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestPatchStateSnapshot(t *testing.T) {
	r0 := multiPatchTestRevision("r0", 1, `{"spec":{"a":"0","b":"0"}}`)
	r1 := multiPatchTestRevision("r1", 2, `{"spec":{"a":"1","b":"0"}}`)
	r2 := multiPatchTestRevision("r2", 3, `{"spec":{"a":"1","b":"2"}}`)
	plan := &api.RevisionPlan{Mode: api.RevisionModeConcurrentPatches, ActivePatches: []api.PatchRevision{
		multiPatchTestPatch(t, "p1", `.spec.a`, `"1"`, `"0"`, 2),
		multiPatchTestPatch(t, "p2", `.spec.b`, `"2"`, `"0"`, 1),
	}}
	state, err := buildMultiPatchExecutionState(plan, nil, r0, r2, []*appsv1.ControllerRevision{r0, r1, r2}, []multiPatchIdentity{{ID: 0, RevisionName: "r0"}, {ID: 1, RevisionName: "r0"}, {ID: 2, RevisionName: "r1"}}, multiPatchTestRevisionBuilder, "patch/", "legacy")
	if err != nil {
		t.Fatal(err)
	}
	owner := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "set", UID: "set-uid", Generation: 12}}
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "instance", Namespace: "ns", UID: "instance-uid", Labels: map[string]string{appsv1.ControllerRevisionHashLabelKey: "r1"}}}
	got, err := targetPatchState(owner, state, &TargetWrapper{Object: target, ID: 2})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.AppliedPatchIDs, []string{"p1"}) || !reflect.DeepEqual(got.DesiredPatchIDs, []string{"p1", "p2"}) || got.ObservedSetUID != "set-uid" || got.ObservedSetGeneration != 12 {
		t.Fatalf("unexpected snapshot: %#v", got)
	}
	// Replacement peers can have the same identity but must use their own revision.
	target.Labels[appsv1.ControllerRevisionHashLabelKey] = "r0"
	got, err = targetPatchState(owner, state, &TargetWrapper{Object: target, ID: 2})
	if err != nil || len(got.AppliedPatchIDs) != 0 {
		t.Fatalf("replacement snapshot: %#v %v", got, err)
	}
	target.Labels[appsv1.ControllerRevisionHashLabelKey] = "lost"
	if _, err = targetPatchState(owner, state, &TargetWrapper{Object: target, ID: 2}); err == nil {
		t.Fatal("unknown revision must not produce an applied vector")
	}
	target.Labels[appsv1.ControllerRevisionHashLabelKey] = "r0"
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(target).Build()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(target), target); err != nil {
		t.Fatal(err)
	}
	r := &RealSyncControl{}
	r.Client = c
	wrapper := &TargetWrapper{Object: target, ID: 2}
	syncContext := &SyncContext{MultiPatch: state, TargetWrappers: []*TargetWrapper{wrapper, {PlaceHolder: true}, {ToExclude: true}, {Object: target, ID: 99}}}
	if err := r.publishPatchStates(context.Background(), owner, syncContext); err != nil {
		t.Fatal(err)
	}
	if target.Annotations[api.TargetPatchStateAnnotation] != "" {
		t.Fatal("snapshot publisher mutated the informer object")
	}
	if wrapper.GetAnnotations()[api.TargetPatchStateAnnotation] == "" {
		t.Fatal("local wrapper must track the updated observation and resourceVersion")
	}
}

func TestPublishPatchState(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	target := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "i", Namespace: "ns", UID: "uid", Annotations: map[string]string{"keep": "yes"}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(target).Build()
	if err := c.Get(context.Background(), client.ObjectKeyFromObject(target), target); err != nil {
		t.Fatal(err)
	}
	state := &api.TargetPatchState{Version: 1, CurrentRevision: "r0", DesiredRevision: "r1", AppliedPatchIDs: []string{}, DesiredPatchIDs: []string{"p1"}}
	changed, err := publishPatchState(context.Background(), c, target, state)
	if err != nil || !changed {
		t.Fatalf("first publish: %v %v", changed, err)
	}
	current := &corev1.Pod{}
	if err = c.Get(context.Background(), client.ObjectKeyFromObject(target), current); err != nil {
		t.Fatal(err)
	}
	var decoded api.TargetPatchState
	if err = json.Unmarshal([]byte(current.Annotations[api.TargetPatchStateAnnotation]), &decoded); err != nil {
		t.Fatal(err)
	}
	if current.Annotations["keep"] != "yes" || decoded.DesiredPatchIDs[0] != "p1" {
		t.Fatal("snapshot or unrelated annotation lost")
	}
	changed, err = publishPatchState(context.Background(), c, current, state)
	if err != nil || changed {
		t.Fatalf("unchanged publish: %v %v", changed, err)
	}
	// Optimistic locking prevents stale writes to a target changed by another writer.
	stale := current.DeepCopy()
	current.Labels = map[string]string{"new": "command"}
	if err = c.Update(context.Background(), current); err != nil {
		t.Fatal(err)
	}
	state.DesiredRevision = "r2"
	if _, err = publishPatchState(context.Background(), c, stale, state); err == nil {
		t.Fatal("stale snapshot write must conflict")
	}
}
