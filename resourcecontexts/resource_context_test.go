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
	"reflect"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	appsv1alpha1 "kusionstack.io/kube-api/apps/v1alpha1"
	"kusionstack.io/kube-utils/controller/expectations"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"kusionstack.io/kube-xset/api"
)

func TestRealResourceContextControl_fulfillOwnedIDs(t *testing.T) {
	type fields struct {
		Client                 client.Client
		EventRecorder          record.EventRecorder
		xsetController         api.XSetController
		resourceContextAdapter api.ResourceContextAdapter
		resourceContextKeys    map[api.ResourceContextKeyEnum]string
		resourceContextGVK     schema.GroupVersionKind
		cacheExpectations      expectations.CacheExpectationsInterface
		xsetLabelManager       api.XSetLabelAnnotationManager
	}
	type args struct {
		ownedIDs        map[int]*api.ContextDetail
		existingIDs     map[int]*api.ContextDetail
		unRecordIDs     map[int]string
		replicas        int
		owner           api.XSetObject
		defaultRevision string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   map[int]*api.ContextDetail
	}{
		{
			name: "want 5, existing [0], alloc [1,2,3,4]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{},
				replicas:        5,
				owner:           &appsv1alpha1.CollaSet{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
				defaultRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				4: {
					ID:   4,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
		{
			name: "want 3, existing [0,1], unrecorded [3], alloc [3]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{3: "defaultRv"},
				replicas:        2,
				owner:           &appsv1alpha1.CollaSet{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
				defaultRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
		{
			name: "want 4, existing [0,1], unrecorded [3], alloc [2,3]",
			args: args{
				ownedIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				existingIDs: map[int]*api.ContextDetail{
					0: {
						ID:   0,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
					1: {
						ID:   1,
						Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
					},
				},
				unRecordIDs:     map[int]string{3: "defaultRv"},
				replicas:        4,
				owner:           &appsv1alpha1.CollaSet{ObjectMeta: metav1.ObjectMeta{Name: "foo"}},
				defaultRevision: "defaultRv",
			},
			fields: fields{
				Client:                 nil,
				EventRecorder:          nil,
				xsetController:         nil,
				resourceContextAdapter: nil,
				resourceContextKeys:    defaultResourceContextKeys,
				resourceContextGVK:     schema.GroupVersionKind{},
				cacheExpectations:      nil,
				xsetLabelManager:       nil,
			},
			want: map[int]*api.ContextDetail{
				0: {
					ID:   0,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				1: {
					ID:   1,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				2: {
					ID:   2,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
				3: {
					ID:   3,
					Data: map[string]string{"Owner": "foo", "Revision": "defaultRv", "TargetJustCreate": "true"},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := &RealResourceContextControl{
				Client:                 tt.fields.Client,
				EventRecorder:          tt.fields.EventRecorder,
				xsetController:         tt.fields.xsetController,
				resourceContextAdapter: tt.fields.resourceContextAdapter,
				resourceContextKeys:    tt.fields.resourceContextKeys,
				resourceContextGVK:     tt.fields.resourceContextGVK,
				cacheExpectations:      tt.fields.cacheExpectations,
				xsetLabelManager:       tt.fields.xsetLabelManager,
			}
			if got := r.fulfillOwnedIDs(tt.args.ownedIDs, tt.args.existingIDs, tt.args.unRecordIDs, tt.args.replicas, tt.args.owner, tt.args.defaultRevision); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("fulfillOwnedIDs() = %v, want %v", got, tt.want)
			}
		})
	}
}
