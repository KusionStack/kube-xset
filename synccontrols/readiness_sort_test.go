package synccontrols

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"testing"
	"time"
)

func TestAfterOrZeroHandlesMissingReadyTimestamp(t *testing.T) {
	zero := &metav1.Time{}
	old := &metav1.Time{Time: time.Unix(100, 0)}
	newer := &metav1.Time{Time: time.Unix(200, 0)}
	for _, tc := range []struct {
		name        string
		left, right *metav1.Time
		want        bool
	}{
		{"both missing", nil, nil, false}, {"nil and zero", nil, zero, false}, {"zero and nil", zero, nil, false}, {"missing sorts first", nil, old, true}, {"known sorts after missing", old, nil, false}, {"newest first", newer, old, true}, {"oldest last", old, newer, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := afterOrZero(tc.left, tc.right); got != tc.want {
				t.Fatalf("afterOrZero = %v, want %v", got, tc.want)
			}
		})
	}
}
