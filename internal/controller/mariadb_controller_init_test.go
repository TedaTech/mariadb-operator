package controller

import (
	"testing"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	"github.com/mariadb-operator/mariadb-operator/v26/pkg/metadata"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestPbInitRetryCount(t *testing.T) {
	for _, tt := range []struct {
		name string
		mdb  *mariadbv1alpha1.MariaDB
		pod  string
		want int
	}{
		{
			name: "no annotations at all",
			mdb:  &mariadbv1alpha1.MariaDB{},
			pod:  "mariadb-2",
			want: 0,
		},
		{
			name: "annotation for another pod does not leak",
			mdb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						metadata.PbInitRetryAnnotationPrefix + "mariadb-1": "3",
					},
				},
			},
			pod:  "mariadb-2",
			want: 0,
		},
		{
			name: "counts for this pod",
			mdb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						metadata.PbInitRetryAnnotationPrefix + "mariadb-2": "4",
					},
				},
			},
			pod:  "mariadb-2",
			want: 4,
		},
		{
			name: "unparseable value counts as zero",
			mdb: &mariadbv1alpha1.MariaDB{
				ObjectMeta: metav1.ObjectMeta{
					Annotations: map[string]string{
						metadata.PbInitRetryAnnotationPrefix + "mariadb-2": "oops",
					},
				},
			},
			pod:  "mariadb-2",
			want: 0,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := pbInitRetryCount(tt.mdb, tt.pod); got != tt.want {
				t.Errorf("pbInitRetryCount() = %d, want %d", got, tt.want)
			}
		})
	}
}
