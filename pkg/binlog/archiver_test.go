package binlog

import (
	"testing"
	"time"

	mariadbv1alpha1 "github.com/mariadb-operator/mariadb-operator/v26/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestShouldRotateBinlog(t *testing.T) {
	now := time.Date(2026, 8, 3, 12, 0, 0, 0, time.UTC)
	archiveTimeout := time.Hour

	archivedAt := func(d time.Duration) *mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus {
		return &mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus{
			LastArchivedTime: metav1.NewTime(now.Add(-d)),
		}
	}

	tests := map[string]struct {
		status               *mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus
		binlogPos            string
		lastRotatedBinlogPos string
		want                 bool
	}{
		"stale archival with unarchived transactions rotates": {
			status:    archivedAt(2 * time.Hour),
			binlogPos: "0-11-17000",
			want:      true,
		},
		"recent archival does not rotate": {
			status:    archivedAt(10 * time.Minute),
			binlogPos: "0-11-17000",
			want:      false,
		},
		"exactly at the timeout rotates": {
			status:    archivedAt(archiveTimeout),
			binlogPos: "0-11-17000",
			want:      true,
		},
		// The production case this patch exists for: archival has never
		// advanced, so waiting for MariaDB to rotate on its own is waiting
		// forever.
		"never archived rotates": {
			status:    nil,
			binlogPos: "0-11-17000",
			want:      true,
		},
		"status present but never archived rotates": {
			status:    &mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus{},
			binlogPos: "0-11-17000",
			want:      true,
		},
		// Guards against rotating an idle primary once per timeout forever:
		// MariaDB rotates unconditionally, so each one leaves an empty binary
		// log behind.
		"idle server never written does not rotate": {
			status:    archivedAt(48 * time.Hour),
			binlogPos: "",
			want:      false,
		},
		"no transactions since our last rotation does not rotate": {
			status:               archivedAt(48 * time.Hour),
			binlogPos:            "0-11-17000",
			lastRotatedBinlogPos: "0-11-17000",
			want:                 false,
		},
		"transactions since our last rotation rotates": {
			status:               archivedAt(48 * time.Hour),
			binlogPos:            "0-11-17050",
			lastRotatedBinlogPos: "0-11-17000",
			want:                 true,
		},
		// A fresh agent has no record of its own rotations; being due wins.
		"unknown last rotation with a recent archival does not rotate": {
			status:    archivedAt(time.Minute),
			binlogPos: "0-11-17000",
			want:      false,
		},
	}

	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got := shouldRotateBinlog(tt.status, archiveTimeout, tt.binlogPos, tt.lastRotatedBinlogPos, now)
			if got != tt.want {
				t.Errorf("shouldRotateBinlog() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestBinlogNumberingRestarted(t *testing.T) {
	archivedAt := metav1.NewTime(time.Date(2026, 9, 30, 18, 1, 8, 0, time.UTC))
	status := &mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus{
		LastArchivedBinaryLog: "mariadb-bin.000005",
		LastArchivedTime:      archivedAt,
		LastArchivedPosition:  8864,
	}
	tests := map[string]struct {
		status             *mariadbv1alpha1.MariaDBPointInTimeRecoveryStatus
		newest             string
		lastArchivedOnDisk *BinlogMetadata
		want               bool
	}{
		"nothing archived yet": {
			status: nil,
			newest: "mariadb-bin.000003",
			want:   false,
		},
		"numbering continues, archived binlog still on disk": {
			status:             status,
			newest:             "mariadb-bin.000007",
			lastArchivedOnDisk: &BinlogMetadata{LastTime: archivedAt, LogPosition: 8864},
			want:               false,
		},
		"numbering continues, archived binlog purged": {
			status: status,
			newest: "mariadb-bin.000007",
			want:   false,
		},
		"no new binlog since the last archival": {
			status:             status,
			newest:             "mariadb-bin.000005",
			lastArchivedOnDisk: &BinlogMetadata{LastTime: archivedAt, LogPosition: 8864},
			want:               false,
		},
		"numbering restarted below the archived number": {
			status: status,
			newest: "mariadb-bin.000003",
			want:   true,
		},
		"numbering restarted and already past the archived number": {
			status: status,
			newest: "mariadb-bin.000007",
			lastArchivedOnDisk: &BinlogMetadata{
				LastTime:    metav1.NewTime(time.Date(2026, 10, 5, 17, 0, 0, 0, time.UTC)),
				LogPosition: 1204,
			},
			want: true,
		},
	}
	for name, tt := range tests {
		t.Run(name, func(t *testing.T) {
			got, err := binlogNumberingRestarted(tt.status, tt.newest, tt.lastArchivedOnDisk)
			if err != nil {
				t.Fatalf("binlogNumberingRestarted() error = %v", err)
			}
			if got != tt.want {
				t.Errorf("binlogNumberingRestarted() = %v, want %v", got, tt.want)
			}
		})
	}
}
