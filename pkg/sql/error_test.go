package sql

import (
	"errors"
	"testing"
)

// Verbatim server messages. 1947 and 1948 are near-identical in wording and differ only in
// which direction the conflict runs, so matching the wrong one is an easy mistake to make
// and an impossible one to spot in review.
const (
	err1947 = "Error 1947 (HY000): Specified GTID 0-10-1291069 conflicts with the binary log " +
		"which contains a more recent GTID 0-11-1294159. If MASTER_GTID_POS=CURRENT_POS is used, " +
		"the binlog position will override the new value of @@gtid_slave_pos"
	err1948 = "Error 1948 (HY000): Specified value for @@gtid_slave_pos contains no value for " +
		"replication domain 0. This conflicts with the binary log which contains GTID 0-11-1176. " +
		"If MASTER_GTID_POS=CURRENT_POS is used, the binlog position will override the new value " +
		"of @@gtid_slave_pos"
)

func TestGtidSlavePosErrorPredicates(t *testing.T) {
	cases := []struct {
		name           string
		err            error
		wantBehind     bool
		wantNoValueFor bool
	}{
		{
			name: "nil",
		},
		{
			name:       "1947 is behind-binlog",
			err:        errors.New(err1947),
			wantBehind: true,
		},
		{
			name:           "1948 is no-value-for-domain",
			err:            errors.New(err1948),
			wantNoValueFor: true,
		},
		{
			name: "an unrelated error is neither",
			err:  errors.New("Error 1236 (HY000): connecting slave requested to start from GTID"),
		},
		{
			// Wrapped by ConfigureReplica before it reaches either predicate.
			name:       "1947 survives wrapping",
			err:        errors.New("error setting slave position 0-10-1291069: " + err1947),
			wantBehind: true,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsGtidSlavePosBehindBinlog(tt.err); got != tt.wantBehind {
				t.Errorf("IsGtidSlavePosBehindBinlog() = %v, want %v", got, tt.wantBehind)
			}
			if got := IsGtidSlavePosNoValueForDomain(tt.err); got != tt.wantNoValueFor {
				t.Errorf("IsGtidSlavePosNoValueForDomain() = %v, want %v", got, tt.wantNoValueFor)
			}
		})
	}
}
