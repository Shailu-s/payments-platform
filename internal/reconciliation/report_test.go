package reconciliation

import (
	"strings"
	"testing"
	"time"
)

func TestInvalidEncodingAndBlankReferencesRejectReport(t *testing.T) {
	prefix := "snapshot_at,2026-10-10T06:00:00Z\nprovider_ref,client_reference,amount,currency,status\n"
	for _, row := range []string{"mb_1,  ,500,USD,settled\n", "  ,tr_1,500,USD,settled\n", "mb_1,tr_\xff,500,USD,settled\n"} {
		if report, err := ReadReport(strings.NewReader(prefix + row)); err == nil || len(report.Records) != 0 {
			t.Fatalf("invalid identity produced report=%+v err=%v", report, err)
		}
	}
}

func TestMalformedReportRejectsWholeSnapshot(t *testing.T) {
	prefix := "snapshot_at,2026-10-10T06:00:00Z\nprovider_ref,client_reference,amount,currency,status\n"
	valid := "mb_1,tr_1,500,USD,settled\n"
	for name, bad := range map[string]string{
		"overflow":       "mb_2,tr_2,9223372036854775808,USD,settled\n",
		"fraction":       "mb_2,tr_2,5.5,USD,settled\n",
		"negative":       "mb_2,tr_2,-1,USD,settled\n",
		"zero":           "mb_2,tr_2,0,USD,settled\n",
		"currency":       "mb_2,tr_2,500,EUR,settled\n",
		"status":         "mb_2,tr_2,500,USD,unresolved\n",
		"missing column": "mb_2,tr_2,500,USD\n",
		"extra column":   "mb_2,tr_2,500,USD,settled,extra\n",
		"broken quoting": "\"mb_2,tr_2,500,USD,settled\n",
		"null reference": "mb_2,tr_\x00,500,USD,settled\n",
	} {
		t.Run(name, func(t *testing.T) {
			if report, err := ReadReport(strings.NewReader(prefix + valid + bad)); err == nil || len(report.Records) != 0 || len(report.Raw) != 0 {
				t.Fatalf("partial report escaped: %+v %v", report, err)
			}
		})
	}
	for _, input := range []string{"", "snapshot_at,not-a-time\n", strings.Replace(prefix, "Z", "+00:00", 1), strings.Replace(prefix, "amount", "value", 1), strings.Repeat("x", MaxReportBytes+1)} {
		if _, err := ReadReport(strings.NewReader(input)); err == nil {
			t.Fatal("invalid metadata/header/size accepted")
		}
	}
	if report, err := ReadReport(strings.NewReader(prefix)); err != nil || len(report.Records) != 0 {
		t.Fatalf("valid empty snapshot rejected: %v", err)
	}
}

func TestReportPreservesSnapshotTimeAndDuplicateRows(t *testing.T) {
	input := "snapshot_at,2026-10-10T06:00:00Z\nprovider_ref,client_reference,amount,currency,status\nmb_1,tr_1,50000,USD,settled\nmb_1,tr_1,50000,USD,settled\n"
	report, err := ReadReport(strings.NewReader(input))
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Records) != 2 || !report.CapturedAt.Equal(time.Date(2026, 10, 10, 6, 0, 0, 0, time.UTC)) || string(report.Raw) != input {
		t.Fatalf("report = %+v, want complete snapshot with both duplicate rows", report)
	}
}
