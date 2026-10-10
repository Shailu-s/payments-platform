package reconciliation

import (
	"fmt"
	"reflect"
	"testing"
)

func TestWrongBankAmountIsDetected(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	external := internal
	external.Amount = 49000
	findings, err := Compare([]Record{internal}, []Record{external})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "AMOUNT_MISMATCH" || len(findings[0].Fields) != 1 || findings[0].Fields[0] != "amount" {
		t.Fatalf("findings = %+v, want amount mismatch", findings)
	}
	if findings[0].Internal.Amount != 50000 || findings[0].External[0].Amount != 49000 {
		t.Error("comparison lost original evidence")
	}
}

func TestLostSettlementCallbackIsDetected(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "processing"}
	external := internal
	external.Status = "settled"
	findings, err := Compare([]Record{internal}, []Record{external})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "STATUS_MISMATCH" {
		t.Fatalf("findings = %+v, want status mismatch", findings)
	}
	if internal.Status != "processing" || findings[0].Internal.Status != "processing" {
		t.Error("reconciliation changed internal status")
	}
}

func TestAcceptedInternalPaymentMissingFromBankIsDetected(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	findings, err := Compare([]Record{internal}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "MISSING_EXTERNAL" || findings[0].Internal == nil || len(findings[0].External) != 0 {
		t.Fatalf("findings = %+v, want missing external with internal evidence", findings)
	}
}

func TestBankPaymentMissingInternallyIsDetected(t *testing.T) {
	external := Record{ProviderRef: "mb_orphan", ClientReference: "tr_unknown", Amount: 50000, Currency: "USD", Status: "settled"}
	findings, err := Compare(nil, []Record{external})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "MISSING_INTERNAL" || findings[0].Internal != nil || len(findings[0].External) != 1 {
		t.Fatalf("findings = %+v, want missing internal with external evidence", findings)
	}
}

func TestRepeatedBankPaymentIsNotCountedAsMatched(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	findings, err := Compare([]Record{internal}, []Record{internal, internal})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "DUPLICATE_EXTERNAL" || len(findings[0].External) != 2 {
		t.Fatalf("findings = %+v, want one duplicate finding retaining both rows", findings)
	}
}

func TestLostAcceptanceResponseMatchesByClientReference(t *testing.T) {
	internal := Record{ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "unresolved"}
	external := internal
	external.ProviderRef, external.Status = "mb_1", "settled"
	findings, err := Compare([]Record{internal}, []Record{external})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "STATUS_MISMATCH" {
		t.Fatalf("findings = %+v, want linked status mismatch, not two missing records", findings)
	}
}

func TestAmountAndStatusDifferencesAreBothRetained(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "processing"}
	external := internal
	external.Amount, external.Status = 49000, "settled"
	findings, err := Compare([]Record{internal}, []Record{external})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 2 || findings[0].Classification != "AMOUNT_MISMATCH" || findings[1].Classification != "STATUS_MISMATCH" {
		t.Fatalf("findings = %+v, want both differences, not an overwritten classification", findings)
	}
}

func TestInvalidRecordsCannotProduceCleanMatches(t *testing.T) {
	invalid := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 0, Currency: "USD", Status: "settled"}
	findings, err := Compare([]Record{invalid}, []Record{invalid})
	if err == nil || len(findings) != 0 {
		t.Fatalf("findings = %+v, err = %v, want rejection without a clean match", findings, err)
	}
}

func TestSameInstructionWithTwoBankReferencesIsDuplicate(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	second := internal
	second.ProviderRef = "mb_2"
	findings, err := Compare([]Record{internal}, []Record{internal, second})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != "DUPLICATE_EXTERNAL" || len(findings[0].External) != 2 {
		t.Fatalf("findings = %+v, want duplicated instruction, not MATCHED plus orphan", findings)
	}
}

func TestConflictingKnownReferencesRejectTheComparison(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	external := internal
	external.ProviderRef = "mb_other"
	findings, err := Compare([]Record{internal}, []Record{external})
	if err == nil || len(findings) != 0 {
		t.Fatalf("findings = %+v, err = %v, want conflicting identity rejected", findings, err)
	}
}

func TestDuplicateOrphanRetainsBothIndependentFindings(t *testing.T) {
	row := Record{ProviderRef: "mb_orphan", ClientReference: "tr_unknown", Amount: 500, Currency: "USD", Status: "settled"}
	findings, err := Compare(nil, []Record{row, row})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 2 || findings[0].Classification != MissingInternal || findings[1].Classification != DuplicateExternal || len(findings[1].External) != 2 {
		t.Fatalf("findings = %+v, want missing internal plus duplicate with both bank rows", findings)
	}
}

func TestFindingOwnsSnapshotEvidence(t *testing.T) {
	row := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 500, Currency: "USD", Status: "settled"}
	internal, external := []Record{row}, []Record{row}
	findings, err := Compare(internal, external)
	if err != nil {
		t.Fatal(err)
	}
	internal[0].Amount, external[0].Amount = 1, 2
	if findings[0].Internal.Amount != 500 || findings[0].External[0].Amount != 500 {
		t.Error("caller mutation changed the saved snapshot evidence")
	}
}

func TestCorruptedSnapshotCoversAllSixOutcomesWithoutMutation(t *testing.T) {
	makeRow := func(id string) Record {
		return Record{ProviderRef: "mb_" + id, ClientReference: "tr_" + id, Amount: 50000, Currency: "USD", Status: "settled"}
	}
	internal := []Record{makeRow("matched"), makeRow("amount"), makeRow("status"), makeRow("absent"), makeRow("duplicate")}
	internal[2].Status = "processing"
	external := []Record{makeRow("orphan"), makeRow("status"), makeRow("matched"), makeRow("duplicate"), makeRow("amount"), makeRow("duplicate")}
	external[4].Amount = 49000
	beforeInternal, beforeExternal := append([]Record(nil), internal...), append([]Record(nil), external...)
	findings, err := Compare(internal, external)
	if err != nil {
		t.Fatal(err)
	}
	counts := map[string]int{}
	for _, finding := range findings {
		counts[finding.Classification]++
	}
	want := map[string]int{Matched: 1, AmountMismatch: 1, StatusMismatch: 1, MissingExternal: 1, MissingInternal: 1, DuplicateExternal: 1}
	if !reflect.DeepEqual(counts, want) {
		t.Fatalf("counts = %v, want %v", counts, want)
	}
	if !reflect.DeepEqual(internal, beforeInternal) || !reflect.DeepEqual(external, beforeExternal) {
		t.Fatal("comparison changed its input snapshots")
	}
	for i, j := 0, len(external)-1; i < j; i, j = i+1, j-1 {
		external[i], external[j] = external[j], external[i]
	}
	reordered, err := Compare(internal, external)
	if err != nil || !reflect.DeepEqual(findings, reordered) {
		t.Fatalf("record ordering changed the findings: %v", err)
	}
}

func BenchmarkCompare10000Payments(b *testing.B) {
	rows := make([]Record, 10000)
	for i := range rows {
		rows[i] = Record{ProviderRef: fmt.Sprintf("mb_%d", i), ClientReference: fmt.Sprintf("tr_%d", i), Amount: 500, Currency: "USD", Status: "settled"}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		findings, err := Compare(rows, rows)
		if err != nil || len(findings) != len(rows) {
			b.Fatalf("comparison produced %d findings: %v", len(findings), err)
		}
	}
}

func TestMatchingPaymentIsMatched(t *testing.T) {
	internal := Record{ProviderRef: "mb_1", ClientReference: "tr_1", Amount: 50000, Currency: "USD", Status: "settled"}
	findings, err := Compare([]Record{internal}, []Record{internal})
	if err != nil {
		t.Fatal(err)
	}
	if len(findings) != 1 || findings[0].Classification != Matched || findings[0].Internal == nil || len(findings[0].External) != 1 {
		t.Fatalf("findings = %+v, want one MATCHED with both records", findings)
	}
}
