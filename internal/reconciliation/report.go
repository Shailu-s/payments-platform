package reconciliation

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"time"
)

const MaxReportBytes = 10 << 20

type Report struct {
	CapturedAt time.Time
	Records    []Record
	Raw        []byte
}

func ReadReport(r io.Reader) (Report, error) {
	raw, err := io.ReadAll(io.LimitReader(r, MaxReportBytes+1))
	if err != nil {
		return Report{}, fmt.Errorf("read reconciliation report: %w", err)
	}
	if len(raw) > MaxReportBytes {
		return Report{}, errors.New("reconciliation report exceeds 10 MiB")
	}
	reader := csv.NewReader(bytes.NewReader(raw))
	reader.FieldsPerRecord = -1
	meta, err := reader.Read()
	if err != nil || len(meta) != 2 || meta[0] != "snapshot_at" || !strings.HasSuffix(meta[1], "Z") {
		return Report{}, errors.New("report requires UTC snapshot_at metadata")
	}
	capturedAt, err := time.Parse(time.RFC3339Nano, meta[1])
	if err != nil || capturedAt.IsZero() {
		return Report{}, errors.New("invalid report capture timestamp")
	}
	header, err := reader.Read()
	if err != nil || !slices.Equal(header, []string{"provider_ref", "client_reference", "amount", "currency", "status"}) {
		return Report{}, errors.New("invalid report column header")
	}
	report := Report{CapturedAt: capturedAt, Raw: raw}
	for line := 3; ; line++ {
		fields, err := reader.Read()
		if errors.Is(err, io.EOF) {
			return report, nil
		}
		if err != nil || len(fields) != 5 {
			return Report{}, fmt.Errorf("invalid report record %d", line)
		}
		amount, err := strconv.ParseInt(fields[2], 10, 64)
		row := Record{ProviderRef: fields[0], ClientReference: fields[1], Amount: amount, Currency: fields[3], Status: fields[4]}
		if err != nil || validate(row, true) != nil {
			return Report{}, fmt.Errorf("invalid report record %d", line)
		}
		report.Records = append(report.Records, row)
	}
}
