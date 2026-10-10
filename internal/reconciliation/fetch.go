package reconciliation

import (
	"context"
	"fmt"
	"net/http"
	"strings"
)

func FetchReport(ctx context.Context, client *http.Client, baseURL string) (Report, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/settlements", nil)
	if err != nil {
		return Report{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Report{}, fmt.Errorf("download provider report: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return Report{}, fmt.Errorf("provider report returned HTTP %d", resp.StatusCode)
	}
	return ReadReport(resp.Body)
}
