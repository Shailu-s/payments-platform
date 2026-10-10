package main

import (
	"encoding/json"
	"testing"
)

func TestSubmitReturnsStableSnapshot(t *testing.T) {
	for _, duplicate := range []bool{false, true} {
		name := "first submission"
		if duplicate {
			name = "duplicate submission"
		}
		t.Run(name, func(t *testing.T) {
			store := NewStore()
			instruction := Payment{ClientReference: "tr_snapshot", Amount: 50000, Currency: "USD", Source: "acc_a", Destination: "acc_b"}
			response, _, err := store.Submit(instruction)
			if err != nil {
				t.Fatal(err)
			}
			if duplicate {
				var replayed bool
				response, replayed, err = store.Submit(instruction)
				if err != nil || !replayed {
					t.Fatalf("duplicate = %t, err = %v", replayed, err)
				}
			}
			if _, changed, err := store.Settle(response.ProviderRef, StatusSettled, nil); err != nil || !changed {
				t.Fatalf("settle changed = %t, err = %v", changed, err)
			}
			encoded, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			var got Payment
			if err := json.Unmarshal(encoded, &got); err != nil {
				t.Fatal(err)
			}
			if got.Status != StatusProcessing || got.SettledAt != nil {
				t.Errorf("submission response changed after settlement: status=%s settled_at=%v", got.Status, got.SettledAt)
			}
			current, err := store.ByProviderRef(response.ProviderRef)
			if err != nil || current.Status != StatusSettled {
				t.Fatalf("stored payment did not settle: %+v, %v", current, err)
			}
		})
	}
}
