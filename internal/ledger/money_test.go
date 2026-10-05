package ledger

import (
	"math"
	"testing"
)

func TestFloatCannotRepresentDecimalMoney(t *testing.T) {
	// Use runtime floats: untyped constant arithmetic is exact and would hide the error.
	a, b := 0.1, 0.2
	sum := a + b
	if sum == 0.3 {
		t.Fatal("0.1 + 0.2 == 0.3 in float64: expected the representation error")
	}
	t.Logf("runtime  : 0.1 + 0.2 = %.20f", sum)
	t.Logf("wanted   :             %.20f", 0.3)
	t.Logf("constant : 0.1 + 0.2 == 0.3 is %v, folded at compile time and not the real case", 0.1+0.2 == 0.3)

	if 10+20 != 30 {
		t.Fatal("integer arithmetic is broken")
	}
}

func TestFloatDriftAccumulatesOverManyOperations(t *testing.T) {
	const operations = 10_000
	const centsPerOperation = 1

	var cents int64
	var dollars float64
	for i := 0; i < operations; i++ {
		cents += centsPerOperation
		dollars += 0.01
	}

	floatAsCents := dollars * 100
	drift := floatAsCents - float64(cents)

	t.Logf("after %d additions of $0.01:", operations)
	t.Logf("  int64 minor units : %d cents  ($%d.%02d)", cents, cents/100, cents%100)
	t.Logf("  float64 dollars   : %.20f", dollars)
	t.Logf("  float64 as cents  : %.20f", floatAsCents)
	t.Logf("  drift             : %.20f cents", drift)

	if cents != operations*centsPerOperation {
		t.Errorf("int64 total = %d, want %d: integers must be exact", cents, operations*centsPerOperation)
	}
	if drift == 0 {
		t.Error("expected float64 to drift from the exact total, but it did not")
	}

	if math.Abs(drift) > 1 {
		t.Logf("drift already exceeds a whole cent after only %d operations", operations)
	}
}

// Binary representation can put a decimal half-cent below the rounding boundary.
func TestRoundingAFloatLosesACent(t *testing.T) {
	// Avoid exact constant folding before multiplication.
	price := 1.005
	rounded := int64(math.Round(price * 100))

	t.Logf("$1.005 stored as      %.20f", price)
	t.Logf("round(price * 100)  = %d cents", rounded)
	t.Logf("a bank charges        101 cents")

	if rounded != 100 {
		t.Fatalf("round(1.005 * 100) = %d, expected the float to land on 100", rounded)
	}

	// Decimal rounding belongs at the input boundary; store the chosen minor units exactly.
	const exact int64 = 101
	if exact-rounded != 1 {
		t.Errorf("expected a one cent loss, got %d", exact-rounded)
	}
	t.Logf("int64 stores exactly  %d cents, with no rounding step to get wrong", exact)
}
