package ledger

import (
	"math"
	"testing"
)

// Evidence for PLAN.md section 5: money is an integer in minor units, never a
// float. This is the claim, and this test is what earns it.
//
// Binary floating point cannot represent most decimal fractions exactly, so the
// textbook identity fails.
func TestFloatCannotRepresentDecimalMoney(t *testing.T) {
	// The values must be runtime variables. Written as literals, Go evaluates
	// 0.1 + 0.2 as untyped constants at arbitrary precision during compilation
	// and rounds once, so the comparison against 0.3 succeeds and hides the
	// problem. Real money arrives at runtime, from a request body or a row.
	a, b := 0.1, 0.2
	sum := a + b
	if sum == 0.3 {
		t.Fatal("0.1 + 0.2 == 0.3 in float64: expected the representation error")
	}
	t.Logf("runtime  : 0.1 + 0.2 = %.20f", sum)
	t.Logf("wanted   :             %.20f", 0.3)
	t.Logf("constant : 0.1 + 0.2 == 0.3 is %v, folded at compile time and not the real case", 0.1+0.2 == 0.3)

	// In minor units the same sum is exact, because it never leaves the integers.
	if 10+20 != 30 {
		t.Fatal("integer arithmetic is broken")
	}
}

// The failure that actually costs money: the error is not a curiosity, it
// accumulates. Add one cent ten thousand times and compare.
func TestFloatDriftAccumulatesOverManyOperations(t *testing.T) {
	const operations = 10_000
	const centsPerOperation = 1

	var cents int64
	var dollars float64
	for i := 0; i < operations; i++ {
		cents += centsPerOperation
		dollars += 0.01
	}

	// The honest comparison: both as cents.
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

	// The drift is small here, and that is the trap. It is not zero, it is
	// sign-dependent, and it compounds with every operation and every
	// multiplication downstream.
	if math.Abs(drift) > 1 {
		t.Logf("drift already exceeds a whole cent after only %d operations", operations)
	}
}

// Rounding a float to cents does not save you: the value being rounded is
// already wrong before the rounding starts.
//
// $1.005 is stored as 1.00499..., so rounding to the nearest cent gives 100
// when a bank charges 101. One cent, silently, on every such price.
func TestRoundingAFloatLosesACent(t *testing.T) {
	// A runtime variable, not a literal: as a constant Go folds this at
	// arbitrary precision and produces the right answer, hiding the bug.
	price := 1.005
	rounded := int64(math.Round(price * 100))

	t.Logf("$1.005 stored as      %.20f", price)
	t.Logf("round(price * 100)  = %d cents", rounded)
	t.Logf("a bank charges        101 cents")

	if rounded != 100 {
		t.Fatalf("round(1.005 * 100) = %d, expected the float to land on 100", rounded)
	}

	// The integer version never asks the question. The caller decides that
	// $1.005 is 101 cents, at the boundary where the decision belongs, and the
	// system stores exactly what it was given.
	const exact int64 = 101
	if exact-rounded != 1 {
		t.Errorf("expected a one cent loss, got %d", exact-rounded)
	}
	t.Logf("int64 stores exactly  %d cents, with no rounding step to get wrong", exact)
}
