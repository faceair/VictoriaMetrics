package aggregator

import (
	"testing"
)

func TestValidateMetricSuccess(t *testing.T) {
	f := func(s string) {
		t.Helper()
		if !validateMetric(s) {
			t.Fatalf("cannot validate %q", s)
		}
	}
	f("a")
	f("_9:8")
	f("a")
	f(`:foo:bar`)
}

func TestValidateMetricError(t *testing.T) {
	f := func(s string) {
		t.Helper()
		if validateMetric(s) {
			t.Fatalf("expecting non-nil error when validating %q", s)
		}
	}
	f("")
	f("{}")

	// superflouos space
	f("a ")
	f(" a")
	f(" a ")
}
