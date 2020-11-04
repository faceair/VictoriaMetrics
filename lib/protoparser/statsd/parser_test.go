package statsd

import (
	"reflect"
	"strings"
	"testing"
)

func TestRowsUnmarshalFailure(t *testing.T) {
	f := func(s string) {
		t.Helper()
		var rows Rows
		rows.Unmarshal(s)
		if len(rows.Rows) != 0 {
			t.Fatalf("unexpected number of rows parsed; got %d; want 0", len(rows.Rows))
		}

		// Try again
		rows.Unmarshal(s)
		if len(rows.Rows) != 0 {
			t.Fatalf("unexpected number of rows parsed; got %d; want 0", len(rows.Rows))
		}
	}

	// Missing metric
	f(" 123 455")

	// Missing value
	f("aaa")

	// missing tag
	f("aa; 12 34")

	// missing tag value
	f("aa;bb 23 34")

	// unexpected space in tag value
	// See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/99
	f("s;tag1=aaa1;tag2=bb b2;tag3=ccc3 1")

	// invalid type
	f("aa:bb")

	// invalid timestamp
	f("aa 123 bar")
}

func TestRowsUnmarshalSuccess(t *testing.T) {
	f := func(s string, rowsExpected *Rows) {
		t.Helper()
		var rows Rows
		rows.Unmarshal(s)
		if !reflect.DeepEqual(rows.Rows, rowsExpected.Rows) {
			t.Fatalf("unexpected rows;\ngot\n%+v;\nwant\n%+v", rows.Rows, rowsExpected.Rows)
		}

		// Try unmarshaling again
		rows.Unmarshal(s)
		if !reflect.DeepEqual(rows.Rows, rowsExpected.Rows) {
			t.Fatalf("unexpected rows;\ngot\n%+v;\nwant\n%+v", rows.Rows, rowsExpected.Rows)
		}

		rows.Reset()
		if len(rows.Rows) != 0 {
			t.Fatalf("non-empty rows after reset: %+v", rows.Rows)
		}
	}

	// Empty line
	f("", &Rows{})
	f("\r", &Rows{})
	f("\n\n", &Rows{})
	f("\n\r\n", &Rows{})

	// Single line
	f("foobar:-123.456|c", &Rows{
		Rows: []Row{{
			Metric:     "foobar",
			Type:       CounterType,
			Value:      -123.456,
			SampleRate: 1.0,
			Tags:       nil,
		}},
	})
	f("foo.bar:123.456|g\n", &Rows{
		Rows: []Row{{
			Metric:     "foo.bar",
			Type:       GaugeType,
			Value:      123.456,
			SampleRate: 1.0,
			Tags:       nil,
		}},
	})

	// Timestamp bigger than 1<<31
	f("aaa 1123 429496729600", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Value:  1123,
		}},
	})

	// Floating-point timestamp
	// See https://github.com/graphite-project/carbon/blob/b0ba62a62d40a37950fed47a8f6ae6d0f02e6af5/lib/carbon/protocols.py#L197
	f("aaa 1123 4294.943", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Value:  1123,
		}},
	})

	// Tags
	f("foo;bar=baz 1 2", &Rows{
		Rows: []Row{{
			Metric: "foo",
			Tags: []Tag{{
				Key:   "bar",
				Value: "baz",
			}},
			Value: 1,
		}},
	})
	// Empty tags
	f("foo;bar=baz;aa=;x=y;=z 1 2", &Rows{
		Rows: []Row{{
			Metric: "foo",
			Tags: []Tag{
				{
					Key:   "bar",
					Value: "baz",
				},
				{
					Key:   "x",
					Value: "y",
				},
			},
			Value: 1,
		}},
	})

	// Multi lines
	f("foo 0.3 2\naaa 3\nbar.baz 0.34 43\n", &Rows{
		Rows: []Row{
			{
				Metric: "foo",
				Value:  0.3,
			},
			{
				Metric: "aaa",
				Value:  3,
			},
			{
				Metric: "bar.baz",
				Value:  0.34,
			},
		},
	})

	// Multi lines with invalid line
	f("foo 0.3 2\naaa\nbar.baz 0.34 43\n", &Rows{
		Rows: []Row{
			{
				Metric: "foo",
				Value:  0.3,
			},
			{
				Metric: "bar.baz",
				Value:  0.34,
			},
		},
	})
}

func Test_streamContext_Read(t *testing.T) {
	f := func(s string, rowsExpected *Rows) {
		t.Helper()
		ctx := getStreamContext(strings.NewReader(s))
		if !ctx.Read() {
			t.Fatalf("expecting successful read")
		}
		uw := getUnmarshalWork()
		callbackCalls := 0
		uw.callback = func(rows []Row) error {
			callbackCalls++
			if len(rows) != len(rowsExpected.Rows) {
				t.Fatalf("different len of expected rows;\ngot\n%+v;\nwant\n%+v", rows, rowsExpected.Rows)
			}
			if !reflect.DeepEqual(rows, rowsExpected.Rows) {
				t.Fatalf("unexpected rows;\ngot\n%+v;\nwant\n%+v", rows, rowsExpected.Rows)
			}
			return nil
		}
		uw.reqBuf = append(uw.reqBuf[:0], ctx.reqBuf...)
		uw.Unmarshal()
		if callbackCalls != 1 {
			t.Fatalf("unexpected number of callback calls; got %d; want 1", callbackCalls)
		}
	}

	// Full line without tags
	f("aaa 1123 345", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Value:  1123,
		}},
	})
	// Full line with tags
	f("aaa;x=y 1123 345", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Tags: []Tag{{
				Key:   "x",
				Value: "y",
			}},
			Value: 1123,
		}},
	})
	// missing timestamp.
	// Note that this test may be flaky due to timing issues. TODO: fix it
	f("aaa 1123", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Value:  1123,
		}},
	})
	// -1 timestamp. See https://github.com/VictoriaMetrics/VictoriaMetrics/issues/610
	// Note that this test may be flaky due to timing issues. TODO: fix it.
	f("aaa 1123 -1", &Rows{
		Rows: []Row{{
			Metric: "aaa",
			Value:  1123,
		}},
	})
}
