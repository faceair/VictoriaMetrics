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
	f(":1|c")

	// Missing value
	f("aaa:|c")

	// missing tag
	f("aaa:1|c#")

	// missing tag value
	f("aaa:1|c#a:")

	// unexpected space in tag value
	f("aaa:1|c#tag1=aaa1,tag2=bb b2,tag3=ccc3 1")

	// invalid value
	f("aa:bb")

	// invalid type
	f("aa:1|bar")
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
		}},
	})
	f("foo.bar:123.456|g\n", &Rows{
		Rows: []Row{{
			Metric:     "foo.bar",
			Type:       GaugeType,
			Value:      123.456,
			SampleRate: 1.0,
		}},
	})
	f("foo.bar:123.456|h\n", &Rows{
		Rows: []Row{{
			Metric:     "foo.bar",
			Type:       HistogramType,
			Value:      123.456,
			SampleRate: 1.0,
		}},
	})

	// Tags
	f("foo:1|ms|#bar:baz", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       TimingType,
			Value:      1,
			SampleRate: 1.0,
			Tags: []Tag{{
				Key:   "bar",
				Value: "baz",
			}},
		}},
	})
	f("foo:2|c|#bar:baz,a:b", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       CounterType,
			Value:      2,
			SampleRate: 1.0,
			Tags: []Tag{{
				Key:   "bar",
				Value: "baz",
			}, {
				Key:   "a",
				Value: "b",
			}},
		}},
	})

	// Sample Rate
	f("foo:1|ms|@0.2", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       TimingType,
			Value:      1,
			SampleRate: 0.2,
		}},
	})

	f("foo:1|ms|@0.3#bar:baz,a:b", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       TimingType,
			Value:      1,
			SampleRate: 0.3,
			Tags: []Tag{{
				Key:   "bar",
				Value: "baz",
			}, {
				Key:   "a",
				Value: "b",
			}},
		}},
	})
	f("foo:1|ms|#bar:baz,a:b@0.3", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       TimingType,
			Value:      1,
			SampleRate: 0.3,
			Tags: []Tag{{
				Key:   "bar",
				Value: "baz",
			}, {
				Key:   "a",
				Value: "b",
			}},
		}},
	})

	// Empty tags
	f("foo:1|c|#bar:baz,aa:,x:y,:z", &Rows{
		Rows: []Row{{
			Metric:     "foo",
			Type:       CounterType,
			Value:      1,
			SampleRate: 1.0,
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
		}},
	})

	// Multi lines
	f("foo:0.3|c\naaa:3|g\nbar.baz:0.34|h\n", &Rows{
		Rows: []Row{
			{
				Metric:     "foo",
				Type:       CounterType,
				Value:      0.3,
				SampleRate: 1.0,
			},
			{
				Metric:     "aaa",
				Type:       GaugeType,
				Value:      3,
				SampleRate: 1.0,
			},
			{
				Metric:     "bar.baz",
				Type:       HistogramType,
				Value:      0.34,
				SampleRate: 1.0,
			},
		},
	})

	// Multi lines with invalid line
	f("foo:0.3|c\naaa\nbar.baz:0.34|h\n", &Rows{
		Rows: []Row{
			{
				Metric:     "foo",
				Type:       CounterType,
				Value:      0.3,
				SampleRate: 1.0,
			},
			{
				Metric:     "bar.baz",
				Type:       HistogramType,
				Value:      0.34,
				SampleRate: 1.0,
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
	f("aaa:1123|c", &Rows{
		Rows: []Row{{
			Metric:     "aaa",
			Type:       CounterType,
			Value:      1123,
			SampleRate: 1.0,
		}},
	})
	// Full line with tags
	f("aaa:1123|c|#x:y", &Rows{
		Rows: []Row{{
			Metric:     "aaa",
			Type:       CounterType,
			Value:      1123,
			SampleRate: 1.0,
			Tags: []Tag{{
				Key:   "x",
				Value: "y",
			}},
		}},
	})
}
