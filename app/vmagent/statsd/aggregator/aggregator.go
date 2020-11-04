package aggregator

import (
	"time"

	parser "github.com/VictoriaMetrics/VictoriaMetrics/lib/protoparser/statsd"
)

var defaultSet *Set

func init() {
	flushInterval := time.Second * 10
	cleanUpInterval := time.Minute

	defaultSet = NewSet(flushInterval, []float64{0.5, 0.9, 0.97, 0.99, 1})
	go func() {
		ticker := time.NewTicker(flushInterval)
		for range ticker.C {
			defaultSet.Push()
			defaultSet.CleanUp(int(cleanUpInterval / flushInterval))
		}
	}()
}

func Insert(key string, row parser.Row) {
	defaultSet.Insert(key, &row)
}
