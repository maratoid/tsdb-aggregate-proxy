package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	RequestsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tsdb_proxy_requests_total",
			Help: "Total number of remote read requests.",
		},
		[]string{"status"},
	)

	RequestDuration = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tsdb_proxy_request_duration_seconds",
			Help:    "Duration of remote read requests in seconds.",
			Buckets: prometheus.DefBuckets,
		},
	)

	QueriesTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "tsdb_proxy_queries_total",
			Help: "Total number of PromQL queries routed, by target table.",
		},
		[]string{"table"},
	)

	TimeSeriesReturned = promauto.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "tsdb_proxy_timeseries_returned",
			Help:    "Number of time series returned per remote read request.",
			Buckets: []float64{0, 1, 5, 10, 50, 100, 500, 1000, 5000},
		},
	)
)
