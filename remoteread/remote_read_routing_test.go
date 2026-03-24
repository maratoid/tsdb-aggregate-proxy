package remoteread

import (
	"testing"
	"time"

	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

func setCLIDefaults() {
	cli.CLI.HighThreshold = 24 * time.Hour
	cli.CLI.NormalThreshold = 48 * time.Hour
	cli.CLI.LowThreshold = 120 * time.Hour
	cli.CLI.TargetRaw = "http://raw.example.com"
	cli.CLI.TargetHigh = "http://high.example.com"
	cli.CLI.TargetNormal = "http://normal.example.com"
	cli.CLI.TargetLow = "http://low.example.com"
	cli.CLI.ChiTableRaw = "timeseries_data_table"
	cli.CLI.ChiTableHigh = "timeseries_1m_table"
	cli.CLI.ChiTableNormal = "timeseries_5m_table"
	cli.CLI.ChiTableLow = "timeseries_1h_table"
	cli.CLI.ChiTsdb = "timeseries_db"
	cli.CLI.ChiQueryMaxThreads = 8
}

func makeQuery(startMs, endMs, rangeMs int64) *prompb.Query {
	q := &prompb.Query{
		StartTimestampMs: startMs,
		EndTimestampMs:   endMs,
	}
	if rangeMs > 0 {
		q.Hints = &prompb.ReadHints{RangeMs: rangeMs}
	}
	return q
}

func TestQueryToEndpoint(t *testing.T) {
	now := time.Now()
	nowMs := now.UnixMilli()

	h := prometheusRemoteReadHandler{App: nil, Chi: nil, Path: "/test"}

	cases := []struct {
		name     string
		startMs  int64
		endMs    int64
		rangeMs  int64
		expected string
	}{
		{
			name:     "duration < 24h → raw",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://raw.example.com",
		},
		{
			name:     "duration < 24h boundary → raw",
			startMs:  now.Add(-24*time.Hour + time.Millisecond).UnixMilli(),
			endMs:    nowMs,
			expected: "http://raw.example.com",
		},
		{
			name:     "duration == 24h → high",
			startMs:  now.Add(-24 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://high.example.com",
		},
		{
			name:     "duration 24h–48h → high",
			startMs:  now.Add(-36 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://high.example.com",
		},
		{
			name:     "duration == 48h → normal",
			startMs:  now.Add(-48 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://normal.example.com",
		},
		{
			name:     "duration 48h–120h → normal",
			startMs:  now.Add(-72 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://normal.example.com",
		},
		{
			name:     "duration == 120h → low",
			startMs:  now.Add(-120 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://low.example.com",
		},
		{
			name:     "duration > 120h → low",
			startMs:  now.Add(-200 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			expected: "http://low.example.com",
		},
		{
			name:     "rangeMs hint 1min → high",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			rangeMs:  1 * 60 * 1000,
			expected: "http://high.example.com",
		},
		{
			name:     "rangeMs hint == 5min → high",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			rangeMs:  5 * 60 * 1000,
			expected: "http://high.example.com",
		},
		{
			name:     "rangeMs hint 30min → normal",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			rangeMs:  30 * 60 * 1000,
			expected: "http://normal.example.com",
		},
		{
			name:     "rangeMs hint == 60min → normal",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			rangeMs:  60 * 60 * 1000,
			expected: "http://normal.example.com",
		},
		{
			name:     "rangeMs hint > 60min → low",
			startMs:  now.Add(-1 * time.Hour).UnixMilli(),
			endMs:    nowMs,
			rangeMs:  90 * 60 * 1000,
			expected: "http://low.example.com",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setCLIDefaults()
			q := makeQuery(tc.startMs, tc.endMs, tc.rangeMs)
			assert.Equal(t, tc.expected, h.queryToEndpoint(q))
		})
	}
}

func TestQueryToTable(t *testing.T) {
	now := time.Now()
	nowMs := now.UnixMilli()

	h := prometheusRemoteReadHandler{App: nil, Chi: nil, Path: "/test"}

	cases := []struct {
		name              string
		startMs           int64
		endMs             int64
		rangeMs           int64
		expectedName      string
		expectedAggregate bool
	}{
		{
			name:              "duration < 24h → raw table, no aggregate",
			startMs:           now.Add(-1 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			expectedName:      "timeseries_data_table",
			expectedAggregate: false,
		},
		{
			name:              "duration 24h–48h → high table, aggregate",
			startMs:           now.Add(-36 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			expectedName:      "timeseries_1m_table",
			expectedAggregate: true,
		},
		{
			name:              "duration 48h–120h → normal table, aggregate",
			startMs:           now.Add(-72 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			expectedName:      "timeseries_5m_table",
			expectedAggregate: true,
		},
		{
			name:              "duration > 120h → low table, aggregate",
			startMs:           now.Add(-200 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			expectedName:      "timeseries_1h_table",
			expectedAggregate: true,
		},
		{
			name:              "rangeMs hint ≤ 5min → high table, aggregate",
			startMs:           now.Add(-1 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			rangeMs:           5 * 60 * 1000,
			expectedName:      "timeseries_1m_table",
			expectedAggregate: true,
		},
		{
			name:              "rangeMs hint 6–60min → normal table, aggregate",
			startMs:           now.Add(-1 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			rangeMs:           30 * 60 * 1000,
			expectedName:      "timeseries_5m_table",
			expectedAggregate: true,
		},
		{
			name:              "rangeMs hint > 60min → low table, aggregate",
			startMs:           now.Add(-1 * time.Hour).UnixMilli(),
			endMs:             nowMs,
			rangeMs:           90 * 60 * 1000,
			expectedName:      "timeseries_1h_table",
			expectedAggregate: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setCLIDefaults()
			q := makeQuery(tc.startMs, tc.endMs, tc.rangeMs)
			table := h.queryToTable(q)
			assert.Equal(t, tc.expectedName, table.Name)
			assert.Equal(t, tc.expectedAggregate, table.Aggregate)
		})
	}
}
