package remoteread

import (
	"testing"
	"time"

	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

func TestGenerateChiQuery(t *testing.T) {
	now := time.Now()
	nowMs := now.UnixMilli()

	h := prometheusRemoteReadHandler{App: nil, Chi: nil, Path: "/test"}

	cases := []struct {
		name           string
		startMs        int64
		endMs          int64
		rangeMs        int64
		matchers       []*prompb.LabelMatcher
		cliOverride    func()
		mustContain    []string
		mustNotContain []string
	}{
		{
			name:    "EQ __name__, raw table uses d.value",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "cpu_usage"},
			},
			mustContain: []string{
				"FROM timeseries_db.timeseries_data_table",
				"t.metric_name = 'cpu_usage'",
				"d.value",
				"max_threads = 8",
			},
			mustNotContain: []string{"d.sum_val"},
		},
		{
			name:    "EQ __name__, aggregate table uses d.sum_val",
			startMs: now.Add(-200 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "cpu_usage"},
			},
			mustContain: []string{
				"FROM timeseries_db.timeseries_1h_table",
				"t.metric_name = 'cpu_usage'",
				"d.sum_val / nullIf(d.cnt, 0)",
			},
			mustNotContain: []string{"d.value"},
		},
		{
			name:    "EQ tag matcher",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "job", Value: "prometheus"},
			},
			mustContain: []string{"t.tags['job'] = 'prometheus'"},
		},
		{
			name:    "NEQ __name__",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "__name__", Value: "cpu_usage"},
			},
			mustContain: []string{"t.metric_name != 'cpu_usage'"},
		},
		{
			name:    "NEQ tag matcher",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NEQ, Name: "job", Value: "prometheus"},
			},
			mustContain: []string{"t.tags['job'] != 'prometheus'"},
		},
		{
			name:    "RE __name__",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "__name__", Value: "cpu.*"},
			},
			mustContain: []string{"match(t.metric_name, 'cpu.*')"},
		},
		{
			name:    "RE tag matcher",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_RE, Name: "job", Value: "prom.*"},
			},
			mustContain: []string{"match(t.tags['job'], 'prom.*')"},
		},
		{
			name:    "NRE __name__",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NRE, Name: "__name__", Value: "cpu.*"},
			},
			mustContain: []string{"NOT match(t.metric_name, 'cpu.*')"},
		},
		{
			name:    "NRE tag matcher",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_NRE, Name: "job", Value: "prom.*"},
			},
			mustContain: []string{"NOT match(t.tags['job'], 'prom.*')"},
		},
		{
			name:    "multiple matchers combined",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "my_metric"},
				{Type: prompb.LabelMatcher_EQ, Name: "job", Value: "my_job"},
			},
			mustContain: []string{
				"t.metric_name = 'my_metric'",
				"t.tags['job'] = 'my_job'",
			},
		},
		{
			name:    "special char escaping in value",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "it's"},
			},
			mustContain: []string{`t.metric_name = 'it\'s'`},
		},
		{
			name:    "timestamp range present in WHERE",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
			},
			mustContain: []string{"toDateTime64("},
		},
		{
			name:    "ChiQueryMaxThreads respected",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
			},
			cliOverride: func() { cli.CLI.ChiQueryMaxThreads = 4 },
			mustContain: []string{"max_threads = 4"},
		},
		{
			name:    "ChiTsdb used in FROM clause",
			startMs: now.Add(-1 * time.Hour).UnixMilli(),
			endMs:   nowMs,
			matchers: []*prompb.LabelMatcher{
				{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "m"},
			},
			cliOverride: func() { cli.CLI.ChiTsdb = "custom_db" },
			mustContain: []string{"FROM custom_db."},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setCLIDefaults()
			if tc.cliOverride != nil {
				tc.cliOverride()
			}
			q := makeQuery(tc.startMs, tc.endMs, tc.rangeMs)
			q.Matchers = tc.matchers
			sql := h.generateChiQuery(q)

			for _, sub := range tc.mustContain {
				assert.Contains(t, sql, sub)
			}
			for _, sub := range tc.mustNotContain {
				assert.NotContains(t, sql, sub)
			}
		})
	}
}
