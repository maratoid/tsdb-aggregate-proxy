package remoteread

import (
	"crypto/sha1"
	"fmt"
	"strings"

	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/prometheus/prometheus/prompb"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type QueryResultsMarshaler struct {
	Results []*prompb.QueryResult
}

type QueryResultMarshaler struct {
	Result *prompb.QueryResult
}

type TimeseriesMarshaler struct {
	TS *prompb.TimeSeries
}

type LabelsMarshaler struct {
	Labels []prompb.Label
}

type LabelMarshaler struct {
	Label prompb.Label
}

type SamplesMarshaler struct {
	Samples []prompb.Sample
}

type SampleMarshaler struct {
	Sample prompb.Sample
}

func (l LabelMarshaler) MarshalZerologObject(e *zerolog.Event) {
	e.Str("name", l.Label.GetName())
	e.Str("value", l.Label.GetValue())
}

func (s SampleMarshaler) MarshalZerologObject(e *zerolog.Event) {
	e.Int64("timestamp", s.Sample.GetTimestamp())
	e.Float64("value", s.Sample.GetValue())
}

func (s SamplesMarshaler) MarshalZerologArray(e *zerolog.Array) {
	if s.Samples == nil {
		return
	}

	for _, sample := range s.Samples {
		e.Object(SampleMarshaler{Sample: sample})
	}
}

func (l LabelsMarshaler) MarshalZerologArray(e *zerolog.Array) {
	if l.Labels == nil {
		return
	}

	for _, label := range l.Labels {
		e.Object(LabelMarshaler{Label: label})
	}
}

func (t TimeseriesMarshaler) MarshalZerologObject(e *zerolog.Event) {
	if t.TS == nil {
		return
	}

	e.Int("num_labels", len(t.TS.GetLabels()))
	e.Int("num_samples", len(t.TS.GetSamples()))
	e.Array("labels", LabelsMarshaler{Labels: t.TS.GetLabels()})
	if cli.CLI.DebugLogSamples {
		e.Array("samples", SamplesMarshaler{Samples: t.TS.GetSamples()})
	}
}

func (q QueryResultMarshaler) MarshalZerologObject(e *zerolog.Event) {
	if q.Result == nil {
		return
	}
	e.Int("num_timeseries", len(q.Result.GetTimeseries()))
	e.Array("timeseries", q)
}

func (q QueryResultMarshaler) MarshalZerologArray(e *zerolog.Array) {
	if q.Result == nil {
		return
	}

	for _, timeSeries := range q.Result.GetTimeseries() {
		e.Object(TimeseriesMarshaler{TS: timeSeries})
	}
}

func (q QueryResultsMarshaler) MarshalZerologObject(e *zerolog.Event) {
	if q.Results == nil {
		return
	}
	e.Int("num_results", len(q.Results))

	e.Array("results", q)
}

func (q QueryResultsMarshaler) MarshalZerologArray(e *zerolog.Array) {
	if q.Results == nil {
		return
	}

	for _, result := range q.Results {
		e.Object(QueryResultMarshaler{Result: result})
	}
}

func escapeString(s string) string {
	return strings.ReplaceAll(strings.ReplaceAll(s, "\\", "\\\\"), "'", "\\'")
}

func escapeIdentifier(s string) string {
	// ClickHouse identifiers - only allow alphanumeric and underscore
	return strings.Map(func(r rune) rune {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') ||
			(r >= '0' && r <= '9') || r == '_' {
			return r
		}
		return '_'
	}, s)
}

func logPrometheusQueryResults(results []*prompb.QueryResult, msg string) {
	if results == nil {
		return
	}

	log.Debug().
		Object("query_results", QueryResultsMarshaler{Results: results}).
		Msg(msg)
}

func queryId(query *prompb.Query) string {
	if query == nil {
		return ""
	}

	h := sha1.New()
	h.Write([]byte(query.String()))

	return fmt.Sprintf("%x", h.Sum(nil))[:7]
}
