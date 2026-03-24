package remoteread

import (
	"errors"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"
	"github.com/prometheus/prometheus/prompb"
	mockhouse "github.com/srikanthccv/ClickHouse-go-mock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// chiCols are the four columns returned by generateChiQuery.
var chiCols = []mockhouse.ColumnType{
	{Name: "id", Type: "String"},
	{Name: "metric_name", Type: "String"},
	{Name: "tags", Type: "Map(String, String)"},
	{Name: "samples", Type: "Array(Tuple(DateTime64(3), Float64))"},
}

func chiBaseQuery() *prompb.Query {
	now := time.Now()
	q := makeQuery(now.Add(-1*time.Hour).UnixMilli(), now.UnixMilli(), 0)
	q.Matchers = []*prompb.LabelMatcher{
		{Type: prompb.LabelMatcher_EQ, Name: "__name__", Value: "cpu_usage"},
	}
	return q
}

// ── error paths ──────────────────────────────────────────────────────────────

func TestRemoteReadWorkerClickhouseQuery_QueryError(t *testing.T) {
	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT").WillReturnError(errors.New("connection refused"))

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	assert.Nil(t, results)
	require.NotNil(t, fiberErr)
	assert.Equal(t, 424, fiberErr.Code)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRemoteReadWorkerClickhouseQuery_ScanError(t *testing.T) {
	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	// Only 1 column but Scan expects 4 → OpError from scan.go.
	mock.ExpectQuery("SELECT").WillReturnRows(
		mockhouse.NewRows(
			[]mockhouse.ColumnType{{Name: "id", Type: "String"}},
			[][]any{{"id1"}},
		),
	)

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	assert.Nil(t, results)
	require.NotNil(t, fiberErr)
	assert.Equal(t, 424, fiberErr.Code)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRemoteReadWorkerClickhouseQuery_EmptyResult(t *testing.T) {
	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT").WillReturnRows(
		mockhouse.NewRows(chiCols, [][]any{}),
	)

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	require.Nil(t, fiberErr)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].Timeseries)
	assert.NoError(t, mock.ExpectationsWereMet())
}

// ── happy paths ───────────────────────────────────────────────────────────────

func TestRemoteReadWorkerClickhouseQuery_HappyPath(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Millisecond)

	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT").WillReturnRows(
		mockhouse.NewRows(chiCols, [][]any{
			{
				"id1",
				"cpu_usage",
				map[string]string{"job": "prometheus", "instance": "localhost:9090"},
				[][]any{{ts, float64(3.14)}},
			},
		}),
	)

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	require.Nil(t, fiberErr)
	require.Len(t, results, 1)
	require.Len(t, results[0].Timeseries, 1)

	series := results[0].Timeseries[0]
	labelMap := make(map[string]string)
	for _, l := range series.Labels {
		labelMap[l.Name] = l.Value
	}
	assert.Equal(t, "cpu_usage", labelMap["__name__"])
	assert.Equal(t, "prometheus", labelMap["job"])
	assert.Equal(t, "localhost:9090", labelMap["instance"])

	require.Len(t, series.Samples, 1)
	assert.Equal(t, 3.14, series.Samples[0].Value)
	assert.Equal(t, ts.UnixMilli(), series.Samples[0].Timestamp)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRemoteReadWorkerClickhouseQuery_MultipleRows(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Millisecond)

	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT").WillReturnRows(
		mockhouse.NewRows(chiCols, [][]any{
			{"id1", "cpu_usage", map[string]string{}, [][]any{{ts, float64(1.0)}}},
			{"id2", "mem_usage", map[string]string{}, [][]any{{ts, float64(2.0)}}},
		}),
	)

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	require.Nil(t, fiberErr)
	require.Len(t, results, 1)
	assert.Len(t, results[0].Timeseries, 2)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestRemoteReadWorkerClickhouseQuery_EmptyTagValuesSkipped(t *testing.T) {
	ts := time.Now().UTC().Truncate(time.Millisecond)

	mock, err := mockhouse.NewClickHouseWithQueryMatcher(nil, sqlmock.QueryMatcherRegexp)
	require.NoError(t, err)
	mock.ExpectQuery("SELECT").WillReturnRows(
		mockhouse.NewRows(chiCols, [][]any{
			{
				"id1",
				"cpu_usage",
				map[string]string{"job": "prometheus", "empty_tag": ""},
				[][]any{{ts, float64(1.0)}},
			},
		}),
	)

	setCLIDefaults()
	h := prometheusRemoteReadHandler{App: nil, Chi: mock, Path: "/test"}
	results, fiberErr := h.remoteReadWorkerClickhouseQuery(chiBaseQuery())

	require.Nil(t, fiberErr)
	require.Len(t, results[0].Timeseries, 1)

	labelNames := make([]string, 0)
	for _, l := range results[0].Timeseries[0].Labels {
		labelNames = append(labelNames, l.Name)
	}
	assert.Contains(t, labelNames, "job")
	assert.NotContains(t, labelNames, "empty_tag")
	assert.NoError(t, mock.ExpectationsWereMet())
}
