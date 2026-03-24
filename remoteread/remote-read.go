package remoteread

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	rowsutils "github.com/EpicStep/clickhouse-go-rows-utils"
	"github.com/gofiber/fiber/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/maratoid/tsdb-aggregate-proxy/metrics"
	"github.com/prometheus/prometheus/prompb"
)

type prometheusRemoteReadHandler struct {
	App  *fiber.App
	Chi  driver.Conn
	Path string
}

type remoteReadResult struct {
	Index int
	Data  []*prompb.QueryResult
	Err   *fiber.Error
}

const RemoteReadContentType = "application/x-protobuf"
const RemoteReadContentEncoding = "snappy"

func (h prometheusRemoteReadHandler) queryToEndpoint(q *prompb.Query) string {
	start := time.UnixMilli(q.StartTimestampMs)
	end := time.UnixMilli(q.EndTimestampMs)
	duration := end.Sub(start)
	rangeMinutes := float64(q.Hints.GetRangeMs()) / (1000.0 * 60.0)

	log.Debug().Msgf("In ms: query start=%d, end=%d, duration=%d, rangeMinutes=%f, highThreshold=%d, normalThreshold=%d, lowThreshold=%d",
		start.UnixMilli(), end.UnixMilli(), duration.Milliseconds(), rangeMinutes,
		cli.CLI.HighThreshold.Milliseconds(), cli.CLI.NormalThreshold.Milliseconds(), cli.CLI.LowThreshold.Milliseconds())

	if rangeMinutes == 0 {
		switch {
		case duration < cli.CLI.HighThreshold:
			return cli.CLI.TargetRaw
		case duration < cli.CLI.NormalThreshold:
			return cli.CLI.TargetHigh
		case duration < cli.CLI.LowThreshold:
			return cli.CLI.TargetNormal
		default:
			return cli.CLI.TargetLow
		}
	} else {
		log.Debug().Msgf("Range minutes hint '%f' found in query '%s' - will use it instead of duration thresholds to determine correct clickhouse endpoint",
			rangeMinutes, queryId(q))
		switch {
		case rangeMinutes <= 5:
			return cli.CLI.TargetHigh
		case rangeMinutes <= 60:
			return cli.CLI.TargetNormal
		default:
			return cli.CLI.TargetLow
		}
	}
}

type chiTable struct {
	Name      string
	Aggregate bool
}

func (h prometheusRemoteReadHandler) queryToTable(q *prompb.Query) chiTable {
	start := time.UnixMilli(q.StartTimestampMs)
	end := time.UnixMilli(q.EndTimestampMs)
	duration := end.Sub(start)
	rangeMinutes := float64(q.Hints.GetRangeMs()) / (1000.0 * 60.0)

	log.Debug().Msgf("In ms: query start=%d, end=%d, duration=%d, rangeMinutes=%f, highThreshold=%d, normalThreshold=%d, lowThreshold=%d",
		start.UnixMilli(), end.UnixMilli(), duration.Milliseconds(), rangeMinutes,
		cli.CLI.HighThreshold.Milliseconds(), cli.CLI.NormalThreshold.Milliseconds(), cli.CLI.LowThreshold.Milliseconds())

	var table chiTable
	if rangeMinutes == 0 {
		switch {
		case duration < cli.CLI.HighThreshold:
			table = chiTable{Name: cli.CLI.ChiTableRaw, Aggregate: false}
		case duration < cli.CLI.NormalThreshold:
			table = chiTable{Name: cli.CLI.ChiTableHigh, Aggregate: true}
		case duration < cli.CLI.LowThreshold:
			table = chiTable{Name: cli.CLI.ChiTableNormal, Aggregate: true}
		default:
			table = chiTable{Name: cli.CLI.ChiTableLow, Aggregate: true}
		}
	} else {
		log.Debug().Msgf("Range minutes hint '%f' found in query '%s' - will use it instead of duration thresholds to determine correct clickhouse endpoint",
			rangeMinutes, queryId(q))
		switch {
		case rangeMinutes <= 5:
			table = chiTable{Name: cli.CLI.ChiTableHigh, Aggregate: true}
		case rangeMinutes <= 60:
			table = chiTable{Name: cli.CLI.ChiTableNormal, Aggregate: true}
		default:
			table = chiTable{Name: cli.CLI.ChiTableLow, Aggregate: true}
		}
	}
	metrics.QueriesTotal.WithLabelValues(table.Name).Inc()
	return table
}

func (h prometheusRemoteReadHandler) generateChiQuery(query *prompb.Query) string {
	chiTable := h.queryToTable(query)

	log.Info().Msgf("Directing query '%s' to clickhouse table '%s'", queryId(query), chiTable.Name)

	startTime := float64(query.StartTimestampMs) / 1000.0
	endTime := float64(query.EndTimestampMs) / 1000.0

	// Extract matchers
	var metricName string
	var conditions []string

	for _, matcher := range query.Matchers {
		switch matcher.Type {
		case prompb.LabelMatcher_EQ:
			if matcher.Name == "__name__" {
				metricName = matcher.Value
			} else {
				conditions = append(conditions, fmt.Sprintf("t.tags['%s'] = '%s'",
					escapeIdentifier(matcher.Name),
					escapeString(matcher.Value)))
			}
		case prompb.LabelMatcher_NEQ:
			if matcher.Name == "__name__" {
				conditions = append(conditions, fmt.Sprintf("t.metric_name != '%s'",
					escapeString(matcher.Value)))
			} else {
				conditions = append(conditions, fmt.Sprintf("t.tags['%s'] != '%s'",
					escapeIdentifier(matcher.Name),
					escapeString(matcher.Value)))
			}
		case prompb.LabelMatcher_RE:
			if matcher.Name == "__name__" {
				conditions = append(conditions, fmt.Sprintf("match(t.metric_name, '%s')",
					escapeString(matcher.Value)))
			} else {
				conditions = append(conditions, fmt.Sprintf("match(t.tags['%s'], '%s')",
					escapeIdentifier(matcher.Name),
					escapeString(matcher.Value)))
			}
		case prompb.LabelMatcher_NRE:
			if matcher.Name == "__name__" {
				conditions = append(conditions, fmt.Sprintf("NOT match(t.metric_name, '%s')",
					escapeString(matcher.Value)))
			} else {
				conditions = append(conditions, fmt.Sprintf("NOT match(t.tags['%s'], '%s')",
					escapeIdentifier(matcher.Name),
					escapeString(matcher.Value)))
			}
		}
	}

	// Determine if we're querying raw or aggregate table
	var valueColumn string
	if chiTable.Aggregate {
		valueColumn = "d.sum_val / nullIf(d.cnt, 0)"
	} else {
		valueColumn = "d.value"
	}

	// Build WHERE clause for labels
	var labelConditions string
	if metricName != "" {
		labelConditions = fmt.Sprintf("t.metric_name = '%s'", escapeString(metricName))
	}
	if len(conditions) > 0 {
		if labelConditions != "" {
			labelConditions += " AND " + strings.Join(conditions, " AND ")
		} else {
			labelConditions = strings.Join(conditions, " AND ")
		}
	}

	// Build complete WHERE clause
	whereClause := fmt.Sprintf("d.timestamp >= toDateTime64(%.3f, 3) AND d.timestamp <= toDateTime64(%.3f, 3)",
		startTime, endTime)
	if labelConditions != "" {
		whereClause += " AND " + labelConditions
	}

	// Generate optimized query with proper formatting
	chiQuery := fmt.Sprintf(`SELECT 
			t.id,
			any(t.metric_name) as metric_name,
			any(t.tags) as tags,
			arraySort(x -> x.1, groupArray((d.timestamp, %s))) AS samples
		FROM %s.%s d
		INNER JOIN (
			SELECT * FROM %s.timeseries_tags_table FINAL
		) t ON d.id = t.id
		WHERE %s
		GROUP BY t.id
		ORDER BY t.id
		SETTINGS use_query_cache = 1, max_threads = %d`,
		valueColumn,
		cli.CLI.ChiTsdb, chiTable.Name,
		cli.CLI.ChiTsdb,
		whereClause, cli.CLI.ChiQueryMaxThreads)

	log.Debug().Msgf("Generated ClickHouse query:\n%s\nfor promQL query '%s'", chiQuery, queryId(query))
	return chiQuery
}

func (h prometheusRemoteReadHandler) sendPrometheusReadRequest(c *fiber.Ctx, query *prompb.Query) (int, []byte, []error) {
	endpoint := h.queryToEndpoint(query)

	log.Info().Msgf("Directing query '%s' to endpoint '%s'", query.String(), endpoint)

	queryRequest := &prompb.ReadRequest{Queries: []*prompb.Query{query}}
	queryRequestBody, _ := proto.Marshal(queryRequest)
	compressedQueryRequest := snappy.Encode(nil, queryRequestBody)

	prometheusQueryRequest := fiber.Post(endpoint).
		Set(fiber.HeaderContentType, RemoteReadContentType).
		Set(fiber.HeaderContentEncoding, RemoteReadContentEncoding)

	if authHeaderVal := c.Get(fiber.HeaderAuthorization); authHeaderVal != "" {
		prometheusQueryRequest = prometheusQueryRequest.Set(fiber.HeaderAuthorization, authHeaderVal)
	}

	return prometheusQueryRequest.Body(compressedQueryRequest).Bytes()
}

func (h prometheusRemoteReadHandler) remoteReadWorker(c *fiber.Ctx, query *prompb.Query, index int, results chan<- remoteReadResult, wg *sync.WaitGroup) {
	defer wg.Done() // Decrement the WaitGroup counter when the goroutine finishes

	var data []*prompb.QueryResult
	var err *fiber.Error

	if cli.CLI.QueryBypass {
		data, err = h.remoteReadWorkerClickhouseQuery(query)
	} else {
		data, err = h.remoteReadWorkerPrometheusQuery(c, query)
	}

	if err != nil {
		results <- remoteReadResult{
			Index: index,
			Data:  nil,
			Err:   err,
		}
		return
	}

	log.Debug().Msgf("Reporting result index=%d, for query %s", index, queryId(query))
	results <- remoteReadResult{Index: index, Data: data, Err: nil}
}

func (h prometheusRemoteReadHandler) remoteReadWorkerPrometheusQuery(c *fiber.Ctx, query *prompb.Query) ([]*prompb.QueryResult, *fiber.Error) {
	log.Debug().Msgf("Forwarding query '%s' to clickhouse prometheus remote read", queryId(query))
	statusCode, body, errors := h.sendPrometheusReadRequest(c, query)
	if len(errors) > 0 {
		log.Error().Msgf("Error while directing query '%s'. Status code: %d. Errors: %v",
			queryId(query), statusCode, errors)
		return nil, fiber.NewError(fiber.StatusFailedDependency, fmt.Sprintf("%v", errors))
	}

	if statusCode >= fiber.StatusBadRequest && statusCode <= fiber.StatusNetworkAuthenticationRequired {
		log.Error().Msgf("Error while directing query '%s'. Status code: %d. Message: %s",
			queryId(query), statusCode, string(body))
		return nil, fiber.NewError(statusCode, string(body))
	}

	responseProto, err := snappy.Decode(nil, body)
	if err != nil {
		log.Error().Msgf("Error decompressing endpoint response for query '%s': %v", queryId(query), err)
		return nil, fiber.NewError(fiber.StatusFailedDependency, err.Error())
	}

	var queryResponse prompb.ReadResponse
	err = proto.Unmarshal(responseProto, &queryResponse)
	if err != nil {
		log.Error().Msgf("Error unmarshaling endpoint response for query '%s': %v", queryId(query), err)
		return nil, fiber.NewError(fiber.StatusFailedDependency, err.Error())
	}

	return queryResponse.Results, nil
}

func (h prometheusRemoteReadHandler) remoteReadWorkerClickhouseQuery(query *prompb.Query) ([]*prompb.QueryResult, *fiber.Error) {
	chiQuery := h.generateChiQuery(query)

	rows, err := h.Chi.Query(context.Background(), chiQuery)
	if err != nil {
		log.Error().Msgf("Clickhouse query execution failed for query '%s': %v", queryId(query), err)
		return nil, fiber.NewError(fiber.StatusFailedDependency, err.Error())
	}

	var timeseries []*prompb.TimeSeries

	err = rowsutils.ForEachRow(rows, func(row rowsutils.CollectableRow) error {
		var (
			id         string
			metricName string
			tagsMap    map[string]string
			samples    [][]any // Array of (timestamp, value) tuples
		)

		if err := row.Scan(&id, &metricName, &tagsMap, &samples); err != nil {
			log.Error().Msgf("Clickhouse rows scan failed for query '%s': %v", queryId(query), err)
			return err
		}

		log.Debug().Msgf("Query '%s' row scan: id=%s, metric_name=%s",
			queryId(query), id, metricName)

		// Build labels
		labels := []prompb.Label{
			{Name: "__name__", Value: metricName},
		}

		// Add tags map, skipping empty values
		for k, v := range tagsMap {
			if v != "" {
				labels = append(labels, prompb.Label{Name: k, Value: v})
			}
		}

		// Convert samples
		var promSamples []prompb.Sample
		for _, sample := range samples {
			if len(sample) != 2 {
				log.Debug().Msg("Skipping sample: length not 2")
				continue
			}

			timestamp, ok := sample[0].(time.Time)
			if !ok {
				log.Debug().Msg("Skipping sample: sample[0] not a timestamp")
				continue
			}

			var value float64
			switch v := sample[1].(type) {
			case float64:
				value = v
			case *float64:
				if v != nil {
					value = *v
				} else {
					log.Debug().Msg("Skipping sample: sample[1] Nil float64 pointer for value")
					continue
				}
			case float32:
				value = float64(v)
			case int64:
				value = float64(v)
			case int:
				value = float64(v)
			default:
				log.Debug().Msgf("Clickhouse rows scan failed for query '%s': unknown value type '%T' for value: '%v' in sample", queryId(query), v, v)
				continue
			}

			promSamples = append(promSamples, prompb.Sample{
				Value:     value,
				Timestamp: timestamp.UnixMilli(),
			})
		}

		log.Debug().Msgf("Added %d samples", len(promSamples))

		timeseries = append(timeseries, &prompb.TimeSeries{
			Labels:  labels,
			Samples: promSamples,
		})

		if len(promSamples) > 0 {
			log.Debug().Msgf("Returning samples for series %s: first_ts=%d (%s), last_ts=%d (%s), count=%d",
				id,
				promSamples[0].Timestamp, time.UnixMilli(promSamples[0].Timestamp),
				promSamples[len(promSamples)-1].Timestamp, time.UnixMilli(promSamples[len(promSamples)-1].Timestamp),
				len(promSamples))
		}

		return nil
	})
	if err != nil {
		log.Error().Msgf("Clickhouse rows processing failed for query '%s': %v", queryId(query), err)
		return nil, fiber.NewError(fiber.StatusFailedDependency, err.Error())
	}

	log.Info().Msgf("ClickHouse query generated for promql query '%s' Returned %d time series", queryId(query), len(timeseries))

	return []*prompb.QueryResult{
		{Timeseries: timeseries},
	}, nil
}

func (h prometheusRemoteReadHandler) handlePrometheusRemoteRead(c *fiber.Ctx) error {
	start := time.Now()
	c.Accepts(RemoteReadContentType)
	c.AcceptsEncodings(RemoteReadContentEncoding)

	// Read and decompress the request body.
	reqBody, err := snappy.Decode(nil, c.Body())
	if err != nil {
		log.Error().Msgf("Error decompressing request: %v", err)
		metrics.RequestsTotal.WithLabelValues("error").Inc()
		metrics.RequestDuration.Observe(time.Since(start).Seconds())
		return fiber.NewError(fiber.StatusUnprocessableEntity, err.Error())
	}

	// Un-marshal the protobuf message.
	var remoteReadReq prompb.ReadRequest
	err = proto.Unmarshal(reqBody, &remoteReadReq)
	if err != nil {
		log.Error().Msgf("Error unmarshaling request proto: %v", err)
		metrics.RequestsTotal.WithLabelValues("error").Inc()
		metrics.RequestDuration.Observe(time.Since(start).Seconds())
		return fiber.NewError(fiber.StatusUnprocessableEntity, err.Error())
	}

	log.Info().Msgf("Got request with %d PromQL queries", len(remoteReadReq.Queries))

	response := &prompb.ReadResponse{}
	var wg sync.WaitGroup
	resultsChan := make(chan remoteReadResult, len(remoteReadReq.Queries))

	// start query workers
	for idx, query := range remoteReadReq.Queries {
		log.Debug().Msgf("Adding worker for request query[%d] id=%s (%s)", idx, queryId(query), query.String())
		wg.Add(1)
		go h.remoteReadWorker(c, query, idx, resultsChan, &wg)
	}

	// wait for all workers to finish
	go func() {
		log.Debug().Msg("Waiting on all workers to finish...")
		wg.Wait()
		close(resultsChan)
	}()

	log.Debug().Msg("All workers finished.")

	// put results into in-order array
	indexedResults := make([][]*prompb.QueryResult, len(remoteReadReq.Queries))
	for res := range resultsChan {
		if res.Err != nil {
			metrics.RequestsTotal.WithLabelValues("error").Inc()
			metrics.RequestDuration.Observe(time.Since(start).Seconds())
			return res.Err
		}
		indexedResults[res.Index] = res.Data
	}
	// flatten into results
	for _, res := range indexedResults {
		response.Results = append(response.Results, res...)
	}

	logPrometheusQueryResults(response.Results, "Final query results")

	var totalSeries int
	for _, r := range response.Results {
		totalSeries += len(r.Timeseries)
	}
	metrics.TimeSeriesReturned.Observe(float64(totalSeries))

	responseProto, err := proto.Marshal(response)
	if err != nil {
		log.Error().Msgf("Error marshaling response: %v", err)
		metrics.RequestsTotal.WithLabelValues("error").Inc()
		metrics.RequestDuration.Observe(time.Since(start).Seconds())
		return c.Status(fiber.StatusInternalServerError).SendString(fmt.Sprintf("%v", err))
	}

	metrics.RequestsTotal.WithLabelValues("success").Inc()
	metrics.RequestDuration.Observe(time.Since(start).Seconds())

	c.Set(fiber.HeaderContentType, RemoteReadContentType)
	c.Set(fiber.HeaderContentEncoding, RemoteReadContentEncoding)
	return c.Send(snappy.Encode(nil, responseProto))
}

func (h prometheusRemoteReadHandler) handle() fiber.Router {
	return h.App.Post(h.Path, h.handlePrometheusRemoteRead)
}

func Handle(path string, app *fiber.App) fiber.Router {
	var remoteReadHandler prometheusRemoteReadHandler

	if cli.CLI.QueryBypass {
		options := &clickhouse.Options{
			Addr: []string{cli.CLI.ChiUrl},
			Auth: clickhouse.Auth{
				Database: cli.CLI.ChiTsdb,
				Username: cli.CLI.ChiUser,
				Password: cli.CLI.ChiPassword,
			},
			Settings: clickhouse.Settings{
				"max_execution_time": cli.CLI.ChiMaxQuery,
			},
			DialTimeout:     cli.CLI.ChiDialTimeout,
			MaxOpenConns:    cli.CLI.ChiMaxOpenConns,
			MaxIdleConns:    cli.CLI.ChiMaxIdleConns,
			ConnMaxLifetime: cli.CLI.ChiConnLifetime,
			Compression: &clickhouse.Compression{
				Method: clickhouse.CompressionLZ4,
			},
			MaxCompressionBuffer: 10240,
			ConnOpenStrategy:     clickhouse.ConnOpenInOrder,
		}

		chi, err := clickhouse.Open(options)
		if err != nil {
			log.Fatal().Msgf("Failed to open ClickHouse connection: %v", err)
		}

		log.Info().Msgf("ClickHouse url: %s, db: %s, user: %s, dial timeout: %d, max open conns: %d, max idle conns: %d, conn lifetime: %d",
			options.Addr,
			options.Auth.Database,
			options.Auth.Username,
			options.DialTimeout,
			options.MaxOpenConns,
			options.MaxIdleConns,
			options.ConnMaxLifetime)

		remoteReadHandler = prometheusRemoteReadHandler{
			App:  app,
			Path: path,
			Chi:  chi,
		}

		if err := remoteReadHandler.Chi.Ping(context.Background()); err != nil {
			log.Fatal().Msgf("Failed to ping ClickHouse: %v", err)
		}
	} else {
		remoteReadHandler = prometheusRemoteReadHandler{
			App:  app,
			Path: path,
			Chi:  nil,
		}
	}

	return remoteReadHandler.handle()
}
