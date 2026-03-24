package remoteread

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/maratoid/tsdb-aggregate-proxy/cli"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
)

// buildReadRequest encodes a ReadRequest as snappy+proto bytes.
func buildReadRequest(t *testing.T, queries []*prompb.Query) *bytes.Reader {
	t.Helper()
	b, err := proto.Marshal(&prompb.ReadRequest{Queries: queries})
	assert.NoError(t, err)
	return bytes.NewReader(snappy.Encode(nil, b))
}

// decodeReadResponse decodes a snappy+proto ReadResponse from the response body.
func decodeReadResponse(t *testing.T, body io.Reader) *prompb.ReadResponse {
	t.Helper()
	raw, err := io.ReadAll(body)
	assert.NoError(t, err)
	decompressed, err := snappy.Decode(nil, raw)
	assert.NoError(t, err)
	var resp prompb.ReadResponse
	err = proto.Unmarshal(decompressed, &resp)
	assert.NoError(t, err)
	return &resp
}

// makeUpstream returns an httptest.Server that responds with the given ReadResponse.
func makeUpstream(t *testing.T, response *prompb.ReadResponse, statusCode int) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if statusCode != http.StatusOK {
			w.WriteHeader(statusCode)
			return
		}
		b, err := proto.Marshal(response)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", RemoteReadContentType)
		w.Header().Set("Content-Encoding", RemoteReadContentEncoding)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(snappy.Encode(nil, b))
	}))
}

func setupHandlerApp(path string) (*fiber.App, prometheusRemoteReadHandler) {
	app := fiber.New()
	h := prometheusRemoteReadHandler{App: app, Chi: nil, Path: path}
	app.Post(path, h.handlePrometheusRemoteRead)
	return app, h
}

func TestHandlePrometheusRemoteRead_HappyPath(t *testing.T) {
	upstream := makeUpstream(t, &prompb.ReadResponse{
		Results: []*prompb.QueryResult{
			{
				Timeseries: []*prompb.TimeSeries{
					{
						Labels:  []prompb.Label{{Name: "__name__", Value: "test_metric"}},
						Samples: []prompb.Sample{{Value: 42.0, Timestamp: 1000}},
					},
				},
			},
		},
	}, http.StatusOK)
	defer upstream.Close()

	setCLIDefaults()
	cli.CLI.QueryBypass = false
	cli.CLI.TargetRaw = upstream.URL

	app, _ := setupHandlerApp("/api/v1/read")

	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/read",
		buildReadRequest(t, []*prompb.Query{
			makeQuery(now.Add(-1*time.Hour).UnixMilli(), now.UnixMilli(), 0),
		}),
	)
	req.Header.Set("Content-Type", RemoteReadContentType)

	resp, err := app.Test(req, 5000)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, RemoteReadContentType, resp.Header.Get("Content-Type"))

	readResp := decodeReadResponse(t, resp.Body)
	assert.Len(t, readResp.Results, 1)
	assert.Len(t, readResp.Results[0].Timeseries, 1)
	ts := readResp.Results[0].Timeseries[0]
	assert.Equal(t, "test_metric", ts.Labels[0].Value)
	assert.Equal(t, float64(42), ts.Samples[0].Value)
}

func TestHandlePrometheusRemoteRead_BadBody(t *testing.T) {
	setCLIDefaults()
	cli.CLI.QueryBypass = false

	app, _ := setupHandlerApp("/api/v1/read")

	req := httptest.NewRequest(http.MethodPost, "/api/v1/read",
		bytes.NewReader([]byte("not snappy compressed garbage")),
	)
	req.Header.Set("Content-Type", RemoteReadContentType)

	resp, err := app.Test(req, 5000)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
}

func TestHandlePrometheusRemoteRead_InvalidProto(t *testing.T) {
	setCLIDefaults()
	cli.CLI.QueryBypass = false

	app, _ := setupHandlerApp("/api/v1/read")

	// Valid snappy compression of garbage (not a valid protobuf)
	garbage := snappy.Encode(nil, []byte("this is not a valid protobuf message at all!!!"))
	req := httptest.NewRequest(http.MethodPost, "/api/v1/read",
		bytes.NewReader(garbage),
	)
	req.Header.Set("Content-Type", RemoteReadContentType)

	resp, err := app.Test(req, 5000)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusUnprocessableEntity, resp.StatusCode)
}

func TestHandlePrometheusRemoteRead_UpstreamError(t *testing.T) {
	upstream := makeUpstream(t, nil, http.StatusInternalServerError)
	defer upstream.Close()

	setCLIDefaults()
	cli.CLI.QueryBypass = false
	cli.CLI.TargetRaw = upstream.URL

	app, _ := setupHandlerApp("/api/v1/read")

	now := time.Now()
	req := httptest.NewRequest(http.MethodPost, "/api/v1/read",
		buildReadRequest(t, []*prompb.Query{
			makeQuery(now.Add(-1*time.Hour).UnixMilli(), now.UnixMilli(), 0),
		}),
	)
	req.Header.Set("Content-Type", RemoteReadContentType)

	resp, err := app.Test(req, 5000)
	assert.NoError(t, err)
	// The handler forwards the upstream status code directly; 424 is only used
	// for connection-level errors (couldn't reach upstream at all).
	assert.Equal(t, http.StatusInternalServerError, resp.StatusCode)
}
