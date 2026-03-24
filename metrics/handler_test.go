package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func TestHandleMetrics(t *testing.T) {
	app := fiber.New()
	Handle("/metrics", app)

	resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	assert.NoError(t, err)

	assert.Contains(t, resp.Header.Get("Content-Type"), "text/plain")
	assert.Contains(t, string(body), "# HELP")
	// Histograms (non-Vec) emit data even with zero observations; CounterVec does not
	// emit until a label combination is first used.
	assert.Contains(t, string(body), "tsdb_proxy_request_duration_seconds")
}
