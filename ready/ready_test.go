package ready

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
)

func TestHandleReady(t *testing.T) {
	app := fiber.New()
	Handle("/health", app)

	t.Run("returns 200 on health path", func(t *testing.T) {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/health", nil))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
	})

	t.Run("returns 404 on unknown path", func(t *testing.T) {
		resp, err := app.Test(httptest.NewRequest(http.MethodGet, "/notfound", nil))
		assert.NoError(t, err)
		assert.Equal(t, http.StatusNotFound, resp.StatusCode)
	})
}
