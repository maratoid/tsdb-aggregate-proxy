package metrics

import (
	"github.com/gofiber/fiber/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/expfmt"
)

type metricsHandler struct {
	App  *fiber.App
	Path string
}

func (h metricsHandler) handleMetrics(c *fiber.Ctx) error {
	mfs, err := prometheus.DefaultGatherer.Gather()
	if err != nil {
		return fiber.NewError(fiber.StatusInternalServerError, err.Error())
	}

	contentType := expfmt.NewFormat(expfmt.TypeTextPlain)
	c.Set(fiber.HeaderContentType, string(contentType))

	enc := expfmt.NewEncoder(c.Response().BodyWriter(), contentType)
	for _, mf := range mfs {
		if err := enc.Encode(mf); err != nil {
			return fiber.NewError(fiber.StatusInternalServerError, err.Error())
		}
	}
	return nil
}

func Handle(path string, app *fiber.App) fiber.Router {
	handler := metricsHandler{App: app, Path: path}
	return app.Get(path, handler.handleMetrics)
}
