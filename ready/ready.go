package ready

import (
	"github.com/gofiber/fiber/v2"
	"github.com/rs/zerolog/log"
)

type readyHandler struct {
	App  *fiber.App
	Path string
}

func (h readyHandler) handleReady(c *fiber.Ctx) error {
	log.Debug().Msg("Ready ping - OK")
	return c.SendStatus(fiber.StatusOK)
}

func (h readyHandler) handle() fiber.Router {
	return h.App.Get(h.Path, h.handleReady)
}

func Handle(path string, app *fiber.App) fiber.Router {
	handler := readyHandler{
		App:  app,
		Path: path,
	}

	return handler.handle()
}
