package helpers

import "github.com/labstack/echo/v4"

func InputError(e echo.Context, error, msg string) error {
	if error == "" {
		return e.NoContent(400)
	}

	resp := map[string]string{}
	resp["error"] = error
	if msg != "" {
		resp["message"] = msg
	}

	return e.JSON(400, resp)
}

func ServerError(e echo.Context, error, msg string) error {
	if error == "" {
		return e.NoContent(500)
	}

	resp := map[string]string{}
	resp["error"] = error
	if msg != "" {
		resp["message"] = msg
	}

	return e.JSON(500, resp)
}
