package httptransport

import "github.com/gin-gonic/gin"

const (
	ErrorCodeInvalidParams = "INVALID_PARAMS"
	ErrorCodeTaskNotFound  = "TASK_NOT_FOUND"
	ErrorCodeMQUnavailable = "MQ_UNAVAILABLE"
	ErrorCodeInternal      = "INTERNAL"
)

type ErrorResponse struct {
	Error APIError `json:"error"`
}

type APIError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func WriteError(c *gin.Context, status int, code string, message string) {
	c.JSON(status, ErrorResponse{
		Error: APIError{
			Code:    code,
			Message: message,
		},
	})
}
