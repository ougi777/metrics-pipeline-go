package messaging

import (
	"context"
	"time"
)

// Ping verifies that RabbitMQ accepts a connection within the caller deadline.
func Ping(ctx context.Context, url string) error {
	session, err := dialAMQPSession(ctx, url, 5*time.Second)
	if err != nil {
		return err
	}
	return session.Close()
}
