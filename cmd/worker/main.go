package main

import (
	"os"

	"github.com/ougi777/metrics-pipeline-go/internal/app"
)

func main() {
	os.Exit(app.Run("worker"))
}
