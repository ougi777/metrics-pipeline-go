package main

import (
	"os"

	simapp "github.com/ougi777/metrics-pipeline-go/internal/app/sim"
)

func main() {
	os.Exit(simapp.Run())
}
