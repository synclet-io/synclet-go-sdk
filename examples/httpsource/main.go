package main

import (
	"log"
	"os"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/examples/httpsource/apisource"
)

func main() {
	hsrc := apisource.NewAPISource("https://dummyjson.com")
	runner := synclet.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
