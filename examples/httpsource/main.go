// Command httpsource is a source that reads users and products from the
// DummyJSON API.
package main

import (
	"github.com/synclet-io/synclet-go-sdk/cli"
	"github.com/synclet-io/synclet-go-sdk/examples/httpsource/apisource"
)

func main() { cli.RunSource(apisource.New("https://dummyjson.com")) }
