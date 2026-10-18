// Package synclet is a Go SDK for writing connectors that speak the Airbyte
// protocol (v0.5.2) to Synclet. A connector implements Source or Destination;
// ServeSource and ServeDestination run one command over an io.Writer, and
// package cli turns that into a process that reads its files and writes
// stdout.
//
// Airbyte is a trademark of Airbyte, Inc.; this project is not affiliated with or endorsed by Airbyte, Inc.
package synclet
