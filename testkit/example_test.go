package testkit_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

	synclet "github.com/synclet-io/synclet-go-sdk"
	"github.com/synclet-io/synclet-go-sdk/examples/httpsource/apisource"
	"github.com/synclet-io/synclet-go-sdk/testkit"
)

func TestExampleSource(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /users/1", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, apisource.User{ID: 1, FirstName: "Ann"})
	})
	mux.HandleFunc("GET /users", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"users": []apisource.User{
			{ID: 1, FirstName: "Ann", LastName: "Lee", Email: "ann@example.com", Phone: "1", Age: 30},
			{ID: 2, FirstName: "Bob", LastName: "Ray", Email: "bob@example.com", Phone: "2", Age: 41},
		}})
	})
	mux.HandleFunc("GET /products", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(t, w, map[string]any{"products": []apisource.Product{
			{ID: 1, Title: "Pen", Description: "Blue", Price: 1.5, Category: "office", Brand: "Acme"},
		}})
	})
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	cfg, err := os.ReadFile("../examples/httpsource/config.json")
	require.NoError(t, err)
	raw, err := os.ReadFile("../examples/httpsource/catalog.json")
	require.NoError(t, err)
	var catalog synclet.ConfiguredCatalog
	require.NoError(t, json.Unmarshal(raw, &catalog))

	testkit.RunSourceSuite(t, apisource.New(srv.URL), testkit.SourceSuite{
		Config:       cfg,
		BrokenConfig: json.RawMessage(`{"limit":"ten"}`),
		Catalog:      &catalog,
	})
}

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Errorf("encode response: %v", err)
	}
}
