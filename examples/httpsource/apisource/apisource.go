// Package apisource is an example source that reads two streams from the
// DummyJSON HTTP API.
package apisource

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

// Config is the source's configuration.
type Config struct {
	Limit int `json:"limit"`
}

func (c *Config) applyDefaults() {
	if c.Limit <= 0 {
		c.Limit = 30
	}
}

// LastSync is the state saved for each stream.
type LastSync struct {
	Timestamp int64 `json:"timestamp"`
}

// APISource reads the users and products streams of a DummyJSON server.
type APISource struct {
	baseURL string
	client  *http.Client
}

// New returns a source reading from baseURL.
func New(baseURL string) *APISource {
	return &APISource{baseURL: baseURL, client: &http.Client{Timeout: 30 * time.Second}}
}

func (s *APISource) Spec(context.Context) (*synclet.ConnectorSpecification, error) {
	return &synclet.ConnectorSpecification{
		DocumentationURL: "https://dummyjson.com",
		ChangeLogURL:     "https://github.com/synclet-io/synclet-go-sdk",
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
		},
		ProtocolVersion: "0.5.2",
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "DummyJSON Source",
			Description: "Example source connector that fetches data from dummyjson.com API",
			Type:        "object",
			Required:    []synclet.PropertyName{},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"limit": {
						Description:  "Maximum number of records to fetch per stream (default: 30)",
						Examples:     []string{"10", "30", "100"},
						PropertyType: synclet.PropertyType{Type: synclet.PropTypes{synclet.Integer}},
					},
				},
			},
		},
	}, nil
}

func (s *APISource) Check(ctx context.Context, _ Config) error {
	var u User
	if err := s.get(ctx, "/users/1", &u); err != nil {
		return fmt.Errorf("cannot reach DummyJSON API: %w", err)
	}
	return nil
}

func (s *APISource) Discover(context.Context, Config) (*synclet.Catalog, error) {
	return &synclet.Catalog{Streams: []synclet.Stream{
		{
			Name: "users",
			JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
				"id":        prop(synclet.Integer, "User ID"),
				"firstName": prop(synclet.String, "First name"),
				"lastName":  prop(synclet.String, "Last name"),
				"email":     prop(synclet.String, "Email address"),
				"phone":     prop(synclet.String, "Phone number"),
				"age":       prop(synclet.Integer, "Age"),
			}),
			SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
			Namespace:          "dummyjson",
		},
		{
			Name: "products",
			JSONSchema: synclet.ObjectSchema(map[synclet.PropertyName]synclet.PropertySpec{
				"id":          prop(synclet.Integer, "Product ID"),
				"title":       prop(synclet.String, "Product title"),
				"description": prop(synclet.String, "Product description"),
				"price":       prop(synclet.Number, "Product price"),
				"category":    prop(synclet.String, "Product category"),
				"brand":       prop(synclet.String, "Product brand"),
			}),
			SupportedSyncModes: []synclet.SyncMode{synclet.SyncModeFullRefresh},
			Namespace:          "dummyjson",
		},
	}}, nil
}

func prop(t synclet.PropType, desc string) synclet.PropertySpec {
	return synclet.PropertySpec{Description: desc, PropertyType: synclet.PropertyType{Type: synclet.PropTypes{t}}}
}

// User is one row of the users stream.
type User struct {
	ID        int    `json:"id"`
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Email     string `json:"email"`
	Phone     string `json:"phone"`
	Age       int    `json:"age"`
}

// Product is one row of the products stream.
type Product struct {
	ID          int     `json:"id"`
	Title       string  `json:"title"`
	Description string  `json:"description"`
	Price       float64 `json:"price"`
	Category    string  `json:"category"`
	Brand       string  `json:"brand"`
}

func (s *APISource) Read(ctx context.Context, cfg Config, cat *synclet.ConfiguredCatalog, st *synclet.InputState, em *synclet.Emitter) error {
	cfg.applyDefaults()
	log := synclet.Logger(ctx)
	for _, cs := range cat.Streams {
		desc := cs.Stream.Descriptor()
		var last LastSync
		if raw := st.Stream(desc); raw != nil {
			if err := json.Unmarshal(raw, &last); err != nil {
				return fmt.Errorf("state of %s: %w", desc.Name, err)
			}
		}
		log.Info("reading", "stream", desc.Name, "last_sync", last.Timestamp)

		rows := int64(cfg.Limit)
		if err := em.Trace(synclet.TraceMessage{
			Type:     synclet.TraceTypeEstimate,
			Estimate: &synclet.EstimateTrace{Name: desc.Name, Namespace: desc.Namespace, Type: synclet.EstimateTypeStream, RowEstimate: &rows},
		}); err != nil {
			return err
		}

		var err error
		switch desc.Name {
		case "users":
			err = readStream(ctx, s, em, desc, fmt.Sprintf("/users?limit=%d", cfg.Limit), func(r struct{ Users []User }) []User { return r.Users })
		case "products":
			err = readStream(ctx, s, em, desc, fmt.Sprintf("/products?limit=%d", cfg.Limit), func(r struct{ Products []Product }) []Product { return r.Products })
		default:
			err = synclet.ConfigError(fmt.Errorf("unknown stream %q", desc.Name))
		}
		if err != nil {
			return err
		}
		if err := em.StreamState(desc, LastSync{Timestamp: time.Now().UnixMilli()}); err != nil {
			return err
		}
	}
	return nil
}

func readStream[R, T any](ctx context.Context, s *APISource, em *synclet.Emitter, desc synclet.StreamDescriptor, path string, rows func(R) []T) error {
	var resp R
	if err := s.get(ctx, path, &resp); err != nil {
		return fmt.Errorf("fetch %s: %w", desc.Name, err)
	}
	for _, row := range rows(resp) {
		if err := em.Record(desc, row); err != nil {
			return err
		}
	}
	return nil
}

func (s *APISource) get(ctx context.Context, path string, v any) (rerr error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.baseURL+path, nil)
	if err != nil {
		return err
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		if cerr := resp.Body.Close(); cerr != nil && rerr == nil {
			rerr = cerr
		}
	}()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return json.NewDecoder(resp.Body).Decode(v)
}
