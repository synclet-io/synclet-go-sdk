package synclet_test

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	synclet "github.com/synclet-io/synclet-go-sdk"
)

type HTTPSource struct {
	baseURL string
}

type LastSyncTime struct {
	Timestamp int64 `json:"timestamp"`
}

type HTTPConfig struct {
	APIKey string `json:"apiKey"`
}

func NewHTTPSource(baseURL string) synclet.Source {
	return HTTPSource{
		baseURL: baseURL,
	}
}

func (h HTTPSource) Spec(logTracker synclet.LogTracker) (*synclet.ConnectorSpecification, error) {
	logTracker.Log(synclet.LogLevelInfo, "Running Spec")
	return &synclet.ConnectorSpecification{
		DocumentationURL:      "https://bitstrapped.com",
		ChangeLogURL:          "https://bitstrapped.com",
		SupportsIncremental:   false,
		SupportsNormalization: true,
		SupportsDBT:           true,
		SupportedDestinationSyncModes: []synclet.DestinationSyncMode{
			synclet.DestinationSyncModeOverwrite,
		},
		ConnectionSpecification: synclet.ConnectionSpecification{
			Title:       "Example HTTP Source",
			Description: "This is an example http source for the docs's",
			Type:        "object",
			Required:    []synclet.PropertyName{"apiKey"},
			Properties: synclet.Properties{
				Properties: map[synclet.PropertyName]synclet.PropertySpec{
					"apiKey": {
						Description: "api key to access http source, valid uuid",
						Examples:    []string{"xxxx-xxxx-xxxx-xxxx"},
						PropertyType: synclet.PropertyType{
							Type: []synclet.PropType{
								synclet.String,
							},
						},
					},
				},
			},
		},
	}, nil
}

func (h HTTPSource) Check(srcCfgPath string, logTracker synclet.LogTracker) error {
	logTracker.Log(synclet.LogLevelDebug, "validating api connection")
	var srcCfg HTTPConfig
	err := synclet.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return err
	}

	resp, err := http.Get(fmt.Sprintf("%s/ping?key=%s", h.baseURL, srcCfg.APIKey))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return errors.New("Invalid status")
	}

	return nil
}

func (h HTTPSource) Discover(srcCfgPath string, logTracker synclet.LogTracker) (*synclet.Catalog, error) {
	var srcCfg HTTPConfig
	err := synclet.UnmarshalFromPath(srcCfgPath, &srcCfg)
	if err != nil {
		return nil, err
	}

	return &synclet.Catalog{Streams: []synclet.Stream{{
		Name: "users",
		JSONSchema: synclet.Properties{
			Properties: map[synclet.PropertyName]synclet.PropertySpec{
				"userid": {
					PropertyType: synclet.PropertyType{
						Type:        []synclet.PropType{synclet.Integer, synclet.Null},
						AirbyteType: synclet.BigInteger},
					Description: "user ID - see the big int",
				},
				"name": {
					PropertyType: synclet.PropertyType{
						Type: []synclet.PropType{synclet.String, synclet.Null},
					},
					Description: "user name",
				},
			},
		},
		SupportedSyncModes: []synclet.SyncMode{
			synclet.SyncModeFullRefresh,
		},
		SourceDefinedCursor: false,
		Namespace:           "bitstrapped",
	},
		{
			Name:       "payments",
			JSONSchema: synclet.InferSchemaFromStruct(Payment{}, logTracker),
			SupportedSyncModes: []synclet.SyncMode{
				synclet.SyncModeFullRefresh,
			},
			SourceDefinedCursor: false,
			Namespace:           "bitstrapped",
		},
	}}, nil
}

type User struct {
	UserID int64  `json:"userid"`
	Name   string `json:"name"`
}

type Payment struct {
	UserID        int64 `json:"userid"`
	PaymentAmount int64 `json:"paymentAmount"`
}

func (h HTTPSource) Read(sourceCfgPath string, prevStatePath string, configuredCat *synclet.ConfiguredCatalog,
	tracker synclet.MessageTracker) error {
	tracker.Log(synclet.LogLevelInfo, "Running read")
	var src HTTPConfig
	err := synclet.UnmarshalFromPath(sourceCfgPath, &src)
	if err != nil {
		return err
	}

	// see if there is a last sync
	var st LastSyncTime
	synclet.UnmarshalFromPath(sourceCfgPath, &st)
	if st.Timestamp <= 0 {
		st.Timestamp = -1
	}

	for _, stream := range configuredCat.Streams {
		if stream.Stream.Name == "users" {
			var u []User
			uri := fmt.Sprintf("https://api.bistrapped.com/users?apiKey=%s", src.APIKey)
			resp, err := http.Get(uri)
			if err != nil {
				return err
			}
			err = json.NewDecoder(resp.Body).Decode(&u)
			if err != nil {
				return err
			}

			for _, ur := range u {
				err := tracker.Record(ur, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}

		if stream.Stream.Name == "payments" {
			var p []Payment
			uri := fmt.Sprintf("%s/payments?apiKey=%s", h.baseURL, src.APIKey)
			resp, err := http.Get(uri)
			if err != nil {
				return err
			}
			err = json.NewDecoder(resp.Body).Decode(&p)
			if err != nil {
				return err
			}

			for _, py := range p {
				err := tracker.Record(py, stream.Stream.Name, stream.Stream.Namespace)
				if err != nil {
					return err
				}
			}
		}
	}

	return tracker.State(synclet.StateTypeLegacy, &LastSyncTime{
		Timestamp: time.Now().UnixMilli(),
	})
}

func Example() {
	hsrc := NewHTTPSource("https://api.bitstrapped.com")
	runner := synclet.NewSourceRunner(hsrc, os.Stdout)
	err := runner.Start()
	if err != nil {
		log.Fatal(err)
	}
}
