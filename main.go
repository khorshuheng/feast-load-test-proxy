package main

import (
	"context"
	"log"
	"net/http"
	"strconv"
	"time"

	feast "github.com/feast-dev/feast/sdk/go"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	FeastServingHost string `default:"localhost" split_words:"true"`
	FeastServingPort int    `default:"6566" split_words:"true"`
	ListenPort       string `default:"8080" split_words:"true"`
}

func main() {

	var c Config
	err := envconfig.Process("LOAD", &c)
	if err != nil {
		log.Fatal(err.Error())
	}

	log.Printf("Creating client to connect to Feast Serving at %s:%d", c.FeastServingHost, c.FeastServingPort)
	client, err := feast.NewGrpcClient(c.FeastServingHost, c.FeastServingPort)
	if err != nil {
		log.Fatalf("Could not connect to: %v", err)
	}

	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		entityCountParam := r.URL.Query().Get("entity_count")
		if len(entityCountParam) < 1 {
			log.Fatal("Url parameter 'entity_count' is missing. Please specify the entity count in order to generate the appropriate load")
		}
		entityCount, err := strconv.Atoi(entityCountParam)
		request := buildRequest(entityCount)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		values, err := client.GetOnlineFeatures(ctx, &request)
		if err != nil {
			log.Fatalf("%v", err)
		}
		if values.RawResponse.FieldValues[0].Fields["float_feature"].GetFloatVal() != 0.1 {
			log.Fatal("Hardcoded float value of 0.1 was not found in response for feature \"float_feature\", please make sure the correct values have been ingested.")
		}
		w.WriteHeader(200)
	})

	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		var req serving.GetFeastServingInfoRequest
		_, err := client.GetFeastServingInfo(ctx, &req)
		if err != nil {
			log.Fatalf("%v", err)
		}
		w.WriteHeader(200)
	})

	log.Printf("Starting server on port %s\n", c.ListenPort)
	err = http.ListenAndServe(":"+c.ListenPort, nil)
	if err != nil {
		log.Fatalf("could not start server")
	}
}

func buildRequest(entityRowCount int) feast.OnlineFeaturesRequest {
	var entityRows []feast.Row

	for i := 0; i <= entityRowCount; i++ {
		row := make(map[string]*types.Value)
		val := feast.Int64Val(int64(1000 + i))
		row["user_id"] = val
		entityRows = append(entityRows, row)
	}

	request := feast.OnlineFeaturesRequest{
		Features: []string{
			"int32_feature",
			"int64_feature",
			"float_feature",
			"double_feature",
			"string_feature",
			"bytes_feature",
			"bool_feature",
			"int32_list_feature",
			"int64_list_feature",
			"float_list_feature",
			"double_list_feature",
			"string_list_feature",
			"bytes_list_feature",
		},
		Entities:     entityRows,
		OmitEntities: false,
	}
	return request
}
