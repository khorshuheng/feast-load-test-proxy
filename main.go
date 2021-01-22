package main

import (
	"context"
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"feast-load-generator/generator"

	feast "github.com/feast-dev/feast/sdk/go"
	"github.com/feast-dev/feast/sdk/go/protos/feast/serving"
	"github.com/kelseyhightower/envconfig"
)

type Config struct {
	FeastServingHost 	string `default:"localhost" split_words:"true"`
	FeastServingPort 	int    `default:"6566" split_words:"true"`
	ListenPort       	string `default:"8080" split_words:"true"`
	ProjectName      	string `default:"default" split_words:"true"`
	SpecificationPath 	string `default:"loadSpec.yml" split_words:"true"`
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

	log.Printf("Loading specification at %s", c.SpecificationPath)
	yamlSpec, err := ioutil.ReadFile(c.SpecificationPath)
    if err != nil {
        log.Fatalf("Error reading specification file at %s", err)
    }
    loadSpec := generator.LoadSpec{}
    err = yaml.Unmarshal(yamlSpec, &loadSpec)
    if err != nil {
        log.Fatalf("Unmarshal: %v", err)
    }
	requestGenerator, err := generator.NewRequestGenerator(loadSpec, c.ProjectName)
	if err != nil {
		log.Fatalf("Unable to instantiate request requestGenerator: %v", err)
	}
	requestsPool := make([][]feast.OnlineFeaturesRequest, 100)
	for i := 0; i < 100; i++ {
		requestsPool[i] = requestGenerator.GenerateRequests()
	}

	http.HandleFunc("/send", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second)
		defer cancel()
		requests := requestsPool[rand.Intn(len(requestsPool))]
		if len(requests) == 1 {
			resp, err := client.GetOnlineFeatures(ctx, &requests[0])
			if err != nil {
				w.WriteHeader(500)
			} else {
				postProcessResponse(resp)
				w.WriteHeader(200)
			}
		} else {
			var wg sync.WaitGroup
			wg.Add(len(requests))

			fatalErrors := make(chan error)
			wgDone := make(chan bool)
			atLeastOneError := false

			for _, request := range requests {
				request := request
				go func() {
					defer wg.Done()
					resp, err := client.GetOnlineFeatures(ctx, &request)
					if err != nil {
						fatalErrors <- err
					}
					postProcessResponse(resp)
				}()
			}

			go func() {
				wg.Wait()
				close(wgDone)
			}()

			select {
			case <-wgDone:
				close(fatalErrors)
				break
			case <-fatalErrors:
				atLeastOneError = true
			}

			if atLeastOneError {
				w.WriteHeader(500)
			} else {
				w.WriteHeader(200)
			}
		}

	})

	http.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(context.Background(), 60 * time.Second)
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

func postProcessResponse(resp *feast.OnlineFeaturesResponse) {
	for _, fieldValue := range resp.RawResponse.FieldValues {
		for _, field := range fieldValue.Fields {
			field.GetVal()
		}
	}
}
