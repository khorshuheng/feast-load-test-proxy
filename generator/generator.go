package generator

import (
	"bufio"
	"errors"
	"fmt"
	feast "github.com/feast-dev/feast/sdk/go"
	"github.com/feast-dev/feast/sdk/go/protos/feast/types"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type LoadSpec struct {
	EntitySpec   []EntitySpec  `yaml:"entities"`
	RequestSpecs []RequestSpec `yaml:"requests"`
}

type EntitySpec struct {
	Name       string     `yaml:"name"`
	Type       string     `yaml:"type"`
	FileSource FileSource `yaml:"fileSource"`
	RandInt    RandInt    `yaml:"randInt"`
}

type FileSource struct {
	Path string `yaml:"path"`
}


type RandInt struct {
	Min int64 `yaml:"min"`
	Max int64 `yaml:"max"`
}

// Generate all possible values for an entity
type EntityPoolGenerator interface {
	GenerateEntityValues() ([]*types.Value, error)
}

// Generate all possible values for an entity from a file source
type FileSourceEntityValueGenerator struct {
	entity EntitySpec
}

func (generator FileSourceEntityValueGenerator) GenerateEntityValues() ([]*types.Value, error) {
	var entityValues []*types.Value
	file, err := os.Open(generator.entity.FileSource.Path)
	if err != nil {
		return nil, err
	}
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanLines)
	for scanner.Scan() {
		parsedValue, err := generator.parseStrToEntityValue(generator.entity.Type, scanner.Text())
		if err != nil {
			return nil, err
		}
		entityValues = append(entityValues, parsedValue)
	}
	return entityValues, nil
}

func (generator FileSourceEntityValueGenerator) parseStrToEntityValue(valueType string, valueStr string) (*types.Value, error) {
	switch valueType {
	case "string":
		return feast.StrVal(valueStr), nil
	case "int32":
		parsedValue, err := strconv.ParseInt(valueStr, 10, 32)
		if err != nil {
			return nil, err
		}
		return feast.Int32Val(int32(parsedValue)), nil
	case "int64":
		parsedValue, err := strconv.ParseInt(valueStr, 10, 64)
		if err != nil {
			return nil, err
		}
		return feast.Int64Val(parsedValue), nil
	case "float":
		parsedValue, err := strconv.ParseFloat(valueStr, 32)
		if err != nil {
			return nil, err
		}
		return feast.FloatVal(float32(parsedValue)), nil
	case "double":
		parsedValue, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			return nil, err
		}
		return feast.DoubleVal(parsedValue), nil
	case "bool":
		parsedValue, err := strconv.ParseBool(valueStr)
		if err != nil {
			return nil, err
		}
		return feast.BoolVal(parsedValue), nil
	}

	return nil, errors.New(fmt.Sprintf("Unrecognized value type: %s", valueType))
}

// Generate all possible values for an entity based on a range of integer
type RandIntEntityValueGenerator struct {
	entity EntitySpec
}

func (generator RandIntEntityValueGenerator) GenerateEntityValues() ([]*types.Value, error) {
	entityType := generator.entity.Type
	switch entityType {
	case "int64":
		minValue := generator.entity.RandInt.Min
		maxValue := generator.entity.RandInt.Max
		poolSize := maxValue - minValue + 1
		entityValues := make([]*types.Value, poolSize)
		for i := int64(0); i < poolSize; i++ {
			entityValues[i] = feast.Int64Val(i + minValue)
		}
		return entityValues, nil
	case "int32":
		minValue := int32(generator.entity.RandInt.Min)
		maxValue := int32(generator.entity.RandInt.Max)
		poolSize := maxValue - minValue + 1
		entityValues := make([]*types.Value, poolSize)
		for i := int32(0); i < poolSize; i++ {
			entityValues[i] = feast.Int32Val(i + minValue)
		}
		return entityValues, nil
	default:
		return nil, errors.New(fmt.Sprintf("Unsupported entity type: %s", entityType))
	}
}

type RequestSpec struct {
	Entities    []string `yaml:"entities"`
	Features    []string `yaml:"features"`
	EntityCount int32    `yaml:"entityCount"`
}

type RequestGenerator struct {
	entityToValuePoolMap 	map[string][]*types.Value
	requests 				[]RequestSpec
	project					string
}

func NewRequestGenerator(loadSpec LoadSpec, project string) (RequestGenerator, error) {
	entityToValuePoolMap := map[string][]*types.Value{}
	for _, entity := range loadSpec.EntitySpec {
		var poolGenerator EntityPoolGenerator
		if entity.FileSource != (FileSource{}) {
			poolGenerator = FileSourceEntityValueGenerator{entity}
		} else {
			poolGenerator = RandIntEntityValueGenerator{entity}
		}
		pool, err := poolGenerator.GenerateEntityValues()
		if err != nil {
			return RequestGenerator{}, err
		}
		entityToValuePoolMap[entity.Name] = pool
	}
	return RequestGenerator{
		entityToValuePoolMap: entityToValuePoolMap,
		requests: loadSpec.RequestSpecs,
		project: project,
	}, nil
}


func (generator *RequestGenerator) GenerateRandomRows(entities []string, entityCount int32) []feast.Row {
	rows := make([]feast.Row, entityCount)
	for _, entity := range entities {
		valuePool := generator.entityToValuePoolMap[entity]
		rand.Seed(time.Now().UnixNano())
		rand.Shuffle(len(valuePool), func(i, j int) { valuePool[i], valuePool[j] = valuePool[j], valuePool[i] })
	}

	for i := int32(0); i < entityCount; i++ {
		row := feast.Row{}
		for _, entity := range entities {
			valuePool := generator.entityToValuePoolMap[entity]
			row[entity] = valuePool[i]
		}
		rows[i]=row
	}

	return rows
}

func (generator *RequestGenerator) GenerateRequests() []feast.OnlineFeaturesRequest {
	var onlineFeatureRequests []feast.OnlineFeaturesRequest
	for _, request := range generator.requests {
		entityRows := generator.GenerateRandomRows(request.Entities, request.EntityCount)
		onlineFeatureRequests = append(onlineFeatureRequests, feast.OnlineFeaturesRequest{
			Features: request.Features,
			Entities: entityRows,
			Project:  generator.project,
		})
	}
	return onlineFeatureRequests
}
