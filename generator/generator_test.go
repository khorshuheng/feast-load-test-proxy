package generator

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
	"testing"
)

func TestGenerateRequests(t *testing.T) {
	yamlSpec, err := ioutil.ReadFile("../example/loadSpec.yml")
	if err != nil {
		t.Errorf(err.Error())
		t.FailNow()
	}
	loadSpec := LoadSpec{}
	err = yaml.Unmarshal(yamlSpec, &loadSpec)
	if err != nil {
		t.Errorf(err.Error())
		t.FailNow()
	}
	loadSpec.EntitySpec[0].FileSource.Path = "../example/restaurant_id.txt"
	requestGenerator, err := NewRequestGenerator(loadSpec, "default")
	if err != nil {
		t.Errorf(err.Error())
		t.FailNow()
	}
	requests := requestGenerator.GenerateRequests()
	if len(requests) != 3 {
		t.Errorf("Request length not equals to 3")
		t.FailNow()
	}

}