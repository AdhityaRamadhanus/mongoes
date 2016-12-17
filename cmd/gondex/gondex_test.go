package main

import (
	"fmt"
	"reflect"
	"testing"
)

func TestElasticSearchMappingSimple(t *testing.T) {
	testInput := map[string]interface{}{
		"title": map[string]interface{}{
			"es_type": "text",
		},
		"slug": map[string]interface{}{
			"es_type": "text",
		},
	}
	testOutput, err := createMapping(testInput)
	if err != nil {
		t.Error("Got Error on creating Mapping", err)
	}
	correctOutput := map[string]interface{}{
		"properties": map[string]interface{}{
			"title": map[string]interface{}{
				"type": "text",
			},
			"slug": map[string]interface{}{
				"type": "text",
			},
		},
	}
	if !reflect.DeepEqual(testOutput, correctOutput) {
		fmt.Println("Actual Val", testOutput)
		fmt.Println("Expected Val", correctOutput)
		t.Error("Unexpected Value")
	}
}

func TestElasticSearchMappingComplex(t *testing.T) {
	testInput := map[string]interface{}{
		"closeDate": map[string]interface{}{
			"es_type":   "date",
			"es_format": "strict_date_optional_time||epoch_millis",
		},
		"numWorks": map[string]interface{}{
			"es_type":           "scaled_float",
			"es_scaling_factor": 1,
		},
	}
	testOutput, err := createMapping(testInput)
	if err != nil {
		t.Error("Got Error on creating Mapping", err)
	}
	correctOutput := map[string]interface{}{
		"properties": map[string]interface{}{
			"closeDate": map[string]interface{}{
				"type":   "date",
				"format": "strict_date_optional_time||epoch_millis",
			},
			"numWorks": map[string]interface{}{
				"type":           "scaled_float",
				"scaling_factor": 1,
			},
		},
	}
	if !reflect.DeepEqual(testOutput, correctOutput) {
		fmt.Println("Actual Val", testOutput)
		fmt.Println("Expected Val", correctOutput)
		t.Error("Unexpected Value")
	}
}
