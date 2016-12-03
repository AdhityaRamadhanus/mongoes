package main

import (
	"context"
	"github.com/AdhityaRamadhanus/mongoes"
	elastic "gopkg.in/olivere/elastic.v5"
	"strings"
)

// CreateMapping create Elastic Search Mapping from mongodb collection
func createMapping(doc map[string]interface{}) (map[string]interface{}, error) {
	mapping := map[string]interface{}{
		"properties": map[string]interface{}{},
	}
	for k, v := range doc {
		fieldMapping := mapping["properties"].(map[string]interface{})
		tempMapping := v.(map[string]interface{})
		name, ok := tempMapping["es_name"].(string)
		if !ok {
			name = k
		}
		fieldMapping[name] = map[string]interface{}{}
		innerAssertMap := fieldMapping[name].(map[string]interface{})
		for key, innerV := range tempMapping {
			if key != "es_name" {
				esKey := strings.Replace(key, "es_", "", -1)
				innerAssertMap[esKey] = innerV
			}
		}
	}
	return mapping, nil
}

// SetupIndexAndMapping will Delete Index and Create new Mapping
func setupIndexAndMapping(es_options mongoes.ESOptions, rawMapping map[string]interface{}, tracer mongoes.Tracer) error {
	tracer.Trace("Connecting to elasticsearch cluster ", es_options.ES_URI)
	client, err := elastic.NewClient(elastic.SetURL(es_options.ES_URI))
	if err != nil {
		return err
	}
	tracer.Trace("Delete current index ", es_options.ES_index)
	client.DeleteIndex(es_options.ES_index).Do(context.Background())

	_, err = client.CreateIndex(es_options.ES_index).Do(context.Background())
	if err != nil {
		return err
	}
	esMapping, _ := createMapping(rawMapping)
	_, err = client.PutMapping().Index(es_options.ES_index).Type(es_options.ES_type).BodyJson(esMapping).Do(context.Background())
	if err != nil {
		return err
	}
	tracer.Trace("Create new mapping ", es_options.ES_index, es_options.ES_type)
	return nil
}
