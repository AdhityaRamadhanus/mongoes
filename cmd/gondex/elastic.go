package main

import (
	"context"
	"github.com/AdhityaRamadhanus/mongoes"
	elastic "gopkg.in/olivere/elastic.v5"
	"strings"
)

/** CreateMapping create Elastic Search Mapping from mongodb collection
	Example of mapping input:
	"mapping": {
        "title": {
            "es_type": "text"
        },
        "slug": {
            "es_type": "keyword"
        }
    }

    Example of mapping output
    "properties": {
		"title": {
			"type": "text"
		}
		"slug": {
			"type": "keyword"
		}
    }
*/
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
// Beware, this will delete your current index and create new mapping
func setupIndexAndMapping(esOptions mongoes.ESOptions, rawMapping map[string]interface{}, tracer mongoes.Tracer) error {
	tracer.Trace("Connecting to elasticsearch cluster ", esOptions.EsURI)
	client, err := elastic.NewSimpleClient(elastic.SetURL(esOptions.EsURI))
	if err != nil {
		return err
	}
	tracer.Trace("Delete current index ", esOptions.EsIndex)
	client.DeleteIndex(esOptions.EsIndex).Do(context.Background())

	_, err = client.CreateIndex(esOptions.EsIndex).Do(context.Background())
	if err != nil {
		return err
	}
	esMapping, _ := createMapping(rawMapping)
	_, err = client.PutMapping().Index(esOptions.EsIndex).Type(esOptions.EsType).BodyJson(esMapping).Do(context.Background())
	if err != nil {
		return err
	}
	tracer.Trace("Create new mapping ", esOptions.EsIndex, esOptions.EsType)
	return nil
}

func createEsIndexBody(mongoDoc *map[string]interface{}, esMapping *map[string]interface{}) map[string]interface{} {
	var esBody = make(map[string]interface{})
	for k, v := range *esMapping {
		mgoVal, ok := (*mongoDoc)[k]
		if ok {
			var key = (v.(map[string]interface{}))["es_name"]
			if key == nil {
				key = k
			}
			esBody[key.(string)] = mgoVal
		}
	}
	return esBody
}
