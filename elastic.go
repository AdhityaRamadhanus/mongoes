package mongoes

import (
	"context"
	elastic "gopkg.in/olivere/elastic.v5"
	"strings"
)

// CreateMapping create Elastic Search Mapping from mongodb collection
func CreateMapping(doc JSON) (JSON, error) {
	mapping := JSON{
		"properties": JSON{},
	}
	for k, v := range doc {
		fieldMapping := mapping["properties"].(JSON)
		tempMapping := v.(map[string]interface{})
		name, ok := tempMapping["es_name"].(string)
		if !ok {
			name = k
		}
		fieldMapping[name] = JSON{}
		innerAssertMap := fieldMapping[name].(JSON)
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
func SetupIndexAndMapping(esURI, indexName, typeName string, rawMapping map[string]interface{}, tracer Tracer) error {
	client, err := elastic.NewClient(elastic.SetURL(esURI))
	if err != nil {
		return err
	}
	tracer.Trace("Delete Current Index")
	client.DeleteIndex(indexName).Do(context.Background())

	_, err = client.CreateIndex(indexName).Do(context.Background())
	if err != nil {
		return err
	}
	tracer.Trace("Create New Index")
	esMapping, _ := CreateMapping(rawMapping)
	_, err = client.PutMapping().Index(indexName).Type(typeName).BodyJson(esMapping).Do(context.Background())
	if err != nil {
		return err
	}
	tracer.Trace("Create New Mapping")
	return nil
}
