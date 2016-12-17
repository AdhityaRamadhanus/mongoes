package main

import (
	"context"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
)

/* 	Get Elasticsearch Mapping
	Example of Mapping :
	"properties": {
      "title": {
        "type": "text"
      },
      "completed": {
        "type": "boolean"
      }
    }

    Now, with this mapping we will take only title and completed field from mongodb
    documents
*/
func getMapping(esOptions mongoes.ESOptions) ([]string, error) {
	// Create Elasticsearch client
	client, err := elastic.NewClient(elastic.SetURL(esOptions.EsURI))
	if err != nil {
		return nil, err
	}

	// Create Elasticsearch client
	rawMapping, err := client.GetMapping().Index(esOptions.EsIndex).Type(esOptions.EsType).Pretty(true).Do(context.Background())
	if err != nil {
		return nil, err
	}
	jsonPath := esOptions.EsIndex + ".mappings." + esOptions.EsType + ".properties"
	esMapping := mongoes.GetObjectJSON(rawMapping, jsonPath)
	selectedField := make([]string, 1)
	for key := range esMapping {
		selectedField = append(selectedField, key)
	}
	return selectedField, nil
}

/* processOplog will return channel of Oplog struct that main groutines
will send oplog taken from mongodb to that channel
*/
func processOplog(esOptions mongoes.ESOptions, selectedField []string) chan<- Oplog {
	oplogs := make(chan Oplog, 1000)
	go func(esOptions mongoes.ESOptions, selectedField []string) {
		client, err := elastic.NewClient(elastic.SetURL(esOptions.EsURI))
		if err != nil {
			return
		}
		indexService := elastic.NewIndexService(client).Index(esOptions.EsIndex).Type(esOptions.EsType)
		deleteService := elastic.NewDeleteService(client).Index(esOptions.EsIndex).Type(esOptions.EsType)

		for p := range oplogs {
			if p.Op == "i" || p.Op == "u" {
				indexRequest := map[string]interface{}{}
				for _, v := range selectedField {
					if p.O[v] != nil {
						indexRequest[v] = p.O[v]
					}
				}
				stringID := p.O["_id"].(bson.ObjectId).Hex()
				if _, err := indexService.Id(stringID).BodyJson(indexRequest).Do(context.Background()); err == nil {
					fmt.Println("Successfully indexed")
				}
			} else if p.Op == "d" {
				deleteRequestID := p.O["_id"].(bson.ObjectId).Hex()
				fmt.Println(deleteRequestID)
				if _, err := deleteService.Id(deleteRequestID).Do(context.Background()); err == nil {
					fmt.Println("Successfully deleted")
				}
			}
		}
	}(esOptions, selectedField)
	return oplogs
}
