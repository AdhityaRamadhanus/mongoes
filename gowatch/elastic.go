package main

import (
	"context"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
)

func getMapping(es_options mongoes.ESOptions) ([]string, error) {
	// Get elastic search mapping
	client, err := elastic.NewClient(elastic.SetURL(es_options.EsURI))
	if err != nil {
		return nil, err
	}

	rawMapping, err := client.GetMapping().Index(es_options.EsIndex).Type(es_options.EsType).Pretty(true).Do(context.Background())
	if err != nil {
		return nil, err
	}
	jsonPath := es_options.EsIndex + ".mappings." + es_options.EsType + ".properties"
	esMapping := mongoes.GetDeepObject(rawMapping, jsonPath)
	selectedField := make([]string, 1)
	for key, _ := range esMapping {
		selectedField = append(selectedField, key)
	}
	return selectedField, nil
}

func processOplog(es_options mongoes.ESOptions, selectedField []string) chan<- Oplog {
	oplogs := make(chan Oplog, 1000)
	go func(es_options mongoes.ESOptions, selectedField []string) {
		client, err := elastic.NewClient(elastic.SetURL(es_options.EsURI))
		if err != nil {
			return
		}
		indexService := elastic.NewIndexService(client).Index(es_options.EsIndex).Type(es_options.EsType)
		deleteService := elastic.NewDeleteService(client).Index(es_options.EsIndex).Type(es_options.EsType)

		for p := range oplogs {
			if p.Op == "i" || p.Op == "u" {
				indexRequest := map[string]interface{}{}
				for _, v := range selectedField {
					if p.O[v] != nil {
						indexRequest[v] = p.O[v]
					}
				}
				stringId := p.O["_id"].(bson.ObjectId).Hex()
				if _, err := indexService.Id(stringId).BodyJson(indexRequest).Do(context.Background()); err == nil {
					fmt.Println("Successfully indexed")
				}
			} else if p.Op == "d" {
				deleteRequestId := p.O["_id"].(bson.ObjectId).Hex()
				fmt.Println(deleteRequestId)
				if _, err := deleteService.Id(deleteRequestId).Do(context.Background()); err == nil {
					fmt.Println("Successfully deleted")
				}
			}
		}
	}(es_options, selectedField)
	return oplogs
}
