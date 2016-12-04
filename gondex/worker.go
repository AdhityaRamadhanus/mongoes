package main

import (
	"context"
	"github.com/AdhityaRamadhanus/mongoes"
	elastic "gopkg.in/olivere/elastic.v5"
	"sync"
)

func dispatchWorkers(numWorkers int, esOptions mongoes.ESOptions) chan<- elastic.BulkableRequest {
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	jobQueue := make(chan elastic.BulkableRequest, 1000)
	for i := 0; i < numWorkers; i++ {
		go func(id int, esOptions mongoes.ESOptions, requests <-chan elastic.BulkableRequest) {
			defer wg.Done()
			client, err := elastic.NewClient(elastic.SetURL(esOptions.EsURI))
			if err != nil {
				return
			}
			bulkService := elastic.NewBulkService(client).Index(esOptions.EsIndex).Type(esOptions.EsType)
			for v := range requests {
				bulkService.Add(v)
				if bulkService.NumberOfActions() == 1000 {
					bulkResponse, _ := bulkService.Do(context.Background())
					ProgressQueue <- len(bulkResponse.Indexed())
				}
			}
			if bulkService.NumberOfActions() > 0 {
				bulkResponse, _ := bulkService.Do(context.Background())
				ProgressQueue <- len(bulkResponse.Indexed())
			}
		}(i, esOptions, jobQueue)
	}
	go func() {
		wg.Wait()
		close(ProgressQueue)

	}()
	return jobQueue
}
