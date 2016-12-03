package main

import (
	"context"
	"github.com/AdhityaRamadhanus/mongoes"
	elastic "gopkg.in/olivere/elastic.v5"
	"sync"
)

func DispatchWorkers(numWorkers int, es_options mongoes.ESOptions) chan<- elastic.BulkableRequest {
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	jobQueue := make(chan elastic.BulkableRequest)
	for i := 0; i < numWorkers; i++ {
		go func(id int, es_options mongoes.ESOptions, requests <-chan elastic.BulkableRequest) {
			defer wg.Done()
			client, err := elastic.NewClient(elastic.SetURL(es_options.ES_URI))
			if err != nil {
				return
			}
			bulkService := elastic.NewBulkService(client).Index(es_options.ES_index).Type(es_options.ES_type)
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
		}(i, es_options, jobQueue)
	}
	go func() {
		wg.Wait()
		close(ProgressQueue)
	}()
	return jobQueue
}
