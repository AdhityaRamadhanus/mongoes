package main

import (
	"context"
	"github.com/AdhityaRamadhanus/mongoes"
	elastic "gopkg.in/olivere/elastic.v5"
	"sync"
)

/* 	dispatch workers to process bulk index request
this function will return buffered channel of elastic.BulkableRequest
Basically this will be run as a new goroutines and it will spawn workers as go routines and wait
for them to finish
*/
func dispatchWorkers(numWorkers int, esOptions mongoes.ESOptions) chan<- elastic.BulkableRequest {
	var wg sync.WaitGroup
	wg.Add(numWorkers)
	jobQueue := make(chan elastic.BulkableRequest, 1000)
	// spwan the workers
	for i := 0; i < numWorkers; i++ {
		// worker function
		go func(id int, esOptions mongoes.ESOptions, requests <-chan elastic.BulkableRequest) {
			defer wg.Done()
			// create new client for each client
			client, err := elastic.NewSimpleClient(elastic.SetURL(esOptions.EsURI))
			if err != nil {
				return
			}
			bulkService := elastic.NewBulkService(client).Index(esOptions.EsIndex).Type(esOptions.EsType)
			for v := range requests {
				bulkService.Add(v)
				// Wait for 1000 request before actualy firing request
				if bulkService.NumberOfActions() == 1000 {
					bulkResponse, _ := bulkService.Do(context.Background())
					ProgressQueue <- len(bulkResponse.Indexed())
				}
			}
			// Bulk Index the left over
			if bulkService.NumberOfActions() > 0 {
				bulkResponse, _ := bulkService.Do(context.Background())
				ProgressQueue <- len(bulkResponse.Indexed())
			}
		}(i, esOptions, jobQueue)
	}
	go func() {
		wg.Wait()
		// Close the progress channel
		// this will make peekProgress goroutine return and send Signal to Done
		close(ProgressQueue)
	}()
	return jobQueue
}
