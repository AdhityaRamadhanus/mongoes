package elastic

import (
	"context"
	"log"
	"sync/atomic"

	"sync"

	"github.com/AdhityaRamadhanus/mongoes"
	"gopkg.in/olivere/elastic.v5"
)

var workerWg sync.WaitGroup

type ElasticWorker struct {
	JobQueue     chan elastic.BulkableRequest
	NumWorkers   int
	done         chan struct{}
	EsOptions    mongoes.ESOptions
	IndexResults int32
}

func NewElasticWorker(jobQueue chan elastic.BulkableRequest, numWorkers int) *ElasticWorker {
	return &ElasticWorker{
		JobQueue:     jobQueue,
		NumWorkers:   numWorkers,
		done:         make(chan struct{}),
		IndexResults: 0,
	}
}

func (w *ElasticWorker) enquiryWork(jobQueue <-chan elastic.BulkableRequest) {
	defer workerWg.Done()
	client, err := elastic.NewSimpleClient(elastic.SetURL(w.EsOptions.EsURI))
	if err != nil {
		return
	}
	bulkService := elastic.NewBulkService(client).Index(w.EsOptions.EsIndex).Type(w.EsOptions.EsType)
	for request := range jobQueue {
		bulkService.Add(request)
		// Wait for 1000 request before actualy firing request
		if bulkService.NumberOfActions() == 1000 {
			bulkResponse, _ := bulkService.Do(context.Background())
			atomic.AddInt32(&w.IndexResults, int32(len(bulkResponse.Indexed())))
			log.Println("Indexed", atomic.LoadInt32(&w.IndexResults))
		}
		// Bulk Index the left over
	}
	if bulkService.NumberOfActions() > 0 {
		bulkResponse, _ := bulkService.Do(context.Background())
		atomic.AddInt32(&w.IndexResults, int32(len(bulkResponse.Indexed())))
		log.Println("Indexed", atomic.LoadInt32(&w.IndexResults))
	}
}

func (w *ElasticWorker) DispatchWorkers() chan struct{} {
	workerWg.Add(w.NumWorkers)
	// spwan the workers
	for i := 0; i < w.NumWorkers; i++ {
		// worker function
		go w.enquiryWork(w.JobQueue)
	}
	go func() {
		workerWg.Wait()
		w.done <- struct{}{}
	}()
	return w.done
}
