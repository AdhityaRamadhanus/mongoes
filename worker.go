package mongoes

import (
	"log"

	"sync"

	"github.com/AdhityaRamadhanus/skyscraper/apiserver/mongo"
	"gopkg.in/olivere/elastic.v5"
)

var workerWg sync.WaitGroup

type WorkerPool struct {
	JobQueue       chan elastic.BulkableRequest
	NumWorkers     int
	done           chan struct{}
	ElasticService *mongo.ProductService
}

func NewWorkerPool(jobQueue chan elastic.BulkableRequest, numWorkers int) *WorkerPool {
	return &WorkerPool{
		JobQueue:   jobQueue,
		NumWorkers: numWorkers,
		done:       make(chan struct{}),
	}
}

func (w *WorkerPool) doWork(request elastic.BulkableRequest) (err error) {
	log.Println("get", request.String())
	return nil
}

func (w *WorkerPool) enquiryWork(jobQueue <-chan elastic.BulkableRequest) {
	defer workerWg.Done()
	for request := range jobQueue {
		err := w.doWork(request)
		if err != nil {
			log.Println(err)
		}
	}
}

func (w *WorkerPool) DispatchWorkers() chan struct{} {
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
