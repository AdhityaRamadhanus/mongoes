package main

import (
	"errors"
	"flag"
	"fmt"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
	// "log"
	"github.com/AdhityaRamadhanus/mongoes"
	"os"
	// "runtime"
	"context"
	"sync"
	"sync/atomic"
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

var wg sync.WaitGroup
var counts int32 = 0
var ProgressQueue = make(chan int)

func peekProgress() {
	for amounts := range ProgressQueue {
		atomic.AddInt32(&counts, int32(amounts))
		fmt.Println(atomic.LoadInt32(&counts), " Indexed")
	}
}

func doService(id int, esUri, indexName, typeName string, requests <-chan elastic.BulkableRequest) {
	defer wg.Done()
	client, err := elastic.NewClient(elastic.SetURL(esUri))
	if err != nil {
		return
	}
	// counts := 0
	bulkService := elastic.NewBulkService(client).Index(indexName).Type(typeName)
	for v := range requests {
		bulkService.Add(v)
		if bulkService.NumberOfActions() == 1000 {
			bulkResponse, _ := bulkService.Do(context.Background())
			// counts.atomicAdd(&counts, int32(len(bulkResponse.Indexed())))
			ProgressQueue <- len(bulkResponse.Indexed())
		}
	}
	// requests closed
	if bulkService.NumberOfActions() > 0 {
		bulkResponse, _ := bulkService.Do(context.Background())
		ProgressQueue <- len(bulkResponse.Indexed())

		// counts += len(bulkResponse.Indexed())
	}
	// fmt.Println("Worker ", id, "finished indexing ", counts, "documents")
}

func main() {
	var dbName = flag.String("db", "", "Mongodb DB Name")
	var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	var indexName = flag.String("index", "", "ES Index Name")
	var typeName = flag.String("type", "", "ES Type Name")
	var mappingFile = flag.String("mapping", "", "Mapping mongodb field to es")
	var queryFile = flag.String("filter", "", "Query to filter mongodb docs")
	var esUri = flag.String("--esUri", "http://localhost:9200", "Elasticsearch URI")
	var numWorkers = flag.Int("--workers", 2, "Number of concurrent workers")

	wg.Add(*numWorkers)
	flag.Parse()

	if len(*dbName) == 0 || len(*collName) == 0 {
		fatal(errors.New("Please provide db and collection name"))
		return
	}

	if len(*indexName) == 0 {
		indexName = dbName
	}

	if len(*typeName) == 0 {
		typeName = collName
	}

	var query map[string]interface{}
	query, _ := mongoes.ReadJSON(*queryFile)

	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at ", *dbUri)
	session, err := mongo.Dial(*dbUri)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()
	rawMapping, err := mongoes.ReadJSON(*mappingFile)
	if err != nil {
		fatal(err)
		return
	}
	if err := mongoes.SetupIndexAndMapping(*esUri, *indexName, *typeName, rawMapping, tracer); err != nil {
		fatal(err)
		return
	}

	p := make(map[string]interface{})
	iter := session.DB(*dbName).C(*collName).Find(query).Iter()
	tracer.Trace("Start Indexing MongoDb")
	requests := make(chan elastic.BulkableRequest)
	// spawn workers
	for i := 0; i < *numWorkers; i++ {
		go doService(i, *esUri, *indexName, *typeName, requests)
	}
	go peekProgress()
	start := time.Now()

	for iter.Next(&p) {
		var esBody = make(map[string]interface{})
		for k, v := range rawMapping {
			mgoVal, ok := p[k]
			if ok {
				var key = (v.(map[string]interface{}))["es_name"]
				if key == nil {
					key = k
				}
				esBody[key.(string)] = mgoVal
			}
		}
		bulkRequest := elastic.NewBulkIndexRequest().
			Index(*indexName).
			Type(*typeName).
			Id(p["_id"].(bson.ObjectId).Hex()).
			Doc(esBody)
		requests <- bulkRequest
	}
	close(requests)
	iter.Close()
	wg.Wait()
	elapsed := time.Since(start)
	close(ProgressQueue)
	tracer.Trace("Documents Indexed in ", elapsed)
}
