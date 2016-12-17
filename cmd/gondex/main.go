package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	counts int32
	// ProgressQueue is channel of int that track how many documents indexed
	ProgressQueue = make(chan int)
	// elasticseach Options, uri, index name and type name
	esOptions mongoes.ESOptions
	// mongodb Options, uri, db name and collection name
	mgoOptions mongoes.MgoOptions
	// Mongodb Query to filter document that will be indexed to elasticsearch
	// It's json formatted query
	mgoQuery map[string]interface{}
	// Elaticsearch Mapping in JSON format
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for more information
	esMapping  map[string]interface{}
	pathConfig = flag.String("config", "", "config path")
	numWorkers = flag.Int("--workers", 2, "Number of concurrent workers")
	// Done channel signal, main goroutines should exit
	Done = make(chan struct{})
)

// Just error helper to make convenient to print error
func fatal(e error) {
	fmt.Println(e)
	fmt.Println("For More information see https://github.com/AdhityaRamadhanus/mongoes/blob/master/README.md")
	flag.PrintDefaults()
}

// should be called as new goroutine to display progress of indexing activity
func peekProgress() {
	for amounts := range ProgressQueue {
		atomic.AddInt32(&counts, int32(amounts))
		fmt.Println(atomic.LoadInt32(&counts), " Indexed")
	}
	Done <- struct{}{}
}

func readConfig() {
	// Parse the flag
	flag.Parse()
	if len(*pathConfig) == 0 {
		fatal(errors.New("Please provide config path"))
		os.Exit(1)
	}
	// Read the json config
	var config map[string]interface{}
	err := mongoes.ReadJSONFromFile(*pathConfig, &config)
	if err != nil {
		fatal(err)
		os.Exit(1)
	}

	mgoOptions.MgoDbname = mongoes.GetStringJSON(config, "mongodb.database")
	mgoOptions.MgoCollname = mongoes.GetStringJSON(config, "mongodb.collection")
	mgoOptions.MgoURI = mongoes.GetStringJSON(config, "mongodb.uri")
	mgoQuery = mongoes.GetObjectJSON(config, "query")

	esOptions.EsIndex = mongoes.GetStringJSON(config, "elasticsearch.index")
	esOptions.EsType = mongoes.GetStringJSON(config, "elasticsearch.type")
	esOptions.EsURI = mongoes.GetStringJSON(config, "elasticsearch.uri")
	esMapping = mongoes.GetObjectJSON(config, "mapping")
}

func main() {
	readConfig()
	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	if err := setupIndexAndMapping(esOptions, esMapping, tracer); err != nil {
		fatal(err)
		return
	}

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at ", mgoOptions.MgoURI)
	session, err := mongo.Dial(mgoOptions.MgoURI)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

	p := make(map[string]interface{})
	// Get the mongodb documents using cursor
	iter := session.DB(mgoOptions.MgoDbname).C(mgoOptions.MgoCollname).Find(mgoQuery).Iter()
	tracer.Trace("Start Indexing MongoDb")
	// Dispatch workers, returned a channel (work queue)
	requests := dispatchWorkers(*numWorkers, esOptions)
	// run a goroutines to watch the progres
	go peekProgress()
	// Handle ctrl+c
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-termChan
		Done <- struct{}{}
	}()
	// Start the timer
	start := time.Now()
	for iter.Next(&p) {
		// take the value from mongodb documents
		// not all the field in documents will be indexed depends on your mapping
		esBody := createEsIndexBody(&p, &esMapping)
		// Create Elasticsearch Bulk Index Request
		bulkRequest := elastic.NewBulkIndexRequest().
			Index(esOptions.EsIndex).
			Type(esOptions.EsType).
			Id(p["_id"].(bson.ObjectId).Hex()).
			Doc(esBody)
		select {
		case <-Done: // Early termination can be caused by no workers spawned (triggered by closing of ProgressQueue) and user hit ctrl+c
			fmt.Println("Early Termination")
			close(requests)
			iter.Close()
			return
		default:
			requests <- bulkRequest
		}
	}
	close(requests)
	iter.Close()
	<-Done
	elapsed := time.Since(start)
	tracer.Trace(atomic.LoadInt32(&counts), " Documents Indexed in ", elapsed)
}
