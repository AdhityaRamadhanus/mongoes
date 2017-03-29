package commands

import (
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AdhityaRamadhanus/mongoes"
	mongoestic "github.com/AdhityaRamadhanus/mongoes/elastic"
	cli "github.com/urfave/cli"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
)

var (
	// elasticseach Options, uri, index name and type name
	esOptions mongoes.ESOptions
	// mongodb Options, uri, db name and collection name
	mgoOptions mongoes.MgoOptions
	// Mongodb Query to filter document that will be indexed to elasticsearch
	// It's json formatted query
	// Elaticsearch Mapping in JSON format
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for more information
	// Done channel signal, main goroutines should exit
	Done = make(chan struct{})
)

func fatalErr(err error) {
	log.Println(err)
	os.Exit(1)
}

// Main Commands Function
func IndexMongoToES(cliContext *cli.Context) {
	if err := mongoes.GetConfig(cliContext.String("config"), &esOptions, &mgoOptions); err != nil {
		fatalErr(err)
	}
	log.Println("Setup ES Index and Mapping")
	if err := mongoestic.SetupIndexAndMapping(esOptions); err != nil {
		fatalErr(err)
	}
	log.Println("Setup Connection to MongoDB")
	// Get connected to mongodb
	session, err := mongo.Dial(mgoOptions.MgoURI)
	if err != nil {
		fatalErr(err)
	}
	defer session.Close()

	p := make(map[string]interface{})
	// Get the mongodb documents using cursor
	iter := session.DB(mgoOptions.MgoDbname).C(mgoOptions.MgoCollname).Find(mgoOptions.MgoQuery).Iter()
	requests := make(chan elastic.BulkableRequest, 1000)
	elasticWorker := mongoestic.NewElasticWorker(requests, cliContext.Int("workers"))
	elasticWorker.EsOptions = esOptions
	workerDone := elasticWorker.DispatchWorkers()
	// Handle ctrl+c
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	// Signal Handler goroutines
	go func() {
		<-termChan
		log.Println("Terminating Index Action")
		Done <- struct{}{}
	}()
	// Waiting for all workers to terminate (calling IndexWg.Done())
	go func() {
		<-workerDone
		log.Println("All Workers terminated succesfully")
		Done <- struct{}{}
	}()

	// Start the timer
	log.Println("Indexing Documents, please wait ...")
	start := time.Now()
	for iter.Next(&p) {
		// take the value from mongodb documents
		// not all the field in documents will be indexed depends on your mapping
		esBody := mongoestic.CreateEsIndexBody(&p, &esOptions.EsMapping)
		// Create Elasticsearch Bulk Index Request
		bulkRequest := elastic.NewBulkIndexRequest().
			Index(esOptions.EsIndex).
			Type(esOptions.EsType).
			Id(p["_id"].(bson.ObjectId).Hex()).
			Doc(esBody)
		select {
		case <-Done: // Early termination can be caused by no workers spawned (triggered by closing of ProgressQueue) and user hit ctrl+c
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
	log.Println(atomic.LoadInt32(&elasticWorker.IndexResults), " Documents Indexed in ", elapsed)
}
