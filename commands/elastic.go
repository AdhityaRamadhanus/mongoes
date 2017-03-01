package commands

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/AdhityaRamadhanus/mongoes"
	"github.com/pkg/errors"
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
	mgoQuery map[string]interface{}
	// Elaticsearch Mapping in JSON format
	// see https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping.html for more information
	esMapping    map[string]interface{}
	IndexResults int32
	// Done channel signal, main goroutines should exit
	Done    = make(chan struct{})
	IndexWg sync.WaitGroup
)

func fatalErr(err error) {
	log.Println(err)
	os.Exit(1)
}

// Main Commands Function
func indexMongoToES(cliContext *cli.Context) {
	if err := setupEsConfig(cliContext); err != nil {
		fatalErr(err)
	}
	log.Println("Setup ES Index and Mapping")
	if err := setupIndexAndMapping(esOptions, esMapping); err != nil {
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
	iter := session.DB(mgoOptions.MgoDbname).C(mgoOptions.MgoCollname).Find(mgoQuery).Iter()
	// tracer.Trace("Start Indexing MongoDb")
	// Dispatch workers, returned a channel (work queue)
	requests := dispatchWorkers(cliContext.Int("workers"), esOptions)
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
		IndexWg.Wait()
		log.Println("All Workers terminated succesfully")
		Done <- struct{}{}
	}()

	// Start the timer
	log.Println("Indexing Documents, please wait ...")
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
	log.Println(atomic.LoadInt32(&IndexResults), " Documents Indexed in ", elapsed)
}

func setupEsConfig(cliContext *cli.Context) error {
	configFilePath := cliContext.String("config")
	if len(configFilePath) == 0 {
		return errors.New("Please provide config file")
	}
	// Read the json config
	var config map[string]interface{}
	err := mongoes.ReadJSONFromFile(configFilePath, &config)
	if err != nil {
		return errors.Wrap(err, "Unable to parse config file")
	}

	mgoOptions.MgoDbname = mongoes.GetStringJSON(config, "mongodb.database")
	mgoOptions.MgoCollname = mongoes.GetStringJSON(config, "mongodb.collection")
	mgoOptions.MgoURI = mongoes.GetStringJSON(config, "mongodb.uri")
	mgoQuery = mongoes.GetObjectJSON(config, "query")

	esOptions.EsIndex = mongoes.GetStringJSON(config, "elasticsearch.index")
	esOptions.EsType = mongoes.GetStringJSON(config, "elasticsearch.type")
	esOptions.EsURI = mongoes.GetStringJSON(config, "elasticsearch.uri")
	esOptions.BulkIndexNum = cliContext.Int("bulk")
	esMapping = mongoes.GetObjectJSON(config, "mapping")
	return nil
}

/** CreateMapping create Elastic Search Mapping from mongodb collection
	Example of mapping input:
	"mapping": {
        "title": {
            "es_type": "text"
        },
        "slug": {
            "es_type": "keyword"
        }
    }

    Example of mapping output
    "properties": {
		"title": {
			"type": "text"
		}
		"slug": {
			"type": "keyword"
		}
    }
*/
func createMapping(doc map[string]interface{}) map[string]interface{} {
	mapping := map[string]interface{}{
		"properties": map[string]interface{}{},
	}
	for k, v := range doc {
		fieldMapping := mapping["properties"].(map[string]interface{})
		tempMapping := v.(map[string]interface{})
		name, ok := tempMapping["es_name"].(string)
		if !ok {
			name = k
		}
		fieldMapping[name] = map[string]interface{}{}
		innerAssertMap := fieldMapping[name].(map[string]interface{})
		for key, innerV := range tempMapping {
			if key != "es_name" {
				esKey := strings.Replace(key, "es_", "", -1)
				innerAssertMap[esKey] = innerV
			}
		}
	}
	return mapping
}

// SetupIndexAndMapping will Delete Index and Create new Mapping
// Beware, this will delete your current index and create new mapping
func setupIndexAndMapping(esOptions mongoes.ESOptions, rawMapping map[string]interface{}) error {
	// tracer.Trace("Connecting to elasticsearch cluster ", esOptions.EsURI)
	client, err := elastic.NewSimpleClient(elastic.SetURL(esOptions.EsURI))
	if err != nil {
		return err
	}
	// tracer.Trace("Delete current index ", esOptions.EsIndex)
	client.DeleteIndex(esOptions.EsIndex).Do(context.Background())

	_, err = client.CreateIndex(esOptions.EsIndex).Do(context.Background())
	if err != nil {
		return err
	}
	esMapping := createMapping(rawMapping)
	_, err = client.PutMapping().Index(esOptions.EsIndex).Type(esOptions.EsType).BodyJson(esMapping).Do(context.Background())
	if err != nil {
		return err
	}
	// tracer.Trace("Create new mapping ", esOptions.EsIndex, esOptions.EsType)
	return nil
}

func createEsIndexBody(mongoDoc *map[string]interface{}, esMapping *map[string]interface{}) map[string]interface{} {
	var esBody = make(map[string]interface{})
	for k, v := range *esMapping {
		mgoVal, ok := (*mongoDoc)[k]
		if ok {
			var key = (v.(map[string]interface{}))["es_name"]
			if key == nil {
				key = k
			}
			esBody[key.(string)] = mgoVal
		}
	}
	return esBody
}
