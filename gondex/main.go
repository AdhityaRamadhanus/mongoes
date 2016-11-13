package main

import (
	"errors"
	"flag"
	"fmt"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v3"
	// "log"
	"mongoes/libs"
	"os"
	// "sync/atomic"
	"sync"
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

var counts int32 = 0
var wg sync.WaitGroup

func doService(id int, client *elastic.Client, indexName string, typeName string, requests <-chan elastic.BulkableRequest) {
	defer wg.Done()
	bulkService := elastic.NewBulkService(client).Index(indexName).Type(typeName)
	counts := 0
	for v := range requests {
		bulkService.Add(v)
		if bulkService.NumberOfActions() == 1000 {
			bulkResponse, _ := bulkService.Do()
			counts += len(bulkResponse.Indexed())
		}
	}
	// requests closed
	if bulkService.NumberOfActions() > 0 {
		bulkResponse, _ := bulkService.Do()
		counts += len(bulkResponse.Indexed())
	}
	fmt.Println("Worker", id, " Finished")
	fmt.Println("Indexed", counts)
}
func main() {
	var dbName = flag.String("db", "", "Mongodb DB Name")
	var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	var indexName = flag.String("index", "", "ES Index Name")
	var typeName = flag.String("type", "", "ES Type Name")
	var mappingFile = flag.String("mapping", "", "Mapping mongodb field to es")
	wg.Add(2)
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

	// Set Tracer
	tracer := libs.NewTracer(os.Stdout)

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at", *dbUri)
	session, err := mongo.Dial(*dbUri)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

	tracer.Trace("Connecting to elasticsearch cluster")
	client, err := elastic.NewClient()
	if err != nil {
		fatal(err)
		return
	}
	client.DeleteIndex(*indexName).Do()
	_, err = client.CreateIndex(*indexName).Do()
	if err != nil {
		fatal(err)
		return
	}
	tracer.Trace("Create Mongodb to ES Mapping")
	rawMapping, _ := libs.ReadMappingJson(*mappingFile)
	esMapping, _ := libs.CreateMapping(rawMapping)
	_, err = client.PutMapping().Index(*indexName).Type(*typeName).BodyJson(esMapping).Do()
	if err != nil {
		fatal(err)
		return
	}
	p := make(map[string]interface{})
	iter := session.DB(*dbName).C(*collName).Find(nil).Iter()
	start := time.Now()
	fmt.Println("Start Indexing MongoDb")
	requests := make(chan elastic.BulkableRequest)
	for i := 0; i < 2; i++ {
		go doService(i, client, *indexName, *typeName, requests)
	}
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
	fmt.Println("Finished indexing documents in", elapsed)
}
