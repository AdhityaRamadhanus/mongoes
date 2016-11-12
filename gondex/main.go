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
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

var counts int32 = 0

type Message struct {
	Id       bson.ObjectId
	Document map[string]interface{}
}

func printStats(stats elastic.BulkProcessorStats) {
	fmt.Println("Flushed:", stats.Flushed)
	fmt.Println("Committed:", stats.Committed)
	fmt.Println("Indexed:", stats.Indexed)
	fmt.Println("Created:", stats.Created)
	fmt.Println("Updated:", stats.Updated)
	fmt.Println("Deleted:", stats.Deleted)
	fmt.Println("Succedeed:", stats.Succeeded)
	fmt.Println("Failed:", stats.Failed)

}
func main() {
	var dbName = flag.String("db", "", "Mongodb DB Name")
	var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	var indexName = flag.String("index", "", "ES Index Name")
	var typeName = flag.String("type", "", "ES Type Name")
	var mappingFile = flag.String("mapping", "", "Mapping mongodb field to es")
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
	bulkProcessorService := elastic.NewBulkProcessorService(client).Workers(4).Stats(true)
	bulkProcessor, _ := bulkProcessorService.Do()
	bulkProcessor.Start()
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

		bulkProcessor.Add(bulkRequest)
	}
	iter.Close()
	elapsed := time.Since(start)
	stats := bulkProcessor.Stats()
	fmt.Println("Finished indexing", stats.Indexed, "documents in", elapsed)
	printStats(stats)
}
