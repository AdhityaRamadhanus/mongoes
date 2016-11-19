package main

import (
	// "errors"
	"flag"
	"fmt"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"mongoes/libs"
	"os"
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

type Oplog struct {
	Ts bson.MongoTimestamp    `bson:"ts"`
	Ns string                 `bson:"ns"`
	O2 map[string]interface{} `bson:"o2"`
	O  map[string]interface{} `bson:"o"`
	Op string                 `bson:"op"`
}

func oplogWorkers(esUri string, requests <-chan Oplog) {
	client, err := elastic.NewClient(elastic.SetURL(esUri))
	if err != nil {
		return
	}

	bulkService := elastic.NewBulkService(client).Index(indexName).Type(typeName)
	// counts := 0
	for v := range requests {
		bulkService.Add(v)
		if bulkService.NumberOfActions() == 1000 {
			bulkResponse, _ := bulkService.Do()
			ProgressQueue <- len(bulkResponse.Indexed())
		}
	}
	// requests closed
	if bulkService.NumberOfActions() > 0 {
		bulkResponse, _ := bulkService.Do()
		ProgressQueue <- len(bulkResponse.Indexed())

	}
}

func main() {
	// var dbName = flag.String("db", "", "Mongodb DB Name")
	// var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	// var indexName = flag.String("index", "", "ES Index Name")
	// var typeName = flag.String("type", "", "ES Type Name")
	// var mappingFile = flag.String("mapping", "", "Mapping mongodb field to es")
	// var queryFile = flag.String("filter", "", "Query to filter mongodb docs")
	// var esUri = flag.String("--esUri", "http://localhost:9200", "Elasticsearch URI")

	flag.Parse()

	// if len(*dbName) == 0 || len(*collName) == 0 {
	// 	fatal(errors.New("Please provide db and collection name"))
	// 	return
	// }

	// if len(*indexName) == 0 {
	// 	indexName = dbName
	// }

	// if len(*typeName) == 0 {
	// 	typeName = collName
	// }

	// var query map[string]interface{}
	// if len(*queryFile) > 0 {
	// 	var queryerr error
	// 	query, queryerr = libs.ReadJson(*queryFile)
	// 	if queryerr != nil {
	// 		fmt.Println(queryerr)
	// 	}
	// }

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
	collection := session.DB("local").C("oplog.rs")
	fmt.Println("Start Tailing MongoDb")
	var lastId bson.MongoTimestamp = bson.MongoTimestamp(time.Now().Unix())
	lastId <<= 32
	lastId |= 1
	fmt.Println(lastId)
	var p Oplog
	iter := collection.Find(bson.M{"ns": "scaleable_dev.tbljobs", "ts": bson.M{"$gt": lastId}}).Tail(5 * time.Second)
	for {
		for iter.Next(&p) {
			lastId = p.Ts
			fmt.Println(p.Ts, p.O2, p.Op)
		}
		if iter.Err() != nil {
			fmt.Println("got error")
			break
		}
		if iter.Timeout() {
			continue
		}
		query := collection.Find(bson.M{"ns": "scaleable_dev.tbljobs", "ts": bson.M{"$gt": lastId}})
		iter = query.Tail(5 * time.Second)
	}
	iter.Close()
}
