package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v3"
	"os"
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

func main() {
	var dbName = flag.String("db", "", "Mongodb DB Name")
	var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	var indexName = flag.String("index", "", "ES Index Name")
	var typeName = flag.String("type", "", "ES Type Name")
	// var mappingFile = flag.String("mapping", "", "Mapping mongodb field to es")
	// var queryFile = flag.String("filter", "", "Query to filter mongodb docs")
	var esUri = flag.String("--esUri", "http://localhost:9200", "Elasticsearch URI")

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

	// var query map[string]interface{}
	// if len(*queryFile) > 0 {
	// 	var queryerr error
	// 	query, queryerr = mongoes.ReadJson(*queryFile)
	// 	if queryerr != nil {
	// 		fmt.Println(queryerr)
	// 	}
	// }

	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	// Get elastic search mapping
	// esMapping := make(map[string]interface{})
	tracer.Trace("Connecting to elasticsearch cluster")
	client, err := elastic.NewClient(elastic.SetURL(*esUri))
	if err != nil {
		fatal(err)
		return
	}

	rawMapping, err := client.GetMapping().Index(*indexName).Type(*typeName).Pretty(true).Do()
	if err != nil {
		fatal(err)
		return
	}
	jsonPath := *indexName + ".mappings." + *typeName + ".properties"
	esMapping := mongoes.GetDeepObject(rawMapping, jsonPath)
	selectedField := make([]string, 1)
	for key, _ := range esMapping {
		selectedField = append(selectedField, key)
	}
	fmt.Println(selectedField)

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
	var p mongoes.Oplog
	var nstring = *dbName + "." + *collName
	iter := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastId}}).Tail(5 * time.Second)
	indexService := elastic.NewIndexService(client).Index(*indexName).Type(*typeName)
	for {
		for iter.Next(&p) {
			lastId = p.Ts
			fmt.Println(p.Ts, p.O2, p.Op)
			// process operations
			if p.Op == "i" {
				indexRequest := map[string]interface{}{}
				for _, v := range selectedField {
					indexRequest[v] = p.O[v]
				}
				fmt.Println(indexRequest)
				stringId := p.O["_id"].(bson.ObjectId).Hex()
				fmt.Println(stringId)
				if _, err := indexService.Id(stringId).BodyJson(indexRequest).Do(); err != nil {
					fmt.Println("Successfully indexed")
				}

			}
		}
		if iter.Err() != nil {
			fmt.Println("got error")
			break
		}
		if iter.Timeout() {
			continue
		}
		query := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastId}})
		iter = query.Tail(5 * time.Second)
	}
	iter.Close()
}
