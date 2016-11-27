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

func processOplog(client *elastic.Client, indexName, typeName string, selectedField []string, oplogs <-chan mongoes.Oplog) {
	indexService := elastic.NewIndexService(client).Index(indexName).Type(typeName)
	deleteService := elastic.NewDeleteService(client).Index(indexName).Type(typeName)

	for p := range oplogs {
		if p.Op == "i" || p.Op == "u" {
			indexRequest := map[string]interface{}{}
			for _, v := range selectedField {
				if p.O[v] != nil {
					indexRequest[v] = p.O[v]
				}
			}
			stringId := p.O["_id"].(bson.ObjectId).Hex()
			if _, err := indexService.Id(stringId).BodyJson(indexRequest).Do(); err == nil {
				fmt.Println("Successfully indexed")
			}
		} else if p.Op == "d" {
			deleteRequestId := p.O["_id"].(bson.ObjectId).Hex()
			fmt.Println(deleteRequestId)
			if _, err := deleteService.Id(deleteRequestId).Do(); err == nil {
				fmt.Println("Successfully deleted")
			}
		}
	}
}

func main() {
	var dbName = flag.String("db", "", "Mongodb DB Name")
	var collName = flag.String("collection", "", "Mongodb Collection Name")
	var dbUri = flag.String("dbUri", "localhost:27017", "Mongodb URI")
	var indexName = flag.String("index", "", "ES Index Name")
	var typeName = flag.String("type", "", "ES Type Name")
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
	oplogs := make(chan mongoes.Oplog, 1000)
	go processOplog(client, *indexName, *typeName, selectedField, oplogs)
	for {
		for iter.Next(&p) {
			lastId = p.Ts
			oplogs <- p
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
	close(oplogs)
	iter.Close()
}
