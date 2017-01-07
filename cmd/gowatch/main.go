package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"
)

// Just error helper to make convenient to print error
func fatal(e error) {
	fmt.Println(e)
	fmt.Println("For More information see https://github.com/AdhityaRamadhanus/mongoes/blob/master/README.md")
	flag.PrintDefaults()
}

var (
	// elasticseach Options, uri, index name and type name
	esOptions mongoes.ESOptions
	// mongodb Options, uri, db name and collection name
	mgoOptions mongoes.MgoOptions
	pathConfig = flag.String("config", "", "config path")
	timestamp  = flag.Int64("ts", time.Now().Unix(), "timestamp oplog")
	lastID     bson.MongoTimestamp
)

func init() {
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

	esOptions.EsIndex = mongoes.GetStringJSON(config, "elasticsearch.index")
	esOptions.EsType = mongoes.GetStringJSON(config, "elasticsearch.type")
	esOptions.EsURI = mongoes.GetStringJSON(config, "elasticsearch.uri")
}

func main() {
	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	// Get connected to Elasticsearch
	tracer.Trace("Connecting to Elasticsearch cluster at", esOptions.EsURI)
	selectedField, err := getMapping(esOptions)
	if err != nil {
		fatal(err)
		return
	}
	// Spawn the oplogs processor
	oplogs := processOplog(esOptions, selectedField)
	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at", mgoOptions.MgoURI)
	session, err := mongo.Dial(mgoOptions.MgoURI)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

	// Take the oplog collection
	collection := session.DB("local").C("oplog.rs")
	fmt.Println("Start Tailing MongoDb")

	lastID = bson.MongoTimestamp(*timestamp)
	lastID <<= 32
	lastID |= 1
	var p Oplog
	// Buld the query and Tail the oplog
	var nstring = mgoOptions.MgoDbname + "." + mgoOptions.MgoCollname
	iter := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastID}}).Tail(5 * time.Second)
	for {
		for iter.Next(&p) {
			// fmt.Println(p)
			lastID = p.Ts
			oplogs <- p
		}
		// Handle Tail Error
		if iter.Err() != nil {
			fmt.Println("got error")
			break
		}
		// Handle Tail Timeout
		if iter.Timeout() {
			continue
		}
		query := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastID}})
		iter = query.Tail(5 * time.Second)
	}
	close(oplogs)
	iter.Close()
}
