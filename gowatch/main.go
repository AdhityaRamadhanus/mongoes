package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	"github.com/spf13/viper"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"os"
	"time"
)

func fatal(e error) {
	fmt.Println(e)
	fmt.Println("For More information see https://github.com/AdhityaRamadhanus/mongoes/blob/master/README.md")
	flag.PrintDefaults()
}

var (
	es_options  mongoes.ESOptions
	mgo_options mongoes.MgoOptions
	configName  = flag.String("config", "", "config file")
	pathConfig  = flag.String("path", ".", "config path")
)

func init() {
	flag.Parse()
	if len(*configName) == 0 {
		fatal(errors.New("Please provide config file and config path"))
		os.Exit(1)
	}
	viper.SetConfigName(*configName)
	viper.AddConfigPath(*pathConfig)

	err := viper.ReadInConfig()
	if err != nil {
		fatal(err)
		os.Exit(1)
	}
	mgo_options.MgoDbname = viper.GetString("mongodb.database")
	mgo_options.MgoCollname = viper.GetString("mongodb.collection")
	mgo_options.MgoURI = viper.GetString("mongodb.uri")

	es_options.EsIndex = viper.GetString("elasticsearch.index")
	es_options.EsType = viper.GetString("elasticsearch.type")
	es_options.EsURI = viper.GetString("elasticsearch.uri")
}

func main() {
	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	// Get connected to Elasticsearch
	tracer.Trace("Connecting to Elasticsearch cluster at", es_options.EsURI)
	selectedField, err := getMapping(es_options)
	if err != nil {
		fatal(err)
		return
	}
	oplogs := processOplog(es_options, selectedField)

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at", mgo_options.MgoURI)
	session, err := mongo.Dial(mgo_options.MgoURI)
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
	var p Oplog
	var nstring = mgo_options.MgoDbname + "." + mgo_options.MgoCollname
	iter := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastId}}).Tail(5 * time.Second)
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
