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
	esOptions  mongoes.ESOptions
	mgoOptions mongoes.MgoOptions
	configName = flag.String("config", "", "config file")
	pathConfig = flag.String("path", ".", "config path")
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
	mgoOptions.MgoDbname = viper.GetString("mongodb.database")
	mgoOptions.MgoCollname = viper.GetString("mongodb.collection")
	mgoOptions.MgoURI = viper.GetString("mongodb.uri")

	esOptions.EsIndex = viper.GetString("elasticsearch.index")
	esOptions.EsType = viper.GetString("elasticsearch.type")
	esOptions.EsURI = viper.GetString("elasticsearch.uri")
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

	oplogs := processOplog(esOptions, selectedField)
	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at", mgoOptions.MgoURI)
	session, err := mongo.Dial(mgoOptions.MgoURI)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()
	collection := session.DB("local").C("oplog.rs")
	fmt.Println("Start Tailing MongoDb")
	var lastID = bson.MongoTimestamp(time.Now().Unix())
	lastID <<= 32
	lastID |= 1
	var p Oplog
	var nstring = mgoOptions.MgoDbname + "." + mgoOptions.MgoCollname
	iter := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastID}}).Tail(5 * time.Second)
	for {
		for iter.Next(&p) {
			lastID = p.Ts
			oplogs <- p
		}
		if iter.Err() != nil {
			fmt.Println("got error")
			break
		}
		if iter.Timeout() {
			continue
		}
		query := collection.Find(bson.M{"ns": nstring, "ts": bson.M{"$gt": lastID}})
		iter = query.Tail(5 * time.Second)
	}
	close(oplogs)
	iter.Close()
}
