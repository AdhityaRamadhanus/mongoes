package main

import (
	// "context"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	"github.com/spf13/viper"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
	"os"
	// "sync"
	"runtime"
	"sync/atomic"
	"time"
)

var (
	counts        int32 = 0
	ProgressQueue       = make(chan int)

	es_options mongoes.ESOptions

	mgo_options mongoes.MgoOptions

	mgoQuery  map[string]interface{}
	esMapping map[string]interface{}

	// Done channel signal, main goroutines should exit
	Done = make(chan struct{})
)

func fatal(e error) {
	fmt.Println(e)
	flag.PrintDefaults()
}

func peekProgress() {
	for amounts := range ProgressQueue {
		atomic.AddInt32(&counts, int32(amounts))
		fmt.Println(atomic.LoadInt32(&counts), " Indexed")
	}
	Done <- struct{}{}
}

func init() {
	viper.SetConfigName("config")
	viper.AddConfigPath(".")

	err := viper.ReadInConfig()
	if err != nil {
		fatal(err)
		return
	}
	mgo_options.Mgo_dbname = viper.GetString("mongodb.database")
	mgo_options.Mgo_collname = viper.GetString("mongodb.collection")
	mgo_options.Mgo_URI = viper.GetString("mongodb.uri")
	mgoQuery = viper.GetStringMap("query")

	es_options.ES_index = viper.GetString("elasticsearch.index")
	es_options.ES_type = viper.GetString("elasticsearch.type")
	es_options.ES_URI = viper.GetString("elasticsearch.uri")
	esMapping = viper.GetStringMap("mapping")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var numWorkers = flag.Int("--workers", 2, "Number of concurrent workers")
	flag.Parse()

	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	if err := setupIndexAndMapping(es_options, esMapping, tracer); err != nil {
		fatal(err)
		return
	}

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at ", mgo_options.Mgo_URI)
	session, err := mongo.Dial(mgo_options.Mgo_URI)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

	p := make(map[string]interface{})
	iter := session.DB(mgo_options.Mgo_dbname).C(mgo_options.Mgo_collname).Find(mgoQuery).Iter()
	tracer.Trace("Start Indexing MongoDb")
	// requests := make(chan elastic.BulkableRequest)
	// spawn workers
	requests := DispatchWorkers(*numWorkers, es_options)
	go peekProgress()
	start := time.Now()

	for iter.Next(&p) {
		var esBody = make(map[string]interface{})
		for k, v := range esMapping {
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
			Index(es_options.ES_index).
			Type(es_options.ES_type).
			Id(p["_id"].(bson.ObjectId).Hex()).
			Doc(esBody)
		requests <- bulkRequest
	}
	close(requests)
	iter.Close()
	<-Done
	elapsed := time.Since(start)
	tracer.Trace("Documents Indexed in ", elapsed)
}
