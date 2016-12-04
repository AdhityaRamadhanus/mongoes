package main

import (
	"errors"
	"flag"
	"fmt"
	"github.com/AdhityaRamadhanus/mongoes"
	"github.com/spf13/viper"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v5"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"
)

var (
	counts int32
	// ProgressQueue is channel of int that track how many documents indexed
	ProgressQueue = make(chan int)
	esOptions     mongoes.ESOptions
	mgoOptions    mongoes.MgoOptions
	mgoQuery      map[string]interface{}
	esMapping     map[string]interface{}
	configName    = flag.String("config", "", "config file")
	pathConfig    = flag.String("path", ".", "config path")
	// Done channel signal, main goroutines should exit
	Done = make(chan struct{})
)

func fatal(e error) {
	fmt.Println(e)
	fmt.Println("For More information see https://github.com/AdhityaRamadhanus/mongoes/blob/master/README.md")
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
	mgoQuery = viper.GetStringMap("query")

	esOptions.EsIndex = viper.GetString("elasticsearch.index")
	esOptions.EsType = viper.GetString("elasticsearch.type")
	esOptions.EsURI = viper.GetString("elasticsearch.uri")
	esMapping = viper.GetStringMap("mapping")
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())
	var numWorkers = flag.Int("--workers", 2, "Number of concurrent workers")
	flag.Parse()

	// Set Tracer
	tracer := mongoes.NewTracer(os.Stdout)

	if err := setupIndexAndMapping(esOptions, esMapping, tracer); err != nil {
		fatal(err)
		return
	}

	// Get connected to mongodb
	tracer.Trace("Connecting to Mongodb at ", mgoOptions.MgoURI)
	session, err := mongo.Dial(mgoOptions.MgoURI)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

	p := make(map[string]interface{})
	iter := session.DB(mgoOptions.MgoDbname).C(mgoOptions.MgoCollname).Find(mgoQuery).Iter()
	tracer.Trace("Start Indexing MongoDb")
	// requests := make(chan elastic.BulkableRequest)
	// Dispatch workers, returned a channel (work queue)
	requests := dispatchWorkers(*numWorkers, esOptions)
	// run a goroutines to watch the progres
	go peekProgress()
	// Handle ctrl+c
	termChan := make(chan os.Signal, 1)
	signal.Notify(termChan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
	go func() {
		<-termChan
		Done <- struct{}{}
	}()

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
			Index(esOptions.EsIndex).
			Type(esOptions.EsType).
			Id(p["_id"].(bson.ObjectId).Hex()).
			Doc(esBody)
		select {
		case <-Done: // Early termination can be caused by no workers spawned (triggered by closing of ProgressQueue) and user hit ctrl+c
			fmt.Println("Early Termination")
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
	tracer.Trace(atomic.LoadInt32(&counts), " Documents Indexed in ", elapsed)
}
