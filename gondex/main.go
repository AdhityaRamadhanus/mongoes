package main

import (
	"errors"
	"flag"
	"fmt"
	mongo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	elastic "gopkg.in/olivere/elastic.v3"
	"log"
	"mongoes/gondex/libs"
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

	session, err := mongo.Dial(*dbUri)
	if err != nil {
		fatal(err)
		return
	}
	defer session.Close()

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
	rawMapping, _ := libs.ReadMappingJson(*mappingFile)
	esMapping, _ := libs.CreateMapping(rawMapping)
	_, err = client.PutMapping().Index(*indexName).Type(*typeName).BodyJson(esMapping).Do()
	if err != nil {
		fatal(err)
		return
	}
	p := make(map[string]interface{})
	iter := session.DB(*dbName).C(*collName).Find(bson.M{"is_active": true, "company_name": "Bukalapak"}).Iter()
	for iter.Next(&p) {
		fmt.Println(p["_id"].(bson.ObjectId).Hex())
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
		fmt.Println(esBody)
		_, err = client.Index().
			Index(*indexName).
			Type(*typeName).
			Id(p["_id"].(bson.ObjectId).Hex()).
			BodyJson(esBody).
			Refresh(true).
			Do()
		if err != nil {
			log.Println(err)
		}
	}
	iter.Close()
}
