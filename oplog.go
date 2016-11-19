package mongoes

import "gopkg.in/mgo.v2/bson"

type Oplog struct {
	Ts bson.MongoTimestamp    `bson:"ts"`
	Ns string                 `bson:"ns"`
	O2 map[string]interface{} `bson:"o2"`
	O  map[string]interface{} `bson:"o"`
	Op string                 `bson:"op"`
}
