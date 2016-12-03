package mongoes

type ESOptions struct {
	ES_index string
	ES_type  string
	ES_URI   string
}

type MgoOptions struct {
	Mgo_dbname   string
	Mgo_collname string
	Mgo_URI      string
}
