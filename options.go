package mongoes

// ESOptions is options struct for elasticsearch, included is index name, type name and URI for ES cluster
type ESOptions struct {
	EsIndex      string
	EsType       string
	EsURI        string
	BulkIndexNum int
	EsMapping    map[string]interface{}
}

// MgoOptions is optiosn struct for mongo, included is database name, collection name, and URI for mongo
type MgoOptions struct {
	MgoDbname   string
	MgoCollname string
	MgoURI      string
	MgoQuery    map[string]interface{}
}
