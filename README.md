# MONGOES
Set of commandline tools to synchronize mongodb documents and elasticsearch index

<p>
  <a href="#installation">Gondex</a> |
  <a href="#documentation">Gowatch</a> |
  <a href="#licenses">License</a>
  <br><br>
  <blockquote>
	Set of commandline tools to synchronize mongodb documents and elasticsearch index
  </blockquote>
</p>

Gondex
------------
* Gondex will index your mongodb collection based on a mapping you provide

Usage
------------
```
gondex --db=<dbname> --collection=<collectioname> --index=<indexname> --type=<typename> --dbUri=<MongoURI> --mapping=/some/mapping.json
```
* db, Your Mongodb DB name
* collection, Your mongodb collection name
* index, Preferred elasticsearch index name (db name will be used if you leave this empty)
* type, Preferred elasticsearch type name (collection name will be used if you leave this empty)
* dbUri, MongoDB URI
* mapping, Json file that defined how you want to map every document in mongodb to elasticsearch
* Mapping Example
```
{
	"title": { // take title field in mongodb collection
		"es_type": "string", // will be mapped to string
		"es_index": "not_analyzed"
	},
	"completed": {
		"es_name": "done", // will be mapped to field called done in elasticsearch
		"es_type": "boolean"
	}
}
```

Gowatch 
------------
* Synchronize your mongodb collection with elasticsearch index using mongodb replicaset oplog (operation log)
* SOON

License
----

MIT © [Adhitya Ramadhanus]

