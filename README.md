# MONGOES
[![Go Report Card](https://goreportcard.com/badge/github.com/AdhityaRamadhanus/mongoes)](https://goreportcard.com/report/github.com/AdhityaRamadhanus/mongoes)

commandline tools to index mongodb documents in elasticsearch

<p>
  <a href="#Installation">Installation |</a>
  <a href="#Mongoes">Mongoes</a> |
  <a href="#Usage">Usage</a> |
  <a href="#licenses">License</a>
  <br><br>
  <blockquote>
	This tool is perfect fit for you if you only need to index some collection to elasticsearch without having to setup replicaset, or if you want to have control over the mapping like what fields to be indexed what type in elasticsearch this field should be indexed
  </blockquote>
</p>

Installation
------------
* git clone
* go get
* make

Mongoes
------------
* Mongoes will index your mongodb collection to elasticsearch based on a mapping you provide

Usage
------------
```
mongoes --help

NAME:
   gondex - Index Mongodb Collection to ES

USAGE:
   mongoes [global options] command [command options] [arguments...]

VERSION:
   1.0.0

AUTHOR:
   Adhitya Ramadhanus <adhitya.ramadhanus@gmail.com>

COMMANDS:
     index    Index collection to elasticsearch
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --verbose      Enable verbose mode
   --help, -h     show help
   --version, -v  print the version
```
* config, Your configuration file (in json)
* Config Example
```
{
    "mongodb": {
        "uri": "localhost:27017",
        "database": "somedb",
        "collection": "somecoll"
    },
    "elasticsearch": {
        "uri": "http://localhost:9200",
        "index": "someindex",
        "type": "sometype"
    },
    "query": {
        "completed": true
    },
	  "mapping": {	
		  "title": { // take title field in mongodb collection
			"es_type": "string", // will be mapped to string
			"es_index": "not_analyzed"
		},
		"completed": {
			"es_name": "done", // will be mapped to field called done in elasticsearch
			"es_type": "boolean"
		}
	}
}
```

License
----

MIT © [Adhitya Ramadhanus]

