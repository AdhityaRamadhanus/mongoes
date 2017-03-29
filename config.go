package mongoes

import (
	"github.com/pkg/errors"
)

func GetConfig(configPath string, esOptions *ESOptions, mgoOptions *MgoOptions) error {
	if len(configPath) == 0 {
		return errors.New("Please provide config file")
	}
	// Read the json config
	var config map[string]interface{}
	err := ReadJSONFromFile(configPath, &config)
	if err != nil {
		return errors.Wrap(err, "Unable to parse config file")
	}

	mgoOptions.MgoDbname = GetStringJSON(config, "mongodb.database")
	mgoOptions.MgoCollname = GetStringJSON(config, "mongodb.collection")
	mgoOptions.MgoURI = GetStringJSON(config, "mongodb.uri")
	mgoOptions.MgoQuery = GetObjectJSON(config, "query")

	esOptions.EsIndex = GetStringJSON(config, "elasticsearch.index")
	esOptions.EsType = GetStringJSON(config, "elasticsearch.type")
	esOptions.EsURI = GetStringJSON(config, "elasticsearch.uri")
	esOptions.EsMapping = GetObjectJSON(config, "mapping")
	return nil
}
