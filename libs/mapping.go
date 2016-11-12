package libs

import (
	"encoding/json"
	"io/ioutil"
	"strings"
)

type JSON map[string]interface{}

func ReadMappingJson(filename string) (JSON, error) {
	res, err := ioutil.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	var mapping JSON
	err = json.Unmarshal(res, &mapping)
	if err != nil {
		return nil, err
	}
	return mapping, nil
}

func CreateMapping(doc JSON) (JSON, error) {
	mapping := JSON{
		"properties": JSON{},
	}
	for k, v := range doc {
		fieldMapping := mapping["properties"].(JSON)
		tempMapping := v.(map[string]interface{})
		name, ok := tempMapping["es_name"].(string)
		if !ok {
			name = k
		}
		fieldMapping[name] = JSON{}
		innerAssertMap := fieldMapping[name].(JSON)
		for key, innerV := range tempMapping {
			if key != "es_name" {
				esKey := strings.Replace(key, "es_", "", -1)
				innerAssertMap[esKey] = innerV
			}
		}
	}
	// fmt.Println(mapping)
	// if jsonString, err := json.Marshal(mapping); err == nil {
	// 	fmt.Println(string(jsonString))
	// }
	return mapping, nil
}
