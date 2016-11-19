package mongoes

import (
	"encoding/json"
	"io/ioutil"
)

type JSON map[string]interface{}

func ReadJson(filename string) (JSON, error) {
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
