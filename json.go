package mongoes

import (
	"encoding/json"
	"io/ioutil"
)

// JSON Shortcut type
type JSON map[string]interface{}

// ReadJSON read json file and return JSON
func ReadJSON(filename string) (JSON, error) {
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
