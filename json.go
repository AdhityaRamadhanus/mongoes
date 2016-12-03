package mongoes

import (
	// "encoding/json"
	// "io/ioutil"
	"strings"
)

// JSON Shortcut type
type JSON map[string]interface{}

// GetDeepObject return object within JSON object
func GetDeepObject(obj map[string]interface{}, path string) JSON {
	ret := obj
	splitted := strings.Split(path, ".")
	for _, v := range splitted {
		// fmt.Println(v)
		ret = ret[v].(map[string]interface{})
	}
	return ret
}
