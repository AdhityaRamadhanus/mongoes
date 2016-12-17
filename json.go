package mongoes

import (
	"encoding/json"
	"io/ioutil"
	"strings"
)

/*ReadJSONFromFile takes a path and pointer to map[string]interface
and return error (if any)
Example of path : ../module/test.json
*/
func ReadJSONFromFile(path string, v *map[string]interface{}) error {
	fileBytes, err := ioutil.ReadFile(path)
	if err != nil {
		return err
	}
	err = json.Unmarshal(fileBytes, v)
	if err != nil {
		return err
	}
	return nil
}

/*GetStringJSON takes a map[string]interface{} and path
and return string
Example of path : mongodb.uri
Path is a string that contains json keys that's splitted by '.'
*/
func GetStringJSON(obj map[string]interface{}, path string) string {
	temp := obj
	var ret string
	// Split the path
	splitted := strings.Split(path, ".")
	// Iterate over the path
	for i, v := range splitted {
		if i == len(splitted)-1 {
			// on the last path, cast expected Type
			ret = temp[v].(string)
		} else {
			// Still not on the last path, cast to JSON (map[string]interface{})
			temp = temp[v].(map[string]interface{})
		}
	}
	return ret
}

/*GetBoolJSON takes a map[string]interface{} and path
and return bool
Example of path : mongodb.uri
Path is a string that contains json keys that's splitted by '.'
*/
func GetBoolJSON(obj map[string]interface{}, path string) bool {
	temp := obj
	var ret bool
	// Split the path
	splitted := strings.Split(path, ".")
	// Iterate over the path
	for i, v := range splitted {
		if i == len(splitted)-1 {
			// on the last path, cast expected Type
			ret = temp[v].(bool)
		} else {
			// Still not on the last path, cast to JSON (map[string]interface{})
			temp = temp[v].(map[string]interface{})
		}
	}
	return ret
}

/*GetObjectJSON takes a map[string]interface{} and path
and return map[string]interface{}
Example of path : mongodb.uri
Path is a string that contains json keys that's splitted by '.'
*/
func GetObjectJSON(obj map[string]interface{}, path string) map[string]interface{} {
	ret := obj
	// Split the path
	splitted := strings.Split(path, ".")
	// Iterate over the path
	for _, v := range splitted {
		ret = ret[v].(map[string]interface{})
	}
	return ret
}
