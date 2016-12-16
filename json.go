package mongoes

import (
	"encoding/json"
	"io/ioutil"
	"strings"
)

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

// GetDeepObject return object within JSON object
func GetDeepObject(obj map[string]interface{}, path string) map[string]interface{} {
	ret := obj
	splitted := strings.Split(path, ".")
	for _, v := range splitted {
		// fmt.Println(v)
		ret = ret[v].(map[string]interface{})
	}
	return ret
}
