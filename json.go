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

func GetStringJSON(obj map[string]interface{}, path string) string {
	temp := obj
	var ret string
	splitted := strings.Split(path, ".")
	for i, v := range splitted {
		if i == len(splitted)-1 {
			ret = temp[v].(string)
		} else {
			temp = temp[v].(map[string]interface{})
		}
	}
	return ret
}

func GetBoolJSON(obj map[string]interface{}, path string) bool {
	temp := obj
	var ret bool
	splitted := strings.Split(path, ".")
	for i, v := range splitted {
		if i == len(splitted)-1 {
			ret = temp[v].(bool)
		} else {
			temp = temp[v].(map[string]interface{})
		}
	}
	return ret
}

// GetDeepObject return object within JSON object
func GetObjectJSON(obj map[string]interface{}, path string) map[string]interface{} {
	ret := obj
	splitted := strings.Split(path, ".")
	for _, v := range splitted {
		// fmt.Println(v)
		ret = ret[v].(map[string]interface{})
	}
	return ret
}
