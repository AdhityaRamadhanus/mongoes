package mongoes

import (
	"strings"
)

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
