package mongoes

import (
	"testing"
)

func TestReadJSONFromFile(t *testing.T) {
	var m map[string]interface{}
	err := ReadJSONFromFile("./test-files/test.json", &m)
	if err != nil {
		t.Error("ReadJSON From File should not return error")
	}
}

func TestGetStringJSON(t *testing.T) {
	var m map[string]interface{}
	err := ReadJSONFromFile("./test-files/test.json", &m)
	if err != nil {
		t.Error("ReadJSON From File should not return error")
	}
	val := GetStringJSON(m, "mongodb.uri")
	if val != "localhost:27017" {
		t.Error("GestSTringJSON return unexpected value")
	}
}

func TestGetBoolJSON(t *testing.T) {
	var m map[string]interface{}
	err := ReadJSONFromFile("./test-files/test.json", &m)
	if err != nil {
		t.Error("ReadJSON From File should not return error")
	}
	val := GetBoolJSON(m, "query.is_active")
	if val != true {
		t.Error("GetBoolJSON return unexpected value")
	}
}
