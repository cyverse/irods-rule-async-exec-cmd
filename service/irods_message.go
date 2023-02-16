package service

import (
	"encoding/json"
	"fmt"
)

// GetIrodsMsgFromJson returns IRODS message map from JSON
func GetIrodsMsgFromJson(data []byte) (map[string]interface{}, error) {
	body := map[string]interface{}{}
	err := json.Unmarshal(data, &body)
	if err != nil {
		return nil, fmt.Errorf("failed to parse message body - %s, %v", string(data), err.Error())
	}

	return body, nil
}

// GetIrodsMsgUserZone returns IRODS user and zone
func GetIrodsMsgUserZone(msg map[string]interface{}) (string, string, error) {
	authorMap, ok := msg["author"].(map[string]interface{})
	user := ""
	zone := ""
	if ok {
		username, ok3 := authorMap["name"].(string)
		if ok3 {
			user = username
		} else {
			return "", "", fmt.Errorf("failed to get author.name string field from the message")
		}

		zonename, ok3 := authorMap["zone"].(string)
		if ok3 {
			zone = zonename
		} else {
			return "", "", fmt.Errorf("failed to get author.zone string field from the message")
		}
	} else {
		return "", "", fmt.Errorf("failed to get author struct field from the message")
	}

	return user, zone, nil
}

// GetIrodsMsgPath returns IRODS path
func GetIrodsMsgPath(msg map[string]interface{}) (string, error) {
	path, ok := msg["path"].(string)
	if !ok {
		return "", fmt.Errorf("failed to get path string field from the message")
	}

	return path, nil
}

// GetIrodsMsgUUID returns IRODS uuid
func GetIrodsMsgUUID(msg map[string]interface{}) (string, error) {
	path, ok := msg["entity"].(string)
	if !ok {
		return "", fmt.Errorf("failed to get uuid string field from the message")
	}

	return path, nil
}

// GetIrodsMsgOldNewPath returns IRODS old path and IRODS new path
func GetIrodsMsgOldNewPath(msg map[string]interface{}) (string, string, error) {
	oldPath, ok1 := msg["old-path"].(string)
	newPath, ok2 := msg["new-path"].(string)
	if !ok1 {
		return "", "", fmt.Errorf("failed to get old-path string field from the message")
	}

	if !ok2 {
		return "", "", fmt.Errorf("failed to get new-path string field from the message")
	}

	return oldPath, newPath, nil
}
