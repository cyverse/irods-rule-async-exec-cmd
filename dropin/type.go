package dropin

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"time"
)

type DropInRequestType string

const (
	SendMessageRequestType  DropInRequestType = "send_message"
	LinkBisqueRequestType   DropInRequestType = "link_bisque"
	RemoveBisqueRequestType DropInRequestType = "remove_bisque"
	MoveBisqueRequestType   DropInRequestType = "move_bisque"
)

// DropInItem is an interface that all drop-in items must implement
type DropInItem interface {
	GetRequestType() DropInRequestType
	GetCreationTime() time.Time
	GetItemFilePath() string
	SetItemFilePath(path string)
	MarshalJson() ([]byte, error)
	ToString() string
	SaveToFile(path string) error
}

// DropInItemBase is a common parts that all drop-in items must contain
type DropInItemBase struct {
	Type         DropInRequestType `json:"type"`          // requred to identify what this item is
	CreationTime time.Time         `json:"creation_time"` // creation time
	FilePath     string            `json:"-"`             // stores physical path of item, to be filled when the item is drop-in
}

// NewDropInRequestFromFile creates DropInItem from a file
func NewDropInRequestFromFile(path string) (DropInItem, error) {
	bytes, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	req, err := NewDropInRequest(bytes)
	if err != nil {
		return nil, err
	}

	req.SetItemFilePath(path)
	return req, nil
}

// NewDropInRequestFromFile creates DropInItem from byte array
func NewDropInRequest(bytes []byte) (DropInItem, error) {
	var content map[string]interface{}

	err := json.Unmarshal(bytes, &content)
	if err != nil {
		return nil, err
	}

	if reqType, ok := content["type"]; ok {
		reqTypeString := reqType.(string)
		switch DropInRequestType(reqTypeString) {
		case SendMessageRequestType:
			return NewSendMessageRequestFromBytes(bytes)
		case LinkBisqueRequestType:
			return NewLinkBisqueRequestFromBytes(bytes)
		case RemoveBisqueRequestType:
			return NewRemoveBisqueRequestFromBytes(bytes)
		case MoveBisqueRequestType:
			return NewMoveBisqueRequestFromBytes(bytes)
		default:
			return nil, fmt.Errorf("unknown request type - %s", reqTypeString)
		}
	}

	return nil, fmt.Errorf("unknown request type - field 'type' not provided")
}

type SendMessageRequest struct {
	DropInItemBase

	Key  string `json:"key"`
	Body string `json:"body"`
}

func NewSendMessageRequest(key string, body string) *SendMessageRequest {
	return &SendMessageRequest{
		DropInItemBase: DropInItemBase{
			Type:         SendMessageRequestType,
			CreationTime: time.Now().Local(),
		},
		Key:  key,
		Body: body,
	}
}

func NewSendMessageRequestFromBytes(bytes []byte) (*SendMessageRequest, error) {
	var request SendMessageRequest
	err := json.Unmarshal(bytes, &request)
	if err != nil {
		return nil, err
	}

	return &request, nil
}

func (request *SendMessageRequest) GetRequestType() DropInRequestType {
	return request.Type
}

func (request *SendMessageRequest) GetCreationTime() time.Time {
	return request.CreationTime
}

func (request *SendMessageRequest) GetItemFilePath() string {
	return request.FilePath
}

func (request *SendMessageRequest) SetItemFilePath(path string) {
	request.FilePath = path
}

func (request *SendMessageRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *SendMessageRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bytes, 0o666)
}

func (request *SendMessageRequest) ToString() string {
	return fmt.Sprintf("send message request - key: '%s', body: '\n%s\n', timestamp: %s", request.Key, request.Body, request.CreationTime.String())
}

type LinkBisqueRequest struct {
	DropInItemBase

	IRODSUsername string `json:"irods_username"`
	IRODSPath     string `json:"irods_path"`
}

func NewLinkBisqueRequest(irodsUsername string, irodsPath string) *LinkBisqueRequest {
	return &LinkBisqueRequest{
		DropInItemBase: DropInItemBase{
			Type:         LinkBisqueRequestType,
			CreationTime: time.Now().Local(),
		},
		IRODSUsername: irodsUsername,
		IRODSPath:     irodsPath,
	}
}

func NewLinkBisqueRequestFromBytes(bytes []byte) (*LinkBisqueRequest, error) {
	var request LinkBisqueRequest
	err := json.Unmarshal(bytes, &request)
	if err != nil {
		return nil, err
	}

	return &request, nil
}

func (request *LinkBisqueRequest) GetRequestType() DropInRequestType {
	return request.Type
}

func (request *LinkBisqueRequest) GetCreationTime() time.Time {
	return request.CreationTime
}

func (request *LinkBisqueRequest) GetItemFilePath() string {
	return request.FilePath
}

func (request *LinkBisqueRequest) SetItemFilePath(path string) {
	request.FilePath = path
}

func (request *LinkBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *LinkBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bytes, 0o666)
}

func (request *LinkBisqueRequest) ToString() string {
	return fmt.Sprintf("link bisque request - irods user: '%s', irods path: '%s', timestamp: %s", request.IRODSUsername, request.IRODSPath, request.CreationTime.String())
}

type RemoveBisqueRequest struct {
	DropInItemBase

	IRODSUsername string `json:"irods_username"`
	IRODSPath     string `json:"irods_path"`
}

func NewRemoveBisqueRequest(irodsUsername string, irodsPath string) *RemoveBisqueRequest {
	return &RemoveBisqueRequest{
		DropInItemBase: DropInItemBase{
			Type:         RemoveBisqueRequestType,
			CreationTime: time.Now().Local(),
		},
		IRODSUsername: irodsUsername,
		IRODSPath:     irodsPath,
	}
}

func NewRemoveBisqueRequestFromBytes(bytes []byte) (*RemoveBisqueRequest, error) {
	var request RemoveBisqueRequest
	err := json.Unmarshal(bytes, &request)
	if err != nil {
		return nil, err
	}

	return &request, nil
}

func (request *RemoveBisqueRequest) GetRequestType() DropInRequestType {
	return request.Type
}

func (request *RemoveBisqueRequest) GetCreationTime() time.Time {
	return request.CreationTime
}

func (request *RemoveBisqueRequest) GetItemFilePath() string {
	return request.FilePath
}

func (request *RemoveBisqueRequest) SetItemFilePath(path string) {
	request.FilePath = path
}

func (request *RemoveBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *RemoveBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bytes, 0o666)
}

func (request *RemoveBisqueRequest) ToString() string {
	return fmt.Sprintf("remove bisque request - irods user: '%s', irods path: '%s', timestamp: %s", request.IRODSUsername, request.IRODSPath, request.CreationTime.String())
}

type MoveBisqueRequest struct {
	DropInItemBase

	IRODSUsername   string `json:"irods_username"`
	SourceIRODSPath string `json:"source_irods_path"`
	DestIRODSPath   string `json:"dest_irods_path"`
}

func NewMoveBisqueRequest(irodsUsername string, sourceIrodsPath string, destIrodsPath string) *MoveBisqueRequest {
	return &MoveBisqueRequest{
		DropInItemBase: DropInItemBase{
			Type:         MoveBisqueRequestType,
			CreationTime: time.Now().Local(),
		},
		IRODSUsername:   irodsUsername,
		SourceIRODSPath: sourceIrodsPath,
		DestIRODSPath:   destIrodsPath,
	}
}

func NewMoveBisqueRequestFromBytes(bytes []byte) (*MoveBisqueRequest, error) {
	var request MoveBisqueRequest
	err := json.Unmarshal(bytes, &request)
	if err != nil {
		return nil, err
	}

	return &request, nil
}

func (request *MoveBisqueRequest) GetRequestType() DropInRequestType {
	return request.Type
}

func (request *MoveBisqueRequest) GetCreationTime() time.Time {
	return request.CreationTime
}

func (request *MoveBisqueRequest) GetItemFilePath() string {
	return request.FilePath
}

func (request *MoveBisqueRequest) SetItemFilePath(path string) {
	request.FilePath = path
}

func (request *MoveBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *MoveBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return ioutil.WriteFile(path, bytes, 0o666)
}

func (request *MoveBisqueRequest) ToString() string {
	return fmt.Sprintf("move bisque request - irods user: '%s', source irods path: '%s', dest irods path: '%s', timestamp: %s", request.IRODSUsername, request.SourceIRODSPath, request.DestIRODSPath, request.CreationTime.String())
}
