package turnin

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

type TurnInRequestType string

const (
	SendMessageRequestType  TurnInRequestType = "send_message"
	LinkBisqueRequestType   TurnInRequestType = "link_bisque"
	RemoveBisqueRequestType TurnInRequestType = "remove_bisque"
	MoveBisqueRequestType   TurnInRequestType = "move_bisque"
)

// TurnInItem is an interface that all turn-in items must implement
type TurnInItem interface {
	GetRequestType() TurnInRequestType
	GetCreationTime() time.Time
	GetItemFilePath() string
	SetItemFilePath(path string)
	MarshalJson() ([]byte, error)
	ToString() string
	SaveToFile(path string) error
}

// TurnInItemBase is a common parts that all turn-in items must contain
type TurnInItemBase struct {
	Type         TurnInRequestType `json:"type"`          // requred to identify what this item is
	CreationTime time.Time         `json:"creation_time"` // creation time
	FilePath     string            `json:"-"`             // stores physical path of item, to be filled when the item is turn-in
}

func (base *TurnInItemBase) GetRequestType() TurnInRequestType {
	return base.Type
}

func (base *TurnInItemBase) GetCreationTime() time.Time {
	return base.CreationTime
}

func (base *TurnInItemBase) GetItemFilePath() string {
	return base.FilePath
}

func (base *TurnInItemBase) SetItemFilePath(path string) {
	base.FilePath = path
}

// NewTurnInRequestFromFile creates TurnInItem from a file
func NewTurnInRequestFromFile(path string) (TurnInItem, error) {
	bytes, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	req, err := NewTurnInRequest(bytes)
	if err != nil {
		return nil, err
	}

	req.SetItemFilePath(path)
	return req, nil
}

// NewTurnInRequest creates TurnInItem from byte array
func NewTurnInRequest(bytes []byte) (TurnInItem, error) {
	var content map[string]interface{}

	err := json.Unmarshal(bytes, &content)
	if err != nil {
		return nil, err
	}

	if reqType, ok := content["type"]; ok {
		reqTypeString := reqType.(string)
		switch TurnInRequestType(reqTypeString) {
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
	TurnInItemBase

	Key  string `json:"key"`
	Body string `json:"body"`
}

func NewSendMessageRequest(key string, body string) *SendMessageRequest {
	return &SendMessageRequest{
		TurnInItemBase: TurnInItemBase{
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

func (request *SendMessageRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *SendMessageRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o666)
}

func (request *SendMessageRequest) ToString() string {
	return fmt.Sprintf("send message request - key: '%s', body: '\n%s\n', timestamp: %s", request.Key, request.Body, request.CreationTime.String())
}

type LinkBisqueRequest struct {
	TurnInItemBase

	IRODSUsername string `json:"irods_username"`
	IRODSPath     string `json:"irods_path"`
}

func NewLinkBisqueRequest(irodsUsername string, irodsPath string) *LinkBisqueRequest {
	return &LinkBisqueRequest{
		TurnInItemBase: TurnInItemBase{
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

func (request *LinkBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *LinkBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o666)
}

func (request *LinkBisqueRequest) ToString() string {
	return fmt.Sprintf("link bisque request - irods user: '%s', irods path: '%s', timestamp: %s", request.IRODSUsername, request.IRODSPath, request.CreationTime.String())
}

type RemoveBisqueRequest struct {
	TurnInItemBase

	IRODSUsername string `json:"irods_username"`
	IRODSPath     string `json:"irods_path"`
}

func NewRemoveBisqueRequest(irodsUsername string, irodsPath string) *RemoveBisqueRequest {
	return &RemoveBisqueRequest{
		TurnInItemBase: TurnInItemBase{
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

func (request *RemoveBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *RemoveBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o666)
}

func (request *RemoveBisqueRequest) ToString() string {
	return fmt.Sprintf("remove bisque request - irods user: '%s', irods path: '%s', timestamp: %s", request.IRODSUsername, request.IRODSPath, request.CreationTime.String())
}

type MoveBisqueRequest struct {
	TurnInItemBase

	IRODSUsername   string `json:"irods_username"`
	SourceIRODSPath string `json:"source_irods_path"`
	DestIRODSPath   string `json:"dest_irods_path"`
}

func NewMoveBisqueRequest(irodsUsername string, sourceIrodsPath string, destIrodsPath string) *MoveBisqueRequest {
	return &MoveBisqueRequest{
		TurnInItemBase: TurnInItemBase{
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

func (request *MoveBisqueRequest) MarshalJson() ([]byte, error) {
	return json.Marshal(request)
}

func (request *MoveBisqueRequest) SaveToFile(path string) error {
	bytes, err := request.MarshalJson()
	if err != nil {
		return err
	}

	return os.WriteFile(path, bytes, 0o666)
}

func (request *MoveBisqueRequest) ToString() string {
	return fmt.Sprintf("move bisque request - irods user: '%s', source irods path: '%s', dest irods path: '%s', timestamp: %s", request.IRODSUsername, request.SourceIRODSPath, request.DestIRODSPath, request.CreationTime.String())
}

// IsItemTypeSendMessage checks if the given turn-in item is SendMessage request type
func IsItemTypeSendMessage(item TurnInItem) bool {
	return item.GetRequestType() == SendMessageRequestType
}

// IsItemTypeBisque checks if the given turn-in item is *Bisque request types
func IsItemTypeBisque(item TurnInItem) bool {
	switch item.GetRequestType() {
	case LinkBisqueRequestType, RemoveBisqueRequestType, MoveBisqueRequestType:
		return true
	default:
		return false
	}
}
