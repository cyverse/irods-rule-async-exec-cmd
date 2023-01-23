package service

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/antchfx/xmlquery"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/turnin"
	log "github.com/sirupsen/logrus"
	amqp_mod "github.com/streadway/amqp"
)

const (
	IRODSKeyValForBisqueID      string = "ipc-bisque-id"
	BisqueLinkPermissionDefault string = "private"
	//BisqueLinkPermissionDefault string = "published"
)

type BisQue struct {
	service *AsyncExecCmdService
	config  *commons.BisqueConfig
	context context.Context
	client  *http.Client
}

// CreateBisque creates a BisQue service object
func CreateBisque(service *AsyncExecCmdService, config *commons.BisqueConfig) (*BisQue, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateBisque",
	})

	defer commons.StackTraceFromPanic(logger)

	context := context.Background()

	client := &http.Client{
		Timeout: 3 * time.Minute,
	}

	return &BisQue{
		service: service,
		config:  config,
		context: context,
		client:  client,
	}, nil
}

// Release releases all resources
func (bisque *BisQue) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "Release",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("trying to release HTTP client")

	if bisque.client != nil {
		bisque.client.CloseIdleConnections()
		bisque.client = nil
	}
}

// ProcessItem processes a turn-in request
func (bisque *BisQue) HandleAmqpEvent(msg amqp_mod.Delivery) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "HandleAmqpEvent",
	})

	defer commons.StackTraceFromPanic(logger)

	if strings.Contains(string(msg.Body), "\r") {
		logger.Error("body with return in it: %s\n", string(msg.Body))
		return
	}

	switch msg.RoutingKey {
	case "data-object.add":
		bisque.processAmqpAddMessage(msg)
		return
	case "data-object.mv":
		bisque.processAmqpMoveMessage(msg)
		return
	case "data-object.rm":
		bisque.processAmqpRemoveMessage(msg)
		return
	default:
		// event is not interested
		return
	}
}

func (bisque *BisQue) processAmqpAddMessage(msg amqp_mod.Delivery) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "processAmqpAddMessage",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("received a message - %s", string(msg.Body))

	msgStruct, err := GetIrodsMsgFromJson(msg.Body)
	if err != nil {
		logger.Error(err)
		return
	}

	user, _, err := GetIrodsMsgUserZone(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	if user == bisque.config.IrodsUsername {
		// raised by irods user via bisque interface
		// we don't need to re-process as it's already processed by bisque.
		logger.Debug("ignoring the request since the request is made by BisQue")
		return
	}

	path, err := GetIrodsMsgPath(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	if !bisque.isIrodsPathForBisque(path) {
		// ignore
		logger.Debugf("ignoring the request since the iRODS path %s is out of BisQue's iRODS root path %s", path, bisque.config.IrodsRootPath)
		return
	}

	bisqueUser := bisque.getHomeUser(path, user)

	logger.Debugf("turn-in a link bisque request %s, %s", bisqueUser, path)

	request := turnin.NewLinkBisqueRequest(bisqueUser, path)
	err = bisque.service.turnin.Turnin(request)
	if err != nil {
		logger.WithError(err).Errorf("failed to turn-in a link bisque request - %s, %s", bisqueUser, path)
		return
	}
}

func (bisque *BisQue) processAmqpMoveMessage(msg amqp_mod.Delivery) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "processAmqpMoveMessage",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("received a message - %s", string(msg.Body))

	msgStruct, err := GetIrodsMsgFromJson(msg.Body)
	if err != nil {
		logger.Error(err)
		return
	}

	user, _, err := GetIrodsMsgUserZone(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	if user == bisque.config.IrodsUsername {
		// raised by irods user via BisQue interface
		// we don't need to re-process as it's already processed by BisQue.
		logger.Debug("ignoring the request since the request is made by BisQue")
		return
	}

	oldPath, newPath, err := GetIrodsMsgOldNewPath(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	if bisque.isIrodsPathForBisque(oldPath) {
		if bisque.isIrodsPathForBisque(newPath) {
			// move
			bisqueUser := bisque.getHomeUser(newPath, user)

			logger.Debugf("turn-in a move bisque request %s, %s to %s", bisqueUser, oldPath, newPath)

			request := turnin.NewMoveBisqueRequest(bisqueUser, oldPath, newPath)
			err = bisque.service.turnin.Turnin(request)
			if err != nil {
				logger.WithError(err).Errorf("failed to turn-in a move bisque request - %s, %s, %s", bisqueUser, oldPath, newPath)
				return
			}
			return
		} else {
			// remove
			bisqueUser := bisque.getHomeUser(oldPath, user)

			logger.Debugf("turn-in a remove bisque request %s, %s", bisqueUser, oldPath)

			request := turnin.NewRemoveBisqueRequest(bisqueUser, oldPath)
			err = bisque.service.turnin.Turnin(request)
			if err != nil {
				logger.WithError(err).Errorf("failed to turn-in a remove bisque request - %s, %s", bisqueUser, oldPath)
				return
			}
			return
		}
	} else {
		if bisque.isIrodsPathForBisque(newPath) {
			// link
			bisqueUser := bisque.getHomeUser(newPath, user)

			logger.Debugf("turn-in a link bisque request %s, %s", bisqueUser, newPath)

			request := turnin.NewLinkBisqueRequest(bisqueUser, newPath)
			err = bisque.service.turnin.Turnin(request)
			if err != nil {
				logger.WithError(err).Errorf("failed to turn-in a link bisque request - %s, %s", bisqueUser, newPath)
				return
			}
			return
		} else {
			// ignore
			logger.Debugf("ignoring the request since the iRODS path %s and %s are out of iRODS root path %s", oldPath, newPath, bisque.config.IrodsRootPath)
			return
		}
	}
}

func (bisque *BisQue) processAmqpRemoveMessage(msg amqp_mod.Delivery) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "processAmqpRemoveMessage",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("received a message - %s", string(msg.Body))

	msgStruct, err := GetIrodsMsgFromJson(msg.Body)
	if err != nil {
		logger.Error(err)
		return
	}

	user, _, err := GetIrodsMsgUserZone(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	if user == bisque.config.IrodsUsername {
		// raised by irods user via bisque interface
		// we don't need to re-process as it's already processed by bisque.
		logger.Debug("ignoring the request since the request is made by BisQue")
		return
	}

	path, err := GetIrodsMsgPath(msgStruct)
	if err != nil {
		logger.Error(err)
		return
	}

	bisqueUser := bisque.getHomeUser(path, user)

	logger.Debugf("turn-in a remove bisque request %s, %s", bisqueUser, path)

	request := turnin.NewRemoveBisqueRequest(bisqueUser, path)
	err = bisque.service.turnin.Turnin(request)
	if err != nil {
		logger.WithError(err).Errorf("failed to turn-in a remove bisque request - %s, %s", bisqueUser, path)
		return
	}
}

// ProcessItem processes a turn-in request
func (bisque *BisQue) ProcessItem(item turnin.TurnInItem) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "ProcessItem",
	})

	switch item.GetRequestType() {
	case turnin.LinkBisqueRequestType:
		request, ok := item.(*turnin.LinkBisqueRequest)
		if !ok {
			err := fmt.Errorf("failed to convert item to LinkBisqueRequest")
			logger.Error(err)
			return err
		}
		return bisque.ProcessLinkBisqueRequest(request)
	case turnin.RemoveBisqueRequestType:
		request, ok := item.(*turnin.RemoveBisqueRequest)
		if !ok {
			err := fmt.Errorf("failed to convert item to RemoveBisqueRequest")
			logger.Error(err)
			return err
		}
		return bisque.ProcessRemoveBisqueRequest(request)
	case turnin.MoveBisqueRequestType:
		request, ok := item.(*turnin.MoveBisqueRequest)
		if !ok {
			err := fmt.Errorf("failed to convert item to MoveBisqueRequest")
			logger.Error(err)
			return err
		}
		return bisque.ProcessMoveBisqueRequest(request)
	default:
		err := fmt.Errorf("unknown item type %s", item.GetRequestType())
		logger.Error(err)
		return err
	}
}

// ProcessLinkBisqueRequest processes a turn-in link_bisque request, sending a HTTP request
func (bisque *BisQue) ProcessLinkBisqueRequest(request *turnin.LinkBisqueRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "ProcessLinkBisqueRequest",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("trying to send a HTTP request for linking an iRODS object %s", request.IRODSPath)

	if len(request.IRODSPath) == 0 || len(request.IRODSUsername) == 0 {
		err := fmt.Errorf("failed to send a HTTP request for linking an iRODS object %s", request.IRODSPath)
		logger.Error(err)
		return err
	}

	bisqueUrl := bisque.getApiUrl("/blob_service/paths/insert")

	params := map[string]string{
		"user": request.IRODSUsername,
	}

	resourceName, err := bisque.getBisqueResourcePath(request.IRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get resource name for linking an iRODS object %s", request.IRODSPath)
		return err
	}

	irodsPathFromBisque, err := bisque.getIrodsPath(request.IRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get iRODS URL for linking an iRODS object %s", request.IRODSPath)
		return err
	}

	body := fmt.Sprintf("<resource name=\"%s\" permission=\"%s\" value=\"%s\" />", resourceName, BisqueLinkPermissionDefault, irodsPathFromBisque)

	resp, err := bisque.post(bisqueUrl, params, body)
	if err != nil {
		logger.WithError(err).Errorf("failed to send a HTTP request for linking an iRODS object %s to %s (%s in bisque)", request.IRODSPath, irodsPathFromBisque, resourceName)
		return err
	}

	logger.Infof("published a HTTP request for linking an iRODS object %s to %s (%s in bisque)", request.IRODSPath, irodsPathFromBisque, resourceName)

	// process response xml
	resp = strings.TrimSpace(resp)
	xmlDoc, err := xmlquery.Parse(strings.NewReader(resp))
	if err != nil {
		logger.WithError(err).Error("failed to parse xml response")
		return err
	}

	if xmlDoc == nil {
		err = fmt.Errorf("failed to parse xml response")
		logger.Error(err)
		return err
	}

	logger.Infof("received a HTTP response for linking an iRODS object %s to %s - \"%s\"", request.IRODSPath, irodsPathFromBisque, resp)

	rootNode, err := xmlquery.Query(xmlDoc, "node()")
	if err != nil {
		logger.WithError(err).Error("failed to find root node")
		return err
	}

	if rootNode == nil {
		err = fmt.Errorf("failed to find root node")
		logger.Error(err)
		return err
	}

	resourceUniqAttr := rootNode.SelectAttr("resource_uniq")
	if len(resourceUniqAttr) == 0 {
		err = fmt.Errorf("failed to find 'resource_uniq' attribute")
		logger.Error(err)
		return err
	}

	logger.Debugf("setting an iRODS key/val for BisqueID to an iRODS object %s", request.IRODSPath)

	err = bisque.service.irods.SetKeyVal(request.IRODSPath, IRODSKeyValForBisqueID, resourceUniqAttr)
	if err != nil {
		logger.WithError(err).Errorf("failed to set iRODS key/val for BisqueID to an iRODS object %s", request.IRODSPath)
		return err
	}

	logger.Infof("set an iRODS key/val for BisqueID to an iRODS object %s", request.IRODSPath)

	return nil
}

// ProcessRemoveBisqueRequest processes a turn-in remove_bisque request, sending a HTTP request
func (bisque *BisQue) ProcessRemoveBisqueRequest(request *turnin.RemoveBisqueRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "ProcessRemoveBisqueRequest",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("trying to send a HTTP request for removing an iRODS object %s", request.IRODSPath)

	if len(request.IRODSPath) == 0 || len(request.IRODSUsername) == 0 {
		err := fmt.Errorf("failed to send a HTTP request for removing an iRODS object %s", request.IRODSPath)
		logger.Error(err)
		return err
	}

	bisqueUrl := bisque.getApiUrl("/blob_service/paths/remove")

	irodsPathFromBisque, err := bisque.getIrodsPath(request.IRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get iRODS URL for removing an iRODS object %s", request.IRODSPath)
		return err
	}

	params := map[string]string{
		"path": irodsPathFromBisque,
	}

	_, err = bisque.get(bisqueUrl, params)
	if err != nil {
		logger.WithError(err).Errorf("failed to send a HTTP request for removing an iRODS object %s", request.IRODSPath)
		return err
	}

	logger.Infof("published a HTTP request for removing an iRODS object %s (bisque path: %s)", request.IRODSPath, irodsPathFromBisque)

	return nil
}

// ProcessMoveBisqueRequest processes a turn-in move_bisque request, sending a HTTP request
func (bisque *BisQue) ProcessMoveBisqueRequest(request *turnin.MoveBisqueRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "ProcessMoveBisqueRequest",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Debugf("trying to send a HTTP request for moving an iRODS object %s to %s", request.SourceIRODSPath, request.DestIRODSPath)

	if len(request.SourceIRODSPath) == 0 || len(request.DestIRODSPath) == 0 || len(request.IRODSUsername) == 0 {
		err := fmt.Errorf("failed to send a HTTP request for moving an iRODS object %s to %s", request.SourceIRODSPath, request.DestIRODSPath)
		logger.Error(err)
		return err
	}

	bisqueUrl := bisque.getApiUrl("/blob_service/paths/move")

	sourceIrodsPathFromBisque, err := bisque.getIrodsPath(request.SourceIRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get iRODS URL for moving an iRODS object %s", request.SourceIRODSPath)
		return err
	}

	destIrodsPathFromBisque, err := bisque.getIrodsPath(request.DestIRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get iRODS URL for moving an iRODS object %s", request.DestIRODSPath)
		return err
	}

	params := map[string]string{
		"path":        sourceIrodsPathFromBisque,
		"destination": destIrodsPathFromBisque,
	}

	_, err = bisque.get(bisqueUrl, params)
	if err != nil {
		logger.WithError(err).Errorf("failed to send a HTTP request for moving an iRODS object %s", request.SourceIRODSPath)
		return err
	}

	logger.Infof("published a HTTP request for moving an iRODS object %s (bisque path: %s) to %s (bisque path: %s", request.SourceIRODSPath, &sourceIrodsPathFromBisque, request.DestIRODSPath, destIrodsPathFromBisque)

	return nil
}

func (bisque *BisQue) get(url string, params map[string]string) (string, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "get",
	})

	defer commons.StackTraceFromPanic(logger)

	req, err := http.NewRequestWithContext(bisque.context, http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	// add params
	query := req.URL.Query()
	for paramKey, paramVal := range params {
		query.Add(paramKey, paramVal)
	}
	req.URL.RawQuery = query.Encode()

	// basic-auth
	req.SetBasicAuth(bisque.config.AdminUsername, bisque.config.AdminPassword)

	resp, err := bisque.client.Do(req)
	if err != nil {
		return "", err
	}

	// read body
	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	resp.Body.Close()

	// check if status is ok
	if resp.StatusCode != http.StatusOK {
		// error
		return "", fmt.Errorf("BisQue responded an error %s (%d) - %s", resp.Status, resp.StatusCode, string(resBody))
	}

	// success, return body
	return string(resBody), nil
}

func (bisque *BisQue) post(url string, params map[string]string, body string) (string, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "post",
	})

	defer commons.StackTraceFromPanic(logger)

	req, err := http.NewRequestWithContext(bisque.context, http.MethodPost, url, nil)
	if err != nil {
		return "", err
	}

	// add params
	query := req.URL.Query()
	for paramKey, paramVal := range params {
		query.Add(paramKey, paramVal)
	}
	req.URL.RawQuery = query.Encode()

	// basic-auth
	req.SetBasicAuth(bisque.config.AdminUsername, bisque.config.AdminPassword)
	req.Header.Add("content-type", "application/xml")

	req.Body = io.NopCloser(strings.NewReader(body))
	resp, err := bisque.client.Do(req)
	if err != nil {
		return "", err
	}

	// read body
	resBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	resp.Body.Close()

	// check if status is ok
	if resp.StatusCode != http.StatusOK {
		// error
		return "", fmt.Errorf("BisQue responded an error %s (%d) - %s", resp.Status, resp.StatusCode, string(resBody))
	}

	// success, return body
	return string(resBody), nil
}

func (bisque *BisQue) getApiUrl(path string) string {
	return fmt.Sprintf("%s/%s", strings.TrimRight(bisque.config.URL, "/"), strings.TrimLeft(path, "/"))
}

func (bisque *BisQue) getIrodsPath(irodsPath string) (string, error) {
	base := fmt.Sprintf("%s/", strings.TrimRight(bisque.config.IrodsRootPath, "/"))
	if !strings.HasPrefix(irodsPath, base) {
		return "", fmt.Errorf("iRODS Path %s is not under iRODS root path %s", irodsPath, bisque.config.IrodsRootPath)
	}

	rel := irodsPath[len(base):]

	return fmt.Sprintf("%s/%s", strings.TrimRight(bisque.config.IrodsBaseURL, "/"), strings.TrimLeft(rel, "/")), nil
}

func (bisque *BisQue) getBisqueResourcePath(irodsPath string) (string, error) {
	base := fmt.Sprintf("%s/", strings.TrimRight(bisque.config.IrodsRootPath, "/"))
	if !strings.HasPrefix(irodsPath, base) {
		return "", fmt.Errorf("iRODS Path %s is not under iRODS root path %s", irodsPath, bisque.config.IrodsRootPath)
	}

	rel := irodsPath[len(base):]

	// remove username dir in front
	rel = strings.TrimLeft(rel, "/")

	relSplit := strings.Split(rel, "/")
	if len(relSplit) > 1 {
		rel = strings.Join(relSplit[1:], "/")
	}

	// do not add "/" in front
	return strings.TrimLeft(rel, "/"), nil
}

func (bisque *BisQue) isIrodsPathForBisque(irodsPath string) bool {
	base := fmt.Sprintf("%s/", strings.TrimRight(bisque.config.IrodsRootPath, "/"))
	return strings.HasPrefix(irodsPath, base)
}

func (bisque *BisQue) getHomeUser(irodsPath string, defaultUser string) string {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "getHomeUser",
	})

	defer commons.StackTraceFromPanic(logger)

	homePrefix := fmt.Sprintf("/%s/home/", bisque.config.IrodsZone)
	trashHomePrefix := fmt.Sprintf("/%s/trash/home/", bisque.config.IrodsZone)
	if strings.HasPrefix(irodsPath, trashHomePrefix) {
		// starts with /zone/trash/home/
		rest := irodsPath[len(trashHomePrefix):]
		if len(rest) > 0 {
			paths := strings.Split(rest, "/")
			return paths[0]
		}
	} else if strings.HasPrefix(irodsPath, homePrefix) {
		// starts with /zone/home/
		rest := irodsPath[len(homePrefix):]
		if len(rest) > 0 {
			paths := strings.Split(rest, "/")
			return paths[0]
		}
	}

	return defaultUser
}
