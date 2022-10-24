package service

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"path"
	"path/filepath"
	"strings"

	"github.com/antchfx/xmlquery"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
)

const (
	IRODSKeyValForBisqueID string = "ipc-bisque-id"
)

type BisQue struct {
	service *AsyncExecCmdService
	config  *commons.BisqueConfig
	context context.Context
	client  *http.Client
}

// CreateBisque creates a BisQue service object
func CreateBisque(service *AsyncExecCmdService, config *commons.BisqueConfig) (*BisQue, error) {
	context := context.Background()

	client := &http.Client{}

	return &BisQue{
		service: service,
		config:  config,
		context: context,
		client:  client,
	}, nil
}

// Release releases all resources
func (bisque *BisQue) Release() {
}

// ProcessItem processes a drop-in link_bisque request, sending a HTTP request
func (bisque *BisQue) ProcessItem(request *dropin.LinkBisqueRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "BisQue",
		"function": "ProcessItem",
	})

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

	resourceName := path.Base(request.IRODSPath)
	irodsPathFromBisque, err := bisque.getIrodsURL(request.IRODSPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to get iRODS URL for linking an iRODS object %s", request.IRODSPath)
		return err
	}

	body := fmt.Sprintf("<resource name=\"%s\" permission=\"published\" value=\"%s\" />", resourceName, irodsPathFromBisque)

	resp, err := bisque.post(bisqueUrl, params, body)
	if err != nil {
		logger.WithError(err).Errorf("failed to send a HTTP request for linking an iRODS object %s", request.IRODSPath)
		return err
	}

	logger.Debugf("published a HTTP request for linking an iRODS object %s", request.IRODSPath)

	// process response xml
	resp = strings.TrimSpace(resp)
	xmlDoc, err := xmlquery.Parse(strings.NewReader(resp))
	if err != nil {
		logger.WithError(err).Error("failed to parse xml response")
		return err
	}

	resourceNode, err := xmlquery.Query(xmlDoc, "//resource")
	if err != nil {
		logger.WithError(err).Error("failed to find 'resource' tag")
		return err
	}

	resourceUniqAttr := resourceNode.SelectAttr("resource_uniq")
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

	logger.Debugf("set an iRODS key/val for BisqueID to an iRODS object %s", request.IRODSPath)

	return nil
}

func (bisque *BisQue) post(url string, params map[string]string, body string) (string, error) {
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

	req.Body = ioutil.NopCloser(strings.NewReader(body))
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

func (bisque *BisQue) getIrodsURL(irodsPath string) (string, error) {
	rel, err := filepath.Rel(bisque.config.IrodsMountPath, irodsPath)
	if err != nil {
		return "", err
	}

	if strings.HasPrefix(rel, "./") || strings.HasPrefix(rel, "../") {
		return "", fmt.Errorf("iRODS Path %s is not under mount path %s", irodsPath, bisque.config.IrodsMountPath)
	}

	return fmt.Sprintf("%s/%s", strings.TrimRight(bisque.config.IrodsBaseURL, "/"), strings.TrimLeft(rel, "/")), nil
}
