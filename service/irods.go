package service

import (
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	log "github.com/sirupsen/logrus"
)

type IRODS struct {
	service *AsyncExecCmdService
	config  *commons.IrodsConfig
}

// CreateIrods creates an iRODS service object and connects to iRODS
func CreateIrods(service *AsyncExecCmdService, config *commons.IrodsConfig) (*IRODS, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateIrods",
	})

	logger.Debugf("connecting to iRODS %s:%d", config.Host, config.Port)

	return &IRODS{
		service: service,
		config:  config,
	}, nil
}

// Release releases all resources, disconnecting from IRODS
func (irods *IRODS) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "Release",
	})

	logger.Debugf("trying to disconnect from %s:%d", irods.config.Host, irods.config.Port)

}

// SetKeyVal sets a new key val to a data object/collection
func (irods *IRODS) SetKeyVal(irodsPath string, key string, val string) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "SetKeyVal",
	})

	logger.Debugf("trying to set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)

	logger.Debugf("set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)
	return nil
}
