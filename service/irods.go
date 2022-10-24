package service

import (
	"fmt"

	irods_fs "github.com/cyverse/go-irodsclient/fs"
	irods_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"

	log "github.com/sirupsen/logrus"
)

const (
	irodsClientName string = "irods-rule-async-exec-cmd"
)

type IRODS struct {
	service  *AsyncExecCmdService
	config   *commons.IrodsConfig
	fsClient *irods_fs.FileSystem
}

// CreateIrods creates an iRODS service object and connects to iRODS
func CreateIrods(service *AsyncExecCmdService, config *commons.IrodsConfig) (*IRODS, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateIrods",
	})

	logger.Debugf("connecting to iRODS host %s:%d, zone %s, user %s", config.Host, config.Port, config.Zone, config.AdminUsername)

	account, err := irods_types.CreateIRODSAccount(config.Host, config.Port, config.AdminUsername, config.Zone, irods_types.AuthSchemeNative, config.AdminPassword, "")
	if err != nil {
		logger.WithError(err).Errorf("failed to create an iRODS account for host %s:%d, zone %s, user %s", config.Host, config.Port, config.Zone, config.AdminUsername)
		return nil, err
	}

	fs, err := irods_fs.NewFileSystemWithDefault(account, irodsClientName)
	if err != nil {
		logger.WithError(err).Errorf("failed to create an iRODS FileSystem Client for iRODS host %s:%d, zone %s, user %s", config.Host, config.Port, config.Zone, config.AdminUsername)
		return nil, err
	}

	return &IRODS{
		service:  service,
		config:   config,
		fsClient: fs,
	}, nil
}

// Release releases all resources, disconnecting from IRODS
func (irods *IRODS) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "Release",
	})

	logger.Debugf("trying to release the iRODS FileSystem Client for %s:%d", irods.config.Host, irods.config.Port)

	if irods.fsClient != nil {
		irods.fsClient.Release()
		irods.fsClient = nil
	}
}

// SetKeyVal sets a new key val to a data object/collection
func (irods *IRODS) SetKeyVal(irodsPath string, key string, val string) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "SetKeyVal",
	})

	logger.Debugf("trying to set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)

	entry, err := irods.fsClient.Stat(irodsPath)
	if err != nil {
		logger.WithError(err).Errorf("failed to find an iRODS collection/data-object %s", irodsPath)
		return err
	}

	if entry.ID == 0 {
		err = fmt.Errorf("failed to find an iRODS collection/data-object %s", irodsPath)
		logger.Error(err)
		return err
	}

	err = irods.fsClient.AddMetadata(irodsPath, key, val, "")
	if err != nil {
		logger.WithError(err).Errorf("failed to set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)
		return err
	}

	logger.Debugf("set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)
	return nil
}
