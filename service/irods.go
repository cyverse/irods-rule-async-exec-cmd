package service

import (
	"fmt"
	"sync"
	"time"

	irods_fs "github.com/cyverse/go-irodsclient/fs"
	irods_types "github.com/cyverse/go-irodsclient/irods/types"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"

	log "github.com/sirupsen/logrus"
)

const (
	irodsClientName string = "irods-rule-async-exec-cmd"
)

type IRODS struct {
	service              *AsyncExecCmdService
	config               *commons.IrodsConfig
	fsClient             *irods_fs.FileSystem
	lastConnectTrialTime time.Time
	connectionLock       sync.Mutex
}

// CreateIrods creates an iRODS service object and connects to iRODS
func CreateIrods(service *AsyncExecCmdService, config *commons.IrodsConfig) (*IRODS, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateIrods",
	})

	defer commons.StackTraceFromPanic(logger)

	// lazy connect
	irods := &IRODS{
		service:              service,
		config:               config,
		lastConnectTrialTime: time.Time{},
		connectionLock:       sync.Mutex{},
	}

	err := irods.ensureConnected()
	if err != nil {
		logger.WithError(err).Warn("will retry again")
		// ignore error
	}

	return irods, nil
}

func (irods *IRODS) ensureConnected() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "ensureConnected",
	})

	defer commons.StackTraceFromPanic(logger)

	irods.connectionLock.Lock()
	defer irods.connectionLock.Unlock()

	if irods.fsClient == nil {
		if time.Now().After(irods.lastConnectTrialTime.Add(commons.ReconnectInterval)) {
			// passed reconnect interval
			return irods.connect()
		} else {
			// too early to reconnect
			return fmt.Errorf("ignore reconnect request. will try after %f seconds from last trial", commons.ReconnectInterval.Seconds())
		}
	}

	return nil
}

func (irods *IRODS) connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "connect",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("connecting to iRODS host %s:%d, zone %s, user %s", irods.config.Host, irods.config.Port, irods.config.Zone, irods.config.AdminUsername)

	irods.lastConnectTrialTime = time.Now()
	irods.fsClient = nil

	account, err := irods_types.CreateIRODSAccount(irods.config.Host, irods.config.Port, irods.config.AdminUsername, irods.config.Zone, irods_types.AuthSchemeNative, irods.config.AdminPassword, "")
	if err != nil {
		logger.WithError(err).Errorf("failed to create an iRODS account for host %s:%d, zone %s, user %s", irods.config.Host, irods.config.Port, irods.config.Zone, irods.config.AdminUsername)
		return err
	}

	fs, err := irods_fs.NewFileSystemWithDefault(account, irodsClientName)
	if err != nil {
		logger.WithError(err).Errorf("failed to create an iRODS FileSystem Client for iRODS host %s:%d, zone %s, user %s", irods.config.Host, irods.config.Port, irods.config.Zone, irods.config.AdminUsername)
		return err
	}

	irods.fsClient = fs
	return nil
}

// Release releases all resources, disconnecting from IRODS
func (irods *IRODS) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "IRODS",
		"function": "Release",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("trying to release the iRODS FileSystem Client for %s:%d", irods.config.Host, irods.config.Port)

	irods.connectionLock.Lock()
	defer irods.connectionLock.Unlock()

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

	defer commons.StackTraceFromPanic(logger)

	err := irods.ensureConnected()
	if err != nil {
		logger.Error(err)
		return err
	}

	irods.connectionLock.Lock()
	defer irods.connectionLock.Unlock()

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

	logger.Infof("set a key/val to an iRODS collection/data-object %s, key: %s", irodsPath, key)
	return nil
}
