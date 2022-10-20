package service

import (
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
)

// AsyncExecCmdService is a service object
type AsyncExecCmdService struct {
	config *commons.ServerConfig
	dropin *dropin.DropIn
}

// Start starts a new async exec cmd service
func Start(config *commons.ServerConfig) (*AsyncExecCmdService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "Start",
	})

	service := &AsyncExecCmdService{
		config: config,
		dropin: dropin.NewDropIn(config.DropInDirPath),
	}

	err := service.dropin.MakeDropInDir()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	logger.Info("Starting the Async Exec Cmd Service")

	return service, nil
}

// Destroy destroys the service
func (svc *AsyncExecCmdService) Stop() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Stop",
	})

	logger.Info("Stopping the Async Exec Cmd Service")

	return nil
}
