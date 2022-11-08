package service

import (
	"fmt"
	"time"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
)

const (
	ScrapeInterval = 3 * time.Second
)

// AsyncExecCmdService is a service object
type AsyncExecCmdService struct {
	config *commons.ServerConfig
	dropin *dropin.DropIn

	bisque *BisQue
	amqp   *AMQP

	irods *IRODS

	terminateChan chan bool
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

		terminateChan: make(chan bool),
	}

	irods, err := CreateIrods(service, &config.IrodsConfig)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.irods = irods

	bisque, err := CreateBisque(service, &config.BisqueConfig)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.bisque = bisque

	amqp, err := CreateAmqp(service, &config.AmqpConfig, bisque.HandleAmqpEvent)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.amqp = amqp

	err = service.dropin.MakeDropInDir()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	logger.Info("Starting the Async Exec Cmd Service")

	go func() {
		scrapeTicker := time.NewTicker(ScrapeInterval)
		defer scrapeTicker.Stop()

		for {
			select {
			case <-service.terminateChan:
				// terminate
				return
			case <-scrapeTicker.C:
				service.Scrape()
			}
		}
	}()

	return service, nil
}

// Stop stops the service
func (svc *AsyncExecCmdService) Stop() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Stop",
	})

	logger.Info("Stopping the Async Exec Cmd Service")

	svc.terminateChan <- true

	if svc.amqp != nil {
		svc.amqp.Release()
		svc.amqp = nil
	}

	if svc.bisque != nil {
		svc.bisque.Release()
		svc.bisque = nil
	}

	if svc.irods != nil {
		svc.irods.Release()
		svc.irods = nil
	}

	return nil
}

// Scrape scrape dropins
func (svc *AsyncExecCmdService) Scrape() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Scrape",
	})

	//logger.Debugf("checking drop-ins in %s", svc.config.DropInDirPath)
	items, err := svc.dropin.Scrape()
	if err != nil {
		logger.Error(err)
		// continue
	}

	if len(items) > 0 {
		logger.Debugf("found %d drop-ins in %s", len(items), svc.config.DropInDirPath)

		for itemIdx, item := range items {
			logger.Debugf("Processing a drop-in item %d", itemIdx)
			err = svc.ProcessItem(item)
			if err != nil {
				logger.WithError(err).Errorf("failed to process drop-in %s", item.GetRequestType())
				svc.dropin.MarkFailed(item)
			} else {
				logger.Debugf("Processed a drop-in item %d", itemIdx)

				if len(item.GetItemFilePath()) > 0 {
					// processed -> delete file
					err = svc.dropin.MarkSuccess(item)
					if err != nil {
						logger.WithError(err).Errorf("failed to mark drop-in %s success", item.GetRequestType())
					}
				}
			}
		}
	}
}

func (svc *AsyncExecCmdService) ProcessItem(item dropin.DropInItem) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "ProcessItem",
	})

	switch item.GetRequestType() {
	case dropin.SendMessageRequestType:
		processed := false
		if svc.amqp != nil {
			logger.Debug("Processing an AMQP request")
			err := svc.amqp.ProcessItem(item)
			if err != nil {
				logger.Error(err)
				return err
			}

			processed = true
		}

		if !processed {
			return fmt.Errorf("failed to process send_message request because AMQP is not configured")
		}
	case dropin.LinkBisqueRequestType, dropin.RemoveBisqueRequestType, dropin.MoveBisqueRequestType:
		processed := false
		if svc.bisque != nil {
			logger.Debug("Processing a BisQue request")
			err := svc.bisque.ProcessItem(item)
			if err != nil {
				logger.Error(err)
				return err
			}

			processed = true
		}

		if !processed {
			return fmt.Errorf("failed to process bisque request because BisQue is not configured")
		}
	default:
		return fmt.Errorf("failed to process unknown request %s", item.GetRequestType())
	}

	return nil
}
