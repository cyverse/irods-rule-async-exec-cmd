package service

import (
	"fmt"
	"sync"
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

// NewService creates a new Service
func NewService(config *commons.ServerConfig) (*AsyncExecCmdService, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "Start",
	})

	defer commons.StackTraceFromPanic(logger)

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

	return service, nil
}

// Start starts the service
func (svc *AsyncExecCmdService) Start() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Start",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Info("Starting the Async Exec Cmd Service")

	go func() {
		scrapeTicker := time.NewTicker(ScrapeInterval)
		defer scrapeTicker.Stop()

		for {
			select {
			case <-svc.terminateChan:
				// terminate
				return
			case <-scrapeTicker.C:
				svc.Scrape()
			}
		}
	}()

	return nil
}

// Stop stops the service
func (svc *AsyncExecCmdService) Stop() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Stop",
	})

	logger.Info("Stopping the Async Exec Cmd Service")

	defer commons.StackTraceFromPanic(logger)

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

		messageChan := make(chan dropin.DropInItem)
		bisqueChan := make(chan dropin.DropInItem)

		wg := sync.WaitGroup{}
		wg.Add(2)

		// we create two goroutines to handle them separately in parallel
		go func() {
			for item := range messageChan {
				svc.ProcessItem(item)
			}
			wg.Done()
		}()

		go func() {
			for item := range bisqueChan {
				svc.ProcessItem(item)
			}
			wg.Done()
		}()

		for _, item := range items {
			if dropin.IsItemTypeSendMessage(item) {
				logger.Debug("sending a drop-in to send_message queue")
				messageChan <- item
			} else if dropin.IsItemTypeBisque(item) {
				logger.Debug("sending a drop-in to bisque queue")
				bisqueChan <- item
			} else {
				logger.Debug("unknown drop-in found, skip")
			}
		}

		close(messageChan)
		close(bisqueChan)

		wg.Wait()
	}
}

func (svc *AsyncExecCmdService) ProcessItem(item dropin.DropInItem) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "ProcessItem",
	})

	logger.Debug("Processing a drop-in item")
	err := svc.distributeItem(item)
	if err != nil {
		logger.WithError(err).Errorf("failed to process drop-in %s", item.GetRequestType())
		svc.dropin.MarkFailed(item)
	} else {
		logger.Debugf("Processed a drop-in item")

		if len(item.GetItemFilePath()) > 0 {
			// processed -> delete file
			err = svc.dropin.MarkSuccess(item)
			if err != nil {
				logger.WithError(err).Errorf("failed to mark drop-in %s success", item.GetRequestType())
			}
		}
	}
}

func (svc *AsyncExecCmdService) distributeItem(item dropin.DropInItem) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "distributeItem",
	})

	switch item.GetRequestType() {
	case dropin.SendMessageRequestType:
		processed := false
		if svc.amqp != nil {
			logger.Debug("Sending an AMQP request")
			err := svc.amqp.ProcessItem(item)
			if err != nil {
				logger.Error(err)
				return err
			}

			processed = true
		}

		if !processed {
			return fmt.Errorf("failed to send a send_message request because AMQP is not configured")
		}
	case dropin.LinkBisqueRequestType, dropin.RemoveBisqueRequestType, dropin.MoveBisqueRequestType:
		processed := false
		if svc.bisque != nil {
			logger.Debug("Sending a BisQue request")
			err := svc.bisque.ProcessItem(item)
			if err != nil {
				logger.Error(err)
				return err
			}

			processed = true
		}

		if !processed {
			return fmt.Errorf("failed to send a bisque request because BisQue is not configured")
		}
	default:
		return fmt.Errorf("failed to distribute an unknown request %s", item.GetRequestType())
	}

	return nil
}
