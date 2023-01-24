package service

import (
	"fmt"
	"sync"
	"time"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/turnin"
	log "github.com/sirupsen/logrus"
)

const (
	ScrapeInterval = 3 * time.Second
)

// AsyncExecCmdService is a service object
type AsyncExecCmdService struct {
	config *commons.ServerConfig
	turnin *turnin.TurnIn

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
		turnin: turnin.NewTurnIn(config.GetTurnInRootDirPath()),

		terminateChan: make(chan bool),
	}

	irods, err := CreateIrods(service, &config.IrodsConfig)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.irods = irods

	var amqpEventHandler AmqpEventHandler

	if len(config.BisqueConfig.URL) > 0 {
		bisque, err := CreateBisque(service, &config.BisqueConfig)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		service.bisque = bisque
		amqpEventHandler = bisque.HandleAmqpEvent
	}

	amqp, err := CreateAmqp(service, &config.AmqpConfig, amqpEventHandler)
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	service.amqp = amqp

	err = service.turnin.MakeTurnInDir()
	if err != nil {
		logger.Error(err)
		return nil, err
	}

	return service, nil
}

// Release releases the service
func (svc *AsyncExecCmdService) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Release",
	})

	logger.Info("Releasing the Async Exec Cmd Service")

	defer commons.StackTraceFromPanic(logger)

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
}

// Scrape scrape turn-ins
func (svc *AsyncExecCmdService) Scrape() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "Scrape",
	})

	// do not uncomment this for release
	//logger.Debugf("checking turn-ins at %s", svc.config.GetTurnInRootDirPath())
	items, err := svc.turnin.Scrape()
	if err != nil {
		logger.Error(err)
		// continue
	}

	if len(items) > 0 {
		logger.Debugf("found %d turn-ins at %s", len(items), svc.config.GetTurnInRootDirPath())

		messageChan := make(chan turnin.TurnInItem)
		bisqueChan := make(chan turnin.TurnInItem)

		wg := sync.WaitGroup{}
		wg.Add(2)

		// we create two goroutines to handle them separately in parallel
		go func() {
			for item := range messageChan {
				if !svc.ProcessItem(item) {
					break
					// ignore all items in the messageChan
					// to be processed in the next iteration
				}
			}
			wg.Done()
		}()

		go func() {
			for item := range bisqueChan {
				if !svc.ProcessItem(item) {
					break
					// ignore all items in the bisqueChan
					// to be processed in the next iteration
				}
			}
			wg.Done()
		}()

		for _, item := range items {
			if turnin.IsItemTypeSendMessage(item) {
				logger.Debug("sending a turn-in to send_message queue")
				messageChan <- item
			} else if turnin.IsItemTypeBisque(item) {
				logger.Debug("sending a turn-in to bisque queue")
				bisqueChan <- item
			} else {
				logger.Debug("unknown turn-in found, skip")
			}
		}

		close(messageChan)
		close(bisqueChan)

		wg.Wait()
	}
}

func (svc *AsyncExecCmdService) ProcessItem(item turnin.TurnInItem) bool {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "ProcessItem",
	})

	logger.Debug("Processing a turn-in item")
	err := svc.distributeItem(item)
	if err != nil {
		if IsServiceNotReadyError(err) {
			logger.WithError(err).Errorf("service is not ready. will retry next time. pending turn-in %s", item.GetRequestType())
			// do not mark failed
			// will retry at next iteration
			// stop
			return false
		} else {
			logger.WithError(err).Errorf("failed to process an item turned-in %s", item.GetRequestType())
			svc.turnin.MarkFailed(item)
		}
	} else {
		logger.Debugf("Processed an item turned-in")

		if len(item.GetItemFilePath()) > 0 {
			// processed -> delete file
			err = svc.turnin.MarkSuccess(item)
			if err != nil {
				logger.WithError(err).Errorf("failed to mark an item turned-in %s success", item.GetRequestType())
			}
		}
	}

	return true
}

func (svc *AsyncExecCmdService) distributeItem(item turnin.TurnInItem) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AsyncExecCmdService",
		"function": "distributeItem",
	})

	switch item.GetRequestType() {
	case turnin.SendMessageRequestType:
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
	case turnin.LinkBisqueRequestType, turnin.RemoveBisqueRequestType, turnin.MoveBisqueRequestType:
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
