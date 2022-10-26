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
	ScrapeInterval = 1 * time.Second
)

// AsyncExecCmdService is a service object
type AsyncExecCmdService struct {
	config *commons.ServerConfig
	dropin *dropin.DropIn

	bisque *BisQue
	amqp   *AMQP
	nats   *NATS

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

	if config.IsNATS() {
		nats, err := CreateNats(service, &config.NatsConfig)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		service.nats = nats
	}

	if config.IsAMQP() {
		amqp, err := CreateAmqp(service, &config.AmqpConfig)
		if err != nil {
			logger.Error(err)
			return nil, err
		}

		service.amqp = amqp
	}

	err := service.dropin.MakeDropInDir()
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
	}

	wg := sync.WaitGroup{}
	for _, item := range items {
		// process items async.
		wg.Add(1)

		go func(item dropin.DropInItem, wg *sync.WaitGroup) {
			defer wg.Done()

			err = svc.ProcessItem(item)
			if err != nil {
				logger.WithError(err).Errorf("failed to process drop-in %s", item.GetRequestType())
				return
			}

			if len(item.GetItemFilePath()) > 0 {
				// processed -> delete file
				err = item.DeleteItemFile()
				if err != nil {
					logger.WithError(err).Errorf("failed to delete drop-in %s", item.GetRequestType())
					return
				}
			}
		}(item, &wg)
	}

	wg.Wait()
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
			if request, ok := item.(*dropin.SendMessageRequest); ok {
				err := svc.amqp.ProcessItem(request)
				if err != nil {
					logger.Error(err)
					return err
				}

				processed = true
			}
		}

		if svc.nats != nil {
			if request, ok := item.(*dropin.SendMessageRequest); ok {
				err := svc.nats.ProcessItem(request)
				if err != nil {
					logger.Error(err)
					return err
				}

				processed = true
			}
		}

		if !processed {
			return fmt.Errorf("failed to process send_message request because neither AMQP nor NATS are configured")
		}
	case dropin.LinkBisqueRequestType:
		processed := false
		if svc.bisque != nil {
			if request, ok := item.(*dropin.LinkBisqueRequest); ok {
				err := svc.bisque.ProcessItem(request)
				if err != nil {
					logger.Error(err)
					return err
				}

				processed = true
			}
		}

		if !processed {
			return fmt.Errorf("failed to process link_bisque request because BisQue is not configured")
		}
	default:
		return fmt.Errorf("failed to process unknown request %s", item.GetRequestType())
	}

	return nil
}
