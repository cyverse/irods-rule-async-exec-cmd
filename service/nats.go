package service

import (
	"fmt"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	"github.com/nats-io/stan.go"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
)

type NATS struct {
	service    *AsyncExecCmdService
	config     *commons.NatsConfig
	connection stan.Conn
}

// CreateNats creates a NATS service object and connects to NATS/STAN
func CreateNats(service *AsyncExecCmdService, config *commons.NatsConfig) (*NATS, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateNats",
	})

	logger.Debugf("connecting to NATS %s", config.URL)

	clientID := fmt.Sprintf("%s%s", config.ClientIDPrefix, xid.New().String())
	sc, err := stan.Connect(config.ClusterID, clientID, stan.NatsURL(config.URL))
	if err != nil {
		logger.WithError(err).Errorf("failed to connect to %s", config.URL)
		return nil, err
	}

	return &NATS{
		service:    service,
		config:     config,
		connection: sc,
	}, nil
}

// Release releases all resources, disconnecting from NATS/STAN
func (nats *NATS) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "NATS",
		"function": "Release",
	})

	logger.Debugf("trying to disconnect from %s", nats.config.URL)

	if nats.connection != nil {
		nats.connection.Close()
		nats.connection = nil
	}
}

// ProcessItem processes a drop-in send_message request, publishing a NATS message
func (nats *NATS) ProcessItem(request *dropin.SendMessageRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "NATS",
		"function": "ProcessItem",
	})

	logger.Debugf("trying to publish a NATS message with a subject %s", request.Key)

	if len(request.Key) == 0 {
		err := fmt.Errorf("failed to send a NATS message due to an empty key")
		logger.Error(err)
		return err
	}

	err := nats.connection.Publish(request.Key, []byte(request.Body))
	if err != nil {
		logger.WithError(err).Errorf("failed to send a NATS message with a subject %s", request.Key)
		return err
	}

	logger.Debugf("published a NATS message with a subject %s", request.Key)
	return nil
}
