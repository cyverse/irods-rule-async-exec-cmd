package service

import (
	"fmt"
	"time"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
	amqp_mod "github.com/streadway/amqp"
)

type AMQP struct {
	service    *AsyncExecCmdService
	config     *commons.AmqpConfig
	connection *amqp_mod.Connection
	channel    *amqp_mod.Channel
}

// CreateAmqp creates a AMQP service object and connects to AMQP
func CreateAmqp(service *AsyncExecCmdService, config *commons.AmqpConfig) (*AMQP, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateAmqp",
	})

	logger.Infof("connecting to AMQP %s", config.URL)

	ac, err := amqp_mod.Dial(config.URL)
	if err != nil {
		logger.WithError(err).Errorf("failed to connect to %s", config.URL)
		return nil, err
	}

	channel, err := ac.Channel()
	if err != nil {
		logger.WithError(err).Error("failed to open a channel")
		return nil, err
	}

	return &AMQP{
		service:    service,
		config:     config,
		connection: ac,
		channel:    channel,
	}, nil
}

// Release releases all resources, disconnecting from AMQP
func (amqp *AMQP) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "Release",
	})

	logger.Infof("trying to disconnect from %s", amqp.config.URL)

	if amqp.channel != nil {
		amqp.channel.Close()
		amqp.channel = nil
	}

	if amqp.connection != nil {
		if !amqp.connection.IsClosed() {
			amqp.connection.Close()
		}
		amqp.connection = nil
	}
}

// ProcessItem processes a drop-in send_message request, publishing a AMQP message
func (amqp *AMQP) ProcessItem(request *dropin.SendMessageRequest) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "ProcessItem",
	})

	logger.Debugf("trying to publish an AMQP message with a subject %s", request.Key)

	if len(request.Key) == 0 {
		err := fmt.Errorf("failed to send an AMQP message due to an empty key")
		logger.Error(err)
		return err
	}

	msg := amqp_mod.Publishing{
		DeliveryMode: amqp_mod.Persistent,
		Timestamp:    time.Now(),
		ContentType:  "text/plain",
		Body:         []byte(request.Body),
	}

	err := amqp.channel.Publish(amqp.config.Exchange, request.Key, false, false, msg)
	if err != nil {
		logger.WithError(err).Errorf("failed to send an AMQP message with a subject %s", request.Key)
		return err
	}

	logger.Infof("published an AMQP message with a subject %s", request.Key)
	return nil
}
