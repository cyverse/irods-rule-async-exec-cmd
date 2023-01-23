package service

import (
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/turnin"
	"github.com/rs/xid"
	log "github.com/sirupsen/logrus"
	amqp_mod "github.com/streadway/amqp"
)

const (
	AMQPConsumerQueueName string        = "irods_rule_async_exec_cmd"
	AMQPConsumeInterval   time.Duration = 1 * time.Second
)

type AmqpEventHandler func(msg amqp_mod.Delivery)

type AMQP struct {
	service              *AsyncExecCmdService
	config               *commons.AmqpConfig
	connection           *amqp_mod.Connection
	channel              *amqp_mod.Channel
	queue                *amqp_mod.Queue
	lastConnectTrialTime time.Time
	connectionLock       sync.Mutex
	eventHandler         AmqpEventHandler
}

// CreateAmqp creates a AMQP service object and connects to AMQP
func CreateAmqp(service *AsyncExecCmdService, config *commons.AmqpConfig, hander AmqpEventHandler) (*AMQP, error) {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"function": "CreateAmqp",
	})

	defer commons.StackTraceFromPanic(logger)

	// lazy connect
	amqp := &AMQP{
		service:              service,
		config:               config,
		lastConnectTrialTime: time.Time{},
		connectionLock:       sync.Mutex{},
		eventHandler:         hander,
	}

	err := amqp.ensureConnected()
	if err != nil {
		logger.WithError(err).Warn("will retry again")
		// ignore error
	}

	return amqp, nil
}

func (amqp *AMQP) ensureConnected() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "ensureConnected",
	})

	defer commons.StackTraceFromPanic(logger)

	amqp.connectionLock.Lock()
	defer amqp.connectionLock.Unlock()

	if amqp.connection != nil {
		if amqp.connection.IsClosed() {
			// clear
			amqp.connection = nil
			amqp.channel = nil
			amqp.queue = nil
		}
	}

	if amqp.connection == nil || amqp.channel == nil || amqp.queue == nil {
		// disconnected - try to connect
		if time.Now().After(amqp.lastConnectTrialTime.Add(commons.ReconnectInterval)) {
			// passed reconnect interval
			return amqp.connect()
		} else {
			// too early to reconnect
			return NewServiceNotReadyErrorf("ignore reconnect request. will try after %f seconds from last trial", commons.ReconnectInterval.Seconds())
		}
	}

	return nil
}

func (amqp *AMQP) connect() error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "connect",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("connecting to AMQP %s", amqp.config.URL)

	amqp.lastConnectTrialTime = time.Now()

	amqp.connection = nil
	amqp.channel = nil
	amqp.queue = nil

	connection, err := amqp_mod.Dial(amqp.config.URL)
	if err != nil {
		logger.WithError(err).Errorf("failed to connect to %s", amqp.config.URL)
		return err
	}

	channel, err := connection.Channel()
	if err != nil {
		logger.WithError(err).Error("failed to open a channel")
		return err
	}

	quename := amqp.getQueueName()
	logger.Infof("Declaring a queue %s", quename)

	queue, err := channel.QueueDeclare(quename, false, true, true, false, amqp_mod.Table{})
	if err != nil {
		logger.WithError(err).Errorf("failed to declare a queue")
		return err
	}

	// bind queue to listen fs events
	err = channel.QueueBind(queue.Name, "#", amqp.config.Exchange, false, amqp_mod.Table{})
	if err != nil {
		logger.WithError(err).Errorf("failed to bind the queue")
		return err
	}

	amqp.connection = connection
	amqp.channel = channel
	amqp.queue = &queue

	logger.Infof("connected to AMQP %s", amqp.config.URL)

	go func() {
		for amqp.connection != nil {
			amqp.connectionLock.Lock()

			if amqp.connection != nil && !amqp.connection.IsClosed() {
				amqp.connectionLock.Unlock()

				msgs, err := amqp.channel.Consume(amqp.queue.Name, "", true, false, false, false, nil)
				if err != nil {
					logger.WithError(err).Error("failed to consume a message")
					return
				}

				for msg := range msgs {
					logger.Debugf("consumed a message %s from AMQP", msg.RoutingKey)
					// pass to handlers registered
					if amqp.eventHandler != nil {
						amqp.eventHandler(msg)
					}
				}
			} else {
				amqp.connectionLock.Unlock()
			}
		}
	}()

	return nil
}

// Release releases all resources, disconnecting from AMQP
func (amqp *AMQP) Release() {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "Release",
	})

	defer commons.StackTraceFromPanic(logger)

	logger.Infof("trying to disconnect from %s", amqp.config.URL)

	// this should be called to break Consume
	if amqp.channel != nil {
		amqp.channel.Close()
		amqp.channel = nil
	}

	amqp.connectionLock.Lock()
	defer amqp.connectionLock.Unlock()

	if amqp.queue != nil {
		amqp.queue = nil
	}

	if amqp.connection != nil {
		if !amqp.connection.IsClosed() {
			amqp.connection.Close()
		}
		amqp.connection = nil
	}

	amqp.eventHandler = nil
}

// ProcessItem processes a turn-in send_message request, publishing a AMQP message
func (amqp *AMQP) ProcessItem(item turnin.TurnInItem) error {
	logger := log.WithFields(log.Fields{
		"package":  "service",
		"struct":   "AMQP",
		"function": "ProcessItem",
	})

	defer commons.StackTraceFromPanic(logger)

	request, ok := item.(*turnin.SendMessageRequest)
	if !ok {
		err := fmt.Errorf("failed to convert item to SendMessageRequest")
		logger.Error(err)
		return err
	}

	err := amqp.ensureConnected()
	if err != nil {
		logger.Error(err)
		return err
	}

	amqp.connectionLock.Lock()
	defer amqp.connectionLock.Unlock()

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

	err = amqp.channel.Publish(amqp.config.Exchange, request.Key, false, false, msg)
	if err != nil {
		logger.WithError(err).Errorf("failed to send an AMQP message with a subject %s", request.Key)
		return err
	}

	logger.Infof("published an AMQP message with a subject %s", request.Key)
	return nil
}

func (amqp *AMQP) getQueueName() string {
	hostname, err := os.Hostname()
	if err != nil {
		hostname = fmt.Sprintf("autocreated.%s", xid.New().String())
	}

	return fmt.Sprintf("%s.%s", AMQPConsumerQueueName, hostname)
}
