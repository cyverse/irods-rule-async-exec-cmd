package subcmd

import (
	"encoding/json"
	"fmt"
	"os"

	cmd_commons "github.com/cyverse/irods-rule-async-exec-cmd/client-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var sendMsgCmd = &cobra.Command{
	Use:   "send_msg [key] [body]",
	Short: "Send a message to MessageBus",
	Long: `This buffers a message to be sent to AMQP or NATS(STAN) message server.
	The message is stored in the drop in dir temporarily, then processed by the service.`,
	RunE: processSendMsgCommand,
}

func AddSendMsgCommand(rootCmd *cobra.Command) {
	// attach common flags
	cmd_commons.SetCommonFlags(sendMsgCmd)

	rootCmd.AddCommand(sendMsgCmd)
}

func processSendMsgCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "processSendMsgCommand",
	})

	config, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}

	if !cont {
		return nil
	}

	// send_msg requires 2 arguments
	// 1. key
	// 2. body
	if len(args) >= 2 {
		key := args[0]
		body := args[1]
		err = dropSendMessageRequestOne(config, key, body)
		if err != nil {
			logger.Error(err)
			fmt.Fprintln(os.Stderr, err.Error())
			return nil
		}
	} else {
		err := fmt.Errorf("not enough input arguments")
		logger.Error(err)
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}
	return nil
}

type sendMessageRequest struct {
	Key  string `json:"key"`
	Body string `json:"body"`
}

func dropSendMessageRequestOne(config *commons.ClientConfig, key string, body string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "dropSendMessageRequestOne",
	})

	di := dropin.NewDropIn(config.DropInDirPath)

	request := sendMessageRequest{
		Key:  key,
		Body: body,
	}

	requestBytes, err := json.Marshal(request)
	if err != nil {
		logger.Error(err)
		return err
	}

	logger.Debugf("drop a send message request %s", key)

	err = di.Drop(requestBytes)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
