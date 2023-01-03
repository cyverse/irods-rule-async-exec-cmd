package subcmd

import (
	"fmt"
	"os"
	"strings"

	cmd_commons "github.com/cyverse/irods-rule-async-exec-cmd/client-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/dropin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var sendMsgCmd = &cobra.Command{
	Use:   "send_msg [key] [body]",
	Short: "Send a message to MessageBus",
	Long: `This buffers a message to be sent to AMQP message server.
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

	logger.Infof("[send_msg] %s", strings.Join(args, " "))

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

func dropSendMessageRequestOne(config *commons.ClientConfig, key string, body string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "dropSendMessageRequestOne",
	})

	di := dropin.NewDropIn(config.DropInDirPath)

	logger.Debugf("drop a send message request %s", key)

	request := dropin.NewSendMessageRequest(key, body)
	err := di.Drop(request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
