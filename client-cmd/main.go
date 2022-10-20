package main

import (
	"os"

	cmd_commons "github.com/cyverse/irods-rule-async-exec-cmd/client-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/client-cmd/subcmd"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "irods-rule-async-exec-cmd [args..]",
	Short: "Queue a command to be exectued asynchronously",
	Long:  "Queue a command to be exectued asynchronously. The comand can be either 'Message' or 'BisQue Data Control Request'. Messages are routed to AMQP or NATS(STAN) service configured, and BisQue Data Control Requests are routed to BisQue server configured.",
	RunE:  processCommand,
}

func Execute() error {
	return rootCmd.Execute()
}

func processCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "processCommand",
	})

	_, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		logger.Error(err)
	}

	if !cont {
		return err
	}

	// if nothing is given
	cmd_commons.PrintHelp(command)

	return nil
}

func main() {
	log.SetFormatter(&log.TextFormatter{
		TimestampFormat: "2006-01-02 15:04:05.000",
		FullTimestamp:   true,
	})

	log.SetLevel(log.FatalLevel)

	logger := log.WithFields(log.Fields{
		"package":  "main",
		"function": "main",
	})

	// attach common flags
	cmd_commons.SetCommonFlags(rootCmd)

	// add sub commands
	subcmd.AddSendMsgCommand(rootCmd)
	subcmd.AddLinkBisqueCommand(rootCmd)

	err := Execute()
	if err != nil {
		logger.Fatal(err)
		os.Exit(1)
	}
}
