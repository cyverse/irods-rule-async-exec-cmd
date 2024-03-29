package subcmd

import (
	"fmt"
	"os"
	"strings"

	cmd_commons "github.com/cyverse/irods-rule-async-exec-cmd/client-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	"github.com/cyverse/irods-rule-async-exec-cmd/turnin"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var removeBisqueCmd = &cobra.Command{
	Use:   "remove_bisque [iRODS username] [iRODS file path]",
	Short: "Remove an iRODS file to BisQue",
	Long: `This buffers a request to be sent to BisQue for removing an iRODS file.
	The message is stored in the turn-in dir temporarily, then processed by the service.`,
	RunE: processRemoveBisqueCommand,
}

func AddRemoveBisqueCommand(rootCmd *cobra.Command) {
	// attach common flags
	cmd_commons.SetCommonFlags(removeBisqueCmd)

	rootCmd.AddCommand(removeBisqueCmd)
}

func processRemoveBisqueCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "processRemoveBisqueCommand",
	})

	config, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}

	if !cont {
		return nil
	}

	logger.Infof("[remove_bisque] %s", strings.Join(args, " "))

	// remove_bisque requires
	// 1. iRODS username who deleted an iRODS file
	// 2. iRODS path
	if len(args) >= 2 {
		irodsUsername := args[0]
		irodsPath := args[1]
		err = turninRemoveBisqueRequestOne(config, irodsUsername, irodsPath)
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

func turninRemoveBisqueRequestOne(config *commons.ClientConfig, irodsUsername string, irodsPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "turninRemoveBisqueRequestOne",
	})

	ti := turnin.NewTurnIn(config.TurnInDirPath)

	logger.Debugf("turn-in a remove bisque request %s, %s", irodsUsername, irodsPath)

	request := turnin.NewRemoveBisqueRequest(irodsUsername, irodsPath)
	err := ti.Turnin(request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
