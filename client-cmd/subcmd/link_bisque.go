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

var linkBisqueCmd = &cobra.Command{
	Use:   "link_bisque [iRODS username] [iRODS file path]",
	Short: "Link an iRODS file to BisQue",
	Long: `This buffers a request to be sent to BisQue for linking an iRODS file.
	The message is stored in the turn in dir temporarily, then processed by the service.`,
	RunE: processLinkBisqueCommand,
}

func AddLinkBisqueCommand(rootCmd *cobra.Command) {
	// attach common flags
	cmd_commons.SetCommonFlags(linkBisqueCmd)

	rootCmd.AddCommand(linkBisqueCmd)
}

func processLinkBisqueCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "processLinkBisqueCommand",
	})

	config, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}

	if !cont {
		return nil
	}

	logger.Infof("[link_bisque] %s", strings.Join(args, " "))

	// link_bisque requires
	// 1. iRODS username who created an iRODS file
	// 2. iRODS path
	if len(args) >= 2 {
		irodsUsername := args[0]
		irodsPath := args[1]
		err = turninLinkBisqueRequestOne(config, irodsUsername, irodsPath)
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

func turninLinkBisqueRequestOne(config *commons.ClientConfig, irodsUsername string, irodsPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "turninLinkBisqueRequestOne",
	})

	ti := turnin.NewTurnIn(config.TurnInDirPath)

	logger.Debugf("turn-in a link bisque request %s", irodsUsername, irodsPath)

	request := turnin.NewLinkBisqueRequest(irodsUsername, irodsPath)
	err := ti.Turnin(request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
