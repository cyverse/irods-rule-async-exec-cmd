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

var moveBisqueCmd = &cobra.Command{
	Use:   "move_bisque [iRODS username] [iRODS source file path] [iRODS dest file path]",
	Short: "Move an iRODS file in BisQue",
	Long: `This buffers a request to be sent to BisQue for moving an iRODS file.
	The message is stored in the drop in dir temporarily, then processed by the service.`,
	RunE: processMoveBisqueCommand,
}

func AddMoveBisqueCommand(rootCmd *cobra.Command) {
	// attach common flags
	cmd_commons.SetCommonFlags(moveBisqueCmd)

	rootCmd.AddCommand(moveBisqueCmd)
}

func processMoveBisqueCommand(command *cobra.Command, args []string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "processMoveBisqueCommand",
	})

	config, cont, err := cmd_commons.ProcessCommonFlags(command)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		return nil
	}

	if !cont {
		return nil
	}

	logger.Infof("[move_bisque] %s", strings.Join(args, " "))

	// move_bisque requires
	// 1. iRODS username who moved an iRODS file
	// 2. iRODS source path
	// 3. iRODS dest path
	if len(args) >= 3 {
		irodsUsername := args[0]
		irodsSrcPath := args[1]
		irodsDestPath := args[2]
		err = dropMoveBisqueRequestOne(config, irodsUsername, irodsSrcPath, irodsDestPath)
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

func dropMoveBisqueRequestOne(config *commons.ClientConfig, irodsUsername string, irodsSourcePath string, irodsDestPath string) error {
	logger := log.WithFields(log.Fields{
		"package":  "subcmd",
		"function": "dropMoveBisqueRequestOne",
	})

	di := dropin.NewDropIn(config.DropInDirPath)

	logger.Debugf("drop a move bisque request %s, %s to %s", irodsUsername, irodsSourcePath, irodsDestPath)

	request := dropin.NewMoveBisqueRequest(irodsUsername, irodsSourcePath, irodsDestPath)
	err := di.Drop(request)
	if err != nil {
		logger.Error(err)
		return err
	}

	return nil
}
