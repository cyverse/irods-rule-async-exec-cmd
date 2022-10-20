package commons

import (
	"fmt"
	"io/ioutil"
	"strconv"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func SetCommonFlags(command *cobra.Command) {
	command.Flags().StringP("config", "c", "", "Set config file (yaml)")
	command.Flags().BoolP("version", "v", false, "Print version")
	command.Flags().BoolP("help", "h", false, "Print help")
	command.Flags().BoolP("debug", "d", false, "Enable debug mode")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.ClientConfig, bool, error) {
	logger := log.WithFields(log.Fields{
		"package":  "commons",
		"function": "ProcessCommonFlags",
	})

	debug := false
	debugFlag := command.Flags().Lookup("debug")
	if debugFlag != nil {
		debugMode, err := strconv.ParseBool(debugFlag.Value.String())
		if err != nil {
			debug = false
		}

		debug = debugMode
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, err := strconv.ParseBool(helpFlag.Value.String())
		if err != nil {
			help = false
		}

		if help {
			PrintHelp(command)
			return nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, err := strconv.ParseBool(versionFlag.Value.String())
		if err != nil {
			version = false
		}

		if version {
			PrintVersion(command)
			return nil, false, nil // stop here
		}
	}

	readConfig := false
	var config *commons.ClientConfig

	configFlag := command.Flags().Lookup("config")
	if configFlag != nil {
		configPath := configFlag.Value.String()
		if len(configPath) > 0 {
			yamlBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				logger.Error(err)
				return nil, false, err // stop here
			}

			clientConfig, err := commons.NewClientConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Error(err)
				return nil, false, err // stop here
			}

			// overwrite config
			config = clientConfig
			readConfig = true
		}
	}

	// default config
	if !readConfig {
		config = commons.NewDefaultClientConfig()
	}

	// prioritize command-line flag over config files
	if debug {
		log.SetLevel(log.DebugLevel)
	}

	err := config.Validate()
	if err != nil {
		logger.Error(err)
		return nil, false, err // stop here
	}

	return config, true, nil // contiue
}

func PrintVersion(command *cobra.Command) error {
	info, err := commons.GetVersionJSON()
	if err != nil {
		return err
	}

	fmt.Println(info)
	return nil
}

func PrintHelp(command *cobra.Command) error {
	return command.Usage()
}
