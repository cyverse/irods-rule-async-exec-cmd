package commons

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"gopkg.in/natefinch/lumberjack.v2"
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
		debug, _ = strconv.ParseBool(debugFlag.Value.String())
	}

	if debug {
		log.SetLevel(log.DebugLevel)
	}

	helpFlag := command.Flags().Lookup("help")
	if helpFlag != nil {
		help, _ := strconv.ParseBool(helpFlag.Value.String())
		if help {
			PrintHelp(command)
			return nil, false, nil // stop here
		}
	}

	versionFlag := command.Flags().Lookup("version")
	if versionFlag != nil {
		version, _ := strconv.ParseBool(versionFlag.Value.String())
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

	var logWriter io.WriteCloser
	logFilePath := config.GetLogFilePath()
	if logFilePath != "-" || len(logFilePath) >= 0 {
		logWriter = getLogWriter(logFilePath)

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, logWriter)
		log.SetOutput(mw)

		logger.Infof("Logging to %s", logFilePath)
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

func getLogWriter(logPath string) io.WriteCloser {
	return &lumberjack.Logger{
		Filename:   logPath,
		MaxSize:    50, // 50MB
		MaxBackups: 5,
		MaxAge:     30, // 30 days
		Compress:   false,
	}
}
