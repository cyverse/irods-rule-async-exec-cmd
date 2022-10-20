package commons

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strconv"

	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/cyverse/irods-rule-async-exec-cmd/commons"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

const (
	ChildProcessArgument = "child_process"
)

func SetCommonFlags(command *cobra.Command) {
	command.Flags().StringP("config", "c", "", "Set config file (yaml)")
	command.Flags().BoolP("version", "v", false, "Print version")
	command.Flags().BoolP("help", "h", false, "Print help")
	command.Flags().BoolP("debug", "d", false, "Enable debug mode")
	command.Flags().BoolP("foreground", "f", false, "Run in foreground")
	command.Flags().BoolP(ChildProcessArgument, "", false, "")
}

func ProcessCommonFlags(command *cobra.Command) (*commons.ServerConfig, io.WriteCloser, bool, error) {
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

	foreground := false
	foregroundFlag := command.Flags().Lookup("foreground")
	if foregroundFlag != nil {
		foregroundMode, err := strconv.ParseBool(foregroundFlag.Value.String())
		if err != nil {
			foreground = false
		}

		foreground = foregroundMode
	}

	childProcess := false
	childProcessFlag := command.Flags().Lookup(ChildProcessArgument)
	if childProcessFlag != nil {
		childProcessMode, err := strconv.ParseBool(childProcessFlag.Value.String())
		if err != nil {
			childProcess = false
		}

		childProcess = childProcessMode
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
			return nil, nil, false, nil // stop here
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
			return nil, nil, false, nil // stop here
		}
	}

	readConfig := false
	var config *commons.ServerConfig

	configFlag := command.Flags().Lookup("config")
	if configFlag != nil {
		configPath := configFlag.Value.String()
		if len(configPath) > 0 {
			yamlBytes, err := ioutil.ReadFile(configPath)
			if err != nil {
				logger.Error(err)
				return nil, nil, false, err // stop here
			}

			serverConfig, err := commons.NewServerConfigFromYAML(yamlBytes)
			if err != nil {
				logger.Error(err)
				return nil, nil, false, err // stop here
			}

			// overwrite config
			config = serverConfig
			readConfig = true
		}
	}

	// default config
	if !readConfig {
		config = commons.NewDefaultServerConfig()
	}

	// prioritize command-line flag over config files
	if debug {
		log.SetLevel(log.DebugLevel)
	}

	if foreground {
		config.Foreground = true
	}

	config.ChildProcess = childProcess

	err := config.Validate()
	if err != nil {
		logger.Error(err)
		return nil, nil, false, err // stop here
	}

	if config.Debug {
		log.SetLevel(log.DebugLevel)
	}

	var logWriter io.WriteCloser
	if config.LogPath == "-" || len(config.LogPath) == 0 {
		log.SetOutput(os.Stderr)
	} else {
		logWriter = getLogWriter(config.LogPath)

		// use multi output - to output to file and stdout
		mw := io.MultiWriter(os.Stderr, logWriter)
		log.SetOutput(mw)

		logger.Infof("Logging to %s", config.LogPath)
	}

	return config, logWriter, true, nil // contiue
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
