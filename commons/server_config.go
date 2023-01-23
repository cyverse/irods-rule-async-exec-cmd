package commons

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v2"
)

const (
	ConfigFilePathDefault string = "/etc/irods_rule_async_exec_cmd/config.yml"

	IrodsPortDefault     int    = 1247
	IrodsRootPathDefault string = "/"

	ReconnectInterval time.Duration = 1 * time.Minute
)

// AmqpConfig is a configuration struct for AMQP Message bus
type AmqpConfig struct {
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
}

type BisqueConfig struct {
	URL           string `yaml:"url"`
	AdminUsername string `yaml:"admin_username"`
	AdminPassword string `yaml:"admin_password"`
	IrodsUsername string `yaml:"irods_username"` // username that will write/read on behalf of all other users in BisQue (user who mounts irods)
	IrodsZone     string `yaml:"irods_zone"`
	IrodsBaseURL  string `yaml:"irods_base_url"`  // include http:// or file://
	IrodsRootPath string `yaml:"irods_root_path"` // e.g., '/ucsb/home' for ucsb
}

type IrodsConfig struct {
	Host          string `yaml:"host"`
	Port          int    `yaml:"port"`
	Zone          string `yaml:"zone"`
	AdminUsername string `yaml:"admin_username"`
	AdminPassword string `yaml:"admin_password"`
}

func getLogFilename() string {
	return "irods_rule_async_exec_cmd.log"
}

func GetDefaultDataRootDirPath() string {
	dirPath, err := os.Getwd()
	if err != nil {
		return "/var/lib/irods_rule_async_exec_cmd"
	}
	return dirPath
}

// ServerConfig is a configuration struct for server
type ServerConfig struct {
	DataRootPath string `yaml:"data_root_path,omitempty"`

	// iRODS FS Event Publish
	AmqpConfig AmqpConfig `yaml:"amqp_config,omitempty"`

	// Bisque
	BisqueConfig BisqueConfig `yaml:"bisque_config,omitempty"`

	// iRODS
	IrodsConfig IrodsConfig `yaml:"irods_config,omitempty"`

	// for Logging
	LogPath string `yaml:"log_path,omitempty"`

	Foreground   bool `yaml:"foreground,omitempty"`
	Debug        bool `yaml:"debug,omitempty"`
	ChildProcess bool `yaml:"childprocess,omitempty"`
}

// NewDefaultServerConfig returns a default server config
func NewDefaultServerConfig() *ServerConfig {
	return &ServerConfig{
		DataRootPath: GetDefaultDataRootDirPath(),

		AmqpConfig: AmqpConfig{
			URL:      "",
			Exchange: "",
		},

		BisqueConfig: BisqueConfig{
			URL:           "",
			AdminUsername: "",
			AdminPassword: "",
			IrodsUsername: "",
			IrodsZone:     "",
			IrodsRootPath: IrodsRootPathDefault,
		},

		IrodsConfig: IrodsConfig{
			Host:          "",
			Port:          IrodsPortDefault,
			Zone:          "",
			AdminUsername: "",
			AdminPassword: "",
		},

		LogPath: "", // use default

		Foreground:   false,
		Debug:        false,
		ChildProcess: false,
	}
}

// NewServerConfigFromYAML creates ServerConfig from YAML
func NewServerConfigFromYAML(yamlBytes []byte) (*ServerConfig, error) {
	config := NewDefaultServerConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	return config, nil
}

// GetLogFilePath returns log file path
func (config *ServerConfig) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	return path.Join(config.DataRootPath, getLogFilename())
}

func (config *ServerConfig) GetTurnInRootDirPath() string {
	return path.Join(config.DataRootPath, "turnin")
}

// MakeLogDir makes a log dir required
func (config *ServerConfig) MakeLogDir() error {
	logFilePath := config.GetLogFilePath()
	logDirPath := filepath.Dir(logFilePath)
	err := config.makeDir(logDirPath)
	if err != nil {
		return err
	}

	return nil
}

// MakeWorkDirs makes dirs required
func (config *ServerConfig) MakeWorkDirs() error {
	turninDirPath := config.GetTurnInRootDirPath()
	err := config.makeDir(turninDirPath)
	if err != nil {
		return err
	}

	return nil
}

// makeDir makes a dir for use
func (config *ServerConfig) makeDir(path string) error {
	if len(path) == 0 {
		return fmt.Errorf("failed to create a dir with empty path")
	}

	dirInfo, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			// make
			mkdirErr := os.MkdirAll(path, 0775)
			if mkdirErr != nil {
				return fmt.Errorf("making a dir (%s) error - %v", path, mkdirErr)
			}

			return nil
		}

		return fmt.Errorf("stating a dir (%s) error - %v", path, err)
	}

	if !dirInfo.IsDir() {
		return fmt.Errorf("a file (%s) exist, not a directory", path)
	}

	dirPerm := dirInfo.Mode().Perm()
	if dirPerm&0200 != 0200 {
		return fmt.Errorf("a dir (%s) exist, but does not have the write permission", path)
	}

	return nil
}

// Validate validates field values and returns error if occurs
func (config *ServerConfig) Validate() error {
	if len(config.DataRootPath) == 0 {
		return fmt.Errorf("data root dir must be given")
	}

	if len(config.AmqpConfig.URL) == 0 {
		return errors.New("AMQP URL is not given")
	}

	if len(config.AmqpConfig.Exchange) == 0 {
		return errors.New("AMQP Exchange is not given")
	}

	// bisque config is optional
	if len(config.BisqueConfig.URL) > 0 {
		if len(config.BisqueConfig.URL) == 0 {
			return errors.New("BisQue URL is not given")
		}

		if len(config.BisqueConfig.AdminUsername) == 0 {
			return errors.New("BisQue Admin Username is not given")
		}

		if len(config.BisqueConfig.AdminPassword) == 0 {
			return errors.New("BisQue Admin Password is not given")
		}

		if len(config.BisqueConfig.IrodsUsername) == 0 {
			return errors.New("BisQue iRODS Username is not given")
		}

		if len(config.BisqueConfig.IrodsZone) == 0 {
			return errors.New("BisQue iRODS Zone is not given")
		}

		if len(config.BisqueConfig.IrodsBaseURL) == 0 {
			return errors.New("BisQue iRODS Base URL is not given")
		}

		if len(config.BisqueConfig.IrodsRootPath) == 0 {
			return errors.New("BisQue iRODS Root Path is not given")
		}
	}

	if len(config.IrodsConfig.Host) == 0 {
		return errors.New("IRODS Host is not given")
	}

	if len(config.IrodsConfig.Zone) == 0 {
		return errors.New("IRODS Zone is not given")
	}

	if config.IrodsConfig.Port <= 0 {
		return errors.New("IRODS Port is not given")
	}

	if len(config.IrodsConfig.AdminUsername) == 0 {
		return errors.New("IRODS Admin Username is not given")
	}

	if len(config.IrodsConfig.AdminPassword) == 0 {
		return errors.New("IRODS Admin Password is not given")
	}

	return nil
}
