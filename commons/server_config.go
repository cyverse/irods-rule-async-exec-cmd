package commons

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
)

const (
	ConfigFilePathDefault string = "/etc/irods_rule_async_exec_cmd/config.yml"

	LogFilePathDefault string = "/tmp/irods_rule_async_exec_cmd.log"

	IrodsPortDefault     int    = 1247
	IrodsRootPathDefault string = "/"
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
	IrodsZone     string `yaml:"zone"`
	IrodsBaseURL  string `yaml:"irods_base_url"`  // include http:// or file://
	IrodsRootPath string `yaml:"irods_root_path"` // e.g., '/' datastore, '/iplant/home' for ucsb
}

type IrodsConfig struct {
	Host          string `yaml:"host"`
	Port          int    `yaml:"port"`
	Zone          string `yaml:"zone"`
	AdminUsername string `yaml:"admin_username"`
	AdminPassword string `yaml:"admin_password"`
}

// ServerConfig is a configuration struct for server
type ServerConfig struct {
	DropInDirPath string `yaml:"dropin_dir_path,omitempty"`

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
		DropInDirPath: DropInDirPathDefault,

		AmqpConfig: AmqpConfig{
			URL:      "",
			Exchange: "",
		},

		BisqueConfig: BisqueConfig{
			URL:           "",
			AdminUsername: "",
			AdminPassword: "",
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

		LogPath: LogFilePathDefault,

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

// Validate validates field values and returns error if occurs
func (config *ServerConfig) Validate() error {
	if len(config.DropInDirPath) == 0 {
		return errors.New("drop in dir path is not given")
	}

	if len(config.AmqpConfig.URL) == 0 {
		return errors.New("AMQP URL is not given")
	}

	if len(config.AmqpConfig.Exchange) == 0 {
		return errors.New("AMQP Exchange is not given")
	}

	if len(config.BisqueConfig.URL) == 0 {
		return errors.New("BisQue URL is not given")
	}

	if len(config.BisqueConfig.AdminUsername) == 0 {
		return errors.New("BisQue Admin Username is not given")
	}

	if len(config.BisqueConfig.AdminPassword) == 0 {
		return errors.New("BisQue Admin Password is not given")
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
