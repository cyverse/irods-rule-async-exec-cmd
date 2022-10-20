package commons

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
)

const (
	LogFilePathDefault string = "/tmp/irods_rule_async_exec_cmd.log"

	NatsClientIDPrefixDefault string = "irods_rule_async_exec_cmd_"
)

// NatsConfig is a configuration struct for NATS/STAN Message bus
type NatsConfig struct {
	URL            string `yaml:"url"`
	ClusterID      string `yaml:"cluster_id"`
	ClientIDPrefix string `yaml:"client_id_prefix,omitempty"`
}

// AmqpConfig is a configuration struct for AMQP Message bus
type AmqpConfig struct {
	URL      string `yaml:"url"`
	Exchange string `yaml:"exchange"`
}

type BisqueConfig struct {
	URL string `yaml:"url"`
}

// ServerConfig is a configuration struct for server
type ServerConfig struct {
	DropInDirPath string `yaml:"dropin_dir_path,omitempty"`

	// iRODS FS Event Publish
	NatsConfig NatsConfig `yaml:"nats_config,omitempty"`
	AmqpConfig AmqpConfig `yaml:"amqp_config,omitempty"`

	// Bisque
	BisqueConfig BisqueConfig `yaml:"bisque_config,omitempty"`

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

		NatsConfig: NatsConfig{
			URL:            "",
			ClusterID:      "",
			ClientIDPrefix: NatsClientIDPrefixDefault,
		},
		AmqpConfig: AmqpConfig{
			URL:      "",
			Exchange: "",
		},

		BisqueConfig: BisqueConfig{
			URL: "",
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

	if len(config.NatsConfig.URL) == 0 && len(config.AmqpConfig.URL) == 0 {
		return errors.New("either NATS or AMQP config must be given")
	}

	if len(config.NatsConfig.URL) > 0 {
		if len(config.NatsConfig.ClusterID) == 0 {
			return errors.New("NATS Cluster ID is not given")
		}

		if len(config.NatsConfig.ClientIDPrefix) == 0 {
			return errors.New("NATS Client ID Prefix is not given")
		}
	}

	if len(config.AmqpConfig.URL) > 0 {
		if len(config.AmqpConfig.Exchange) == 0 {
			return errors.New("AMQP Exchange is not given")
		}
	}

	if len(config.BisqueConfig.URL) == 0 {
		return errors.New("BisQue URL is not given")
	}

	return nil
}

// IsNats checks if the server config uses NATS
func (config *ServerConfig) IsNATS() bool {
	return len(config.NatsConfig.URL) > 0
}

// IsAMQP checks if the server config uses AMQP
func (config *ServerConfig) IsAMQP() bool {
	return len(config.AmqpConfig.URL) > 0
}
