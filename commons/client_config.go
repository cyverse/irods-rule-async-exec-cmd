package commons

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
)

const (
	TurnInDirPathDefault string = "/var/lib/irods_rule_async_exec_cmd/turnin"
)

// ClientConfig is a configuration struct for client
type ClientConfig struct {
	TurnInDirPath string `yaml:"turnin_dir_path,omitempty"`

	// for Logging
	LogPath string `yaml:"log_path,omitempty"`
}

// NewDefaultClientConfig returns a default client config
func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		TurnInDirPath: TurnInDirPathDefault,

		LogPath: "", // use default
	}
}

// NewClientConfigFromYAML creates ClientConfig from YAML
func NewClientConfigFromYAML(yamlBytes []byte) (*ClientConfig, error) {
	config := NewDefaultClientConfig()

	err := yaml.Unmarshal(yamlBytes, config)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal YAML - %v", err)
	}

	return config, nil
}

// Validate validates field values and returns error if occurs
func (config *ClientConfig) Validate() error {
	if len(config.TurnInDirPath) == 0 {
		return errors.New("turn-in dir path is not given")
	}

	return nil
}

// GetLogFilePath returns log file path
func (config *ClientConfig) GetLogFilePath() string {
	if len(config.LogPath) > 0 {
		return config.LogPath
	}

	// default
	return "/var/lib/irods_rule_async_exec_cmd/irods_rule_async_exec_cmd_client.log"
}
