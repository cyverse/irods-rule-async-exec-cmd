package commons

import (
	"errors"
	"fmt"

	"gopkg.in/yaml.v2"
)

// ClientConfig is a configuration struct for client
type ClientConfig struct {
	DropInDirPath string `yaml:"dropin_dir_path,omitempty"`
}

// NewDefaultClientConfig returns a default client config
func NewDefaultClientConfig() *ClientConfig {
	return &ClientConfig{
		DropInDirPath: DropInDirPathDefault,
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
	if len(config.DropInDirPath) == 0 {
		return errors.New("drop in dir path is not given")
	}

	return nil
}
