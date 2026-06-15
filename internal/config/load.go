package config

import (
	"fmt"
	"os"
	"gopkg.in/yaml.v3"
)

// Load 从指定路径读取 yaml 配置。
// configs/config.yaml 是本地私有配置文件，已被 .gitignore 忽略。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file %q: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config yaml: %w", err)
	}

	return &cfg, nil
}


