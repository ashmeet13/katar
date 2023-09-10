package config

import "os"

type Config struct {
	KatarDir string
}

func NewConfig() *Config {
	// Katar Dir - Root directory to save all the data
	katarDir := os.Getenv("KATAR_DIR")
	if katarDir == "" {
		katarDir = "/tmp/katar"
	}

	err := os.MkdirAll(katarDir, os.ModePerm)
	if err != nil {
		panic(err)
	}

	return &Config{
		KatarDir: katarDir,
	}
}
