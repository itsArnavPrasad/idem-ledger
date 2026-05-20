package config

import "os"

type Config struct {
	DatabaseURL string
	Port        string
	Strategy    string // "conditional_update" | "select_for_update" | "optimistic"
}

func Load() Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	strategy := os.Getenv("STRATEGY")
	if strategy == "" {
		strategy = "conditional_update"
	}
	return Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		Port:        port,
		Strategy:    strategy,
	}
}
