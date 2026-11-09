package main

import (
	"log"

	"monitor-remote/internal/config"
	"monitor-remote/internal/database"
)

func main() {
	databasePath := config.DatabasePath()

	db, err := database.Open(databasePath)
	if err != nil {
		log.Fatalf(
			"Failed to initialize SQLite database: %v",
			err,
		)
	}
	defer db.Close()

	log.Printf(
		"SQLite database initialized at %s",
		databasePath,
	)
}
