package main

import (
	"flag"
	"log"
	"os"
)

var (
	flagMailDir = flag.String("maildir", "~/Mail", "directory where all mail is stored")
	logger      = log.New(os.Stderr, "[gomaild] ", log.Ltime)
)

func main() {
	flag.Parse()

	cfg, err := OpenConfig(*flagMailDir)
	if err != nil {
		logger.Fatalln("could not open configuration DB", err)
	}

	defer cfg.Close()

	if err = cfg.StartSync(); err != nil {
		logger.Fatalln("could not start mail sync", err)
	}

	RunWeb(cfg)
}
