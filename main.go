package main

import (
	"flag"
	"log"
)

var (
	flagMailDir = flag.String("maildir", "~/Mail", "directory where all mail is stored")
)

func main() {
	flag.Parse()

	cfg, err := OpenConfig(*flagMailDir)
	if err != nil {
		log.Fatalln("could not open configuration DB", err)
	}

	defer cfg.Close()

	//var wg sync.WaitGroup
	//defer wg.Wait()

	//quitCh := make(chan struct{})
	//defer close(quitCh)

	//accounts, err := cfg.GetAccounts()
	//if err != nil {
	//	log.Fatalln("could not read accounts from configuration DB", err)
	//}

	//wg.Add(len(accounts))

	//for _, account := range accounts {
	//	go func() {
	//		defer wg.Done()
	//		SyncAccount(account, quitCh)
	//	}()
	//}
	if err = cfg.StartSync(); err != nil {
		log.Fatalln("could not start mail sync", err)
	}

	RunWeb(cfg)
}
