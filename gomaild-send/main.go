package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"path"
	"strings"
	"time"
)

var (
	flagAccount = flag.String("a", "", "which account to use")
	flagFrom    = flag.String("f", "", "set from address")
	flagMailDir = flag.String("maildir", "~/Mail", "directory where all mail is stored")
)

type Envelope struct {
	From string
	To   []string
}

func getMailDir() (string, error) {
	dir := *flagMailDir

	if strings.HasPrefix(dir, "~/") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}

		dir = strings.Replace(dir, "~", usr.HomeDir, 1)
	}

	return dir, nil
}

func main() {
	flag.Parse()

	if *flagFrom == "" {
		fmt.Println("Missing -f parameter.")
		os.Exit(1)
	}

	to := flag.Args()
	if len(to) == 0 {
		fmt.Println("Missing recipient addresses.")
		os.Exit(1)
	}

	envelope := &Envelope{
		From: *flagFrom,
		To:   to,
	}

	mailDir, err := getMailDir()
	if err != nil {
		fmt.Println("Could not determine a mail directory.")
		os.Exit(1)
	}

	account := *flagFrom

	if *flagAccount != "" {
		account = *flagAccount
	}

	socketPath := path.Join(mailDir, account, "smtp.sock")

	addr := net.UnixAddr{socketPath, "unix"}
	conn, err := net.DialUnix("unix", nil, &addr)
	if err != nil {
		fmt.Println("Couldn't connect to smtp socket:", err)
		os.Exit(1)
	}

	defer conn.Close()

	encoder := json.NewEncoder(conn)
	if err := encoder.Encode(envelope); err != nil {
		fmt.Println("Couldn't send the envelope.")
		os.Exit(1)
	}

	if _, err := io.Copy(conn, os.Stdin); err != nil {
		fmt.Println("Error sending message to gomaild daemon.")
		os.Exit(1)
	}

	conn.CloseWrite()
	conn.SetReadDeadline(time.Now().Add(time.Second * 30))

	// Wait for confirmation from gomaild daemon.
	buf := make([]byte, 1)
	if n, err := conn.Read(buf); err != nil || n != 1 || buf[0] != 1 {
		fmt.Println("No confirmation received.", err, buf)
		os.Exit(1)
	}
}
