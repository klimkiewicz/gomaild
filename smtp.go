package main

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/smtp"
	"os"
	"path"
	"sync"
	"time"

	"github.com/miGlanz/gomaild/oauth"
)

const gmailSMTPAddr = "smtp.gmail.com:587"

type Envelope struct {
	From string
	To   []string
}

func sendMessage(fileName string, account *Account, accessToken *oauth.AccessToken) error {
	f, err := os.Open(fileName)
	if err != nil {
		return err
	}

	defer f.Close()

	var envelope Envelope
	decoder := json.NewDecoder(f)
	if err := decoder.Decode(&envelope); err != nil {
		return err
	}

	reader := io.MultiReader(decoder.Buffered(), f)

	// Skip the newline
	if _, err := reader.Read(make([]byte, 1)); err != nil {
		return err
	}

	token, err := accessToken.Get()
	if err != nil {
		return err
	}

	c, err := smtp.Dial(gmailSMTPAddr)
	if err != nil {
		return err
	}

	defer c.Quit()

	if err = c.StartTLS(&tls.Config{ServerName: "smtp.gmail.com"}); err != nil {
		return err
	}

	if err = c.Auth(oauth.NewXOAuthSMTP(account.Email, token)); err != nil {
		return err
	}

	if err = c.Mail(envelope.From); err != nil {
		return err
	}

	for _, recipient := range envelope.To {
		if err = c.Rcpt(recipient); err != nil {
			return err
		}
	}

	writer, err := c.Data()
	if err != nil {
		return err
	}

	defer writer.Close()

	if _, err := io.Copy(writer, reader); err != nil {
		return err
	}

	os.Remove(fileName)
	return nil
}

func syncSMTP(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) error {
	socketPath := path.Join(account.Path, "smtp.sock")

	// Remove the socket file if it already exists.
	if err := os.Remove(socketPath); err != nil {
		if !os.IsNotExist(err) {
			return err
		}
	}

	// Create maildir structure for outgoing mail.
	tmpDir := path.Join(account.Path, "Outgoing", "tmp")
	newDir := path.Join(account.Path, "Outgoing", "new")
	if err := os.MkdirAll(tmpDir, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(newDir, 0700); err != nil {
		return err
	}

	l, err := net.ListenUnix("unix", &net.UnixAddr{socketPath, "unix"})
	if err != nil {
		return err
	}

	defer os.Remove(socketPath)

	go func() {
		<-quitCh
		l.Close()
	}()

	var wg sync.WaitGroup
	defer wg.Wait()

	for {
		conn, err := l.AcceptUnix()
		if err != nil {
			// If quitCh is closed then it's not really an error.
			if _, ok := <-quitCh; ok {
				return err
			}

			return nil
		}

		wg.Add(1)
		go func() {
			defer wg.Done()
			defer conn.Close()

			f, err := ioutil.TempFile(tmpDir, "smtp-")
			if err != nil {
				fmt.Println("Couldn't create tmp file:", err)
				return
			}

			defer f.Close()

			fileName := f.Name()
			fmt.Println("fileName", fileName)

			if _, err := io.Copy(f, conn); err != nil {
				fmt.Println("Couldn't write mail to tmp file:", err)
				return
			}

			if err := f.Sync(); err != nil {
				fmt.Println("Couldn't sync file:", err)
				return
			}

			dst := path.Join(newDir, path.Base(fileName))

			if err := os.Rename(fileName, dst); err != nil {
				fmt.Println("Couldn't rename tmp file:", err)
				return
			}

			// Send confirmation to gomaild-send.
			if _, err := conn.Write([]byte{1}); err != nil {
				fmt.Println("Error sending data:", err)
				return
			}

			if err := sendMessage(dst, account, accessToken); err != nil {
				waitFor := 500 * time.Millisecond
				lastError := time.Now()

				for err != nil {
					logger.Printf("Error sending message to SMTP server: %s", err)

					if time.Since(lastError) > time.Second*90 {
						waitFor = 500 * time.Millisecond
					} else {
						waitFor *= 2
						if waitFor > time.Minute {
							waitFor = time.Minute
						}
					}

					lastError = time.Now()

					select {
					case <-time.After(waitFor):
					case <-quitCh:
						return
					}

					err = syncSMTP(account, accessToken, quitCh)
				}
			}

		}()
	}

	return nil
}

func SyncSMTP(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) {
	// Keep listening for SMTP requests
	if err := syncSMTP(account, accessToken, quitCh); err != nil {
		waitFor := 500 * time.Millisecond
		lastError := time.Now()

		for err != nil {
			logger.Printf("Error listening for SMTP requests: %s", err)

			if time.Since(lastError) > time.Second*90 {
				waitFor = 500 * time.Millisecond
			} else {
				waitFor *= 2
				if waitFor > time.Minute {
					waitFor = time.Minute
				}
			}

			lastError = time.Now()

			select {
			case <-time.After(waitFor):
			case <-quitCh:
				return
			}

			err = syncSMTP(account, accessToken, quitCh)
		}
	}
}
