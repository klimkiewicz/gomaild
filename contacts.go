package main

import (
	"encoding/json"
	"encoding/xml"
	"net/http"
	"net/url"
	"os"
	"path"
	"time"

	"github.com/miGlanz/gomaild/oauth"
)

const contactsUrl = "https://www.google.com/m8/feeds/contacts/default/full?"

type googleEmail struct {
	Email string `xml:"address,attr"`
}

type googleContact struct {
	Emails []*googleEmail `xml:"email"`
	Id     string         `xml:"id"`
	Title  string         `xml:"title"`
}

type googleContacts struct {
	Contacts []*googleContact `xml:"entry"`
}

func saveContacts(account *Account, contacts Contacts) error {
	dst := path.Join(account.Path, "contacts.json")
	f, err := os.OpenFile(dst, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	defer f.Close()

	encoder := json.NewEncoder(f)
	return encoder.Encode(contacts)
}

func syncContacts(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) error {
	client := http.Client{}

	for {
		logger.Println("Synchronizing contacts...")

		token, err := accessToken.Get()
		if err != nil {
			return err
		}

		values := url.Values{
			"access_token": {token},
			"max-results":  {"10000"},
		}

		var configContacts Contacts

		if !account.LastContactsSync.IsZero() && time.Since(account.LastContactsSync) <= time.Hour*8 {
			updatedMin := account.LastContactsSync.Format(time.RFC3339)
			values["updated-min"] = []string{updatedMin}
			if configContacts, err = account.GetContacts(); err != nil {
				return err
			}
		} else {
			configContacts = make(Contacts)
		}

		req, err := http.NewRequest("GET", contactsUrl+values.Encode(), nil)
		if err != nil {
			return err
		}

		req.Header.Set("GData-Version", "3.0")
		resp, err := client.Do(req)
		if err != nil {
			return err
		}

		var contacts googleContacts
		decoder := xml.NewDecoder(resp.Body)
		if err := decoder.Decode(&contacts); err != nil {
			return err
		}

		resp.Body.Close()

		for _, contact := range contacts.Contacts {
			if contact.Title == "" || len(contact.Emails) == 0 {
				continue
			}

			emails := make([]string, 0, len(contact.Emails))
			for _, email := range contact.Emails {
				emails = append(emails, email.Email)
			}

			configContacts[contact.Id] = &Contact{
				Title:  contact.Title,
				Emails: emails,
			}
		}

		account.UpdateContacts(configContacts)
		account.LastContactsSync = time.Now().UTC()
		account.Save()

		if err := saveContacts(account, configContacts); err != nil {
			return err
		}

		logger.Println("Contacts synchronized.")

		select {
		case <-time.After(time.Minute * 5):
		case <-quitCh:
			return nil
		}
	}
}

func SyncContacts(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) {
	// Keep synchronizing and retrying
	if err := syncContacts(account, accessToken, quitCh); err != nil {
		waitFor := 500 * time.Millisecond
		lastError := time.Now()

		for err != nil {
			logger.Printf("Error synchronizing contacts: %s", err)

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

			err = syncContacts(account, accessToken, quitCh)
		}
	}
}
