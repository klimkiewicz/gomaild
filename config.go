package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/user"
	"path"
	"strings"
	"sync"
	"time"

	"git.notmuchmail.org/git/notmuch.git/bindings/go/src/notmuch"
	"github.com/boltdb/bolt"
)

const configFileName = "mail.db"

var accountsBucket = []byte("accounts")

var ErrAccountDoesntExist = errors.New("account doesn't exist")
var ErrMessageDoesntExist = errors.New("message doesn't exist")

type Message struct {
	ModSeq    uint32    `json:"mod_seq"`
	NotmuchId string    `json:"notmuch_id"`
	Tags      []string  `json:"tags"`
	UID       uint32    `json:"uid"`
	DeletedAt time.Time `json:"deleted_at"`
}

type Account struct {
	Email        string `json:"email"`
	Name         string `json:"name"`
	RefreshToken string `json:"refresh_token"`
	Scope        string `json:"scope"`
	Disabled     bool   `json:"disabled"`
	Path         string `json:"-"`

	// IMAP state
	LastMailSync  time.Time `json:"last_mail_sync"`
	UIDValidity   uint32    `json:"uid_validity"`
	HighestModSeq uint32    `json:"highest_mod_seq"`

	// Contacts state
	LastContactsSync time.Time `json:"last_contacts_sync"`

	// Drafts state
	LastDraftsSync time.Time `json:"last_drafts_sync"`

	db *bolt.DB
}

func (a *Account) GetAllMessageIds() ([]uint32, error) {
	ids := make([]uint32, 0)

	err := a.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(a.Email)); b != nil {
			return b.ForEach(func(k, v []byte) error {
				ids = append(ids, binary.BigEndian.Uint32(k))
				return nil
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	} else {
		return ids, nil
	}
}

func (a *Account) GetMessage(uid uint32) (*Message, error) {
	var message Message

	err := a.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(a.Email)); b != nil {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uid)
			if value := b.Get(key); value != nil {
				return json.Unmarshal(value, &message)
			}
		}

		return ErrMessageDoesntExist
	})

	if err != nil {
		return nil, err
	} else {
		return &message, nil
	}
}

func (a *Account) RemoveMessage(uid uint32) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(a.Email)); b != nil {
			key := make([]byte, 4)
			binary.BigEndian.PutUint32(key, uid)
			return b.Delete(key)
		}
		return nil
	})
}

func (a *Account) LastSeenUID() (uint32, error) {
	var lastSeenUid uint32

	err := a.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(a.Email)); b != nil {
			if key, _ := b.Cursor().Last(); key != nil {
				lastSeenUid = binary.BigEndian.Uint32(key)
			}
		}

		return nil
	})

	if err != nil {
		return 0, err
	} else {
		return lastSeenUid, nil
	}
}

func (a *Account) UpdateMessage(message *Message) error {
	return a.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(a.Email))
		if err != nil {
			return err
		}

		key := make([]byte, 4)
		binary.BigEndian.PutUint32(key, message.UID)
		encoded, err := json.Marshal(message)
		if err != nil {
			return err
		}

		return b.Put(key, encoded)
	})
}

func (a *Account) Save() error {
	return a.db.Update(func(tx *bolt.Tx) error {
		if b := tx.Bucket(accountsBucket); b != nil {
			encoded, err := json.Marshal(a)
			if err != nil {
				return err
			}

			return b.Put([]byte(a.Email), encoded)
		}

		return ErrAccountDoesntExist
	})
}

// Empty local cache (when UIDVALIDITY changes).
func (a *Account) Empty() error {
	return nil
}

func (a *Account) OpenNotmuch(readOnly bool) (*notmuch.Database, error) {
	notmuchPath := path.Join(a.Path, ".notmuch")
	_, err := os.Stat(notmuchPath)

	var status notmuch.Status
	var notmuchDb *notmuch.Database

	if err != nil {
		if os.IsNotExist(err) {
			err := os.MkdirAll(a.Path, 0700)
			if err != nil {
				return nil, err
			}

			notmuchDb, status = notmuch.NewDatabase(a.Path)
		} else {
			return nil, err
		}
	} else {
		// There is a bug in notmuch.go which assigns 0 to both
		// DATABASE_MODE_READ_ONLY and DATABASE_MODE_READ_WRITE!
		var mode notmuch.DatabaseMode
		if !readOnly {
			mode = 1
		}
		notmuchDb, status = notmuch.OpenDatabase(a.Path, mode)
	}

	if status != notmuch.STATUS_SUCCESS {
		return nil, fmt.Errorf("error creating notmuch DB: %s", status)
	}

	return notmuchDb, nil
}

type Config struct {
	db        *bolt.DB
	mutex     *sync.RWMutex
	path      string
	quitChans map[string]chan struct{}
	wg        *sync.WaitGroup
}

func OpenConfig(dir string) (*Config, error) {
	if strings.HasPrefix(dir, "~/") {
		usr, err := user.Current()
		if err != nil {
			return nil, err
		}

		dir = strings.Replace(dir, "~", usr.HomeDir, 1)
	}

	if err := os.MkdirAll(dir, 0700); err != nil {
		return nil, err
	}

	if db, err := bolt.Open(path.Join(dir, configFileName), 0600, nil); err == nil {
		return &Config{
			db:        db,
			mutex:     &sync.RWMutex{},
			path:      dir,
			quitChans: make(map[string]chan struct{}),
			wg:        &sync.WaitGroup{},
		}, nil
	} else {
		return nil, err
	}
}

func (c *Config) syncAccount(account *Account, accessToken *AccessToken) {
	// syncAccount expects c.mutex to be locked.
	quitCh := make(chan struct{}, 1)
	c.quitChans[account.Email] = quitCh

	c.wg.Add(3)

	go func() {
		defer c.wg.Done()
		SyncMail(account, accessToken, quitCh)
	}()

	go func() {
		defer c.wg.Done()
		SyncContacts(account, accessToken, quitCh)
	}()

	go func() {
		defer c.wg.Done()
		SyncDrafts(account, accessToken, quitCh)
	}()
}

func (c *Config) AddAccount(oauthAccount *OAuthAccount) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	account := &Account{
		Path: path.Join(c.path, oauthAccount.Email),
	}

	err := c.db.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists(accountsBucket)
		if err != nil {
			return err
		}

		key := []byte(oauthAccount.Email)

		if value := b.Get(key); value != nil {
			if err := json.Unmarshal(value, &account); err != nil {
				return err
			}
		}

		account.Email = oauthAccount.Email
		account.Name = oauthAccount.Name
		account.RefreshToken = oauthAccount.RefreshToken
		account.Scope = oauthAccount.Scope
		account.db = c.db

		encoded, err := json.Marshal(account)
		if err != nil {
			return err
		}

		return b.Put(key, encoded)
	})

	if err == nil {
		if quitCh, ok := c.quitChans[account.Email]; ok {
			// Stop active synchronization if it's already running for this account.
			close(quitCh)
		}

		c.syncAccount(account, oauthAccount.AccessToken)
	}

	return err
}

func (c *Config) DisableAccount(email string) error {
	return nil
}

func (c *Config) GetAccount(email string) (*Account, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	var account Account

	err := c.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(accountsBucket); b != nil {
			if v := b.Get([]byte(email)); v != nil {
				return json.Unmarshal(v, &account)
			}
		}

		return ErrAccountDoesntExist
	})

	if err != nil {
		return nil, err
	} else {
		account.Path = path.Join(c.path, email)
		account.db = c.db
		return &account, nil
	}
}

func (c *Config) GetAccounts() ([]*Account, error) {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	accounts := make([]*Account, 0)

	err := c.db.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket(accountsBucket); b != nil {
			return b.ForEach(func(k, v []byte) error {
				var account Account
				err := json.Unmarshal(v, &account)

				if err == nil {
					account.Path = path.Join(c.path, account.Email)
					account.db = c.db
					accounts = append(accounts, &account)
				}

				return err
			})
		}

		return nil
	})

	if err != nil {
		return nil, err
	} else {
		return accounts, nil
	}
}

func (c *Config) StartSync() error {
	accounts, err := c.GetAccounts()
	if err != nil {
		return err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, account := range accounts {
		if account.Disabled {
			continue
		}

		if _, ok := c.quitChans[account.Email]; ok {
			continue
		}

		c.syncAccount(account, NewAccessToken(account.RefreshToken))
	}

	return nil
}

func (c *Config) Close() {
	c.mutex.Lock()
	// We never Unlock() so that other methods can't be run after Close().

	for _, quitCh := range c.quitChans {
		close(quitCh)
	}

	c.wg.Wait()
	c.db.Close()
}
