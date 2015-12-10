package notmuch

import (
	"fmt"
	"os"
	"path"

	"git.notmuchmail.org/git/notmuch.git/bindings/go/src/notmuch"
	"github.com/miGlanz/gomaild/common"
)

var ignoredTags = map[string]struct{}{
	"attachment": {},
	"signed":     {},
}

type DB struct {
	dir      string
	notmuch  *notmuch.Database
	writable bool
}

func openNotmuch(dir string, writable bool) (*notmuch.Database, error) {
	var db *notmuch.Database
	var status notmuch.Status
	notmuchDir := path.Join(dir, ".notmuch")

	if _, err := os.Stat(notmuchDir); err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(dir, 0700); err != nil {
				return nil, err
			}

			db, status = notmuch.NewDatabase(dir)
		} else {
			return nil, err
		}
	} else {
		var mode notmuch.DatabaseMode

		if writable {
			// There is a bug in notmuch.go which assigns 0 to both
			// DATABASE_MODE_READ_ONLY and DATABASE_MODE_READ_WRITE!
			mode = 1
		}

		db, status = notmuch.OpenDatabase(dir, mode)
	}

	if status != notmuch.STATUS_SUCCESS {
		return nil, fmt.Errorf("error opening notmuch DB: %s", status)
	}

	return db, nil
}

func Open(dir string) (*DB, error) {
	db, err := openNotmuch(dir, false)
	if err != nil {
		return nil, err
	}

	return &DB{
		dir:     dir,
		notmuch: db,
	}, nil
}

func (db *DB) checkWritable() error {
	if db.writable {
		return nil
	}

	db.notmuch.Close()

	writableDb, err := openNotmuch(db.dir, true)
	if err != nil {
		return err
	}

	db.notmuch = writableDb
	db.writable = true
	return nil
}

func (db *DB) AddMessage(fileName string, tags common.TagsSet) (string, error) {
	if err := db.checkWritable(); err != nil {
		return "", err
	}

	message, status := db.notmuch.AddMessage(fileName)
	if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
		return "", fmt.Errorf("error adding message to notmuch: %s", status)
	}

	defer message.Destroy()

	message.Freeze()
	defer message.Thaw()

	for tag, _ := range tags {
		message.AddTag(tag)
	}

	return message.GetMessageId(), nil
}

func checkFileExists(fileName string) (bool, error) {
	if _, err := os.Stat(fileName); err != nil {
		if os.IsNotExist(err) {
			return false, nil
		} else {
			return false, err
		}
	} else {
		return true, nil
	}
}

func (db *DB) getMessage(id string) (*notmuch.Message, error) {
	message, status := db.notmuch.FindMessage(id)
	if status != notmuch.STATUS_SUCCESS {
		return nil, fmt.Errorf("invlid notmuch status: %s", status)
	}

	if message.GetMessageId() == "" {
		message.Destroy()
		return nil, nil
	}

	fileName := message.GetFileName()

	if exists, err := checkFileExists(fileName); err != nil {
		return nil, err
	} else if !exists {
		message.Destroy()
		if err := db.checkWritable(); err != nil {
			return nil, err
		}

		status := db.notmuch.RemoveMessage(fileName)
		switch status {
		case notmuch.STATUS_DUPLICATE_MESSAGE_ID:
			return db.getMessage(id)
		case notmuch.STATUS_SUCCESS:
			return nil, nil
		default:
			return nil, fmt.Errorf("could not remove a file name from message: %s", status)
		}
	}

	return message, nil
}

func (db *DB) GetMessageTags(id string) (common.TagsSet, error) {
	message, err := db.getMessage(id)
	if err != nil {
		return nil, err
	}

	if message != nil {
		defer message.Destroy()
	}

	tags := make(common.TagsSet)

	if message == nil {
		tags.Add("deleted")
	} else {
		notmuchTags := message.GetTags()
		for notmuchTags.Valid() {
			tag := notmuchTags.Get()
			if _, ok := ignoredTags[tag]; !ok {
				tags.Add(tag)
			}
			notmuchTags.MoveToNext()
		}
	}

	return tags, nil
}

func (db *DB) UpdateTags(id string, add, remove common.TagsSet) error {
	if err := db.checkWritable(); err != nil {
		return err
	}

	message, status := db.notmuch.FindMessage(id)
	if status != notmuch.STATUS_SUCCESS {
		return fmt.Errorf("could not get notmuch message: %s", status)
	}

	defer message.Destroy()

	message.Freeze()
	defer message.Thaw()

	if add != nil {
		for tag, _ := range add {
			message.AddTag(tag)
		}
	}

	if remove != nil {
		for tag, _ := range remove {
			message.RemoveTag(tag)
		}
	}

	return nil
}

func (db *DB) ChangeMessageFileName(src, dst string) error {
	_, status := db.notmuch.AddMessage(dst)
	if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
		return fmt.Errorf("Error adding message to notmuch: %s", status)
	}

	status = db.notmuch.RemoveMessage(src)
	if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
		return fmt.Errorf("Error removing message from notmuch: %s", status)
	}

	return nil
}

func (db *DB) RemoveMessage(id string) error {
	message, err := db.getMessage(id)
	if err != nil {
		return err
	}

	if message == nil {
		return nil
	}

	fileName := message.GetFileName()
	message.Destroy()

	if err := db.checkWritable(); err != nil {
		return err
	}

	status := db.notmuch.RemoveMessage(fileName)

	switch status {
	case notmuch.STATUS_SUCCESS:
		if err := os.Remove(fileName); err != nil {
			return err
		}
		return nil
	case notmuch.STATUS_DUPLICATE_MESSAGE_ID:
		if err := os.Remove(fileName); err != nil {
			return err
		}
		return db.RemoveMessage(id)
	default:
		return fmt.Errorf("could not remove notmuch message: %s", status)
	}
}

func (db *DB) GetMessageFileName(id string) (string, error) {
	message, err := db.getMessage(id)
	if err != nil {
		return "", err
	}

	if message == nil {
		return "", fmt.Errorf("no such message: %s", id)
	} else {
		return message.GetFileName(), nil
	}
}

func (db *DB) Close() {
	db.notmuch.Close()
}
