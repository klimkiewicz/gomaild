package main

import (
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"git.notmuchmail.org/git/notmuch.git/bindings/go/src/notmuch"
	"github.com/miGlanz/gomaild/common"
	"github.com/miGlanz/gomaild/gmail"
	"github.com/miGlanz/gomaild/oauth"
	"golang.org/x/exp/inotify"
)

var errQuit = errors.New("quit")
var errTimeout = errors.New("imap timeout")

var notmuchIgnoredTags = map[string]struct{}{
	"attachment": {},
	"signed":     {},
}

var maildirDirs = []string{
	"cur", "new", "tmp",
}

func tagsToMaildirFlags(tags common.TagsSet) string {
	flags := make([]string, 0)

	if tags.Contains("flagged") {
		flags = append(flags, "F")
	}

	if tags.Contains("passed") {
		flags = append(flags, "P")
	}

	if tags.Contains("replied") {
		flags = append(flags, "R")
	}

	if !tags.Contains("unread") {
		flags = append(flags, "S")
	}

	sort.Strings(flags)
	return strings.Join(flags, "")
}

// Make sure the maildir filename is in sync with tags.
func syncMaildirFlags(db *notmuch.Database, fileName string, tags common.TagsSet) error {
	var dst string
	flags := tagsToMaildirFlags(tags)
	base := path.Base(fileName)
	dir := path.Dir(fileName)

	if strings.Contains(fileName, ":2,") {
		// We're in cur
		parts := strings.Split(base, ":2,")

		if parts[1] != flags {
			dst = path.Join(dir, strings.Join([]string{
				parts[0],
				flags,
			}, ":2,"))
		}
	} else if flags != "" {
		// We're in new - need to move the message to cur
		parentDir := path.Base(dir)
		if parentDir != "new" {
			// Something's wrong - it shoud be "new"!
			return fmt.Errorf("Wrong message path: %s", fileName)
		}

		dir = path.Dir(dir)
		dst = path.Join(dir, "cur", fmt.Sprintf("%s:2,%s", base, flags))
	}

	if dst != "" {
		if err := os.Rename(fileName, dst); err != nil {
			return err
		}

		_, status := db.AddMessage(dst)
		if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
			return fmt.Errorf("Error adding message to notmuch: %s", status)
		}

		status = db.RemoveMessage(fileName)
		if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
			return fmt.Errorf("Error removing message from notmuch: %s", status)
		}
	}

	return nil
}

func saveMessage(account *Account, mailbox *gmail.MailboxStatus, message *gmail.Message) error {
	// First save the file to tmp dir.
	fileName := fmt.Sprintf("%d.%d", mailbox.UIDValidity, message.UID)
	tmpName := path.Join(account.Path, "All", "tmp", fileName)
	f, err := os.Create(tmpName)
	if err != nil {
		return err
	}

	n, err := f.Write(message.Body)
	f.Close()
	if err != nil {
		return err
	} else if n != len(message.Body) {
		return errors.New("could not write full message body to file")
	}

	var dst string

	if flags := tagsToMaildirFlags(message.Tags); flags != "" {
		dst = path.Join(account.Path, "All", "cur", fmt.Sprintf("%s:2,%s", fileName, flags))
	} else {
		dst = path.Join(account.Path, "All", "new", fileName)
	}

	// Now move to either cur or new.
	if err := os.Rename(tmpName, dst); err != nil {
		return err
	}

	notmuchDb, err := account.OpenNotmuch(false)
	if err != nil {
		return err
	}

	defer notmuchDb.Close()

	notmuchMsg, status := notmuchDb.AddMessage(dst)
	if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
		return fmt.Errorf("error adding message to notmuch: %s", status)
	}

	defer notmuchMsg.Destroy()

	notmuchMsg.Freeze()
	for tag, _ := range message.Tags {
		notmuchMsg.AddTag(tag)
	}
	notmuchMsg.Thaw()

	cacheMessage := &Message{
		NotmuchId: notmuchMsg.GetMessageId(),
		Tags:      message.Tags.Slice(),
		UID:       message.UID,
	}

	if err = account.UpdateMessage(cacheMessage); err != nil {
		return err
	}

	return nil
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

func removeNotmuchFileName(db *notmuch.Database, message *notmuch.Message, fileName string) error {
	if status := db.RemoveMessage(fileName); status == notmuch.STATUS_DUPLICATE_MESSAGE_ID {
		fileName := message.GetFileName()
		if exists, err := checkFileExists(fileName); err != nil {
			return err
		} else if !exists {
			return removeNotmuchFileName(db, message, fileName)
		}
	}

	return nil
}

func getNotmuchMessage(db *notmuch.Database, messageId string) (*notmuch.Message, error) {
	message, status := db.FindMessage(messageId)
	if status != notmuch.STATUS_SUCCESS {
		return nil, fmt.Errorf("invlid notmuch status: %s", status)
	}

	if message.GetMessageId() == "" {
		message.Destroy()
		return nil, nil
	}

	if message != nil && message.GetMessageId() == "" {
		message.Destroy()
		return nil, nil
	}

	return message, nil
}

func getNotmuchTags(message *notmuch.Message) common.TagsSet {
	localTags := make(common.TagsSet)

	if message == nil {
		localTags.Add("deleted")
	} else {
		notmuchTags := message.GetTags()
		for notmuchTags.Valid() {
			tag := notmuchTags.Get()
			if _, ok := notmuchIgnoredTags[tag]; !ok {
				localTags.Add(tag)
			}
			notmuchTags.MoveToNext()
		}
	}

	return localTags
}

func syncExisting(
	account *Account,
	c *gmail.Client,
	remoteIds, cacheIds common.UIDSlice,
	highestModSeq uint32,
) error {
	// cacheIds are sorted in descending order.
	lastSeenUID := cacheIds[0]

	remoteTags, err := c.FetchTagChanges(lastSeenUID, highestModSeq)
	if err != nil {
		return err
	}

	readOnly := true
	notmuchDb, err := account.OpenNotmuch(readOnly)
	if err != nil {
		return err
	}

	defer func() {
		// We use the closure because notmuchDb may change (swtiching from
		// read-only to read-write).
		notmuchDb.Close()
	}()

	deleteRemote := make(common.UIDSlice, 0)
	deleteCache := make(common.UIDSlice, 0)

	for _, uid := range cacheIds {
		message, err := account.GetMessage(uid)
		if err != nil {
			return err
		}

		cacheTags := common.TagsSetFromSlice(message.Tags)

		remoteTags, hasRemote := remoteTags[uid]
		if !remoteIds.Contains(uid) {
			// Message has been deleted in Gmail.
			remoteTags = common.TagsSet{"deleted": {}}
		} else if !hasRemote {
			remoteTags = cacheTags
		}

		notmuchMessage, err := getNotmuchMessage(notmuchDb, message.NotmuchId)
		if err != nil {
			return err
		}

		if notmuchMessage != nil {
			fileName := notmuchMessage.GetFileName()

			if exists, err := checkFileExists(fileName); err != nil {
				return err
			} else if !exists {
				if readOnly {
					notmuchDb.Close()
					if notmuchDb, err = account.OpenNotmuch(false); err != nil {
						return err
					}
					readOnly = false
				}

				if err = removeNotmuchFileName(notmuchDb, notmuchMessage, fileName); err != nil {
					return err
				}
			}
		}

		localTags := getNotmuchTags(notmuchMessage)

		allTags := make(common.TagsSet)
		allTags.Update(cacheTags, remoteTags, localTags)

		if allTags.Contains("deleted") {
			if !remoteTags.Contains("deleted") {
				deleteRemote = append(deleteRemote, uid)
			}

			if notmuchMessage != nil {
				fileName := notmuchMessage.GetFileName()
				notmuchMessage.Destroy()

				if readOnly {
					notmuchDb.Close()
					if notmuchDb, err = account.OpenNotmuch(false); err != nil {
						return err
					}
					readOnly = false
				}

				status := notmuchDb.RemoveMessage(fileName)
				if status != notmuch.STATUS_SUCCESS && status != notmuch.STATUS_DUPLICATE_MESSAGE_ID {
					return fmt.Errorf("couldn't remove message from notmuch: %s", status)
				}

				if err := os.Remove(fileName); err != nil {
					return err
				}
			}

			deleteCache = append(deleteCache, uid)
		} else {
			var cacheChanged bool

			remoteAddTags := make(common.TagsSet)
			remoteRemoveTags := make(common.TagsSet)
			localAddTags := make(common.TagsSet)
			localRemoveTags := make(common.TagsSet)

			fileName := notmuchMessage.GetFileName()

			for tag, _ := range allTags {
				inLocal := localTags.Contains(tag)
				inCache := cacheTags.Contains(tag)
				inRemote := remoteTags.Contains(tag)

				switch {
				case inLocal == inRemote && inLocal != inCache:
					// both notmuch and remote changed - update cache
					cacheTags.Set(tag, inLocal)
					cacheChanged = true
				case inLocal != inCache && inCache == inRemote:
					// notmuch changed - update remote
					if inLocal {
						remoteAddTags.Add(tag)
					} else {
						remoteRemoveTags.Add(tag)
					}
					cacheTags.Set(tag, inLocal)
					cacheChanged = true
				case inLocal == inCache && inCache != inRemote:
					// remote changed - update notmuch
					if inRemote {
						localAddTags.Add(tag)
					} else {
						localRemoveTags.Add(tag)
					}
					cacheTags.Set(tag, inRemote)
					cacheChanged = true
				}
			}

			if len(localAddTags) > 0 || len(localRemoveTags) > 0 {
				if readOnly {
					notmuchMessage.Destroy()
					notmuchDb.Close()
					if notmuchDb, err = account.OpenNotmuch(false); err != nil {
						return err
					}
					readOnly = false

					notmuchMessage, err = getNotmuchMessage(notmuchDb, message.NotmuchId)
					if err != nil {
						return err
					}
				}

				notmuchMessage.Freeze()
				for tag, _ := range localAddTags {
					notmuchMessage.AddTag(tag)
				}
				for tag, _ := range localRemoveTags {
					notmuchMessage.RemoveTag(tag)
				}
				notmuchMessage.Thaw()
			}

			if notmuchMessage != nil {
				notmuchMessage.Destroy()
			}

			if len(remoteAddTags) > 0 {
				if err := c.AddMessageTags(uid, remoteAddTags); err != nil {
					return err
				}
			}

			if len(remoteRemoveTags) > 0 {
				if err := c.RemoveMessageTags(uid, remoteRemoveTags); err != nil {
					return err
				}
			}

			if cacheChanged {
				if err := syncMaildirFlags(notmuchDb, fileName, cacheTags); err != nil {
					return err
				}

				slice := cacheTags.Slice()
				sort.Strings(slice)
				message.Tags = slice
				account.UpdateMessage(message)
			}
		}
	}

	if len(deleteRemote) > 0 {
		if err := c.DeleteMessages(deleteRemote); err != nil {
			return err
		}
	}

	for _, uid := range deleteCache {
		if err := account.RemoveMessage(uid); err != nil {
			return err
		}
	}

	return nil
}

func syncMailOnce(account *Account, c *gmail.Client, quitCh <-chan struct{}) error {
	mailbox, err := c.SelectAll()
	if err != nil {
		return err
	}

	defer c.Close()

	if mailbox.UIDValidity != account.UIDValidity {
		// Local cache is invalid - delete.
		fmt.Println("DIFFERENT UIDVALIDITY")

		account.UIDValidity = mailbox.UIDValidity
		account.Save()
	}

	remoteIds, err := c.FetchAllUIDs()
	if err != nil {
		return err
	}

	cacheIds, err := account.GetAllUIDs()
	if err != nil {
		return err
	}

	// Determine which messages are new (are not present in cache).
	newMessageIds := remoteIds.Diff(cacheIds)

	// Fetch new messages
	var idx int
	count := 1

	for idx < len(newMessageIds) {
		select {
		case <-quitCh:
			return errQuit
		default:
		}

		start := idx
		stop := idx + count
		if stop > len(newMessageIds) {
			stop = len(newMessageIds)
		}

		messages, err := c.FetchMessages(newMessageIds[start:stop])
		if err != nil {
			return err
		}

		for _, message := range messages {
			if err := saveMessage(account, mailbox, message); err != nil {
				return err
			}
		}

		idx += count

		count *= 2
		if count > 10 {
			count = 10
		}

		if c.HasNewMail() {
			freshIds, err := c.FetchNewUIDs()
			if err != nil {
				return err
			}

			messages, err := c.FetchMessages(freshIds)
			if err != nil {
				return err
			}

			for _, message := range messages {
				if err := saveMessage(account, mailbox, message); err != nil {
					return err
				}
			}
		}
	}

	// Sync existing messages.
	if len(cacheIds) > 0 {
		err = syncExisting(account, c, remoteIds, cacheIds, account.HighestModSeq)
		if err != nil {
			return err
		}
	}

	// Update account state information.
	account.UIDValidity = mailbox.UIDValidity
	account.HighestModSeq = mailbox.HighestModSeq
	account.LastMailSync = time.Now()
	account.Save()

	// IDLE and wait for new messages.
	if err = c.Idle(); err != nil {
		return err
	}

	defer c.IdleTerm()

	syncTimeoutCh := time.After(time.Minute * 10)
	connTicker := time.NewTicker(time.Minute * 4)
	defer connTicker.Stop()

	watcher, err := inotify.NewWatcher()
	if err != nil {
		return err
	}

	defer watcher.Close()

	watchPath := path.Join(account.Path, ".notmuch", "xapian", "flintlock")
	err = watcher.AddWatch(watchPath, inotify.IN_CLOSE_WRITE)
	if err != nil {
		return err
	}

	for {
		select {
		case <-watcher.Event:
			// Wait until we have 30 seconds without inotify events.
			for {
				select {
				case <-time.After(time.Second * 30):
					return nil
				case <-watcher.Event:
				case <-quitCh:
					return errQuit
				}
			}
			return nil
		case <-syncTimeoutCh:
			return nil
		case <-connTicker.C:
			// Send IDLE TERM and IDLE again to keep connection alive.
			if err := c.IdleTerm(); err != nil {
				return err
			}
			if err := c.Idle(); err != nil {
				return err
			}
		case <-quitCh:
			return errQuit
		case err := <-c.IdleWait(time.Second):
			if err != nil {
				return err
			}

			if c.HasNewMail() {
				return nil
			}
		}
	}
}

func syncMail(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) error {
	// Make Maildir (cur, new, tmp) directories if necessary.
	for _, dir := range maildirDirs {
		if err := os.MkdirAll(path.Join(account.Path, "All", dir), 0700); err != nil {
			return err
		}
	}

	token, err := accessToken.Get()
	if err != nil {
		return err
	}

	c, err := gmail.Dial(account.Email, token)
	if err != nil {
		return err
	}

	defer c.Logout()

	for {
		if err := syncMailOnce(account, c, quitCh); err != nil {
			if err == errQuit {
				return nil
			} else {
				return err
			}
		}
	}
}

func SyncMail(account *Account, accessToken *oauth.AccessToken, quitCh chan struct{}) {
	// Keep synchronizing and retrying
	if err := syncMail(account, accessToken, quitCh); err != nil {
		waitFor := 500 * time.Millisecond
		lastError := time.Now()

		for err != nil {
			logger.Printf("Error synchronizing mail: %s", err)

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

			err = syncMail(account, accessToken, quitCh)
		}
	}
}
