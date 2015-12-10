package main

import (
	"bufio"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/miGlanz/gomaild/common"
	"github.com/miGlanz/gomaild/gmail"
	"github.com/miGlanz/gomaild/notmuch"
	"github.com/miGlanz/gomaild/oauth"
	"golang.org/x/exp/inotify"
)

var errQuit = errors.New("quit")
var errTimeout = errors.New("imap timeout")

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
func syncMaildirFlags(db *notmuch.DB, fileName string, tags common.TagsSet) error {
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

		if err := db.ChangeMessageFileName(fileName, dst); err != nil {
			return err
		}
	}

	return nil
}

func bogofilterClassify(account *Account, message *gmail.Message) (common.TagsSet, error) {
	tagsPath := path.Join(account.Path, ".tags")
	f, err := os.Open(tagsPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		} else {
			return nil, err
		}
	}

	defer f.Close()

	names, err := f.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

	for _, tag := range names {
		tagPath := path.Join(tagsPath, tag)
		cmd := exec.Command("bogofilter", "-d", tagPath, "-T")
		defer cmd.Wait()

		stdIn, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}

		stdOut, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}

		if err := cmd.Start(); err != nil {
			return nil, err
		}

		if _, err := stdIn.Write(message.Body); err != nil {
			return nil, err
		}

		stdIn.Close()

		classification, err := ioutil.ReadAll(stdOut)
		if err != nil {
			return nil, err
		}

		logger.Println("classification", string(classification))
	}

	return nil, nil
}

func saveMessage(account *Account, db *notmuch.DB, mailbox *gmail.MailboxStatus, message *gmail.Message) error {
	bogofilterClassify(account, message)

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

	notmuchId, err := db.AddMessage(dst, message.Tags)
	if err != nil {
		return err
	}

	cacheMessage := &Message{
		GmailId:   message.ID,
		NotmuchId: notmuchId,
		Tags:      message.Tags.Slice(),
		UID:       message.UID,
	}

	if err = account.UpdateMessage(cacheMessage); err != nil {
		return err
	}

	return nil
}

func syncExisting(
	account *Account,
	c *gmail.Client,
	db *notmuch.DB,
	remoteIds, cacheIds common.UIDSlice,
	categories map[uint32]common.TagsSet,
	highestModSeq uint32,
) error {
	// cacheIds are sorted in descending order.
	lastSeenUID := cacheIds[0]

	remoteTags, err := c.FetchTagChanges(lastSeenUID, highestModSeq)
	if err != nil {
		return err
	}

	deleteRemote := make(common.UIDSlice, 0)
	deleteCache := make(common.UIDSlice, 0)

	for _, uid := range cacheIds {
		message, err := account.GetMessage(uid)
		if err != nil {
			return err
		}

		cacheTags := common.TagsSetFromSlice(message.Tags)

		categoryTags, ok := categories[uid]
		if !ok {
			categoryTags = make(common.TagsSet)
		}

		remoteTags, hasRemote := remoteTags[uid]
		if !remoteIds.Contains(uid) {
			// Message has been deleted in Gmail.
			remoteTags = common.TagsSet{"deleted": {}}
		} else if !hasRemote {
			if len(categoryTags) > 0 {
				categoryTags.Update(cacheTags)
				remoteTags = categoryTags
			} else {
				remoteTags = cacheTags
			}
		} else {
			remoteTags.Update(categoryTags)
		}

		localTags, err := db.GetMessageTags(message.NotmuchId)
		if err != nil {
			return err
		}

		allTags := make(common.TagsSet)
		allTags.Update(cacheTags, remoteTags, localTags)

		if allTags.Contains("deleted") {
			if !remoteTags.Contains("deleted") {
				deleteRemote = append(deleteRemote, uid)
			}

			if err := db.RemoveMessage(message.NotmuchId); err != nil {
				return err
			}

			deleteCache = append(deleteCache, uid)
		} else {
			var cacheChanged bool

			remoteAddTags := make(common.TagsSet)
			remoteRemoveTags := make(common.TagsSet)
			localAddTags := make(common.TagsSet)
			localRemoveTags := make(common.TagsSet)

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
				if err := db.UpdateTags(message.NotmuchId, localAddTags, localRemoveTags); err != nil {
					return err
				}
			}

			fileName, err := db.GetMessageFileName(message.NotmuchId)
			if err != nil {
				return err
			}

			if len(remoteAddTags) > 0 || len(remoteRemoveTags) > 0 {
				gmailId := message.GmailId

				if gmailId == "" {
					f, err := os.Open(fileName)
					if err != nil {
						return err
					}

					defer f.Close()

					scanner := bufio.NewScanner(f)
					for scanner.Scan() {
						line := scanner.Text()
						if strings.HasPrefix(line, "X-Gmail-Message-Id: ") {
							gmailId = line[len("X-Gmail-Message-Id: "):]
							message.GmailId = gmailId
							break
						}
					}
				}

				logger.Printf("Updating Gmail tags for message %s", gmailId)

				if err := c.UpdateMessageTags(gmailId, remoteAddTags, remoteRemoveTags); err != nil {
					return err
				}
			}

			if cacheChanged {
				if err := syncMaildirFlags(db, fileName, cacheTags); err != nil {
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

	// Get information about Gmail categories
	categories, err := c.GetCategories()
	if err != nil {
		return err
	}

	db, err := notmuch.Open(account.Path)
	if err != nil {
		return err
	}

	defer db.Close()

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
			if categoryTags, ok := categories[message.UID]; ok {
				message.Tags.Update(categoryTags)
			}

			if err := saveMessage(account, db, mailbox, message); err != nil {
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
				if err := saveMessage(account, db, mailbox, message); err != nil {
					return err
				}
			}
		}
	}

	// Sync existing messages.
	if len(cacheIds) > 0 {
		err = syncExisting(account, c, db, remoteIds, cacheIds, categories, account.HighestModSeq)
		if err != nil {
			return err
		}
	}

	// Update account state information.
	account.UIDValidity = mailbox.UIDValidity
	account.HighestModSeq = mailbox.HighestModSeq
	account.LastMailSync = time.Now()
	account.Save()

	return nil
}

func idle(account *Account, c *gmail.Client, quitCh <-chan struct{}) error {
	// IDLE and wait for new messages.
	if err := c.Idle(); err != nil {
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

	c, err := gmail.Dial(account.Email, accessToken)
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

		if err := idle(account, c, quitCh); err != nil {
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
