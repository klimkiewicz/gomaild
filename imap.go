package main

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"git.notmuchmail.org/git/notmuch.git/bindings/go/src/notmuch"
	"github.com/mxk/go-imap/imap"
	"golang.org/x/exp/inotify"
)

const gmailIMAPAddr = "imap.gmail.com:993"

const messagesPerSearch = 5000

const compressionLevel = 9

// Gmail special folder flags
const (
	folderFlagAll       = "\\All"
	folderFlagDraft     = "\\Draft"
	folderFlagDrafts    = "\\Drafts"
	folderFlagFlagged   = "\\Flagged"
	folderFlagImportant = "\\Important"
	folderFlagInbox     = "\\Inbox"
	folderFlagJunk      = "\\Junk"
	folderFlagSent      = "\\Sent"
	folderFlagStarred   = "\\Starred"
	folderFlagTrash     = "\\Trash"
)

const gmailHeaders = `
X-Gmail-Message-Id: %s
X-Gmail-Thread-Id: %s

`

var ErrInconsistentState = errors.New("inconsistent state")

// Add HIGHESTMODSEQ filter for CONDSTORE extension
var selectFilter = imap.LabelFilter(
	"EXISTS",
	"FLAGS",
	"HIGHESTMODSEQ",
	"PERMANENTFLAGS",
	"RECENT",
	"UIDNEXT",
	"UIDNOTSTICKY",
	"UIDVALIDITY",
	"UNSEEN",
)

var notmuchIgnoredTags = map[string]struct{}{
	"attachment": {},
	"signed":     {},
}

var maildirDirs = []string{
	"cur", "new", "tmp",
}

var fetchDescriptors = []string{
	"BODY.PEEK[]",
	"FLAGS",
	"X-GM-LABELS",
	"X-GM-MSGID",
	"X-GM-THRID",
}

type deleteReq struct {
	uid    uint32
	path   string
	remote bool
}

type xOAuth []byte

func newXOAuth(email string, token string) xOAuth {
	return []byte("user=" + email + "\x01auth=Bearer " + token + "\x01\x01")
}

func (x xOAuth) Start(s *imap.ServerInfo) (mech string, ir []byte, err error) {
	return "XOAUTH2", x, nil
}

func (x xOAuth) Next(challenge []byte) (response []byte, err error) {
	return []byte(""), nil
}

type UIDSlice []uint32

func (uid UIDSlice) Len() int {
	return len(uid)
}

func (uid UIDSlice) Less(i, j int) bool {
	return uid[i] < uid[j]
}

func (uid UIDSlice) Swap(i, j int) {
	uid[i], uid[j] = uid[j], uid[i]
}

func imapLabelsToTags(info *imap.MessageInfo) []string {
	tags := make([]string, 0)

	if !info.Flags["\\Seen"] {
		tags = append(tags, "unread")
	}

	for _, field := range info.Attrs["X-GM-LABELS"].([]imap.Field) {
		field := field.(string)

		// Handle special labels, such as "\Inbox", "\Important"
		if strings.HasPrefix(field, `"\\`) && strings.HasSuffix(field, "\"") {
			field = strings.ToLower(field[3 : len(field)-1])
			if field == "starred" {
				field = "flagged"
			}
		}

		tags = append(tags, field)
	}

	return tags
}

func tagsToMaildirFlags(tags map[string]struct{}) string {
	flags := make([]string, 0)

	if _, flagged := tags["flagged"]; flagged {
		flags = append(flags, "F")
	}

	if _, passed := tags["passed"]; passed {
		flags = append(flags, "P")
	}

	if _, replied := tags["replied"]; replied {
		flags = append(flags, "R")
	}

	if _, unread := tags["unread"]; !unread {
		flags = append(flags, "S")
	}

	sort.Strings(flags)
	return strings.Join(flags, "")
}

// Make sure the maildir filename is in sync with tags.
func syncMaildirFlags(db *notmuch.Database, message *notmuch.Message, tags map[string]struct{}) error {
	var dst string
	flags := tagsToMaildirFlags(tags)
	fileName := message.GetFileName()
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

func saveNewMessage(
	account *Account,
	resp *imap.Response,
	uidValidity uint32,
) error {
	messageInfo := resp.MessageInfo()
	uid := messageInfo.UID
	seen := messageInfo.Flags["\\Seen"]

	var bufSize int
	for _, literal := range resp.Literals {
		bufSize += int(literal.Info().Len)
	}

	buf := bytes.NewBuffer(make([]byte, 0, bufSize))
	for _, literal := range resp.Literals {
		if _, err := literal.WriteTo(buf); err != nil {
			return err
		}
	}

	fileName := fmt.Sprintf("%d.%d", uidValidity, uid)
	tmpName := path.Join(account.Path, "All", "tmp", fileName)
	f, err := os.Create(tmpName)
	if err != nil {
		return err
	}

	b := buf.Bytes()
	b = bytes.Replace(b, []byte("\r\n"), []byte("\n"), -1)

	// Add X-Gmail-Message-Id and X-Gmail-Thread-Id special headers.
	gmailMessageId := messageInfo.Attrs["X-GM-MSGID"].(string)
	gmailThreadId := messageInfo.Attrs["X-GM-THRID"].(string)
	headers := fmt.Sprintf(gmailHeaders, gmailMessageId, gmailThreadId)
	b = bytes.Replace(b, []byte("\n\n"), []byte(headers), 1)

	_, err = f.Write(b)
	f.Close()
	if err != nil {
		return err
	}

	var dst string

	if seen {
		dst = path.Join(account.Path, "All", "cur", fmt.Sprintf("%s:2,S", fileName))
	} else {
		dst = path.Join(account.Path, "All", "new", fileName)
	}

	os.Rename(tmpName, dst)

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

	tags := imapLabelsToTags(messageInfo)

	notmuchMsg.Freeze()
	for _, tag := range tags {
		notmuchMsg.AddTag(tag)
	}
	notmuchMsg.Thaw()

	message := &Message{
		NotmuchId: notmuchMsg.GetMessageId(),
		Tags:      tags,
		UID:       uid,
	}

	if err = account.UpdateMessage(message); err != nil {
		return err
	}

	return nil
}

func handleNewMailEvent(
	account *Account,
	c *imap.Client,
) error {
	lastSeenUID, err := account.LastSeenUID()
	if err != nil {
		return err
	}

	var seq imap.SeqSet
	seq.AddRange(lastSeenUID+1, 0)

	cmd, err := c.UIDFetch(&seq, fetchDescriptors...)
	if err != nil {
		return err
	}

	_, err = imap.Wait(cmd, nil)
	if err != nil {
		return err
	}

	for _, resp := range cmd.Data {
		if err := saveNewMessage(account, resp, c.Mailbox.UIDValidity); err != nil {
			return err
		}
	}

	return nil
}

func syncSingleMessage(
	account *Account,
	c *imap.Client,
	notmuchDb *notmuch.Database,
	uid uint32,
	remoteTags []string,
	hasRemote bool,
) (*deleteReq, error) {
	message, err := account.GetMessage(uid)
	if err != nil {
		return nil, err
	}

	allTags := make(map[string]struct{})

	// Bolt
	cache := make(map[string]struct{})
	for _, tag := range message.Tags {
		cache[tag] = struct{}{}
		allTags[tag] = struct{}{}
	}

	notmuchMessage, status := notmuchDb.FindMessage(message.NotmuchId)
	if status != notmuch.STATUS_SUCCESS {
		return nil, fmt.Errorf("invlid notmuch status: %s", status)
	}

	if notmuchMessage.GetMessageId() == "" {
		notmuchMessage.Destroy()
		notmuchMessage = nil
	}

	if notmuchMessage != nil {
		// Check if the file notmuch points to exists.
		path := notmuchMessage.GetFileName()
		if _, err := os.Stat(path); err != nil {
			if os.IsNotExist(err) {
				// The file that notmuch points to doesn't exist. It means
				// there is some consistency issue. Let's remove the message
				// from notmuch and bolt and return an error. The sync process
				// will restart and everything should be back to normal.
				notmuchMessage.Destroy()
				notmuchDb.RemoveMessage(path)
				account.RemoveMessage(uid)
				return nil, ErrInconsistentState
			} else {
				return nil, err
			}
		} else {
			defer notmuchMessage.Destroy()
		}
	}

	// Notmuch
	local := make(map[string]struct{})
	if notmuchMessage == nil {
		local["deleted"] = struct{}{}
		allTags["deleted"] = struct{}{}
	} else {
		notmuchTags := notmuchMessage.GetTags()
		for notmuchTags.Valid() {
			tag := notmuchTags.Get()
			if _, ok := notmuchIgnoredTags[tag]; !ok {
				local[tag] = struct{}{}
				allTags[tag] = struct{}{}
			}
			notmuchTags.MoveToNext()
		}
	}

	// Gmail
	var remote map[string]struct{}
	if hasRemote {
		remote = make(map[string]struct{})
		for _, tag := range remoteTags {
			remote[tag] = struct{}{}
			allTags[tag] = struct{}{}
		}
	} else {
		remote = cache
	}

	if _, hasDeletedFlag := allTags["deleted"]; hasDeletedFlag {
		_, remoteDeleted := remote["deleted"]

		req := deleteReq{
			uid:    uid,
			remote: !remoteDeleted,
		}

		if notmuchMessage != nil {
			req.path = notmuchMessage.GetFileName()
		}

		return &req, nil
	} else {
		var cacheChanged bool
		notmuchMessage.Freeze()

		var seqSet imap.SeqSet
		seqSet.AddNum(uid)
		imapAddLabels := make([]imap.Field, 0)
		imapRemoveLabels := make([]imap.Field, 0)

		// Determine required actions for each tag
		for tag, _ := range allTags {
			_, inLocal := local[tag]
			_, inCache := cache[tag]
			_, inRemote := remote[tag]

			switch {
			case inLocal == inRemote && inLocal != inCache:
				// both notmuch and remote changed - update cache
				if inLocal {
					cache[tag] = struct{}{}
				} else {
					delete(cache, tag)
				}
				cacheChanged = true
			case inLocal != inCache && inCache == inRemote:
				// notmuch changed - update remote
				if tag == "unread" {
					if inLocal {
						if _, err := imap.Wait(c.UIDStore(&seqSet, "-FLAGS.SILENT", "\\Seen")); err != nil {
							return nil, err
						}
					} else {
						if _, err := imap.Wait(c.UIDStore(&seqSet, "+FLAGS.SILENT", "\\Seen")); err != nil {
							return nil, err
						}
					}
				} else {
					var label string

					switch tag {
					case "flagged":
						label = "\\Starred"
					case "inbox", "important", "sent":
						label = "\\" + strings.Title(tag)
					default:
						label = tag
					}

					if inLocal {
						imapAddLabels = append(imapAddLabels, label)
					} else {
						imapRemoveLabels = append(imapRemoveLabels, label)
					}
				}

				if inLocal {
					cache[tag] = struct{}{}
				} else {
					delete(cache, tag)
				}

				cacheChanged = true
			case inLocal == inCache && inCache != inRemote:
				// remote changed - update notmuch
				if inRemote {
					notmuchMessage.AddTag(tag)
					cache[tag] = struct{}{}
				} else {
					notmuchMessage.RemoveTag(tag)
					delete(cache, tag)
				}
				cacheChanged = true
			}
		}

		notmuchMessage.Thaw()

		if len(imapAddLabels) > 0 {
			if _, err := imap.Wait(c.UIDStore(&seqSet, "+X-GM-LABELS", imapAddLabels)); err != nil {
				return nil, err
			}
		}

		if len(imapRemoveLabels) > 0 {
			if _, err := imap.Wait(c.UIDStore(&seqSet, "-X-GM-LABELS", imapRemoveLabels)); err != nil {
				return nil, err
			}
		}

		if cacheChanged {
			if err := syncMaildirFlags(notmuchDb, notmuchMessage, cache); err != nil {
				return nil, err
			}

			newCacheTags := make([]string, 0, len(cache))
			for tag, _ := range cache {
				newCacheTags = append(newCacheTags, tag)
			}
			sort.Strings(newCacheTags)

			message.Tags = newCacheTags
			account.UpdateMessage(message)
		}
	}

	return nil, nil
}

func syncExistingMessages(
	account *Account,
	c *imap.Client,
	remoteIds, cacheIds UIDSlice,
	highestModSeq uint32,
	folderTrash string,
) error {
	remoteTags := make(map[uint32][]string)

	// Detect changes to existing messages on Gmail server
	if account.HighestModSeq != 0 && account.HighestModSeq != highestModSeq {
		lastSeenUID, err := account.LastSeenUID()
		if err != nil {
			return err
		}

		var seq imap.SeqSet
		seq.AddRange(1, lastSeenUID)
		fields := []imap.Field{"FLAGS", "X-GM-LABELS"}
		changedSince := []imap.Field{"CHANGEDSINCE", account.HighestModSeq}
		cmd, err := imap.Wait(c.Send("UID FETCH", seq, fields, changedSince))
		if err != nil {
			return err
		}

		for _, resp := range cmd.Data {
			messageInfo := resp.MessageInfo()
			tags := imapLabelsToTags(messageInfo)
			remoteTags[messageInfo.UID] = tags
		}
	}

	notmuchDb, err := account.OpenNotmuch(false)
	if err != nil {
		return err
	}

	defer notmuchDb.Close()

	deleteReqs := make([]*deleteReq, 0)

	for _, uid := range cacheIds {
		remoteTags, hasRemote := remoteTags[uid]

		// Maybe Gmail message has been deleted - check in remoteIds.
		i := sort.Search(len(remoteIds), func(i int) bool {
			return remoteIds[i] <= uid
		})
		remoteDeleted := i >= len(remoteIds) || remoteIds[i] != uid

		if remoteDeleted {
			remoteTags = []string{"deleted"}
		}

		deleteReq, err := syncSingleMessage(account, c, notmuchDb, uid, remoteTags, hasRemote)
		if err != nil {
			return err
		}

		if deleteReq != nil {
			deleteReqs = append(deleteReqs, deleteReq)
		}
	}

	toDeleteSeqSet := &imap.SeqSet{}

	for _, deleteReq := range deleteReqs {
		if deleteReq.remote {
			toDeleteSeqSet.AddNum(deleteReq.uid)
		}
	}

	// Delete message on server (ie. copy to [Gmail]/Trash).
	if !toDeleteSeqSet.Empty() {
		if _, err := imap.Wait(c.UIDCopy(toDeleteSeqSet, folderTrash)); err != nil {
			return err
		}
	}

	for _, req := range deleteReqs {
		if req.path != "" {
			// Remove the message from notmuch.
			notmuchDb.RemoveMessage(req.path)

			// Remove the message from the filesystem.
			if err := os.Remove(req.path); err != nil {
				return err
			}
		}

		// Remove the message from bolt.
		if err := account.RemoveMessage(req.uid); err != nil {
			return err
		}
	}

	return nil
}

func syncMailOnce(account *Account, accessToken *AccessToken, quitCh chan struct{}) error {
	// Make Maildir (cur, new, tmp) directories if necessary.
	for _, dir := range maildirDirs {
		if err := os.MkdirAll(path.Join(account.Path, "All", dir), 0700); err != nil {
			return err
		}
	}

	c, err := imap.DialTLS(gmailIMAPAddr, nil)
	if err != nil {
		return err
	}

	//c.SetLogMask(imap.LogAll)
	c.SetLogMask(imap.LogConn | imap.LogState | imap.LogCmd)

	c.CommandConfig["SELECT"].Filter = selectFilter
	c.CommandConfig["EXAMINE"].Filter = selectFilter

	defer c.Logout(3 * time.Second)

	c.Data = nil

	if c.State() == imap.Login {
		token, err := accessToken.Get()
		if err != nil {
			return err
		}

		if _, err := c.Auth(newXOAuth(account.Email, token)); err != nil {
			return err
		}
	}

	// Enable compression
	_, err = c.CompressDeflate(compressionLevel)
	if err != nil && err != imap.ErrCompressionActive {
		return err
	}

	// Fetch Gmail special folder names (they are locale-dependent)
	cmd, err := imap.Wait(c.List("", "[Gmail]/%"))
	if err != nil {
		return err
	}

	// Determine the name of "All" folder
	var folderAll, folderTrash string

	for _, resp := range cmd.Data {
		info := resp.MailboxInfo()
		if info.Attrs[folderFlagAll] {
			folderAll = info.Name
		} else if info.Attrs[folderFlagTrash] {
			folderTrash = info.Name
		}
	}

	if folderAll == "" || folderTrash == "" {
		return errors.New("could not determine the name of All or Trash folder")
	}

	lastInteraction := time.Now()

	for {
		c.Data = nil

		if time.Since(lastInteraction) >= time.Minute*5 {
			return errors.New("imap timeout")
		}

		cmd, err = c.Select(folderAll, false)
		if err != nil {
			return err
		}

		if c.Mailbox.UIDValidity != account.UIDValidity {
			// Local cache is invalid - delete.
			fmt.Println("DIFFERENT UIDVALIDITY")

			account.UIDValidity = c.Mailbox.UIDValidity
			account.Save()
		}

		var highestModSeq uint32

		for _, resp := range cmd.Data {
			if resp.Label == "HIGHESTMODSEQ" {
				highestModSeq = resp.Fields[1].(uint32)
				break
			}
		}

		remoteIds := make(UIDSlice, 0, c.Mailbox.Messages)
		steps := c.Mailbox.Messages / messagesPerSearch
		if steps*messagesPerSearch < c.Mailbox.Messages {
			steps += 1
		}

		var i, start, stop uint32
		cmds := make([]*imap.Command, steps)
		stop = c.Mailbox.Messages

		for i = 0; i < steps; i++ {
			var seq imap.SeqSet

			if stop > messagesPerSearch {
				start = stop - messagesPerSearch + 1
			} else {
				start = 1
			}

			seq.AddRange(start, stop)

			cmd, err = c.UIDSearch(seq)
			if err != nil {
				return err
			}

			cmds[i] = cmd
			stop = start - 1
		}

		for _, cmd := range cmds {
			if _, err = imap.Wait(cmd, nil); err != nil {
				return err
			}

			for _, resp := range cmd.Data {
				remoteIds = append(remoteIds, resp.SearchResults()...)
			}
		}

		sort.Sort(sort.Reverse(remoteIds))

		cacheIds, err := account.GetAllMessageIds()
		if err != nil {
			return err
		}

		cacheMap := make(map[uint32]struct{})
		for _, uid := range cacheIds {
			cacheMap[uid] = struct{}{}
		}

		// Fetch new messages
		newMessageIds := make(UIDSlice, 0)
		for _, uid := range remoteIds {
			if _, ok := cacheMap[uid]; !ok {
				newMessageIds = append(newMessageIds, uid)
			}
		}

		var idx int
		count := 1

		for idx < len(newMessageIds) {
			select {
			case <-quitCh:
				return nil
			default:
			}

			var seq imap.SeqSet
			start := idx
			stop := idx + count
			if stop >= len(newMessageIds) {
				stop = len(newMessageIds)
			}

			for _, uid := range newMessageIds[start:stop] {
				seq.AddNum(uid)
			}

			cmd, err = c.UIDFetch(&seq, fetchDescriptors...)
			if err != nil {
				return err
			}

			_, err = imap.Wait(cmd, nil)
			if err != nil {
				return err
			}

			for _, resp := range cmd.Data {
				if err := saveNewMessage(account, resp, c.Mailbox.UIDValidity); err != nil {
					return err
				}
			}

			idx += count

			count *= 2
			if count > 10 {
				count = 10
			}

			for _, resp := range c.Data {
				if resp.Label == "EXISTS" {
					// New mail arrived in the meantime - fetch it now.
					if err := handleNewMailEvent(account, c); err != nil {
						return err
					}
					break
				}
			}

			c.Data = nil
		}

		err = syncExistingMessages(account, c, remoteIds, cacheIds, highestModSeq, folderTrash)
		if err != nil {
			return err
		}

		// Update account state information
		account.UIDValidity = c.Mailbox.UIDValidity
		account.HighestModSeq = highestModSeq
		account.LastMailSync = time.Now()
		account.Save()

		c.Data = nil

		if _, err := c.Idle(); err != nil {
			return err
		}

		lastInteraction = time.Now()

		idle := func() (bool, error) {
			timeoutCh := time.After(time.Minute * 4)

			watcher, err := inotify.NewWatcher()
			if err != nil {
				return false, err
			}

			defer watcher.Close()

			watchPath := path.Join(account.Path, ".notmuch", "xapian", "flintlock")
			err = watcher.AddWatch(watchPath, inotify.IN_CLOSE_WRITE)
			if err != nil {
				return false, err
			}

		idle:
			for {
				select {
				case <-watcher.Event:
					imap.Wait(c.IdleTerm())
					if _, err := imap.Wait(c.Close(false)); err != nil {
						return false, err
					}
					return true, nil
				case <-timeoutCh:
					imap.Wait(c.IdleTerm())
					if _, err := imap.Wait(c.Close(false)); err != nil {
						return false, err
					}
					return true, nil
				case <-quitCh:
					imap.Wait(c.IdleTerm())
					return false, nil
				default:
				}

				if err := c.Recv(time.Second); err == nil {
					imap.Wait(c.IdleTerm())

					for _, resp := range c.Data {
						if resp.Label == "EXISTS" {
							// New mail arrived.
							if err := handleNewMailEvent(account, c); err != nil {
								return false, err
							}

							c.Data = nil

							if _, err := c.Idle(); err != nil {
								return false, err
							}

							continue idle
						}
					}

					c.Data = nil

					if _, err := imap.Wait(c.Close(false)); err != nil {
						return false, err
					}

					return true, nil
				} else if err != imap.ErrTimeout {
					return false, err
				}
			}
		}

		if cont, err := idle(); err != nil {
			return err
		} else if !cont {
			return nil
		}
	}
}

func SyncMail(account *Account, accessToken *AccessToken, quitCh chan struct{}) {
	// Keep synchronizing and retrying
	if err := syncMailOnce(account, accessToken, quitCh); err != nil {
		waitFor := 500 * time.Millisecond

		for err != nil {
			fmt.Println("ERROR", err)

			waitFor *= 2
			if waitFor > time.Minute {
				waitFor = time.Minute
			}

			select {
			case <-time.After(waitFor):
			case <-quitCh:
				return
			}

			err = syncMailOnce(account, accessToken, quitCh)
		}
	}
}
