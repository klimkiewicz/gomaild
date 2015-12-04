package gmail

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/miGlanz/gomaild/common"
	"github.com/miGlanz/gomaild/oauth"
	"github.com/mxk/go-imap/imap"
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

var netTimeout = time.Minute * 5

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

var fetchDescriptors = []string{
	"BODY.PEEK[]",
	"FLAGS",
	"X-GM-LABELS",
	"X-GM-MSGID",
	"X-GM-THRID",
}

type Client struct {
	conn        net.Conn
	folderAll   string
	folderTrash string
	imap        *imap.Client
	mailbox     *MailboxStatus
	mutex       *sync.Mutex
}

type MailboxStatus struct {
	Messages      uint32
	UIDNext       uint32
	UIDValidity   uint32
	HighestModSeq uint32
}

type Message struct {
	UID  uint32
	Tags common.TagsSet
	Body []byte
}

func imapLabelsToTags(info *imap.MessageInfo) common.TagsSet {
	tags := make(common.TagsSet)

	if !info.Flags["\\Seen"] {
		tags.Add("unread")
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

		tags.Add(field)
	}

	return tags
}

func tagsToImapLabels(tags common.TagsSet) []imap.Field {
	labels := make([]imap.Field, 0, len(tags))

	for tag, _ := range tags {
		var label string

		switch tag {
		case "unread", "replied":
			continue
		case "flagged":
			label = "\\Starred"
		case "inbox", "important", "sent":
			label = "\\" + strings.Title(tag)
		default:
			label = tag
		}

		labels = append(labels, label)
	}

	return labels
}

func Dial(email, accessToken string) (*Client, error) {
	conn, err := net.DialTimeout("tcp", gmailIMAPAddr, time.Second*30)
	if err != nil {
		return nil, err
	}

	conn.SetDeadline(time.Now().Add(netTimeout))

	host, _, _ := net.SplitHostPort(gmailIMAPAddr)
	tlsConn := tls.Client(conn, &tls.Config{ServerName: host})

	c, err := imap.NewClient(tlsConn, host, time.Second*30)
	if err != nil {
		return nil, err
	}

	c.SetLogMask(imap.LogConn | imap.LogState | imap.LogCmd)

	c.CommandConfig["SELECT"].Filter = selectFilter
	c.CommandConfig["EXAMINE"].Filter = selectFilter

	c.Data = nil

	if c.State() == imap.Login {
		if _, err := c.Auth(oauth.NewXOAuth(email, accessToken)); err != nil {
			return nil, err
		}
	}

	// Enable compression
	_, err = c.CompressDeflate(compressionLevel)
	if err != nil && err != imap.ErrCompressionActive {
		return nil, err
	}

	// Fetch Gmail special folder names (they are locale-dependent)
	cmd, err := imap.Wait(c.List("", "[Gmail]/%"))
	if err != nil {
		return nil, err
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
		return nil, errors.New("could not determine the name of All or Trash folder")
	}

	c.Data = nil

	return &Client{
		conn:        conn,
		imap:        c,
		folderAll:   folderAll,
		folderTrash: folderTrash,
		mutex:       &sync.Mutex{},
	}, nil
}

func (c *Client) SelectAll() (*MailboxStatus, error) {
	c.conn.SetDeadline(time.Now().Add(netTimeout))

	cmd, err := c.imap.Select(c.folderAll, false)
	if err != nil {
		return nil, err
	}

	var highestModSeq uint32

	for _, resp := range cmd.Data {
		if resp.Label == "HIGHESTMODSEQ" {
			highestModSeq = resp.Fields[1].(uint32)
			break
		}
	}

	c.mailbox = &MailboxStatus{
		Messages:      c.imap.Mailbox.Messages,
		UIDNext:       c.imap.Mailbox.UIDNext,
		UIDValidity:   c.imap.Mailbox.UIDValidity,
		HighestModSeq: highestModSeq,
	}

	return c.mailbox, nil
}

func (c *Client) Close() error {
	_, err := imap.Wait(c.imap.Close(false))
	return err
}

func (c *Client) FetchAllUIDs() (common.UIDSlice, error) {
	uids := make(common.UIDSlice, 0, c.imap.Mailbox.Messages)
	steps := c.imap.Mailbox.Messages / messagesPerSearch
	if steps*messagesPerSearch < c.imap.Mailbox.Messages {
		steps += 1
	}

	var i, start, stop uint32
	cmds := make([]*imap.Command, steps)
	stop = c.imap.Mailbox.Messages

	for i = 0; i < steps; i++ {
		var seq imap.SeqSet

		if stop > messagesPerSearch {
			start = stop - messagesPerSearch + 1
		} else {
			start = 1
		}

		seq.AddRange(start, stop)

		cmd, err := c.imap.UIDSearch(seq)
		if err != nil {
			return nil, err
		}

		cmds[i] = cmd
		stop = start - 1
	}

	for _, cmd := range cmds {
		if _, err := imap.Wait(cmd, nil); err != nil {
			return nil, err
		}

		for _, resp := range cmd.Data {
			uids = append(uids, resp.SearchResults()...)
		}
	}

	uids.Sort()
	return uids, nil
}

func (c *Client) FetchNewUIDs() (common.UIDSlice, error) {
	var seq imap.SeqSet
	seq.AddRange(c.mailbox.UIDNext, 0)

	cmd, err := imap.Wait(c.imap.UIDFetch(&seq, "UID"))
	if err != nil {
		return nil, err
	}

	uids := make(common.UIDSlice, 0, len(cmd.Data))

	for _, resp := range cmd.Data {
		uids = append(uids, resp.MessageInfo().UID)
	}

	uids.Sort()
	return uids, nil
}

func (c *Client) FetchMessages(uids common.UIDSlice) ([]*Message, error) {
	c.conn.SetDeadline(time.Now().Add(netTimeout))

	var seq imap.SeqSet
	for _, uid := range uids {
		seq.AddNum(uid)
	}

	cmd, err := imap.Wait(c.imap.UIDFetch(&seq, fetchDescriptors...))
	if err != nil {
		return nil, err
	}

	messages := make([]*Message, 0, len(uids))

	for _, resp := range cmd.Data {
		info := resp.MessageInfo()
		tags := imapLabelsToTags(info)

		var bufSize int
		for _, literal := range resp.Literals {
			bufSize += int(literal.Info().Len)
		}

		// Add 1KB to the capacity so that we can add Gmail headers without
		// reallocating the byte slice.
		buf := bytes.NewBuffer(make([]byte, 0, bufSize+1024))
		for _, literal := range resp.Literals {
			if _, err := literal.WriteTo(buf); err != nil {
				return nil, err
			}
		}

		body := buf.Bytes()

		// Fix newline characters.
		body = bytes.Replace(body, []byte("\r\n"), []byte("\n"), -1)

		// Add X-Gmail-Message-Id and X-Gmail-Thread-Id special headers.
		gmailMessageId := info.Attrs["X-GM-MSGID"].(string)
		gmailThreadId := info.Attrs["X-GM-THRID"].(string)
		headers := fmt.Sprintf(gmailHeaders, gmailMessageId, gmailThreadId)
		body = bytes.Replace(body, []byte("\n\n"), []byte(headers), 1)

		messages = append(messages, &Message{
			UID:  info.UID,
			Tags: tags,
			Body: body,
		})
	}

	return messages, nil
}

func (c *Client) FetchTagChanges(lastSeenUID, highestModSeq uint32) (map[uint32]common.TagsSet, error) {
	var seq imap.SeqSet
	seq.AddRange(1, lastSeenUID)

	fields := []imap.Field{"FLAGS", "X-GM-LABELS"}
	changedSince := []imap.Field{"CHANGEDSINCE", highestModSeq}

	cmd, err := imap.Wait(c.imap.Send("UID FETCH", seq, fields, changedSince))
	if err != nil {
		return nil, err
	}

	tags := make(map[uint32]common.TagsSet)

	for _, resp := range cmd.Data {
		messageInfo := resp.MessageInfo()
		messageTags := imapLabelsToTags(messageInfo)
		tags[messageInfo.UID] = messageTags
	}

	return tags, nil
}

func (c *Client) DeleteMessages(uids common.UIDSlice) error {
	var seq imap.SeqSet
	for _, uid := range uids {
		seq.AddNum(uid)
	}

	_, err := imap.Wait(c.imap.UIDCopy(&seq, c.folderTrash))
	return err
}

func (c *Client) HasNewMail() bool {
	var hasNewMail bool

	for _, resp := range c.imap.Data {
		if resp.Label == "EXISTS" {
			hasNewMail = true
			break
		}
	}

	c.imap.Data = nil
	return hasNewMail
}

func (c *Client) AddMessageTags(uid uint32, tags common.TagsSet) error {
	var seq imap.SeqSet
	seq.AddNum(uid)

	if tags.Contains("unread") {
		if _, err := imap.Wait(c.imap.UIDStore(&seq, "-FLAGS.SILENT", "\\Seen")); err != nil {
			return err
		}
	}

	labels := tagsToImapLabels(tags)

	if len(labels) > 0 {
		if _, err := imap.Wait(c.imap.UIDStore(&seq, "+X-GM-LABELS", labels)); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) RemoveMessageTags(uid uint32, tags common.TagsSet) error {
	var seq imap.SeqSet
	seq.AddNum(uid)

	if tags.Contains("unread") {
		if _, err := imap.Wait(c.imap.UIDStore(&seq, "+FLAGS.SILENT", "\\Seen")); err != nil {
			return err
		}
	}

	labels := tagsToImapLabels(tags)

	if len(labels) > 0 {
		if _, err := imap.Wait(c.imap.UIDStore(&seq, "-X-GM-LABELS", labels)); err != nil {
			return err
		}
	}

	return nil
}

func (c *Client) Idle() error {
	c.conn.SetDeadline(time.Now().Add(netTimeout))

	_, err := c.imap.Idle()
	return err
}

func (c *Client) IdleTerm() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	_, err := imap.Wait(c.imap.IdleTerm())
	return err
}

func (c *Client) IdleWait(timeout time.Duration) <-chan error {
	ch := make(chan error, 1)

	go func() {
		defer close(ch)

		// We're using mutex here so that it's safe to call c.IdleTerm() while
		// this goroutine waits in c.imap.Recv() - imap.Client is not thread
		// safe.
		c.mutex.Lock()
		defer c.mutex.Unlock()

		if err := c.imap.Recv(timeout); err == nil || err == imap.ErrTimeout {
			ch <- nil
		} else {
			ch <- err
		}
	}()

	return ch
}

func (c *Client) Logout() {
	c.imap.Logout(3 * time.Second)
}
