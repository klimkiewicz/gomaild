package oauth

import (
	"net/smtp"

	"github.com/mxk/go-imap/imap"
)

type XOAuth []byte

func NewXOAuth(email, token string) imap.SASL {
	return XOAuth("user=" + email + "\x01auth=Bearer " + token + "\x01\x01")
}

func (x XOAuth) Start(s *imap.ServerInfo) (mech string, ir []byte, err error) {
	return "XOAUTH2", x, nil
}

func (x XOAuth) Next(challenge []byte) (response []byte, err error) {
	return []byte(""), nil
}

type XOAuthSMTP []byte

func NewXOAuthSMTP(email, token string) smtp.Auth {
	return XOAuthSMTP("user=" + email + "\x01auth=Bearer " + token + "\x01\x01")
}

func (x XOAuthSMTP) Start(s *smtp.ServerInfo) (mech string, ir []byte, err error) {
	return "XOAUTH2", x, nil
}

func (x XOAuthSMTP) Next(fromServer []byte, more bool) (toServer []byte, err error) {
	return nil, nil
}
