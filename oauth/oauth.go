package oauth

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const (
	oauthUrl      = "https://accounts.google.com/o/oauth2/auth?"
	oauthTokenUrl = "https://accounts.google.com/o/oauth2/token"
	oauthScope    = "https://mail.google.com/ https://www.google.com/m8/feeds"
	clientId      = "426962572750.apps.googleusercontent.com"
	// Not really secret in installed apps
	clientSecret = "f7xf2kZS8aB1HrJjjnV5GAQX"
	contactsUrl  = "https://www.google.com/m8/feeds/contacts/default/full?"
	redirectUri  = "http://localhost:8080/return"
)

type OAuthAccount struct {
	Email        string
	Name         string
	RefreshToken string
	Scope        string
	AccessToken  *AccessToken
}

type AccessToken struct {
	RefreshToken string
	accessToken  string
	expiresAt    time.Time
	mutex        *sync.Mutex
}

func NewAccessToken(refreshToken string) *AccessToken {
	return &AccessToken{
		RefreshToken: refreshToken,
		mutex:        &sync.Mutex{},
	}
}

func (token *AccessToken) Get() (string, error) {
	token.mutex.Lock()
	defer token.mutex.Unlock()

	if token.accessToken == "" || time.Since(token.expiresAt) > 0 {
		accessToken, expiresIn, err := refreshAccessToken(token.RefreshToken)
		if err != nil {
			return "", err
		}

		token.accessToken = accessToken
		token.expiresAt = time.Now().Add(time.Second*time.Duration(expiresIn) - 10*time.Second)
	}

	return token.accessToken, nil
}

type oauthResponse struct {
	AccessToken  string `json:"access_token"`
	ExpiresIn    int    `json:"expires_in"`
	RefreshToken string `json:"refresh_token"`
	Error        string `json:"error"`
}

type profileInfoResponse struct {
	Name  string `xml:"author>name"`
	Email string `xml:"author>email"`
}

func init() {
	http.DefaultClient.Timeout = 30 * time.Second
}

func GetOAuthUrl() string {
	v := url.Values{}
	v.Set("response_type", "code")
	v.Set("client_id", clientId)
	v.Set("redirect_uri", redirectUri)
	v.Set("scope", oauthScope)
	return oauthUrl + v.Encode()
}

func GetOAuthAccount(code string) (*OAuthAccount, error) {
	resp, err := http.PostForm(oauthTokenUrl, url.Values{
		"code":          {code},
		"client_id":     {clientId},
		"client_secret": {clientSecret},
		"redirect_uri":  {redirectUri},
		"grant_type":    {"authorization_code"},
	})

	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var info oauthResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&info); err != nil {
		return nil, err
	}

	if info.Error != "" {
		return nil, fmt.Errorf("Error fetching access token: %s", info.Error)
	}

	params := url.Values{
		"max-results":  {"1"},
		"access_token": {info.AccessToken},
	}
	resp, err = http.Get(contactsUrl + params.Encode())
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()

	var profileInfo profileInfoResponse
	xmlDec := xml.NewDecoder(resp.Body)
	if err := xmlDec.Decode(&profileInfo); err != nil {
		return nil, err
	}

	return &OAuthAccount{
		Email:        profileInfo.Email,
		Name:         profileInfo.Name,
		RefreshToken: info.RefreshToken,
		Scope:        oauthScope,
		AccessToken: &AccessToken{
			RefreshToken: info.RefreshToken,
			accessToken:  info.AccessToken,
			expiresAt:    time.Now().Add(time.Second*time.Duration(info.ExpiresIn) - time.Second*10),
			mutex:        &sync.Mutex{},
		},
	}, nil
}

func refreshAccessToken(refreshToken string) (string, int, error) {
	resp, err := http.PostForm(oauthTokenUrl, url.Values{
		"refresh_token": {refreshToken},
		"client_id":     {clientId},
		"client_secret": {clientSecret},
		"grant_type":    {"refresh_token"},
	})
	if err != nil {
		return "", 0, err
	}

	defer resp.Body.Close()

	var info oauthResponse
	decoder := json.NewDecoder(resp.Body)
	if err := decoder.Decode(&info); err != nil {
		return "", 0, err
	}

	return info.AccessToken, info.ExpiresIn, nil
}
