package main

import (
	"net/http"
	"time"

	"github.com/facebookgo/httpdown"
	"github.com/gin-gonic/gin"
	"github.com/miGlanz/gomaild/oauth"
)

const defaultBind = ":8080"

func indexHandler(c *gin.Context) {
	config := c.MustGet("config").(*Config)
	accounts, _ := config.GetAccounts()
	c.HTML(http.StatusOK, "index.html", gin.H{
		"accounts": accounts,
	})
}

func oauthInitHandler(c *gin.Context) {
	c.Redirect(http.StatusFound, oauth.GetOAuthUrl())
}

func oauthReturnHandler(c *gin.Context) {
	code := c.Query("code")

	if code == "" {
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "OAuth flow unsuccessful.",
		})

		return
	}

	oauthAccount, err := oauth.GetOAuthAccount(code)
	if err != nil {
		c.HTML(http.StatusUnauthorized, "error.html", gin.H{
			"error": "Could not get OAuth access token.",
		})

		return
	}

	config := c.MustGet("config").(*Config)

	if err := config.AddAccount(oauthAccount); err != nil {
		c.HTML(http.StatusInternalServerError, "error.html", gin.H{
			"error": "Could not add the account.",
		})

		return
	}

	c.Redirect(http.StatusFound, "/")
}

func RunWeb(config *Config) {
	r := gin.Default()
	r.LoadHTMLGlob("templates/*.html")

	r.Use(func(c *gin.Context) {
		c.Set("config", config)
	})

	r.GET("/", indexHandler)
	r.GET("/connect", oauthInitHandler)
	r.GET("/return", oauthReturnHandler)

	server := &http.Server{
		Addr:    defaultBind,
		Handler: r,
	}

	hd := &httpdown.HTTP{
		StopTimeout: 10 * time.Second,
		KillTimeout: 1 * time.Second,
	}

	httpdown.ListenAndServe(server, hd)
}
