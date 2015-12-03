package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/user"
	"path"
	"sort"
	"strings"
	"unicode"
)

type Contacts map[string]*Contact

type Contact struct {
	Title  string   `json:"title"`
	Emails []string `json:"emails"`
}

type ContactsSlice []*Contact

func (c ContactsSlice) Len() int {
	return len(c)
}

func (c ContactsSlice) Less(i, j int) bool {
	return c[i].Title < c[j].Title
}

func (c ContactsSlice) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

var (
	flagAccount = flag.String("a", "", "which account to use")
	flagMailDir = flag.String("maildir", "~/Mail", "directory where all mail is stored")
)

var logger = log.New(os.Stderr, "[gomaild-contacts] ", log.Ltime)

func getMailDir() (string, error) {
	dir := *flagMailDir

	if strings.HasPrefix(dir, "~/") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}

		dir = strings.Replace(dir, "~", usr.HomeDir, 1)
	}

	return dir, nil
}

func normalize(input string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			return r
		} else {
			return -1
		}
	}, strings.ToLower(input))
}

func matches(contact *Contact, input string) bool {
	if strings.Contains(normalize(contact.Title), input) {
		return true
	}

	for _, email := range contact.Emails {
		if strings.Contains(normalize(email), input) {
			return true
		}
	}

	return false
}

func main() {
	flag.Parse()

	mailDir, err := getMailDir()
	if err != nil {
		logger.Fatal("Could not determine a mail directory.")
	}

	account := *flagAccount
	if account == "" {
		logger.Fatal("Missing -a argument.")
	}

	args := flag.Args()
	if len(args) != 1 {
		logger.Fatal("Invalid number of arguments.")
	}

	contactsPath := path.Join(mailDir, account, "contacts.json")
	f, err := os.Open(contactsPath)
	if err != nil {
		logger.Fatalf("Could not open contacts.json file: %s", err)
	}

	defer f.Close()

	var contacts Contacts
	decoder := json.NewDecoder(f)

	if err := decoder.Decode(&contacts); err != nil {
		logger.Fatalf("Error decoding contacts: %s", err)
	}

	query := normalize(args[0])
	results := make(ContactsSlice, 0)
	for _, contact := range contacts {
		if matches(contact, query) {
			results = append(results, contact)
		}
	}
	sort.Sort(results)

	fmt.Println("Matching contacts:")

	for _, result := range results {
		for _, email := range result.Emails {
			fmt.Printf("%s\t%s\n", email, result.Title)
		}
	}
}
