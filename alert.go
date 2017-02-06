package main

import (
	"fmt"

	"strings"

	gomail "gopkg.in/gomail.v2"
)

func alert(informEmail string, subject string, content []byte,
	smtpHost string, smtpPort int, smtpUser string, smtpPassword string) {
	m := gomail.NewMessage()
	m.SetHeader("From", "kafka_monitor@sina.com")
	m.SetHeader("To", informEmail)
	m.SetHeader("Subject", subject)

	c := string(content)
	c = strings.Replace(c, "[30m", "", -1)
	c = strings.Replace(c, "[31m", "", -1)
	c = strings.Replace(c, "[32m", "", -1)
	c = strings.Replace(c, "[33m", "", -1)
	c = strings.Replace(c, "[34m", "", -1)
	c = strings.Replace(c, "[35m", "", -1)
	c = strings.Replace(c, "[36m", "", -1)
	c = strings.Replace(c, "[37m", "", -1)
	c = strings.Replace(c, "[0m", "", -1)

	m.SetBody("text/plain", string(content))

	d := gomail.NewDialer(smtpHost, smtpPort, smtpUser, smtpPassword)
	if err := d.DialAndSend(m); err != nil {
		fmt.Printf("failed to send alarm: %v\n", err)
	}

}
