package main

import (
	"fmt"

	gomail "gopkg.in/gomail.v2"
)

func alert(informEmail string, subject string, content []byte,
	smtpHost string, smtpPort int, smtpUser string, smtpPassword string) {
	m := gomail.NewMessage()
	m.SetHeader("From", "kafka_monitor@sina.com")
	m.SetHeader("To", informEmail)
	m.SetHeader("Subject", subject)
	m.SetBody("text/plain", string(content))

	d := gomail.NewDialer(smtpHost, smtpPort, smtpUser, smtpPassword)
	if err := d.DialAndSend(m); err != nil {
		fmt.Printf("failed to send alarm: %v\n", err)
	}

}
