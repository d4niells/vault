package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/rabbitmq/amqp091-go"
)

type File struct {
	Filename string
	Chunk    []byte
}

func main() {
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("couldn't open a new connection: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("couldn't create a unique channel: %s", err)
	}

	msgs, err := ch.Consume("encrypt_chunks", "encryptor", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("couldn't start consuming encrypt_chunks queue: %s", err)
	}

	var file File
	for msg := range msgs {
		// TODO: encrypt the hole chunk
		err := json.Unmarshal(msg.Body, &file)
		if err != nil {
			log.Fatalf("couldn't unmarshal message: %s", err)
		}

		path := fmt.Sprintf("./tmp/%s", file.Filename)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			f, err = os.Create(path)
			if err != nil {
				log.Fatalf("couldn't create file: %s", err)
			}
		}

		_, err = f.Write(file.Chunk)
		if err != nil {
			log.Fatalf("couldn't write file: %s", err)
		}
	}
}
