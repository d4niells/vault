package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/d4niells/vault/internal/messaging"
)

type File struct {
	Filename string
	Chunk    []byte
}

func main() {
	rabbitMQ, err := messaging.NewRabbitMQ("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("couldn't open RabbitMQ connection: %s", err)
	}
	defer rabbitMQ.Close()

	msgs, err := rabbitMQ.Ch.Consume("encrypt_chunks", "encryptor", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("couldn't consume messages: %s", err)
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
