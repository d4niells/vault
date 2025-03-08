package main

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"

	"github.com/d4niells/vault/internal/messaging"
)

type File struct {
	Filename string
	Chunk    []byte
}

func encrypt(chunk []byte) ([]byte, error) {
	// TODO: create a new one and put it as an environment variable
	key := []byte("thisisaverysecurekeyforAES123!")

	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}

	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	encryptedChunk := make([]byte, (1024*4)+gcm.NonceSize()) // 4KB + 12 bytes
	encryptedChunk = gcm.Seal(nonce, nonce, chunk, nil)

	return encryptedChunk, nil
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
		err := json.Unmarshal(msg.Body, &file)
		if err != nil {
			log.Fatalf("couldn't unmarshal message: %s", err)
		}

		encryptedChunk, err := encrypt(file.Chunk)
		if err != nil {
			log.Fatalf("couldn't encrypt chunk: %s", err)
		}

		// TODO: publish the encrypted chunk into a encrypted_chunks queue

		path := fmt.Sprintf("./tmp/%s", file.Filename)
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			f, err = os.Create(path)
			if err != nil {
				log.Fatalf("couldn't create file: %s", err)
			}
		}

		_, err = f.Write(encryptedChunk)
		if err != nil {
			log.Fatalf("couldn't write file: %s", err)
		}
	}
}
