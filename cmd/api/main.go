package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/d4niells/vault/internal/messaging"
	"github.com/rabbitmq/amqp091-go"
)

type Payload struct {
	Filename string `json:"filename"`
	Chunk    []byte
}

func publish(ch *amqp091.Channel, body []byte) error {
	queue, err := ch.QueueDeclare("encrypt_chunks", true, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("couldn't declare encrypt_chunks queue: %w", err)
	}

	err = ch.Publish("", queue.Name, false, false, amqp091.Publishing{
		ContentType: "application/octet-stream",
		Body:        body,
	})
	if err != nil {
		return fmt.Errorf("couldn't publish message to queue %s: %w", queue.Name, err)
	}

	return nil
}

func main() {
	rabbitMQ, err := messaging.NewRabbitMQ("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Fatalf("couldn't open RabbitMQ connection: %s", err)
	}
	defer rabbitMQ.Close()

	r := http.NewServeMux()
	r.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		f, fh, err := r.FormFile("file")
		if err != nil {
			log.Printf("couldn't read form file: %s", err)
			http.Error(w, "missing or invalid file", http.StatusBadRequest)
			return
		}
		defer f.Close()

		if fh.Size == 0 {
			log.Printf("received empty file: %s", fh.Filename)
			http.Error(w, "file is empty", http.StatusBadRequest)
			return
		}

		reader := bufio.NewReader(f)
		buf := make([]byte, 1024*4) // 4KB chunks
		var chunkCount uint

		for {
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				log.Printf("couldn't read file %s: %s", fh.Filename, err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}
			if n == 0 {
				break
			}

			msg, err := json.Marshal(&Payload{
				Filename: fh.Filename,
				Chunk:    buf[:n],
			})
			if err != nil {
				log.Printf("couldn't marshal payload for %s (chunk %d: %s", fh.Filename, chunkCount, err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}

			err = publish(rabbitMQ.Ch, msg)
			if err != nil {
				log.Printf("couldn't publish chunk %d for %s: %s", chunkCount, fh.Filename, err)
				http.Error(w, "internal server error", http.StatusInternalServerError)
			}

			chunkCount++
		}

		log.Printf("file %s split into %d chunks and queued for encryption", fh.Filename, chunkCount)
		w.WriteHeader(http.StatusCreated)
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	srv := http.Server{
		Addr:    ":" + port,
		Handler: r,
	}

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Printf("couldn't listen on port %s: %s", port, err)
	}
}
