package main

import (
	"bufio"
	"encoding/json"
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
		log.Printf("couldn't create encrypt_chunks queue: %s", err)
		return err
	}

	err = ch.Publish("", queue.Name, false, false, amqp091.Publishing{
		ContentType: "application/octet-stream",
		Body:        body,
	})
	if err != nil {
		return err
	}

	return nil
}

func main() {
	rabbitMQ, err := messaging.NewRabbitMQ("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("couldn't open RabbitMQ connection: %s", err)
	}
	defer rabbitMQ.Close()

	r := http.NewServeMux()
	r.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		f, fh, err := r.FormFile("file")
		if err != nil {
			http.Error(w, "missing file on body request", http.StatusBadRequest)
		}
		defer f.Close()

		reader := bufio.NewReader(f)
		buf := make([]byte, 1024*4) // 4KB chunks
		for {
			n, err := reader.Read(buf)
			if err != nil && err != io.EOF {
				log.Printf("couldn't read the uploaded file with 4KB buffer: %s", err)
				http.Error(w, "couldn't read the file", http.StatusInternalServerError)
			}
			if n == 0 {
				break
			}

			msg, err := json.Marshal(&Payload{
				Filename: fh.Filename,
				Chunk:    buf[:n],
			})
			if err != nil {
				log.Printf("couldn't marshal the message payload: %s", err)
				http.Error(w, "Internal server error", http.StatusInternalServerError)
			}

			err = publish(rabbitMQ.Ch, msg)
			if err != nil {
				log.Printf("couldn't publish the chunk's message: %s", err)
				http.Error(w, "couldn't read the file", http.StatusInternalServerError)
			}
		}

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
