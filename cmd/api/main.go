package main

import (
	"bufio"
	"io"
	"log"
	"net/http"
	"os"

	"github.com/rabbitmq/amqp091-go"
)

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
	conn, err := amqp091.Dial("amqp://guest:guest@localhost:5672/")
	if err != nil {
		log.Printf("couldn't open a new rabbitmq connection: %s", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Printf("couldn't open a unique channel: %s", err)
	}
	defer ch.Close()

	r := http.NewServeMux()

	r.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
		f, _, err := r.FormFile("file")
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

			chunk := buf[:n]

			err = publish(ch, chunk)
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
