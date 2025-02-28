package main

import (
  "net/http"
  "io"
  "os"
  "bufio"
  "log"
)

func main() {
  r := http.NewServeMux()  

  r.HandleFunc("POST /upload", func(w http.ResponseWriter, r *http.Request) {
    f, fh, err := r.FormFile("file") 
    if err != nil {
      http.Error(w, "missing file on body request", http.StatusBadRequest)
    }
    defer f.Close()

    reader := bufio.NewReader(f)
    buf := make([]byte, 1024 * 4) // 4KB chunks
    for {
      n, err := reader.Read(buf) 
      if err != nil && err != io.EOF {
        http.Error(w, "couldn't read the file", http.StatusInternalServerError)
      }
      if n == 0 {
        break
      }

      chunk := buf[:n]

      log.Println("Name: ", fh.Filename)
      log.Println("Size: ", fh.Size)
      log.Println("Chunk: ", chunk)
    }
  })

  port := os.Getenv("PORT")
  if port == "" {
    port = "8080"
  }

  srv := http.Server{
    Addr:     ":"+port,
    Handler:  r,
  }

  if err := srv.ListenAndServe(); err != http.ErrServerClosed {
    log.Fatalf("couldn't listen on port %s: %s", port, err)
  }
}
