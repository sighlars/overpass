// Command echo is a tiny demo backend for overpass: it answers with its
// own name so you can watch traffic rotate, and optionally sleeps to
// simulate a slow upstream (useful for least_conn demos).
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	port := flag.Int("port", 9001, "port to listen on")
	name := flag.String("name", "backend-1", "backend name reported in responses")
	delayMs := flag.Int("delay-ms", 0, "artificial response delay in milliseconds")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"status":"ok","backend":%q}`+"\n", *name)
	})

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if *delayMs > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(time.Duration(*delayMs) * time.Millisecond):
			}
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"backend": *name,
			"time":    time.Now().UTC().Format(time.RFC3339),
		})
	})

	addr := fmt.Sprintf(":%d", *port)
	log.Info("demo backend listening", "addr", addr, "name", *name)
	if err := http.ListenAndServe(addr, mux); err != nil {
		log.Error("backend failed", "err", err)
		os.Exit(1)
	}
}
