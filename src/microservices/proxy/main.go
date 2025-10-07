package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"
)

func main() {
	cfg := NewConfig()
	monolithProxy := NewReverseProxy(cfg.MonolithURL)
	moviesProxy := NewReverseProxy(cfg.MoviesServiceURL)
	server := NewServer(cfg, monolithProxy, moviesProxy)

	log.Printf(
		"Proxy starting on :%s (gradual_migration=%t, movies_migration_percent=%d%%)",
		cfg.Port,
		cfg.GradualMigrationEnabled,
		cfg.MoviesMigrationPercent,
	)

	err := server.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("Failed to start server: %v", err)
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = server.Shutdown(ctx)
	if err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}

type Config struct {
	Port                    string
	MonolithURL             string
	MoviesServiceURL        string
	GradualMigrationEnabled bool
	MoviesMigrationPercent  int
}

func NewConfig() *Config {
	return &Config{
		Port:                    getEnv("PORT", "8000"),
		MonolithURL:             getEnv("MONOLITH_URL", "http://monolith:8080"),
		MoviesServiceURL:        getEnv("MOVIES_SERVICE_URL", "http://movies-service:8081"),
		GradualMigrationEnabled: getEnv("GRADUAL_MIGRATION", "false") == "true",
		MoviesMigrationPercent:  getEnvInt("MOVIES_MIGRATION_PERCENT", 0),
	}
}

func NewServer(
	cfg *Config,
	monolithProxy *httputil.ReverseProxy,
	moviesProxy *httputil.ReverseProxy,
) *http.Server {

	rand.Seed(time.Now().UnixNano())

	http.HandleFunc("/api/movies", func(w http.ResponseWriter, r *http.Request) {
		if cfg.GradualMigrationEnabled && rand.Intn(100) < cfg.MoviesMigrationPercent {
			log.Printf("Routing to MOVIES service [%s] -> %s", r.Method, r.URL.Path)
			moviesProxy.ServeHTTP(w, r)

			return
		}

		log.Printf("Routing to MONOLITH service [%s] -> %s", r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Printf("Routing to MONOLITH service [%s] -> %s", r.Method, r.URL.Path)
		monolithProxy.ServeHTTP(w, r)
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":true}`))
		if err != nil {
			log.Fatalf("Failed to write health check response: %v", err)
		}
	})

	return &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: nil, // Use default handler
	}
}

func NewReverseProxy(targetURL string) *httputil.ReverseProxy {
	target, err := url.Parse(targetURL)
	if err != nil {
		log.Fatalf("Failed to parse target URL: %v", err)
	}

	return httputil.NewSingleHostReverseProxy(target)
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}

	return fallback
}

func getEnvInt(key string, fallback int) int {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}

	return fallback
}
