package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/IBM/sarama"
)

func main() {
	cfg := NewConfig()
	consumer := NewConsumer(cfg)
	producer := NewProducer(cfg)
	handler := NewHandler(producer)
	server := NewServer(cfg, handler)

	go func() {
		err := server.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Failed to start server: %v", err)
		}
	}()

	topics := []string{"user-events", "payment-events", "movie-events"}

	for _, topic := range topics {
		go func(topic string) {
			partitionConsumer, err := consumer.ConsumePartition(topic, 0, sarama.OffsetOldest)
			if err != nil {
				log.Fatalf("Error creating consumer for topic %s: %v", topic, err)
			}

			defer func() {
				err = partitionConsumer.Close()
				if err != nil {
					log.Printf("Error closing consumer for topic %s: %v", topic, err)
				}
			}()

			for {
				select {
				case msg := <-partitionConsumer.Messages():
					log.Printf("Received message from topic %s: %s", topic, string(msg.Value))
				case err = <-partitionConsumer.Errors():
					log.Printf("Error consuming from topic %s: %v", topic, err)
				}
			}
		}(topic)
	}

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err := producer.Close()
	if err != nil {
		log.Println("Error closing Kafka producer:", err)
	}

	err = consumer.Close()
	if err != nil {
		log.Println("Error closing Kafka consumer:", err)
	}

	err = server.Shutdown(ctx)
	if err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}
}

type Config struct {
	Port         string
	KafkaBrokers []string
}

func NewConfig() *Config {
	return &Config{
		Port:         getEnv("PORT", "8083"),
		KafkaBrokers: strings.Split(getEnv("KAFKA_BROKERS", "kafka:9092"), ","),
	}
}

type Event struct {
	ID        string    `json:"id"`
	EventType string    `json:"event_type"`
	Payload   any       `json:"payload"`
	Timestamp time.Time `json:"timestamp"`
}

func NewConsumer(cfg *Config) sarama.Consumer {
	consumerConfig := sarama.NewConfig()
	consumerConfig.Consumer.Offsets.AutoCommit.Enable = true
	consumerConfig.Consumer.Offsets.AutoCommit.Interval = 1 * time.Second

	consumer, err := sarama.NewConsumer(cfg.KafkaBrokers, consumerConfig)
	if err != nil {
		log.Fatal("Failed to start Kafka consumer", err)
	}

	return consumer
}

func NewProducer(cfg *Config) sarama.SyncProducer {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.RequiredAcks = sarama.WaitForAll
	producerConfig.Producer.Retry.Max = 10
	producerConfig.Producer.Return.Successes = true

	var err error
	producer, err := sarama.NewSyncProducer(cfg.KafkaBrokers, producerConfig)
	if err != nil {
		log.Fatal("Failed to start Kafka producer", err)
	}

	return producer
}

func NewServer(cfg *Config, handler *Handler) *http.Server {
	http.HandleFunc("/api/events/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{"status":true}`))
		if err != nil {
			log.Fatalf("Failed to write health check response: %v", err)
		}
	})

	http.HandleFunc("/api/events/{event_type}", handler.handleEvent)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: nil, // Use default handler
	}

	return server
}

type Handler struct {
	producer sarama.SyncProducer
}

func NewHandler(producer sarama.SyncProducer) *Handler {
	return &Handler{
		producer: producer,
	}
}

func (h *Handler) handleEvent(w http.ResponseWriter, r *http.Request) {
	eventType := r.URL.Path[len("/api/events/"):] // This is a simplified approach

	if eventType != "user" && eventType != "payment" && eventType != "movie" {
		http.Error(w, "Invalid event type", http.StatusBadRequest)
		return
	}

	event := Event{
		ID:        generateID(),
		EventType: eventType,
		Payload:   fmt.Sprintf("Event of type %s created", eventType),
		Timestamp: time.Now(),
	}

	topic := fmt.Sprintf("%s-events", eventType)

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(fmt.Sprintf("{\"id\":\"%s\",\"event_type\":\"%s\",\"payload\":\"%s\",\"timestamp\":\"%s\"}",
			event.ID, event.EventType, event.Payload, event.Timestamp.Format(time.RFC3339))),
	}

	_, _, err := h.producer.SendMessage(msg)
	if err != nil {
		http.Error(w, "Failed to send message to Kafka", http.StatusInternalServerError)
		log.Println("Error sending message to Kafka:", err)
		return
	}

	log.Printf("Event %s sent to topic %s", event.ID, topic)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, err = fmt.Fprintf(w, `{"status": "success", "event_id": "%s", "topic": "%s"}`, event.ID, topic)
	if err != nil {
		log.Println("Error writing response:", err)
		return
	}
}

func generateID() string {
	return fmt.Sprintf("%d", time.Now().UnixNano())
}

func getEnv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}

	return fallback
}
