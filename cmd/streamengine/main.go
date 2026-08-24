package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"streamengine/internal/model"
	"streamengine/internal/topology"
)

func main() {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	webDir := flag.String("web", "web", "directory of the topology monitoring page")
	windowSize := flag.Int64("window-size-ms", 5000, "tumbling window size in milliseconds")
	sessionTimeout := flag.Int64("session-timeout-ms", 10000, "session window timeout in milliseconds")
	partitions := flag.Int("partitions", 4, "initial partition count")
	flag.Parse()

	cfg := model.DefaultConfig()
	cfg.WindowSizeMS = *windowSize
	cfg.SessionTimeout = *sessionTimeout
	cfg.Partitions = *partitions
	if err := cfg.Validate(); err != nil {
		log.Fatalf("invalid engine config: %v", err)
	}
	log.Printf("engine config: %s", cfg.Describe())
	log.Printf("idle timeout: %v", model.Duration(cfg.IdleTimeoutMS))

	metrics := model.NewMetrics()
	pipeline := topology.NewPipeline(cfg, metrics)
	server := newHTTPServer(*addr, *webDir, pipeline)

	stopFeed := make(chan struct{})
	feedDone := make(chan struct{})
	go runDemoFeed(pipeline, stopFeed, feedDone)

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-signalCh
		close(stopFeed)
		<-feedDone
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	log.Printf("streamengine listening on %s (web=%s)", *addr, *webDir)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("http server: %v", err)
	}
}
