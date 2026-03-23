package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/manaflow-ai/cmux/daemon/im-bridge/bridge"
	"github.com/manaflow-ai/cmux/daemon/im-bridge/channels"
	"github.com/manaflow-ai/cmux/daemon/im-bridge/config"
)

func main() {
	configPath := flag.String("config", "config.yaml", "path to config file")
	flag.Parse()

	// Load config
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// Connect to cmux
	cmuxClient := bridge.NewCmuxClient(cfg.Cmux.SocketPath)
	if err := cmuxClient.Connect(); err != nil {
		log.Fatalf("failed to connect to cmux: %v", err)
	}
	defer cmuxClient.Close()

	// Verify connection
	if err := cmuxClient.Ping(); err != nil {
		log.Fatalf("cmux ping failed: %v", err)
	}
	log.Println("[main] connected to cmux")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup channel manager
	mgr := channels.NewManager()

	// Register enabled channels
	if cfg.Telegram.Enabled {
		tg, err := channels.NewTelegramChannel(cfg.Telegram.BotToken)
		if err != nil {
			log.Fatalf("failed to create telegram channel: %v", err)
		}
		mgr.Register(tg)
		log.Println("[main] telegram channel registered")
	}

	// Setup session manager
	sessionMgr := bridge.NewSessionManager(ctx, cmuxClient, mgr, cfg.AI)

	// Route all IM messages to session manager
	mgr.OnMessage(func(msg channels.InboundMessage) {
		// Check user whitelist
		if !cfg.IsUserAllowed(msg.UserID) {
			log.Printf("[main] unauthorized user %s, ignoring", msg.UserID)
			return
		}
		sessionMgr.HandleMessage(msg)
	})

	// Start channels
	if err := mgr.Start(ctx); err != nil {
		log.Fatalf("failed to start channels: %v", err)
	}

	log.Println("[main] im-bridge started, waiting for messages...")

	// Wait for shutdown signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("[main] shutting down...")
	mgr.Stop()
	log.Println("[main] stopped")
}
