package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Initialize multi-instance manager
	instances := bridge.NewInstanceManager()
	// Register primary cmux instance from config
	if _, err := instances.Register(cfg.Cmux.SocketPath); err != nil {
		log.Printf("[main] warning: primary cmux not reachable: %v", err)
	}
	// Discover additional instances (staging, debug, nightly)
	if err := instances.Discover(); err != nil {
		log.Printf("[main] instance discovery: %v", err)
	}
	// Start periodic health checks
	instances.StartHealthLoop(ctx, 30*time.Second)

	// Obtain the primary client (falls back to any healthy instance)
	cmuxClient, err := instances.Default()
	if err != nil {
		log.Fatalf("no reachable cmux instance: %v", err)
	}

	// Verify connection
	if err := cmuxClient.Ping(); err != nil {
		log.Fatalf("cmux ping failed: %v", err)
	}
	log.Println("[main] connected to cmux")

	// Setup channel manager
	mgr := channels.NewManager()

	// Build a map from channel name to its typed config pointer.
	channelConfigs := map[string]interface{}{
		"telegram": &cfg.Telegram,
		"slack":    &cfg.Slack,
		"feishu":   &cfg.Feishu,
		// TODO: add "discord": &cfg.Discord when DiscordConfig is added to config.Config
	}

	// Determine which channels are enabled via config flags.
	enabledChannels := []string{}
	if cfg.Telegram.Enabled {
		enabledChannels = append(enabledChannels, "telegram")
	}
	if cfg.Slack.Enabled {
		enabledChannels = append(enabledChannels, "slack")
	}
	if cfg.Feishu.Enabled {
		enabledChannels = append(enabledChannels, "feishu")
	}
	// TODO: add Discord when DiscordConfig is wired into config.Config

	// Register enabled channels via factory pattern
	for _, name := range enabledChannels {
		factory, ok := channels.GetFactory(name)
		if !ok {
			log.Printf("[main] unknown channel: %s, skipping", name)
			continue
		}

		channelCfg, ok := channelConfigs[name]
		if !ok {
			log.Printf("[main] no config mapping for channel %s, skipping", name)
			continue
		}

		ch, err := factory(channelCfg)
		if err != nil {
			log.Fatalf("[main] failed to create channel %s: %v", name, err)
		}
		mgr.Register(ch)
		log.Printf("[main] channel %s registered", name)
	}

	// Setup session manager
	// TODO: pass instances to SessionManager when it supports multi-instance routing
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
