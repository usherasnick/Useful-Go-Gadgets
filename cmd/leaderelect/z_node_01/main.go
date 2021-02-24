package main

import (
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	leaderelect "github.com/usherasnick/Useful-Go-Gadgets/leader-elect-by-zk"
)

func main() {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339})

	cfg := &leaderelect.ElectorCfg{
		ZkEndpoints: []string{"127.0.0.1:2181", "127.0.0.1:2182", "127.0.0.1:2183"},
		Heartbeat:   2,
	}
	e := leaderelect.NewElector(cfg)
	go e.ElectLeader("z_node_01")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)

	for range sigCh {
		e.Close()
		break
	}
}
