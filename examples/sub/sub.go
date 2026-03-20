package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/DineshAdhi/moq-go/moqt"
	"github.com/DineshAdhi/moq-go/moqt/api"
	"github.com/DineshAdhi/moq-go/moqt/wire"
	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

var ALPNS = []string{"moq-00"} // Application Layer Protocols ["H3" - WebTransport]
const (
	RELAY    = "localhost:4443"
	CERTPATH = "./examples/certs/localhost.crt"
	NS       = "bbb"
	TRACK    = "demo"
)

func main() {
	go func() {
		http.ListenAndServe(":8080", nil)
	}()

	debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	zerolog.CallerMarshalFunc = func(pc uintptr, file string, line int) string {
		return filepath.Base(file) + ":" + strconv.Itoa(line)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.StampMilli}).With().Caller().Logger()
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if *debug {
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	Options := moqt.DialerOptions{
		ALPNs: ALPNS,
		QuicConfig: &quic.Config{
			EnableDatagrams: true,
			KeepAlivePeriod: 2 * time.Second,
		},
		TLSConfig:                 mustLoadTLSConfig(CERTPATH),
		EnableConnectionMigration: true,
	}

	sub := api.NewMOQSub(Options, RELAY)

	_, err := sub.Connect()

	sub.OnStream(func(ss moqt.SubStream) {
		go handleStream(&ss)
	})

	sub.OnAnnounce(func(ns string) {
		sub.Subscribe(ns, TRACK, 0)
	})

	if err != nil {
		log.Error().Msgf("Error - %s", err)
		return
	}

	sub.Subscribe(NS, TRACK, 0)

	<-sub.Ctx.Done()
}

func mustLoadTLSConfig(certPath string) *tls.Config {
	certPEM, err := os.ReadFile(certPath)
	if err != nil {
		panic(fmt.Sprintf("read cert %s: %v", certPath, err))
	}

	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(certPEM) {
		panic(fmt.Sprintf("append cert %s: invalid pem", certPath))
	}

	return &tls.Config{
		RootCAs:    pool,
		ServerName: "localhost",
	}
}

func handleStream(ss *moqt.SubStream) {

	log.Debug().Msgf("New Stream Header")

	for moqtstream := range ss.StreamsChan {
		log.Debug().Msgf("New Group Stream - %s", moqtstream.GetStreamID())
		go handleMOQStream(moqtstream)
	}
}

func handleMOQStream(stream wire.MOQTStream) {

	for {
		groupid, object, err := stream.ReadObject()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Error().Msgf("Error Reading Objects - %s", err)
			break
		}

		msg := string(object.Payload[:])
		log.Printf("Payload - %d %s - %d", groupid, msg, object.ID)
	}

	log.Printf("Group Ended")

}
