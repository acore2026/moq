package main

import (
	"crypto/tls"
	"crypto/x509"
	"flag"
	"fmt"
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

const PORT = 4443

var ALPNS = []string{"moq-00"} // Application Layer Protocols ["H3" - WebTransport]
const (
	RELAY    = "localhost:4443"
	CERTPATH = "./examples/certs/localhost.crt"
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

	pub := api.NewMOQPub(Options, RELAY)
	handler, err := pub.Connect()

	pub.OnSubscribe(func(ps moqt.PubStream) {
		log.Debug().Msgf("New Subscribe Request - %s", ps.TrackName)
		go handleStream(&ps)
	})

	if err != nil {
		log.Error().Msgf("error - %s", err)
		return
	}

	handler.SendAnnounce("bbb")

	<-pub.Ctx.Done()
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

func handleStream(stream *moqt.PubStream) {
	stream.Accept()

	groupid := uint64(0)

	for {
		gs, err := stream.NewGroup(groupid)

		if err != nil {
			log.Error().Msgf("Err - %s", err)
			return
		}

		objectid := uint64(0)

		for range 10 {
			gs.WriteObject(&wire.Object{
				GroupID: groupid,
				ID:      objectid,
				Payload: []byte("Test"),
			})
			objectid++
		}

		gs.Close()

		groupid++

		<-time.After(time.Millisecond * 50)
	}
}
