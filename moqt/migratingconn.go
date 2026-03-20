package moqt

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"
)

const defaultPathMigrationPollInterval = 2 * time.Second

type migratingClientConn struct {
	quic.Connection

	ctx        context.Context
	cancel     context.CancelFunc
	remoteAddr *net.UDPAddr

	pollInterval time.Duration

	transportLock sync.Mutex
	transports    []*quic.Transport

	sourceIPLock sync.RWMutex
	sourceIP     string

	closeOnce sync.Once
}

func dialMigratableConn(ctx context.Context, addr string, tlsConfig *tls.Config, quicConfig *quic.Config, pollInterval time.Duration) (quic.Connection, error) {
	remoteAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}

	transport, sourceIP, err := newMigrationTransport(remoteAddr)
	if err != nil {
		return nil, err
	}

	conn, err := transport.Dial(ctx, remoteAddr, tlsConfig, quicConfig)
	if err != nil {
		transport.Close()
		return nil, err
	}

	if pollInterval <= 0 {
		pollInterval = defaultPathMigrationPollInterval
	}

	monitorCtx, cancel := context.WithCancel(context.Background())
	mc := &migratingClientConn{
		Connection:   conn,
		ctx:          monitorCtx,
		cancel:       cancel,
		remoteAddr:   remoteAddr,
		pollInterval: pollInterval,
		transports:   []*quic.Transport{transport},
		sourceIP:     sourceIP,
	}

	go mc.monitorPathChanges()
	go mc.closeOnConnectionDone()

	return mc, nil
}

func newMigrationTransport(remoteAddr *net.UDPAddr) (*quic.Transport, string, error) {
	sourceIP, err := preferredSourceIP(remoteAddr)
	if err != nil {
		return nil, "", err
	}

	localAddr := &net.UDPAddr{Port: 0}
	if sourceIP != "" {
		localAddr.IP = net.ParseIP(sourceIP)
	}

	udpConn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, "", err
	}

	return &quic.Transport{Conn: udpConn}, sourceIP, nil
}

func preferredSourceIP(remoteAddr *net.UDPAddr) (string, error) {
	conn, err := net.DialUDP("udp", nil, remoteAddr)
	if err != nil {
		return "", err
	}
	defer conn.Close()

	localAddr, ok := conn.LocalAddr().(*net.UDPAddr)
	if !ok || localAddr.IP == nil {
		return "", nil
	}

	return localAddr.IP.String(), nil
}

func (mc *migratingClientConn) monitorPathChanges() {
	ticker := time.NewTicker(mc.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mc.ctx.Done():
			return
		case <-mc.Connection.Context().Done():
			return
		case <-ticker.C:
		}

		sourceIP, err := preferredSourceIP(mc.remoteAddr)
		if err != nil || sourceIP == "" {
			continue
		}

		mc.sourceIPLock.RLock()
		currentSourceIP := mc.sourceIP
		mc.sourceIPLock.RUnlock()

		if sourceIP == currentSourceIP {
			continue
		}

		if err := mc.switchPath(sourceIP); err != nil {
			log.Warn().Err(err).Str("source_ip", sourceIP).Msg("connection migration failed")
			continue
		}

		mc.sourceIPLock.Lock()
		mc.sourceIP = sourceIP
		mc.sourceIPLock.Unlock()

		log.Info().Str("source_ip", sourceIP).Msg("connection migrated to new source ip")
	}
}

func (mc *migratingClientConn) switchPath(sourceIP string) error {
	transport, _, err := newMigrationTransport(mc.remoteAddr)
	if err != nil {
		return err
	}

	path, err := mc.Connection.AddPath(transport)
	if err != nil {
		transport.Close()
		return err
	}

	probeCtx, cancel := context.WithTimeout(mc.ctx, 5*time.Second)
	defer cancel()

	if err := path.Probe(probeCtx); err != nil {
		path.Close()
		transport.Close()
		return err
	}

	if err := path.Switch(); err != nil {
		path.Close()
		transport.Close()
		return err
	}

	mc.transportLock.Lock()
	mc.transports = append(mc.transports, transport)
	mc.transportLock.Unlock()

	return nil
}

func (mc *migratingClientConn) closeOnConnectionDone() {
	<-mc.Connection.Context().Done()
	mc.shutdown()
}

func (mc *migratingClientConn) CloseWithError(code quic.ApplicationErrorCode, msg string) error {
	err := mc.Connection.CloseWithError(code, msg)
	mc.shutdown()
	return err
}

func (mc *migratingClientConn) shutdown() {
	mc.closeOnce.Do(func() {
		mc.cancel()

		mc.transportLock.Lock()
		transports := append([]*quic.Transport(nil), mc.transports...)
		mc.transportLock.Unlock()

		for _, transport := range transports {
			if transport == nil {
				continue
			}

			if err := transport.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Debug().Err(err).Msg("closing quic transport")
			}
		}
	})
}
