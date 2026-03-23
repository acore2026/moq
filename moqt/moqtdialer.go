package moqt

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/quic-go/quic-go"
	"github.com/rs/zerolog/log"
)

// DialerOptions 包含拨号器配置
type DialerOptions struct {
	QuicConfig      *quic.Config
	ALPNs           []string
	EnableMigration bool
}

// MOQTDialer 支持连接迁移的MOQT拨号器
type MOQTDialer struct {
	Options   DialerOptions
	Role      uint64
	Ctx       context.Context
	transport *quic.Transport
}

// Dial 拨号并创建支持迁移的会话
func (d *MOQTDialer) Dial(addr string) (*MOQTSession, error) {
	Options := d.Options

	tlsConfig := tls.Config{
		NextProtos: Options.ALPNs,
	}

	var conn *quic.Conn
	var err error

	if Options.EnableMigration {
		// 使用支持迁移的连接
		conn, err = dialMigratableConn(d.Ctx, addr, &tlsConfig, Options.QuicConfig, 2*time.Second)
		if err != nil {
			return nil, fmt.Errorf("failed to dial with migration support: %w", err)
		}
		log.Info().Msg("MOQT connection established with migration support")
	} else {
		// 解析目标地址
		udpAddr, err := net.ResolveUDPAddr("udp", addr)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve address: %w", err)
		}

		// 创建UDP socket
		udpConn, err := net.ListenUDP("udp", &net.UDPAddr{Port: 0})
		if err != nil {
			return nil, fmt.Errorf("failed to create UDP socket: %w", err)
		}

		// 创建Transport
		transport := &quic.Transport{
			Conn: udpConn,
		}
		d.transport = transport

		// 使用Transport Dial
		conn, err = transport.Dial(d.Ctx, udpAddr, &tlsConfig, Options.QuicConfig)
		if err != nil {
			transport.Close()
			return nil, fmt.Errorf("failed to dial: %w", err)
		}
	}

	log.Info().
		Str("local_addr", conn.LocalAddr().String()).
		Str("remote_addr", conn.RemoteAddr().String()).
		Msg("MOQT connection established")

	session, err := CreateMOQSession(conn, d.Role, CLIENT_MODE)
	if err != nil {
		conn.CloseWithError(0, "session creation failed")
		if d.transport != nil {
			d.transport.Close()
		}
		return nil, err
	}

	session.ServeMOQ()

	timeout := time.After(time.Second * 5)

	select {
	case <-session.HandshakeDone:
		return session, nil
	case <-timeout:
		return nil, fmt.Errorf("[Error Dialing MOQT][Timeout]")
	}
}

// GetTransport 返回底层Transport
func (d *MOQTDialer) GetTransport() *quic.Transport {
	return d.transport
}

// Close 关闭拨号器和所有资源
func (d *MOQTDialer) Close() error {
	if d.transport != nil {
		d.transport.Close()
	}
	return nil
}
