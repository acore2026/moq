package api

import (
	"context"
	"sync"
	"time"

	"github.com/DineshAdhi/moq-go/moqt"
	"github.com/DineshAdhi/moq-go/moqt/wire"
	"github.com/rs/zerolog/log"
)

type MOQSub struct {
	Options           moqt.DialerOptions
	Relay             string
	Ctx               context.Context
	onStreamHandler   func(moqt.SubStream)
	onAnnounceHandler func(string)
	handler           *moqt.SubHandler
	resumeTracker     *moqt.ResumeTracker
	lock              sync.RWMutex
	monitorOnce       sync.Once
}

func NewMOQSub(options moqt.DialerOptions, relay string) *MOQSub {
	sub := &MOQSub{
		Options:       options,
		Relay:         relay,
		Ctx:           context.TODO(),
		resumeTracker: moqt.NewResumeTracker(),
	}

	return sub
}

func (sub *MOQSub) OnStream(f func(moqt.SubStream)) {
	sub.onStreamHandler = f
}

func (sub *MOQSub) OnAnnounce(f func(string)) {
	sub.onAnnounceHandler = f
}

func (pub *MOQSub) Connect() (*moqt.SubHandler, error) {
	handler, err := pub.connectOnce()

	if err != nil {
		return nil, err
	}

	pub.monitorOnce.Do(func() {
		go pub.monitorSessions()
	})

	return handler, nil
}

func (sub *MOQSub) connectOnce() (*moqt.SubHandler, error) {
	log.Info().Msgf("subscriber dialing relay %s", sub.Relay)

	dialer := moqt.MOQTDialer{
		Options: sub.Options,
		Role:    wire.ROLE_SUBSCRIBER,
		Ctx:     sub.Ctx,
	}

	session, err := dialer.Dial(sub.Relay)

	if err != nil {
		log.Warn().Err(err).Msgf("subscriber dial failed %s", sub.Relay)
		return nil, err
	}

	log.Info().Msg("subscriber connected")

	handler := session.SubHandler()
	handler.AttachResumeTracker(sub.resumeTracker)

	sub.lock.Lock()
	sub.handler = handler
	sub.lock.Unlock()

	go func() {
		for stream := range handler.StreamsChan {
			if sub.onStreamHandler != nil {
				sub.onStreamHandler(stream)
			}
		}
	}()

	go func() {
		for ns := range handler.AnnounceChan {
			if sub.onAnnounceHandler != nil {
				sub.onAnnounceHandler(ns)
			}
		}
	}()

	sub.resumeTracker.ResubscribeAll(handler)

	return session.SubHandler(), nil
}

func (sub *MOQSub) Subscribe(ns string, name string, alias uint64) {
	sub.resumeTracker.Register(ns, name, alias)

	sub.lock.RLock()
	handler := sub.handler
	sub.lock.RUnlock()

	if handler != nil {
		handler.Subscribe(ns, name, alias)
	}
}

func (sub *MOQSub) monitorSessions() {
	for {
		sub.lock.RLock()
		handler := sub.handler
		sub.lock.RUnlock()

		if handler == nil {
			if !sub.waitReconnect() {
				return
			}

			continue
		}

		select {
		case <-sub.Ctx.Done():
			return
		case <-handler.MOQTSession.Done():
		}

		log.Warn().Msg("subscriber session closed, starting reconnect loop")

		if !sub.reconnectLoop() {
			return
		}
	}
}

func (sub *MOQSub) reconnectLoop() bool {
	backoff := time.Second

	for {
		select {
		case <-sub.Ctx.Done():
			return false
		default:
		}

		if _, err := sub.connectOnce(); err == nil {
			log.Info().Msg("subscriber reconnect succeeded")
			return true
		} else {
			log.Warn().Err(err).Dur("backoff", backoff).Msg("subscriber reconnect failed")
		}

		if !sub.waitBackoff(backoff) {
			return false
		}

		if backoff < 8*time.Second {
			backoff *= 2
		}
	}
}

func (sub *MOQSub) waitReconnect() bool {
	return sub.waitBackoff(time.Second)
}

func (sub *MOQSub) waitBackoff(d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-sub.Ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
