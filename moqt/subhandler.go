package moqt

import (
	"math/rand"

	"github.com/DineshAdhi/moq-go/moqt/wire"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/rs/zerolog/log"
)

type SubHandler struct {
	*MOQTSession
	SubscribedStreams StreamsMap[*SubStream]
	StreamsChan       chan SubStream
	AnnounceChan      chan string
	ResumeTracker     *ResumeTracker
}

func NewSubHandler(session *MOQTSession) *SubHandler {
	return &SubHandler{
		MOQTSession:       session,
		SubscribedStreams: NewStreamsMap[*SubStream](session),
		StreamsChan:       make(chan SubStream),
		AnnounceChan:      make(chan string),
		ResumeTracker:     nil,
	}
}

func (sub *SubHandler) AttachResumeTracker(tracker *ResumeTracker) {
	sub.ResumeTracker = tracker
}

func (sub *SubHandler) Subscribe(ns string, name string, alias uint64) {

	subid := uint64(rand.Uint32())

	msg := wire.Subscribe{
		SubscribeID:    subid,
		TrackAlias:     alias,
		TrackName:      name,
		TrackNameSpace: ns,
		FilterType:     wire.LatestGroup,
	}

	if sub.ResumeTracker != nil {
		msg = sub.ResumeTracker.BuildSubscribe(ns, name, alias, subid)
	}

	sub.CS.WriteControlMessage(&msg)

	substream := NewSubStream(msg.GetStreamID(), subid, sub.ResumeTracker)
	sub.SubscribedStreams.AddStream(subid, substream)
}

func (sub *SubHandler) HandleAnnounce(msg *wire.Announce) {
	log.Debug().Msg(msg.String())
	sub.AnnounceChan <- msg.TrackNameSpace
}

func (sub *SubHandler) HandleSubscribe(msg *wire.Subscribe) {
}

func (sub *SubHandler) HandleSubscribeOk(msg *wire.SubscribeOk) {
	sub.Slogger.Info().Msg(msg.String())

	ss, ok := sub.SubscribedStreams.SubIDGetStream(msg.SubscribeID)

	if ok {
		sub.StreamsChan <- *ss
	} else {
		log.Error().Msgf("[Cannot find Substream with SubID - %X]", msg.SubscribeID)
	}
}

func (sub *SubHandler) HandleAnnounceOk(msg *wire.AnnounceOk) {

}

func (sub *SubHandler) HandleUnsubscribe(msg *wire.Unsubcribe) {

}

func (sub *SubHandler) HandleSubscribeDone(msg *wire.SubscribeDone) {

}

func (sub *SubHandler) DoHandle() {

	for {
		unistream, err := sub.Conn.AcceptUniStream(sub.ctx)

		if err != nil {
			sub.Slogger.Error().Msgf("[Error Accepting Unistream][%s]", err)
			sub.Close(wire.MOQERR_INTERNAL_ERROR, "[Subscriber Unistream Closed]")
			return
		}

		reader := quicvarint.NewReader(unistream)
		subid, stream, err := wire.ParseMOQTStream(reader)

		if err != nil {
			sub.Slogger.Error().Msgf("[Error Parsing MOQT Stream][%s]", err)
			continue
		}

		if ss, ok := sub.SubscribedStreams.SubIDGetStream(subid); ok {
			go ss.ProcessObjects(stream, reader)
		} else {
			sub.Slogger.Error().Msgf("Received Header with unknown subid - %X", subid)
		}
	}
}

func (sub *SubHandler) HandleClose() {
	close(sub.StreamsChan)
	close(sub.AnnounceChan)
}
