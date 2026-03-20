package moqt

import (
	"io"

	"github.com/DineshAdhi/moq-go/moqt/wire"
)

type PubStream struct {
	session      *MOQTSession
	StreamID     string
	SubID        uint64
	Namespace    string
	TrackName    string
	Alias        uint64
	FilterType   uint64
	StartGroup   uint64
	StartObject  uint64
	EndGroup     uint64
	EndObject    uint64
	GroupCounter uint64
}

func NewPubStream(session *MOQTSession, msg *wire.Subscribe) *PubStream {
	return &PubStream{
		session:      session,
		StreamID:     msg.GetStreamID(),
		SubID:        msg.SubscribeID,
		Namespace:    msg.TrackNameSpace,
		TrackName:    msg.TrackName,
		Alias:        msg.TrackAlias,
		FilterType:   msg.FilterType,
		StartGroup:   msg.StartGroup,
		StartObject:  msg.StartObject,
		EndGroup:     msg.EndGroup,
		EndObject:    msg.EndObject,
		GroupCounter: 0,
	}
}

func (pub *PubStream) Accept() {
	okmsg := wire.SubscribeOk{
		SubscribeID:   pub.SubID,
		Expires:       1024,
		ContentExists: 0,
	}

	pub.session.CS.WriteControlMessage(&okmsg)
}

func (pub *PubStream) NewGroup(id uint64) (wire.MOQTStream, error) {
	stream := wire.NewGroupStream(pub.SubID, id, pub.Alias)
	return pub.NewStream(stream)
}

func (pub *PubStream) NewTrack() (wire.MOQTStream, error) {
	stream := wire.NewTrackStream(pub.SubID, pub.Alias)
	return pub.NewStream(stream)
}

func (pub *PubStream) NewStream(stream wire.MOQTStream) (wire.MOQTStream, error) {

	stream.WgAdd()

	unistream, err := pub.session.Conn.OpenUniStream()

	if err != nil {
		return stream, err
	}

	unistream.Write(stream.GetHeaderBytes())

	go func() {
		itr := 0

		stream.WgDone()

		for {
			itr, err = stream.Pipe(itr, unistream)

			if err == io.EOF {
				break
			}

			if err != nil {
				pub.session.Slogger.Error().Msgf("[Error Piping Objects][%s]", err)
			}
		}

		unistream.Close()
	}()

	stream.WgWait()

	return stream, nil
}

func (pub PubStream) GetStreamID() string {
	return pub.StreamID
}

func (pub PubStream) GetSubID() uint64 {
	return pub.SubID
}
