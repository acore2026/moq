package moqt

import (
	"io"
	"sync"

	"github.com/DineshAdhi/moq-go/moqt/wire"

	"github.com/quic-go/quic-go/quicvarint"
	"github.com/rs/zerolog/log"
)

type RelayStream struct {
	SubID           uint64
	StreamID        string
	Map             *StreamsMap[*RelayStream]
	Subscribers     map[string]*RelaySubscriber
	SubscribersLock sync.RWMutex
	ObjectCache     []wire.Object
	ObjectCacheLock sync.RWMutex
	SourceStream    wire.MOQTStream
}

type RelaySubscriber struct {
	Handler    *RelayHandler
	StartIndex int
}

func (rs *RelayStream) GetSubID() uint64 {
	return rs.SubID
}

func (rs *RelayStream) GetStreamID() string {
	return rs.StreamID
}

func NewRelayStream(subid uint64, id string, smap *StreamsMap[*RelayStream]) *RelayStream {

	rs := &RelayStream{}
	rs.SubID = subid
	rs.StreamID = id
	rs.Map = smap
	rs.Subscribers = map[string]*RelaySubscriber{}
	rs.SubscribersLock = sync.RWMutex{}
	rs.ObjectCache = make([]wire.Object, 0)
	rs.ObjectCacheLock = sync.RWMutex{}

	return rs
}

func (rs *RelayStream) AddSubscriber(handler *RelayHandler, msg *wire.Subscribe) int {
	startIndex := rs.GetStartIndex(msg)

	rs.SubscribersLock.Lock()
	defer rs.SubscribersLock.Unlock()

	rs.Subscribers[handler.Id] = &RelaySubscriber{
		Handler:    handler,
		StartIndex: startIndex,
	}

	return startIndex
}

func (os *RelayStream) RemoveSubscriber(id string) {
	os.SubscribersLock.Lock()
	defer os.SubscribersLock.Unlock()

	delete(os.Subscribers, id)
}

func (rs *RelayStream) SetSourceStream(stream wire.MOQTStream) {
	rs.ObjectCacheLock.Lock()
	defer rs.ObjectCacheLock.Unlock()

	rs.SourceStream = stream
}

func (rs *RelayStream) GetSourceStream() wire.MOQTStream {
	rs.ObjectCacheLock.RLock()
	defer rs.ObjectCacheLock.RUnlock()

	return rs.SourceStream
}

func (rs *RelayStream) AppendObject(object wire.Object) {
	rs.ObjectCacheLock.Lock()
	defer rs.ObjectCacheLock.Unlock()

	rs.ObjectCache = append(rs.ObjectCache, object)
}

func (rs *RelayStream) GetStartIndex(msg *wire.Subscribe) int {
	if msg == nil || (msg.FilterType != wire.AbsoluteStart && msg.FilterType != wire.AbsoluteRange) {
		return 0
	}

	rs.ObjectCacheLock.RLock()
	defer rs.ObjectCacheLock.RUnlock()

	for i, object := range rs.ObjectCache {
		if object.GroupID > msg.StartGroup {
			return i
		}

		if object.GroupID == msg.StartGroup && object.ID >= msg.StartObject {
			return i
		}
	}

	return len(rs.ObjectCache)
}

func (rs *RelayStream) ForwardSubscribeOk(msg wire.SubscribeOk) {

	for _, sub := range rs.Subscribers {
		if handler := sub.Handler; handler != nil {
			handler.SendSubscribeOk(rs.GetStreamID(), msg)
		}
	}
}

func (rs *RelayStream) ForwardStream(stream wire.MOQTStream) {
	rs.SubscribersLock.RLock()
	defer rs.SubscribersLock.RUnlock()

	for _, sub := range rs.Subscribers {
		stream.WgAdd()
		go sub.Handler.ProcessMOQTStreamFrom(stream, sub.StartIndex)
	}

	stream.WgWait() // Wait till all the subcribers are ready to read the Objects.
}

func (rs *RelayStream) ProcessObjects(stream wire.MOQTStream, reader quicvarint.Reader) {
	rs.SetSourceStream(stream)

	// Forwarding Streams to all Subscribers. Wait for all subscribers to write the Stream Header in CS and then start reading the Objects.
	rs.ForwardStream(stream)

	for {
		_, object, err := stream.ReadObject()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Debug().Msgf("[Error Reading Object][%s]", err)
			return
		}

		rs.AppendObject(*object)
	}
}
