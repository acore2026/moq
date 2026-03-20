package moqt

import (
	"sync"

	"github.com/DineshAdhi/moq-go/moqt/wire"
)

type ResumeSubscription struct {
	Namespace string
	TrackName string
	Alias     uint64
}

func (s ResumeSubscription) StreamID() string {
	return s.Namespace + "_" + s.TrackName
}

type ResumeCheckpoint struct {
	GroupID   uint64
	ObjectID  uint64
	Available bool
}

type ResumeTracker struct {
	lock          sync.RWMutex
	subscriptions map[string]ResumeSubscription
	checkpoints   map[string]ResumeCheckpoint
}

func NewResumeTracker() *ResumeTracker {
	return &ResumeTracker{
		subscriptions: map[string]ResumeSubscription{},
		checkpoints:   map[string]ResumeCheckpoint{},
	}
}

func (rt *ResumeTracker) Register(ns string, name string, alias uint64) {
	rt.lock.Lock()
	defer rt.lock.Unlock()

	rt.subscriptions[ns+"_"+name] = ResumeSubscription{
		Namespace: ns,
		TrackName: name,
		Alias:     alias,
	}
}

func (rt *ResumeTracker) Update(streamID string, groupID uint64, objectID uint64) {
	rt.lock.Lock()
	defer rt.lock.Unlock()

	rt.checkpoints[streamID] = ResumeCheckpoint{
		GroupID:   groupID,
		ObjectID:  objectID,
		Available: true,
	}
}

func (rt *ResumeTracker) BuildSubscribe(ns string, name string, alias uint64, subid uint64) wire.Subscribe {
	rt.Register(ns, name, alias)

	msg := wire.Subscribe{
		SubscribeID:    subid,
		TrackAlias:     alias,
		TrackName:      name,
		TrackNameSpace: ns,
		FilterType:     wire.LatestGroup,
	}

	rt.lock.RLock()
	checkpoint, ok := rt.checkpoints[msg.GetStreamID()]
	rt.lock.RUnlock()

	if ok && checkpoint.Available {
		msg.FilterType = wire.AbsoluteStart
		msg.StartGroup = checkpoint.GroupID
		msg.StartObject = checkpoint.ObjectID + 1
	}

	return msg
}

func (rt *ResumeTracker) ResubscribeAll(handler *SubHandler) {
	rt.lock.RLock()
	subs := make([]ResumeSubscription, 0, len(rt.subscriptions))
	for _, sub := range rt.subscriptions {
		subs = append(subs, sub)
	}
	rt.lock.RUnlock()

	for _, sub := range subs {
		handler.Subscribe(sub.Namespace, sub.TrackName, sub.Alias)
	}
}

type ResumableStream struct {
	wire.MOQTStream
	streamID string
	tracker  *ResumeTracker
}

func NewResumableStream(stream wire.MOQTStream, streamID string, tracker *ResumeTracker) wire.MOQTStream {
	if tracker == nil {
		return stream
	}

	return &ResumableStream{
		MOQTStream: stream,
		streamID:   streamID,
		tracker:    tracker,
	}
}

func (rs *ResumableStream) ReadObject() (uint64, *wire.Object, error) {
	groupID, object, err := rs.MOQTStream.ReadObject()

	if err == nil && object != nil {
		rs.tracker.Update(rs.streamID, groupID, object.ID)
	}

	return groupID, object, err
}
