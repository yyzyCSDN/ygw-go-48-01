package model

// Event is a single record flowing through the stream engine. The timestamp is
// the event time assigned by the producing application, while Offset is the
// monotonically increasing position assigned by the source.
type Event struct {
	Key       string
	Value     int64
	Timestamp int64
	Offset    int64
	Kind      EventKind
}

// EventKind describes the payload category of an event. It is used by the
// window layer to decide whether an event contributes to aggregation or only
// advances the session liveness of a key.
type EventKind int

const (
	KindData EventKind = iota
	KindSessionPing
	KindControl
)

// NewData builds a data event with the given routing key and payload.
func NewData(key string, value int64, ts int64, offset int64) Event {
	return Event{Key: key, Value: value, Timestamp: ts, Offset: offset, Kind: KindData}
}

// NewPing builds a session liveness event that does not carry aggregation
// value. Pings still participate in session timeout evaluation.
func NewPing(key string, ts int64, offset int64) Event {
	return Event{Key: key, Timestamp: ts, Offset: offset, Kind: KindSessionPing}
}
