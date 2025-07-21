package conductor

import (
	"encoding/json"
	"fmt"
	"reflect"
	"runtime/debug"
	"strings"
	"time"
	"unicode"
)

// DefaultEventCodec is lazily initialized once MustRegisterEventType is used.
var DefaultEventCodec *EventCodec

var mustGetVCSRevisionFromBuildInfo = func() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		panic("no build info to read vcs.revision from")
	}
	for _, kv := range info.Settings {
		if kv.Key == "vcs.revision" {
			return kv.Value
		}
	}
	panic("no vcs.revision in build info")
}

// MustRegisterEventType registers a global event type in the default codec.
func MustRegisterEventType[T Event](name string) {
	if DefaultEventCodec == nil {
		revisionVCS := mustGetVCSRevisionFromBuildInfo()
		DefaultEventCodec = NewTypeCodec(revisionVCS)
	}
	MustRegisterEventTypeIn[T](DefaultEventCodec, name)
}

// EventCodec handles event marshaling and unmarshaling.
type EventCodec struct {
	revisionVCS         string
	eventTypeByName     map[string]reflect.Type
	eventTypeNameByType map[reflect.Type]string

	inUse bool // Set to true by Conductor once this codec is in use.
}

// NewTypeCodec creates a new type codec.
// revisionVCS is the version control system revision of this instance and
// can be populated using MustGetVCSRevisionFromBuildInfo.
func NewTypeCodec(revisionVCS string) *EventCodec {
	return &EventCodec{
		revisionVCS:         revisionVCS,
		eventTypeByName:     map[string]reflect.Type{},
		eventTypeNameByType: map[reflect.Type]string{},
	}
}

// MustRegisterEventTypeIn registers an event type to codec.
func MustRegisterEventTypeIn[T Event](codec *EventCodec, name string) {
	if codec.inUse {
		panic("attempting to register event type at Conductor runtime")
	}
	switch {
	case name == "":
		panic("empty event name")
	case unicode.IsSpace(rune(name[0])):
		panic("event name starts with space characters")
	case unicode.IsSpace(rune(name[len(name)-1])):
		panic("event name ends with space characters")
	}
	if _, ok := codec.eventTypeByName[name]; ok {
		panic(fmt.Sprintf("event already registered: %q", name))
	}

	var zero T
	t := reflect.TypeOf(zero)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	codec.eventTypeByName[name] = t
	codec.eventTypeNameByType[t] = name
}

func (r *EventCodec) createEventObject(name string) Event {
	t, ok := r.eventTypeByName[name]
	if !ok {
		return nil
	}
	e := reflect.New(t).Interface().(Event)
	e.metadata().name = name
	return e
}

func (r *EventCodec) initializeEvent(
	e Event, now func() time.Time,
) error {
	evType := reflect.TypeOf(e)
	if evType.Kind() == reflect.Ptr {
		evType = evType.Elem()
	}
	typeName := r.eventTypeNameByType[evType]
	if typeName == "" {
		return fmt.Errorf("event %T not registered", e)
	}

	tm := e.Time()
	if tm.IsZero() {
		// Set time only if it wasn't set already.
		tm = now()
	}

	m := e.metadata()
	m.t, m.revisionVCS, m.name = tm, r.revisionVCS, typeName

	return nil
}

type noimpl struct{}

// Event is any of the types with prefix "Event" from package domain.
type Event interface {
	// This prevents anything but the EventMetadata implementing this interface.
	noimpl() noimpl

	metadata() *EventMetadata

	// Version returns the unique version of the event.
	Version() int64

	// Name returns the event type name.
	Name() string

	// Time returns the event production time.
	Time() time.Time

	// RevisionVCS returns the version control system revision of the event producer.
	RevisionVCS() string
}

func (r *EventCodec) EncodeJSON(e Event) (string, error) {
	var b strings.Builder
	d := json.NewEncoder(&b)
	err := d.Encode(e)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}

func (r *EventCodec) DecodeJSON(name string, payload []byte) (Event, error) {
	e := r.createEventObject(name)
	if e == nil {
		return nil, fmt.Errorf("event %q not registered", name)
	}
	if err := json.Unmarshal(payload, e); err != nil {
		return nil, err
	}
	return e, nil
}

// EventMetadata must be embedded by every type that implements Event.
// Example event type:
//
//	type SomethingHappened struct {
//		conductor.EventMetadata
//
//		MyDataField string `json:"my-data-field"`
//	}
//
// The event must be registered using MustRegisterEventType:
//
//	conductor.MustRegisterEventType[*SomethingHappened]("something-happened")
//
// By default, events are registered globally.
// It is adviced to register the type during package init.
type EventMetadata struct {
	name        string
	t           time.Time
	revisionVCS string
	version     int64
}

func (e *EventMetadata) metadata() *EventMetadata { return e }
func (e *EventMetadata) noimpl() noimpl           { return noimpl{} }

func (e *EventMetadata) Version() int64      { return e.version }
func (e *EventMetadata) Time() time.Time     { return e.t }
func (e *EventMetadata) Name() string        { return e.name }
func (e *EventMetadata) RevisionVCS() string { return e.revisionVCS }
