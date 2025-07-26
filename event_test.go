package conductor

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type EventTest struct {
	EventMetadata

	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

// EventTestUnregistered is intentionally not added to byTypeName.
type EventTestUnregistered struct {
	EventMetadata

	Foo string `json:"foo"`
	Bar int    `json:"bar"`
}

func NewTestCodec(t *testing.T) *EventCodec {
	t.Helper()
	ec := NewTypeCodec("test-revision")
	MustRegisterEventTypeIn[*EventTest](ec, "test-event")
	return ec
}

func TestJSONCodec(t *testing.T) {
	t.Parallel()
	ec := NewTestCodec(t)

	newEvent := &EventTest{
		Foo: "foo-value",
		Bar: 42,
	}
	newEvent.typeName = "test-event"

	s, err := ec.EncodeJSON(newEvent)
	require.NoError(t, err)
	require.JSONEq(t, `{
		"foo": "foo-value",
		"bar": 42
	}`, s)

	unmarshaled, err := ec.DecodeJSON("test-event", []byte(s))

	require.NoError(t, err)
	require.Equal(t, newEvent, unmarshaled)
	require.Equal(t, "test-event", unmarshaled.TypeName())
}

func TestNew(t *testing.T) {
	t.Parallel()
	tm := time.Date(2025, 1, 1, 1, 1, 1, 0, time.UTC)
	e := &EventTest{}
	e.metadata().t = tm
	e.metadata().revisionVCS = "test-revision-1234"
	require.Equal(t, tm, e.Time())
	require.Equal(t, "test-revision-1234", e.RevisionVCS())
}

func TestErrJSONUnmarshalUnregistered(t *testing.T) {
	t.Parallel()
	ec := NewTestCodec(t)
	unmarshaled, err := ec.DecodeJSON("Unregistered", []byte(`{
		"foo": "foo-value",
		"bar": 42
	}`))
	require.Error(t, err)
	require.Equal(t, `event "Unregistered" not registered`, err.Error())
	require.Nil(t, unmarshaled)
}

func TestErrJSONUnmarshalNoType(t *testing.T) {
	t.Parallel()
	ec := NewTestCodec(t)
	unmarshaled, err := ec.DecodeJSON("", []byte(`{
		"foo": "foo-value",
		"bar": 42
	}`))
	require.Nil(t, unmarshaled)
	require.Error(t, err)
}

func TestErrJSONUnmarshalMalformedEnvelopeEOF(t *testing.T) {
	t.Parallel()
	ec := NewTestCodec(t)
	unmarshaled, err := ec.DecodeJSON("test-event", []byte(""))
	require.ErrorContains(t, err, "unexpected end of JSON input")
	require.Nil(t, unmarshaled)
}

func TestErrJSONUnmarshalMalformedEnvelopePayload(t *testing.T) {
	t.Parallel()
	ec := NewTestCodec(t)
	unmarshaled, err := ec.DecodeJSON("test-event", []byte(`[]`))
	require.ErrorContains(t, err,
		"json: cannot unmarshal array into Go value of type conductor.EventTest")
	require.Nil(t, unmarshaled)
}

func requiresTypeRegisterPanics[T Event](
	t *testing.T, eventTypeName, fieldName, jsonTag string,
) {
	t.Helper()
	expected := fmt.Sprintf(
		`event type %s has field %q with JSON tag %q which collides with EventMetadata`,
		eventTypeName, fieldName, jsonTag,
	)
	ec := NewTypeCodec("test-revision")
	require.PanicsWithValue(t, expected, func() {
		MustRegisterEventTypeIn[T](ec, "test-event")
	})
}

// TestPanicFieldNameConflict ensures MustRegisterEventTypeIn panics when the event type
// has a json fields that conflicts with type EventMetadata to make sure types that
// can't be unmarshaled later can't be registered in the first place.
func TestPanicFieldNameConflict(t *testing.T) {
	t.Parallel()

	type TName struct {
		EventMetadata
		TPName string `json:"typeName"`
	}
	requiresTypeRegisterPanics[*TName](t, "TName", "TPName", "typeName")

	type TRevisionVCS struct {
		EventMetadata
		RevVCS string `json:"revisionVCS"`
	}
	requiresTypeRegisterPanics[*TRevisionVCS](t, "TRevisionVCS", "RevVCS", "revisionVCS")

	type TTime struct {
		EventMetadata
		TM time.Time `json:"time"`
	}
	requiresTypeRegisterPanics[*TTime](t, "TTime", "TM", "time")

	type TVersion struct {
		EventMetadata
		V string `json:"version"`
	}
	requiresTypeRegisterPanics[*TVersion](t, "TVersion", "V", "version")

	type TVersionCap struct {
		EventMetadata
		V string `json:"VERSION"`
	}
	requiresTypeRegisterPanics[*TVersionCap](t, "TVersionCap", "V", "VERSION")
}

func TestPanicInUse(t *testing.T) {
	// Don't use t.Parallel(), this test relies on global state.
	require.Nil(t, DefaultEventCodec)
	origBuildInfo := mustGetVCSRevisionFromBuildInfo

	// This prevents "panic: no vcs.revision in build info"
	mustGetVCSRevisionFromBuildInfo = func() string { return "test-revision" }

	t.Cleanup(func() {
		mustGetVCSRevisionFromBuildInfo = origBuildInfo
		DefaultEventCodec = nil
	})

	MustRegisterEventType[*EventTest]("name")

	DefaultEventCodec.inUse = true // Simulate Conductor start.

	require.Panics(t,
		func() { MustRegisterEventType[*EventTest]("some-other-event") })

	require.PanicsWithValue(t,
		"attempting to register event type at Conductor runtime",
		func() { MustRegisterEventType[*EventTest]("some-other-event") })
}
