package event

import (
	"context"
	"testing"

	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestNewGeneratesIdentifiersAndInheritsCorrelationID(t *testing.T) {
	first, err := New(t.Context(), "users", "user-created", wrapperspb.String("first"))
	require.NoError(t, err)

	ctx := contextmeta.WithCorrelationID(t.Context(), first.CorrelationID())
	second, err := New(ctx, "billing", "invoice-created", wrapperspb.String("second"))
	require.NoError(t, err)

	assert.NotEmpty(t, first.ID())
	assert.NotEmpty(t, first.CorrelationID())
	assert.NotEqual(t, first.ID(), second.ID())
	assert.Equal(t, first.CorrelationID(), second.CorrelationID())
}

func TestNewRejectsRoutingCharacters(t *testing.T) {
	_, err := New(t.Context(), "users.*", "created", wrapperspb.String("payload"))
	assert.ErrorContains(t, err, "reserved routing character")
}

func TestProtobufEnvelopeRoundTrip(t *testing.T) {
	original, err := New(context.Background(), "users", "user-created", wrapperspb.String("payload"))
	require.NoError(t, err)

	encoded, err := original.Marshal()
	require.NoError(t, err)
	decoded, err := Unmarshal(encoded)
	require.NoError(t, err)

	assert.Equal(t, original.ID(), decoded.ID())
	assert.Equal(t, original.CorrelationID(), decoded.CorrelationID())
	assert.Equal(t, original.OccurredAt(), decoded.OccurredAt())
	var payload wrapperspb.StringValue
	require.NoError(t, decoded.UnmarshalPayload(&payload))
	assert.Equal(t, "payload", payload.Value)
}
