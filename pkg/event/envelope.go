package event

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/contextmeta"
	"github.com/distributed-programming-2026/go-sdk/pkg/event/internal/eventpb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Envelope is an immutable view of a protobuf event envelope.
type Envelope struct {
	message *eventpb.Envelope
}

func New(
	ctx context.Context,
	producer string,
	eventType string,
	payload proto.Message,
) (Envelope, error) {
	if ctx == nil {
		return Envelope{}, errors.New("event: context must not be nil")
	}
	if err := validateSegment("producer", producer); err != nil {
		return Envelope{}, err
	}
	if err := validateSegment("type", eventType); err != nil {
		return Envelope{}, err
	}
	if payload == nil {
		return Envelope{}, errors.New("event: payload must not be nil")
	}

	eventID, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("event: generate event id: %w", err)
	}
	encodedPayload, err := proto.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("event: marshal payload for correlation id: %w", err)
	}
	correlationID := contextmeta.CorrelationID(ctx)
	if correlationID == "" {
		correlationID, err = newCorrelationID(producer, encodedPayload)
		if err != nil {
			return Envelope{}, err
		}
	}
	anyPayload, err := anypb.New(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("event: pack payload: %w", err)
	}

	return Envelope{message: &eventpb.Envelope{
		EventId:       eventID.String(),
		EventType:     eventType,
		Producer:      producer,
		OccurredAt:    timestamppb.New(time.Now().UTC()),
		CorrelationId: correlationID,
		Payload:       anyPayload,
	}}, nil
}

func (e Envelope) ID() string            { return e.message.GetEventId() }
func (e Envelope) Type() string          { return e.message.GetEventType() }
func (e Envelope) Producer() string      { return e.message.GetProducer() }
func (e Envelope) CorrelationID() string { return e.message.GetCorrelationId() }

func (e Envelope) OccurredAt() time.Time {
	if e.message == nil || e.message.OccurredAt == nil {
		return time.Time{}
	}
	return e.message.OccurredAt.AsTime()
}

func (e Envelope) UnmarshalPayload(message proto.Message) error {
	if message == nil {
		return errors.New("event: payload destination must not be nil")
	}
	if e.message == nil || e.message.Payload == nil {
		return errors.New("event: envelope payload must not be nil")
	}
	if err := e.message.Payload.UnmarshalTo(message); err != nil {
		return fmt.Errorf("event: unmarshal payload: %w", err)
	}
	return nil
}

func (e Envelope) Marshal() ([]byte, error) {
	if err := validateEnvelope(e); err != nil {
		return nil, err
	}
	encoded, err := proto.Marshal(e.message)
	if err != nil {
		return nil, fmt.Errorf("event: marshal envelope: %w", err)
	}
	return encoded, nil
}

func Unmarshal(encoded []byte) (Envelope, error) {
	message := &eventpb.Envelope{}
	if err := proto.Unmarshal(encoded, message); err != nil {
		return Envelope{}, fmt.Errorf("event: unmarshal envelope: %w", err)
	}
	envelope := Envelope{message: message}
	if err := validateEnvelope(envelope); err != nil {
		return Envelope{}, err
	}
	return envelope, nil
}

func newCorrelationID(producer string, payload []byte) (string, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("event: generate correlation id: %w", err)
	}
	digest := sha256.Sum256(payload)
	return strings.Join([]string{
		producer,
		base64.RawURLEncoding.EncodeToString(digest[:]),
		id.String(),
	}, ":"), nil
}

func validateEnvelope(envelope Envelope) error {
	if envelope.message == nil {
		return errors.New("event: envelope must not be nil")
	}
	if envelope.ID() == "" {
		return errors.New("event: event id must not be empty")
	}
	if err := validateSegment("producer", envelope.Producer()); err != nil {
		return err
	}
	if err := validateSegment("type", envelope.Type()); err != nil {
		return err
	}
	if envelope.message.OccurredAt == nil || !envelope.message.OccurredAt.IsValid() {
		return errors.New("event: occurred at must be valid")
	}
	if envelope.CorrelationID() == "" {
		return errors.New("event: correlation id must not be empty")
	}
	if envelope.message.Payload == nil {
		return errors.New("event: payload must not be nil")
	}
	return nil
}

func validateSegment(name, value string) error {
	if value == "" {
		return fmt.Errorf("event: %s must not be empty", name)
	}
	if strings.ContainsAny(value, ".*#") {
		return fmt.Errorf("event: %s %q contains a reserved routing character", name, value)
	}
	return nil
}
