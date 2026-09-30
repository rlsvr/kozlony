// Package messaging provides NATS JetStream communication and message routing.
package messaging

import "fmt"

const (
	// DefaultStreamName is the default JetStream stream name.
	DefaultStreamName = "BOARD"

	// DefaultGroupID is the default partition group identifier.
	DefaultGroupID = "root"

	// EventCreatedSuffix is the subject suffix for created events.
	EventCreatedSuffix = "evt.created"

	// EventEditedSuffix is the subject suffix for edited events.
	EventEditedSuffix = "evt.edited"

	// CommandCreateSuffix is the subject suffix for create commands.
	CommandCreateSuffix = "cmd.create"
)

// SubjectInteractionCreated returns the subject for an interaction created event.
func SubjectInteractionCreated(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.%s", stream, groupID, EventCreatedSuffix)
}

// SubjectInteractionEdited returns the subject for an interaction edited event.
func SubjectInteractionEdited(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.%s", stream, groupID, EventEditedSuffix)
}

// SubjectCreateInteractionCmd returns the subject for a create interaction command.
func SubjectCreateInteractionCmd(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.%s", stream, groupID, CommandCreateSuffix)
}

// SubjectAllEvents returns the wildcard subject matching all events across all groups in the stream.
func SubjectAllEvents(stream string) string {
	return fmt.Sprintf("%s.*.evt.>", stream)
}

// SubjectAllCommands returns the wildcard subject matching all commands across all groups in the stream.
func SubjectAllCommands(stream string) string {
	return fmt.Sprintf("%s.*.cmd.>", stream)
}

// SubjectStreamFilter returns the stream-level wildcard capturing all events and commands.
func SubjectStreamFilter(stream string) string {
	return fmt.Sprintf("%s.>", stream)
}

// SubjectDeadLetter returns the dead letter subject for unprocessable messages in the stream.
// This is the single source of truth for the DLQ subject; publishers must not hardcode it.
func SubjectDeadLetter(stream string) string {
	return fmt.Sprintf("%s.dlq", stream)
}
