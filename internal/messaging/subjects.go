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

	// EventEditedSuffix is the subject suffix for edit notifications.
	//
	// It sits deliberately outside the "evt." namespace because an edit is NOT a write
	// command: the handler has already committed the edit to PostgreSQL synchronously
	// before publishing this. The event exists only to fan the change out to SSE clients,
	// so it must never match the drainer's filter (BOARD.<group>.evt.>), which would
	// otherwise try to insert an edit notification as if it were an interaction.
	EventEditedSuffix = "audit.edited"

	// CommandCreateSuffix is the subject suffix for create commands.
	CommandCreateSuffix = "cmd.create"

	// EventReplyCountsSuffix is the subject suffix for recalculated reply counts.
	// It is deliberately outside the "evt." namespace so the drainer (which
	// consumes BOARD.<group>.evt.>) never consumes its own derived output.
	EventReplyCountsSuffix = "counter.replied"
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

// SubjectAllAuditEvents returns the wildcard subject matching every edit notification
// across all groups in the stream.
func SubjectAllAuditEvents(stream string) string {
	return fmt.Sprintf("%s.*.%s", stream, EventEditedSuffix)
}

// SubjectGroupAuditEvents returns the wildcard subject matching edit notifications for one
// group.
func SubjectGroupAuditEvents(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.%s", stream, groupID, EventEditedSuffix)
}

// SubjectReplyCountsRecalculated returns the subject for recalculated reply counts.
func SubjectReplyCountsRecalculated(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.%s", stream, groupID, EventReplyCountsSuffix)
}

// SubjectAllReplyCounts returns the wildcard subject matching every reply count
// recalculation across all groups in the stream.
func SubjectAllReplyCounts(stream string) string {
	return fmt.Sprintf("%s.*.%s", stream, EventReplyCountsSuffix)
}

// SubjectDeadLetter returns the dead letter subject for unprocessable messages in the stream.
// This is the single source of truth for the DLQ subject; publishers must not hardcode it.
func SubjectDeadLetter(stream string) string {
	return fmt.Sprintf("%s.dlq", stream)
}
