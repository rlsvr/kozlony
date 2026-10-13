// Package messaging provides NATS JetStream communication and message routing.
package messaging

import (
	"fmt"
	"hash/fnv"
)

const (
	// DefaultStreamName is the default JetStream stream name.
	DefaultStreamName = "BOARD"

	// DefaultGroupID is the default partition group identifier.
	DefaultGroupID = "root"

	// DefaultLivePrefix is the default subject prefix for ephemeral Core NATS fanout.
	DefaultLivePrefix = "LIVE"

	// DefaultIngestPartitions is the default number of ingestion partitions.
	DefaultIngestPartitions = 16

	// EventCreatedSuffix is the subject suffix for created events.
	EventCreatedSuffix = "evt.created"

	// EventEditedSuffix is the subject suffix for edit notifications.
	EventEditedSuffix = "audit.edited"

	// CommandCreateSuffix is the subject suffix for create commands.
	CommandCreateSuffix = "cmd.create"

	// EventReplyCountsSuffix is the subject suffix for recalculated reply counts.
	EventReplyCountsSuffix = "counter.replied"
)

// SubjectIngestPartition returns the JetStream subject for an ingestion partition.
func SubjectIngestPartition(partition int) string {
	return fmt.Sprintf("INGEST.p.%d", partition)
}

// SubjectIngestFilter returns the wildcard capturing all ingest partitions.
func SubjectIngestFilter() string {
	return "INGEST.p.*"
}

// SubjectLiveFeed returns the ephemeral Core NATS topic for board feed events.
func SubjectLiveFeed(prefix, groupID string) string {
	if prefix == "" {
		prefix = DefaultLivePrefix
	}
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.feed", prefix, groupID)
}

// SubjectLiveThread returns the ephemeral Core NATS topic for a specific thread's events.
func SubjectLiveThread(prefix, groupID, threadRootID string) string {
	if prefix == "" {
		prefix = DefaultLivePrefix
	}
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.thread.%s", prefix, groupID, threadRootID)
}

// PartitionForThread deterministically computes the partition index for an interaction.
func PartitionForThread(threadRootID string, numPartitions int) int {
	if numPartitions <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(threadRootID))
	//nolint:gosec // numPartitions is validated > 1 and fits in uint32
	return int(h.Sum32() % uint32(numPartitions))
}

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

// SubjectGroupEvents returns the wildcard subject matching all events for one group.
func SubjectGroupEvents(stream, groupID string) string {
	if groupID == "" {
		groupID = DefaultGroupID
	}
	return fmt.Sprintf("%s.%s.evt.>", stream, groupID)
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

// SubjectGroupReplyCounts returns the subject matching reply counts for one group.
func SubjectGroupReplyCounts(stream, groupID string) string {
	return SubjectReplyCountsRecalculated(stream, groupID)
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
