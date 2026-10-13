package messaging_test

import (
	"strings"
	"testing"

	"github.com/rlsvr/kozlony/internal/messaging"
)

func TestSubjectGenerators(t *testing.T) {
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{
			name:     "InteractionCreated with group",
			got:      messaging.SubjectInteractionCreated("BOARD", "dev"),
			expected: "BOARD.dev.evt.created",
		},
		{
			name:     "InteractionCreated with empty group (defaults to root)",
			got:      messaging.SubjectInteractionCreated("BOARD", ""),
			expected: "BOARD.root.evt.created",
		},
		{
			name:     "InteractionEdited with group",
			got:      messaging.SubjectInteractionEdited("BOARD", "announcements"),
			expected: "BOARD.announcements.audit.edited",
		},
		{
			name:     "CreateInteractionCmd with group",
			got:      messaging.SubjectCreateInteractionCmd("BOARD", "dev"),
			expected: "BOARD.dev.cmd.create",
		},
		{
			name:     "SubjectAllEvents",
			got:      messaging.SubjectAllEvents("BOARD"),
			expected: "BOARD.*.evt.>",
		},
		{
			name:     "SubjectAllCommands",
			got:      messaging.SubjectAllCommands("BOARD"),
			expected: "BOARD.*.cmd.>",
		},
		{
			name:     "SubjectStreamFilter",
			got:      messaging.SubjectStreamFilter("BOARD"),
			expected: "BOARD.>",
		},
		{
			name:     "SubjectReplyCountsRecalculated",
			got:      messaging.SubjectReplyCountsRecalculated("BOARD", "dev"),
			expected: "BOARD.dev.counter.replied",
		},
		{
			name:     "SubjectReplyCountsRecalculated with empty group",
			got:      messaging.SubjectReplyCountsRecalculated("BOARD", ""),
			expected: "BOARD.root.counter.replied",
		},
		{
			name:     "SubjectAllAuditEvents",
			got:      messaging.SubjectAllAuditEvents("BOARD"),
			expected: "BOARD.*.audit.edited",
		},
		{
			name:     "SubjectGroupEvents",
			got:      messaging.SubjectGroupEvents("BOARD", "dev"),
			expected: "BOARD.dev.evt.>",
		},
		{
			name:     "SubjectGroupAuditEvents",
			got:      messaging.SubjectGroupAuditEvents("BOARD", "dev"),
			expected: "BOARD.dev.audit.edited",
		},
		{
			name:     "SubjectGroupReplyCounts",
			got:      messaging.SubjectGroupReplyCounts("BOARD", "dev"),
			expected: "BOARD.dev.counter.replied",
		},
		{
			name:     "SubjectAllReplyCounts",
			got:      messaging.SubjectAllReplyCounts("BOARD"),
			expected: "BOARD.*.counter.replied",
		},
		{
			name:     "SubjectDeadLetter",
			got:      messaging.SubjectDeadLetter("BOARD"),
			expected: "BOARD.dlq",
		},
		{
			name:     "SubjectIngestPartition",
			got:      messaging.SubjectIngestPartition(3),
			expected: "INGEST.p.3",
		},
		{
			name:     "SubjectIngestFilter",
			got:      messaging.SubjectIngestFilter(),
			expected: "INGEST.p.*",
		},
		{
			name:     "SubjectLiveFeed",
			got:      messaging.SubjectLiveFeed("LIVE", "dev"),
			expected: "LIVE.dev.feed",
		},
		{
			name:     "SubjectLiveThread",
			got:      messaging.SubjectLiveThread("LIVE", "dev", "01923f12-0000-7000-8000-000000000001"),
			expected: "LIVE.dev.thread.01923f12-0000-7000-8000-000000000001",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("got %q, expected %q", tt.got, tt.expected)
			}
		})
	}
}

// An edit notification is not a write command: the handler already committed the edit
// synchronously before publishing it. If the edit subject ever moved back inside "evt."
// the drainer would consume it and try to insert an InteractionEditedEvent (which has no
// author or created_at) as an interaction row.
func TestEditSubjectIsOutsideDrainerFilter(t *testing.T) {
	drainerFilter := messaging.SubjectAllEvents("BOARD")
	editSubject := messaging.SubjectInteractionEdited("BOARD", "root")

	if strings.Contains(editSubject, ".evt.") {
		t.Fatalf("edit subject %q is matched by the drainer filter %q", editSubject, drainerFilter)
	}
	if !strings.Contains(editSubject, ".audit.") {
		t.Errorf("edit subject %q should use the audit namespace, got %q", editSubject, editSubject)
	}

	// It must still be group-scoped so the stream filter captures it.
	if !strings.HasPrefix(editSubject, "BOARD.root.") {
		t.Errorf("edit subject %q is not group-scoped and would be dropped by the stream filter", editSubject)
	}
}

// The counter subject must sit outside the drainer's filter. If it ever fell inside
// "evt." the drainer would consume its own derived output and try to insert reply
// counts as if they were interactions.
func TestCounterSubjectIsOutsideDrainerFilter(t *testing.T) {
	drainerFilter := messaging.SubjectAllEvents("BOARD")
	counterSubject := messaging.SubjectReplyCountsRecalculated("BOARD", "root")

	if strings.HasPrefix(counterSubject, "BOARD.root.evt.") {
		t.Fatalf("counter subject %q is inside the drainer filter %q", counterSubject, drainerFilter)
	}
	if !strings.Contains(drainerFilter, ".evt.") {
		t.Fatalf("drainer filter %q no longer targets the evt namespace", drainerFilter)
	}
	if !strings.HasPrefix(counterSubject, "BOARD.root.") {
		t.Errorf("counter subject %q is not group-scoped, so the stream filter would drop it", counterSubject)
	}
	if !strings.HasPrefix(messaging.SubjectStreamFilter("BOARD"), "BOARD.") {
		t.Error("stream filter must still capture every group so counter events are retained")
	}
}

func TestPartitionForThread(t *testing.T) {
	numPartitions := 16
	p1 := messaging.PartitionForThread("01923f12-0000-7000-8000-000000000001", numPartitions)
	p2 := messaging.PartitionForThread("01923f12-0000-7000-8000-000000000001", numPartitions)
	if p1 != p2 {
		t.Fatalf("expected deterministic partition, got %d and %d", p1, p2)
	}
	if p1 < 0 || p1 >= numPartitions {
		t.Fatalf("partition %d out of range [0, %d)", p1, numPartitions)
	}

	// Empty thread defaults to valid partition range
	pEmpty := messaging.PartitionForThread("", numPartitions)
	if pEmpty < 0 || pEmpty >= numPartitions {
		t.Fatalf("empty thread partition %d out of range", pEmpty)
	}

	// 1 partition edge case
	pSingle := messaging.PartitionForThread("any-thread", 1)
	if pSingle != 0 {
		t.Fatalf("expected partition 0 for single partition, got %d", pSingle)
	}
}
