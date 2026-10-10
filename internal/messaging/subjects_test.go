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
			expected: "BOARD.announcements.evt.edited",
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
			name:     "SubjectAllReplyCounts",
			got:      messaging.SubjectAllReplyCounts("BOARD"),
			expected: "BOARD.*.counter.replied",
		},
		{
			name:     "SubjectDeadLetter",
			got:      messaging.SubjectDeadLetter("BOARD"),
			expected: "BOARD.dlq",
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
