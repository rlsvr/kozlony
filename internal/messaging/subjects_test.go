package messaging_test

import (
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
