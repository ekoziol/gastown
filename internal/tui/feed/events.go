package feed

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/steveyegge/gastown/internal/beads"
)

// EventSource represents a source of events
type EventSource interface {
	Events() <-chan Event
	Close() error
}

// bdIssue represents an issue from bd list --json output.
type bdIssue struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Status    string `json:"status"`
	Priority  int    `json:"priority"`
	Type      string `json:"issue_type"`
	Assignee  string `json:"assignee"`
	CreatedBy string `json:"created_by"`
	CreatedAt string `json:"created_at"`
	UpdatedAt string `json:"updated_at"`
}

// BdActivitySource polls bd list for issue changes and emits events.
// This replaces the original implementation that depended on the
// nonexistent "bd activity" command.
type BdActivitySource struct {
	events  chan Event
	cancel  context.CancelFunc
	workDir string
}

// NewBdActivitySource creates a source that polls bd list for issue changes.
func NewBdActivitySource(workDir string) (*BdActivitySource, error) {
	ctx, cancel := context.WithCancel(context.Background())

	source := &BdActivitySource{
		events:  make(chan Event, 100),
		cancel:  cancel,
		workDir: workDir,
	}

	go source.poll(ctx)

	return source, nil
}

// Events returns the event channel.
func (s *BdActivitySource) Events() <-chan Event {
	return s.events
}

// Close stops the polling source.
func (s *BdActivitySource) Close() error {
	s.cancel()
	return nil
}

func (s *BdActivitySource) poll(ctx context.Context) {
	defer close(s.events)

	known := make(map[string]string) // id → updated_at
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	// Seed with current state without emitting events
	s.fetchIssues(ctx, known, true)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.fetchIssues(ctx, known, false)
		}
	}
}

func (s *BdActivitySource) fetchIssues(
	ctx context.Context,
	known map[string]string,
	initial bool,
) {
	cmd := exec.CommandContext(ctx, "bd", "list",
		"--json", "--sort", "updated", "--reverse",
		"--all", "--limit", "50")
	cmd.Dir = s.workDir

	out, err := cmd.Output()
	if err != nil {
		return
	}

	var issues []bdIssue
	if err := json.Unmarshal(out, &issues); err != nil {
		return
	}

	for _, issue := range issues {
		prevUpdated, seen := known[issue.ID]
		known[issue.ID] = issue.UpdatedAt

		if initial {
			continue
		}

		if !seen {
			s.emitEvent(ctx, issueToEvent(issue, "create"))
		} else if prevUpdated != issue.UpdatedAt {
			eventType := "update"
			if issue.Status == "closed" {
				eventType = "complete"
			}
			s.emitEvent(ctx, issueToEvent(issue, eventType))
		}
	}
}

func (s *BdActivitySource) emitEvent(ctx context.Context, event Event) {
	select {
	case s.events <- event:
	case <-ctx.Done():
	}
}

func issueToEvent(issue bdIssue, eventType string) Event {
	t, err := time.Parse(time.RFC3339, issue.UpdatedAt)
	if err != nil {
		t = time.Now()
	}

	actor, rig, role := parseBeadContext(issue.ID)
	if actor == "" {
		actor = issue.CreatedBy
	}

	var message string
	switch eventType {
	case "create":
		message = "created: " + issue.Title
	case "complete":
		message = "closed: " + issue.Title
	default:
		message = "updated: " + issue.Title
	}

	return Event{
		Time:    t,
		Type:    eventType,
		Actor:   actor,
		Target:  issue.ID,
		Message: message,
		Rig:     rig,
		Role:    role,
	}
}

// parseBeadContext extracts actor/rig/role from a bead ID
// Uses canonical naming: prefix-rig-role-name
// Examples: gt-gastown-crew-joe, gt-gastown-witness, gt-mayor
func parseBeadContext(beadID string) (actor, rig, role string) {
	if beadID == "" {
		return
	}

	// Use the canonical parser
	parsedRig, parsedRole, name, ok := beads.ParseAgentBeadID(beadID)
	if !ok {
		return
	}

	rig = parsedRig
	role = parsedRole

	// Build actor identifier
	switch parsedRole {
	case "mayor", "deacon":
		actor = parsedRole
	case "witness", "refinery":
		actor = parsedRole
	case "crew":
		if name != "" {
			actor = parsedRig + "/crew/" + name
		} else {
			actor = parsedRole
		}
	case "polecat":
		if name != "" {
			actor = parsedRig + "/" + name
		} else {
			actor = parsedRole
		}
	}

	return
}

// GtEventsSource reads events from ~/gt/.events.jsonl (gt activity log)
type GtEventsSource struct {
	file   *os.File
	events chan Event
	cancel context.CancelFunc
}

// GtEvent is the structure of events in .events.jsonl
type GtEvent struct {
	Timestamp  string                 `json:"ts"`
	Source     string                 `json:"source"`
	Type       string                 `json:"type"`
	Actor      string                 `json:"actor"`
	Payload    map[string]interface{} `json:"payload"`
	Visibility string                 `json:"visibility"`
}

// NewGtEventsSource creates a source that tails ~/gt/.events.jsonl
func NewGtEventsSource(townRoot string) (*GtEventsSource, error) {
	eventsPath := filepath.Join(townRoot, ".events.jsonl")
	file, err := os.Open(eventsPath)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	source := &GtEventsSource{
		file:   file,
		events: make(chan Event, 100),
		cancel: cancel,
	}

	go source.tail(ctx)

	return source, nil
}

// tail follows the file and sends events
func (s *GtEventsSource) tail(ctx context.Context) {
	defer close(s.events)

	// Seek to end for live tailing
	_, _ = s.file.Seek(0, 2)

	scanner := bufio.NewScanner(s.file)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for scanner.Scan() {
				line := scanner.Text()
				if event := parseGtEventLine(line); event != nil {
					select {
					case s.events <- *event:
					default:
					}
				}
			}
		}
	}
}

// Events returns the event channel
func (s *GtEventsSource) Events() <-chan Event {
	return s.events
}

// Close stops the source
func (s *GtEventsSource) Close() error {
	s.cancel()
	return s.file.Close()
}

// parseGtEventLine parses a line from .events.jsonl
func parseGtEventLine(line string) *Event {
	if strings.TrimSpace(line) == "" {
		return nil
	}

	var ge GtEvent
	if err := json.Unmarshal([]byte(line), &ge); err != nil {
		return nil
	}

	// Only show feed-visible events
	if ge.Visibility != "feed" && ge.Visibility != "both" {
		return nil
	}

	t, err := time.Parse(time.RFC3339, ge.Timestamp)
	if err != nil {
		t = time.Now()
	}

	// Extract rig from payload or actor
	rig := ""
	if ge.Payload != nil {
		if r, ok := ge.Payload["rig"].(string); ok {
			rig = r
		}
	}
	if rig == "" && ge.Actor != "" {
		// Extract rig from actor like "gastown/witness"
		parts := strings.Split(ge.Actor, "/")
		if len(parts) > 0 && parts[0] != "mayor" && parts[0] != "deacon" {
			rig = parts[0]
		}
	}

	// Extract role from actor
	role := ""
	if ge.Actor != "" {
		parts := strings.Split(ge.Actor, "/")
		if len(parts) >= 2 {
			role = parts[len(parts)-1]
			// Check for known roles
			switch parts[len(parts)-1] {
			case "witness", "refinery":
				role = parts[len(parts)-1]
			default:
				// Could be polecat name - check second-to-last part
				if len(parts) >= 2 {
					switch parts[len(parts)-2] {
					case "polecats":
						role = "polecat"
					case "crew":
						role = "crew"
					}
				}
			}
		} else if len(parts) == 1 {
			role = parts[0]
		}
	}

	// Build message from event type and payload
	message := buildEventMessage(ge.Type, ge.Payload)

	return &Event{
		Time:    t,
		Type:    ge.Type,
		Actor:   ge.Actor,
		Target:  getPayloadString(ge.Payload, "bead"),
		Message: message,
		Rig:     rig,
		Role:    role,
		Raw:     line,
	}
}

// buildEventMessage creates a human-readable message from event type and payload
func buildEventMessage(eventType string, payload map[string]interface{}) string {
	switch eventType {
	case "patrol_started":
		count := getPayloadInt(payload, "polecat_count")
		if msg := getPayloadString(payload, "message"); msg != "" {
			return msg
		}
		if count > 0 {
			return fmt.Sprintf("patrol started (%d polecats)", count)
		}
		return "patrol started"

	case "patrol_complete":
		count := getPayloadInt(payload, "polecat_count")
		if msg := getPayloadString(payload, "message"); msg != "" {
			return msg
		}
		if count > 0 {
			return fmt.Sprintf("patrol complete (%d polecats)", count)
		}
		return "patrol complete"

	case "polecat_checked":
		polecat := getPayloadString(payload, "polecat")
		status := getPayloadString(payload, "status")
		if polecat != "" {
			if status != "" {
				return fmt.Sprintf("checked %s (%s)", polecat, status)
			}
			return fmt.Sprintf("checked %s", polecat)
		}
		return "polecat checked"

	case "polecat_nudged":
		polecat := getPayloadString(payload, "polecat")
		reason := getPayloadString(payload, "reason")
		if polecat != "" {
			if reason != "" {
				return fmt.Sprintf("nudged %s: %s", polecat, reason)
			}
			return fmt.Sprintf("nudged %s", polecat)
		}
		return "polecat nudged"

	case "escalation_sent":
		target := getPayloadString(payload, "target")
		to := getPayloadString(payload, "to")
		reason := getPayloadString(payload, "reason")
		if target != "" && to != "" {
			if reason != "" {
				return fmt.Sprintf("escalated %s to %s: %s", target, to, reason)
			}
			return fmt.Sprintf("escalated %s to %s", target, to)
		}
		return "escalation sent"

	case "sling":
		bead := getPayloadString(payload, "bead")
		target := getPayloadString(payload, "target")
		if bead != "" && target != "" {
			return fmt.Sprintf("slung %s to %s", bead, target)
		}
		return "work slung"

	case "hook":
		bead := getPayloadString(payload, "bead")
		if bead != "" {
			return fmt.Sprintf("hooked %s", bead)
		}
		return "bead hooked"

	case "handoff":
		subject := getPayloadString(payload, "subject")
		if subject != "" {
			return fmt.Sprintf("handoff: %s", subject)
		}
		return "session handoff"

	case "done":
		bead := getPayloadString(payload, "bead")
		if bead != "" {
			return fmt.Sprintf("done: %s", bead)
		}
		return "work done"

	case "mail":
		subject := getPayloadString(payload, "subject")
		to := getPayloadString(payload, "to")
		if subject != "" {
			if to != "" {
				return fmt.Sprintf("→ %s: %s", to, subject)
			}
			return subject
		}
		return "mail sent"

	case "merged":
		worker := getPayloadString(payload, "worker")
		if worker != "" {
			return fmt.Sprintf("merged work from %s", worker)
		}
		return "merged"

	case "merge_failed":
		reason := getPayloadString(payload, "reason")
		if reason != "" {
			return fmt.Sprintf("merge failed: %s", reason)
		}
		return "merge failed"

	default:
		if msg := getPayloadString(payload, "message"); msg != "" {
			return msg
		}
		return eventType
	}
}

// getPayloadString extracts a string from payload
func getPayloadString(payload map[string]interface{}, key string) string {
	if payload == nil {
		return ""
	}
	if v, ok := payload[key].(string); ok {
		return v
	}
	return ""
}

// getPayloadInt extracts an int from payload
func getPayloadInt(payload map[string]interface{}, key string) int {
	if payload == nil {
		return 0
	}
	if v, ok := payload[key].(float64); ok {
		return int(v)
	}
	return 0
}

// FindBeadsDir finds the beads directory for the given working directory
func FindBeadsDir(workDir string) (string, error) {
	// Walk up looking for .beads
	dir := workDir
	for {
		beadsPath := filepath.Join(dir, ".beads")
		if info, err := os.Stat(beadsPath); err == nil && info.IsDir() {
			return beadsPath, nil
		}

		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}

	return "", os.ErrNotExist
}
