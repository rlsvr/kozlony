package drainer

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=drainer.go -destination=mocks/mock_drainer.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/database"
	"github.com/rlsvr/kozlony/internal/messaging"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

// BatchFetcher fetches a micro-batch of messages from the broker.
type BatchFetcher interface {
	Fetch(batch int, opts ...jetstream.FetchOpt) (jetstream.MessageBatch, error)
}

// DLQPublisher forwards dead-letter / poison messages to the DLQ subject.
type DLQPublisher interface {
	PublishDLQ(ctx context.Context, originalSubject string, data []byte, reason string) error
}

// ReplyCountPublisher emits recalculated reply counts for the counter consumer.
type ReplyCountPublisher interface {
	PublishReplyCountsRecalculated(ctx context.Context, evt *events.ReplyCountsRecalculatedEvent) error
}

// Drainer micro-batches interaction events from NATS JetStream and bulk-inserts them into PostgreSQL.
type Drainer struct {
	fetcher        BatchFetcher
	repo           database.Repository
	dlq            DLQPublisher
	countPublisher ReplyCountPublisher
	batchSize      int
	flushInterval  time.Duration
	workers        int
}

// New creates a new Drainer instance.
func New(fetcher BatchFetcher, repo database.Repository, dlq DLQPublisher, cfg *config.Config) *Drainer {
	return newDrainer(fetcher, repo, dlq, nil, cfg)
}

// NewWithCountPublisher creates a Drainer that emits recalculated reply counts after each
// committed batch. The publisher may be nil, in which case counts are skipped.
func NewWithCountPublisher(
	fetcher BatchFetcher,
	repo database.Repository,
	dlq DLQPublisher,
	countPublisher ReplyCountPublisher,
	cfg *config.Config,
) *Drainer {
	return newDrainer(fetcher, repo, dlq, countPublisher, cfg)
}

func newDrainer(
	fetcher BatchFetcher,
	repo database.Repository,
	dlq DLQPublisher,
	countPublisher ReplyCountPublisher,
	cfg *config.Config,
) *Drainer {
	batchSize := 500
	flushInterval := 50 * time.Millisecond
	workers := 1
	if cfg != nil {
		if cfg.DrainBatchSize > 0 {
			batchSize = cfg.DrainBatchSize
		}
		if cfg.DrainFlushInterval > 0 {
			flushInterval = cfg.DrainFlushInterval
		}
		if cfg.NATSConcurrentSubscribers > 0 {
			workers = cfg.NATSConcurrentSubscribers
		}
	}

	return &Drainer{
		fetcher:        fetcher,
		repo:           repo,
		dlq:            dlq,
		countPublisher: countPublisher,
		batchSize:      batchSize,
		flushInterval:  flushInterval,
		workers:        workers,
	}
}

// ProcessBatch fetches a single batch of messages up to batchSize within flushInterval,
// deserializes interaction events, writes them in bulk to PostgreSQL, and acknowledges them.
func (d *Drainer) ProcessBatch(ctx context.Context) (int, error) {
	mb, err := d.fetcher.Fetch(d.batchSize, jetstream.FetchMaxWait(d.flushInterval))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, nil
		}
		return 0, fmt.Errorf("fetch batch from jetstream: %w", err)
	}

	var (
		rawMsgs      []jetstream.Msg
		interactions []*database.Interaction
	)

	for msg := range mb.Messages() {
		var evt events.InteractionCreatedEvent
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			log.Warn().Err(err).Str("subject", msg.Subject()).Msg("failed to unmarshal message in drainer, routing to DLQ")
			d.routeToDLQ(ctx, msg, fmt.Sprintf("unmarshal error: %v", err))
			continue
		}

		item, err := eventToInteraction(&evt)
		if err != nil {
			log.Warn().Err(err).Str("id", evt.ID).Msg("invalid event payload in drainer, routing to DLQ")
			d.routeToDLQ(ctx, msg, fmt.Sprintf("invalid payload: %v", err))
			continue
		}

		rawMsgs = append(rawMsgs, msg)
		interactions = append(interactions, item)
	}

	if mb.Error() != nil && !errors.Is(mb.Error(), context.DeadlineExceeded) {
		log.Warn().Err(mb.Error()).Msg("message batch fetch error")
	}

	if len(interactions) == 0 {
		return 0, nil
	}

	counts, missing, err := d.repo.InsertBatchWithCounts(ctx, interactions)
	if err != nil {
		log.Error().Err(err).Int("batch_size", len(interactions)).Msg("failed to insert batch into postgres, naking messages")
		for _, m := range rawMsgs {
			_ = m.Nak()
		}
		return 0, fmt.Errorf("insert batch: %w", err)
	}

	// A parent of this batch is still being written by another worker, so its reply count
	// cannot be computed yet. Do not commit a partial count: redeliver the batch instead.
	// The inserts are ON CONFLICT DO NOTHING, so the retry only recomputes the counts.
	if len(missing) > 0 {
		log.Warn().
			Int("missing", len(missing)).
			Int("batch_size", len(interactions)).
			Msg("drainer: parent interaction not visible yet, redelivering batch")

		for _, m := range rawMsgs {
			_ = m.NakWithDelay(missingParentRetryDelay)
		}
		return 0, nil
	}

	// Only ack after the rows are durably committed. The reply count event is published
	// after that too: publishing it first would announce counts for rows that may not
	// exist yet if the transaction rolls back.
	for _, m := range rawMsgs {
		_ = m.Ack()
	}

	d.publishReplyCounts(ctx, interactions, counts)

	log.Debug().Int("count", len(interactions)).Msg("successfully drained micro-batch to postgres")
	return len(interactions), nil
}

// publishReplyCounts emits absolute reply counts for the parents touched by a committed
// batch. Counts are keyed per group because the counter event is subject-routed by group.
//
// A publish failure is logged and swallowed rather than propagated: the batch itself is
// already committed and acked, so failing here would not roll anything back and would only
// turn a healthy insert into a retry loop. Counts are derived data, so a missed event
// self-heals on the next batch that touches the same parent.
func (d *Drainer) publishReplyCounts(ctx context.Context, items []*database.Interaction, counts map[uuid.UUID]int) {
	if d.countPublisher == nil || len(counts) == 0 {
		return
	}

	// A parent always belongs to the same group as the replies hanging off it, so each
	// count is attributed to the group of the reply that pulled it in. Counts whose id
	// is not referenced by any item in the batch are skipped rather than guessed.
	groupOfParent := make(map[uuid.UUID]string, len(counts))
	for _, item := range items {
		group := item.GroupID
		if group == "" {
			group = messaging.DefaultGroupID
		}
		for _, parentID := range []*uuid.UUID{item.ParentID, item.RootID} {
			if parentID == nil {
				continue
			}
			if _, known := counts[*parentID]; !known {
				continue
			}
			groupOfParent[*parentID] = group
		}
	}

	byGroup := make(map[string]map[string]int)
	for id, total := range counts {
		group, known := groupOfParent[id]
		if !known {
			continue
		}
		if byGroup[group] == nil {
			byGroup[group] = make(map[string]int)
		}
		byGroup[group][id.String()] = total
	}

	for group, groupCounts := range byGroup {
		evt := &events.ReplyCountsRecalculatedEvent{
			GroupID: group,
			Counts:  groupCounts,
		}
		if err := d.countPublisher.PublishReplyCountsRecalculated(ctx, evt); err != nil {
			log.Error().Err(err).Str("group_id", group).Int("interactions", len(groupCounts)).
				Msg("failed to publish recalculated reply counts")
		}
	}
}

func (d *Drainer) routeToDLQ(ctx context.Context, msg jetstream.Msg, reason string) {
	if d.dlq != nil {
		if err := d.dlq.PublishDLQ(ctx, msg.Subject(), msg.Data(), reason); err != nil {
			log.Error().Err(err).Str("subject", msg.Subject()).Msg("failed to publish poison message to DLQ, naking")
			_ = msg.Nak()
			return
		}
	}
	_ = msg.Ack()
}

// Workers returns the configured number of concurrent drainer workers.
func (d *Drainer) Workers() int {
	return d.workers
}

// RunWorker continuously processes micro-batches for a specific worker until ctx is canceled.
func (d *Drainer) RunWorker(ctx context.Context, workerID int) error {
	log.Info().
		Int("worker_id", workerID).
		Int("batch_size", d.batchSize).
		Dur("flush_interval", d.flushInterval).
		Msg("starting JetStream micro-batch drainer worker")

	for {
		select {
		case <-ctx.Done():
			log.Info().Int("worker_id", workerID).Msg("stopping micro-batch drainer worker")
			return nil
		default:
			if _, err := d.ProcessBatch(ctx); err != nil {
				log.Error().Err(err).Int("worker_id", workerID).Msg("error processing micro-batch")
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

// Run continuously processes micro-batches until ctx is canceled (worker 1).
func (d *Drainer) Run(ctx context.Context) error {
	return d.RunWorker(ctx, 1)
}

// Start runs the drainer workers in background goroutines until ctx is canceled.
func (d *Drainer) Start(ctx context.Context) {
	for i := 0; i < d.workers; i++ {
		workerID := i + 1
		go func(id int) {
			if err := d.RunWorker(ctx, id); err != nil && !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Int("worker_id", id).Msg("micro-batch drainer worker exited with error")
			}
		}(workerID)
	}
}

// Start creates a new Drainer and starts its background processing loop.
func Start(ctx context.Context, fetcher BatchFetcher, repo database.Repository, dlq DLQPublisher, cfg *config.Config) *Drainer {
	d := New(fetcher, repo, dlq, cfg)
	d.Start(ctx)
	return d
}

// StartWithCountPublisher creates a Drainer wired to publish recalculated reply counts,
// and starts its background processing loop.
func StartWithCountPublisher(
	ctx context.Context,
	fetcher BatchFetcher,
	repo database.Repository,
	dlq DLQPublisher,
	countPublisher ReplyCountPublisher,
	cfg *config.Config,
) *Drainer {
	d := NewWithCountPublisher(fetcher, repo, dlq, countPublisher, cfg)
	d.Start(ctx)
	return d
}

func eventToInteraction(evt *events.InteractionCreatedEvent) (*database.Interaction, error) {
	parsedID, err := uuid.Parse(evt.ID)
	if err != nil {
		return nil, fmt.Errorf("parse interaction id: %w", err)
	}

	// Guard against decoding a non-creation event as an interaction. An
	// InteractionEditedEvent shares id/group_id/body/title/version with the created
	// schema, so json.Unmarshal succeeds and silently leaves author and created_at at
	// their zero values, which would insert a row with an empty author and a year-1
	// timestamp. The subject namespace keeps edits out of the drainer's filter, but this
	// check makes the failure loud instead of corrupting data if that ever changes.
	if evt.Author == "" {
		return nil, errors.New("interaction event has empty author")
	}
	if evt.CreatedAt.IsZero() {
		return nil, errors.New("interaction event has zero created_at")
	}

	var rootID *uuid.UUID
	if evt.RootID != nil && *evt.RootID != "" {
		if pRoot, err := uuid.Parse(*evt.RootID); err == nil {
			rootID = &pRoot
		}
	}

	var parentID *uuid.UUID
	if evt.ParentID != nil && *evt.ParentID != "" {
		if pParent, err := uuid.Parse(*evt.ParentID); err == nil {
			parentID = &pParent
		}
	}

	version := evt.Version
	if version <= 0 {
		version = 1
	}

	return &database.Interaction{
		ID:         parsedID,
		GroupID:    evt.GroupID,
		RootID:     rootID,
		ParentID:   parentID,
		Title:      evt.Title,
		Body:       evt.Body,
		Author:     evt.Author,
		CreatedAt:  evt.CreatedAt,
		ReplyCount: evt.ReplyCount,
		Depth:      evt.Depth,
		Version:    version,
	}, nil
}
