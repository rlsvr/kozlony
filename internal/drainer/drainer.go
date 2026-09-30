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

// Drainer micro-batches interaction events from NATS JetStream and bulk-inserts them into PostgreSQL.
type Drainer struct {
	fetcher       BatchFetcher
	repo          database.Repository
	dlq           DLQPublisher
	batchSize     int
	flushInterval time.Duration
}

// New creates a new Drainer instance.
func New(fetcher BatchFetcher, repo database.Repository, dlq DLQPublisher, cfg *config.Config) *Drainer {
	batchSize := 500
	flushInterval := 50 * time.Millisecond
	if cfg != nil {
		if cfg.DrainBatchSize > 0 {
			batchSize = cfg.DrainBatchSize
		}
		if cfg.DrainFlushInterval > 0 {
			flushInterval = cfg.DrainFlushInterval
		}
	}

	return &Drainer{
		fetcher:       fetcher,
		repo:          repo,
		dlq:           dlq,
		batchSize:     batchSize,
		flushInterval: flushInterval,
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

	if err := d.repo.InsertBatch(ctx, interactions); err != nil {
		log.Error().Err(err).Int("batch_size", len(interactions)).Msg("failed to insert batch into postgres, naking messages")
		for _, m := range rawMsgs {
			_ = m.Nak()
		}
		return 0, fmt.Errorf("insert batch: %w", err)
	}

	for _, m := range rawMsgs {
		_ = m.Ack()
	}

	log.Debug().Int("count", len(interactions)).Msg("successfully drained micro-batch to postgres")
	return len(interactions), nil
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

// Run continuously processes micro-batches until ctx is canceled.
func (d *Drainer) Run(ctx context.Context) error {
	log.Info().
		Int("batch_size", d.batchSize).
		Dur("flush_interval", d.flushInterval).
		Msg("starting JetStream micro-batch drainer worker")

	for {
		select {
		case <-ctx.Done():
			log.Info().Msg("stopping micro-batch drainer worker")
			return nil
		default:
			if _, err := d.ProcessBatch(ctx); err != nil {
				log.Error().Err(err).Msg("error processing micro-batch")
				select {
				case <-ctx.Done():
					return nil
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

// Start runs the drainer worker in a background goroutine until ctx is canceled.
func (d *Drainer) Start(ctx context.Context) {
	go func() {
		if err := d.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error().Err(err).Msg("micro-batch drainer worker exited with error")
		}
	}()
}

// Start creates a new Drainer and starts its background processing loop.
func Start(ctx context.Context, fetcher BatchFetcher, repo database.Repository, dlq DLQPublisher, cfg *config.Config) *Drainer {
	d := New(fetcher, repo, dlq, cfg)
	d.Start(ctx)
	return d
}

func eventToInteraction(evt *events.InteractionCreatedEvent) (*database.Interaction, error) {
	parsedID, err := uuid.Parse(evt.ID)
	if err != nil {
		return nil, fmt.Errorf("parse interaction id: %w", err)
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
