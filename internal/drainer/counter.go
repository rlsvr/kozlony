package drainer

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=counter.go -destination=mocks/mock_counter.go -package=mocks

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog/log"

	"github.com/rlsvr/kozlony/internal/config"
	"github.com/rlsvr/kozlony/internal/messaging/events"
)

// missingParentRetryDelay is how long to wait before retrying counts whose parent row has
// not committed yet. It is comfortably longer than a drainer batch takes to commit, so the
// parent is normally present on redelivery.
const missingParentRetryDelay = 250 * time.Millisecond

// ReplyCountWriter persists absolute reply counts. It returns the ids whose row does not
// exist yet, which must be redelivered once the row is committed.
type ReplyCountWriter interface {
	ApplyReplyCounts(ctx context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error)
}

// Counter applies recalculated reply counts produced by the drainer.
//
// It exists to take reply_count writes out of the drainer's insert transaction. The
// drainer previously incremented reply_count while holding a transaction open for a whole
// batch, which held a row-level exclusive lock on every touched parent until COMMIT and
// serialized all other writers against hot threads. Here each batch is applied in its own
// small transaction, so those locks are held for microseconds.
//
// Redelivery is safe without any dedup bookkeeping: the drainer sends absolute totals, and
// applying the same total twice leaves the same value. ApplyReplyCounts additionally guards
// monotonicity so a delayed event cannot walk the count backwards.
type Counter struct {
	fetcher       BatchFetcher
	repo          ReplyCountWriter
	batchSize     int
	flushInterval time.Duration
	workers       int

	wg     sync.WaitGroup
	cancel context.CancelFunc
}

// NewCounter creates a new Counter instance.
func NewCounter(fetcher BatchFetcher, repo ReplyCountWriter, cfg *config.Config) *Counter {
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

	return &Counter{
		fetcher:       fetcher,
		repo:          repo,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		workers:       workers,
	}
}

// ProcessBatch fetches one batch of reply count events, merges them into the highest count
// seen per interaction, applies the result, and acknowledges the messages.
//
// Counts are merged with max() rather than overwritten because events for one interaction
// may arrive across several batches and out of order. Only the highest total is applied,
// which is both correct under reordering and immune to double application.
func (c *Counter) ProcessBatch(ctx context.Context) (int, error) {
	mb, err := c.fetcher.Fetch(c.batchSize, jetstream.FetchMaxWait(c.flushInterval))
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return 0, nil
		}
		return 0, fmt.Errorf("fetch reply count batch from jetstream: %w", err)
	}

	var (
		rawMsgs []jetstream.Msg
		counts  = make(map[uuid.UUID]int)
	)

	for msg := range mb.Messages() {
		var evt events.ReplyCountsRecalculatedEvent
		if err := json.Unmarshal(msg.Data(), &evt); err != nil {
			log.Warn().Err(err).Str("subject", msg.Subject()).Msg("counter: failed to unmarshal event, skipping")
			_ = msg.Ack()
			continue
		}

		for idStr, total := range evt.Counts {
			id, parseErr := uuid.Parse(idStr)
			if parseErr != nil {
				log.Warn().Err(parseErr).Str("id", idStr).Msg("counter: invalid interaction id in event, skipping")
				continue
			}
			if total < 0 {
				continue
			}
			// Keep the highest total: a slower batch must never overwrite a newer one.
			if existing, seen := counts[id]; !seen || total > existing {
				counts[id] = total
			}
		}
		rawMsgs = append(rawMsgs, msg)
	}

	if mb.Error() != nil && !errors.Is(mb.Error(), context.DeadlineExceeded) {
		log.Warn().Err(mb.Error()).Msg("counter: message batch fetch error")
	}

	if len(counts) == 0 {
		for _, m := range rawMsgs {
			_ = m.Ack()
		}
		return 0, nil
	}

	missing, err := c.repo.ApplyReplyCounts(ctx, counts)
	if err != nil {
		log.Error().Err(err).Int("interactions", len(counts)).Msg("counter: failed to apply reply counts, naking messages")
		for _, m := range rawMsgs {
			_ = m.Nak()
		}
		return 0, fmt.Errorf("apply reply counts: %w", err)
	}

	// A reply can be drained before the batch that creates its parent commits, so the
	// count for that parent has nowhere to land yet. NAK with a delay rather than acking,
	// because the batch that creates the parent emits counts for its own parent, not for
	// itself, so nothing else would ever retry this id. Re-applying counts that already
	// landed is harmless: they are absolute totals and the store only moves forward.
	if len(missing) > 0 {
		log.Warn().
			Int("missing", len(missing)).
			Int("interactions", len(counts)).
			Msg("counter: parent interaction not committed yet, redelivering after delay")

		for _, m := range rawMsgs {
			_ = m.NakWithDelay(missingParentRetryDelay)
		}
		return 0, nil
	}

	for _, m := range rawMsgs {
		_ = m.Ack()
	}

	log.Debug().Int("interactions", len(counts)).Msg("counter: applied recalculated reply counts")
	return len(counts), nil
}

// RunWorker continuously applies reply count batches until ctx is canceled.
func (c *Counter) RunWorker(ctx context.Context, workerID int) error {
	log.Info().
		Int("worker_id", workerID).
		Int("batch_size", c.batchSize).
		Msg("starting reply count counter worker")

	for {
		select {
		case <-ctx.Done():
			log.Info().Int("worker_id", workerID).Msg("stopping reply count counter worker")
			return nil
		default:
			if _, err := c.ProcessBatch(ctx); err != nil {
				log.Error().Err(err).Int("worker_id", workerID).Msg("counter: error processing batch")
				select {
				case <-ctx.Done():
					log.Info().Int("worker_id", workerID).Msg("stopping reply count counter worker")
					return nil
				case <-time.After(100 * time.Millisecond):
				}
			}
		}
	}
}

// Run continuously applies reply count batches until ctx is canceled (worker 1).
func (c *Counter) Run(ctx context.Context) error {
	return c.RunWorker(ctx, 1)
}

// Workers returns the configured number of concurrent counter workers.
func (c *Counter) Workers() int {
	return c.workers
}

// Start runs the counter workers and registers them for shutdown via Stop.
func (c *Counter) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	c.cancel = cancel

	for i := 0; i < c.workers; i++ {
		workerID := i + 1
		c.wg.Add(1)
		go func(id int) {
			defer c.wg.Done()
			if err := c.RunWorker(ctx, id); err != nil && !errors.Is(err, context.Canceled) {
				log.Error().Err(err).Int("worker_id", id).Msg("reply count counter worker exited with error")
			}
		}(workerID)
	}
}

// Stop signals the workers to finish and blocks until they have exited, so the caller can
// close the database pool without racing an in-flight ApplyReplyCounts.
func (c *Counter) Stop() {
	if c.cancel != nil {
		c.cancel()
	}
	c.wg.Wait()
}
