package database

//go:generate go run go.uber.org/mock/mockgen@v0.6.0 -source=postgres.go -destination=mocks/mock_postgres.go -package=mocks

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// ErrNotFound indicates the requested entity was not found in the database.
var ErrNotFound = errors.New("interaction not found")

// ErrVersionConflict indicates an optimistic concurrency version mismatch.
var ErrVersionConflict = errors.New("version conflict: interaction was modified by another request")

// PgxPool defines the interface for PostgreSQL operations.
// Satisfied by *pgxpool.Pool.
type PgxPool interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Ping(ctx context.Context) error
	Close()
}

// Interaction represents a persisted post or reply in PostgreSQL.
type Interaction struct {
	ID         uuid.UUID  `json:"id"`
	GroupID    string     `json:"group_id"`
	RootID     *uuid.UUID `json:"root_id,omitempty"`
	ParentID   *uuid.UUID `json:"parent_id,omitempty"`
	Title      *string    `json:"title,omitempty"`
	Body       string     `json:"body"`
	Author     string     `json:"author"`
	CreatedAt  time.Time  `json:"created_at"`
	UpdatedAt  *time.Time `json:"updated_at,omitempty"`
	ReplyCount int        `json:"reply_count"`
	Depth      int        `json:"depth"`
	Version    int        `json:"version"`
}

// Connect parses the database URL, configures connection pool parameters with defaults, and validates connectivity.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	return ConnectWithLimits(ctx, databaseURL, 50, 10)
}

// ConnectWithLimits parses the database URL, configures custom connection pool limits, and validates connectivity.
func ConnectWithLimits(ctx context.Context, databaseURL string, maxConns, minConns int) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	if maxConns > 0 && maxConns <= 10000 {
		poolCfg.MaxConns = int32(maxConns)
	} else if maxConns > 10000 {
		poolCfg.MaxConns = 10000
	} else {
		poolCfg.MaxConns = 50
	}

	if minConns > 0 && minConns <= 10000 {
		poolCfg.MinConns = int32(minConns)
	} else if minConns > 10000 {
		poolCfg.MinConns = 10000
	} else {
		poolCfg.MinConns = 10
	}
	poolCfg.MaxConnLifetime = 1 * time.Hour
	poolCfg.MaxConnIdleTime = 30 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create pgx pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
	}

	return pool, nil
}

// Repository defines interactions storage methods.
type Repository interface {
	Ping(ctx context.Context) error
	Insert(ctx context.Context, item *Interaction) error
	InsertBatch(ctx context.Context, items []*Interaction) error
	// InsertBatchWithCounts inserts a batch and returns the recalculated reply
	// count for every parent it touched, plus the ids whose rows are not
	// visible yet. A non-empty missing list means the batch should be
	// redelivered rather than committed, since a count cannot be anchored to
	// a row that does not exist.
	InsertBatchWithCounts(ctx context.Context, items []*Interaction) (map[uuid.UUID]int, []uuid.UUID, error)
	// CountDescendants returns the total number of descendants for each supplied
	// interaction id, counted over committed rows only.
	CountDescendants(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int, error)
	// ApplyReplyCounts writes absolute reply counts and returns the ids whose row
	// does not exist yet, which the caller must redeliver once the row is committed.
	// Storing absolute totals rather than deltas makes that redelivery idempotent, and
	// the monotonic guard stops an out-of-order event from walking the count backwards.
	ApplyReplyCounts(ctx context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error)
	Update(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*Interaction, error)
	// UpdateWithIdempotency applies an update and records its outcome under a
	// client-supplied key in the same transaction. A retry presenting a key that was
	// already applied replays the recorded outcome rather than failing the version
	// check the client can no longer satisfy.
	//
	// A nil key skips recording and behaves exactly like Update.
	UpdateWithIdempotency(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body, idempotencyKey string) (*Interaction, bool, error)
	GetByID(ctx context.Context, id uuid.UUID) (*Interaction, error)
	GetThread(ctx context.Context, rootID uuid.UUID) ([]*Interaction, error)
	ListFeed(ctx context.Context, groupID string, limit int, before *time.Time) ([]*Interaction, error)
}

// InteractionRepository implements Repository using PgxPool.
type InteractionRepository struct {
	pool PgxPool
}

// NewInteractionRepository constructs a new InteractionRepository.
func NewInteractionRepository(pool PgxPool) *InteractionRepository {
	return &InteractionRepository{pool: pool}
}

// Ping verifies database connectivity.
func (r *InteractionRepository) Ping(ctx context.Context) error {
	if r.pool == nil {
		return errors.New("database pool is nil")
	}
	return r.pool.Ping(ctx)
}

// Insert writes a single interaction to PostgreSQL.
func (r *InteractionRepository) Insert(ctx context.Context, item *Interaction) error {
	version := item.Version
	if version <= 0 {
		version = 1
	}

	query := `
		INSERT INTO interactions (
			id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		) ON CONFLICT (id) DO NOTHING;
	`
	_, err := r.pool.Exec(ctx, query,
		item.ID,
		item.GroupID,
		item.RootID,
		item.ParentID,
		item.Title,
		item.Body,
		item.Author,
		item.CreatedAt,
		item.UpdatedAt,
		item.ReplyCount,
		item.Depth,
		version,
	)
	if err != nil {
		return fmt.Errorf("insert interaction: %w", err)
	}
	return nil
}

// InsertBatch writes a batch of interactions to PostgreSQL using a single atomic transaction.
//
// Reply counts are deliberately NOT written here. Incrementing reply_count inside this
// transaction holds a row-level exclusive lock on every touched parent until COMMIT, so a
// 2000-row batch holds those locks for the whole batch duration and serializes all other
// writers against hot threads. Counts are instead recalculated and applied by a separate
// consumer (see ApplyReplyCounts).
func (r *InteractionRepository) InsertBatch(ctx context.Context, items []*Interaction) error {
	_, _, err := r.InsertBatchWithCounts(ctx, items)
	return err
}

// InsertBatchWithCounts inserts a batch and returns the recalculated reply count for every
// parent the batch touched.
//
// The count runs on the same connection as the insert, before COMMIT, so it observes the
// batch's own uncommitted rows. That is what makes a batch self-counting: the drainer can
// emit a correct absolute total without waiting for a second pass over the data.
//
// The returned counts are absolute totals, not deltas, which is what lets the counter
// consumer be idempotent under JetStream's at-least-once delivery.
func (r *InteractionRepository) InsertBatchWithCounts(ctx context.Context, items []*Interaction) (map[uuid.UUID]int, []uuid.UUID, error) {
	if len(items) == 0 {
		return map[uuid.UUID]int{}, nil, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := insertBatchRows(ctx, tx, items); err != nil {
		return nil, nil, err
	}

	affected := affectedParentIDs(items)
	counts, err := countDescendants(ctx, tx, affected)
	if err != nil {
		return nil, nil, err
	}

	// A parent whose row is not visible yet cannot be counted. Redelivering the batch is
	// safe: the inserts are ON CONFLICT DO NOTHING, so only the counts are recomputed.
	var missing []uuid.UUID
	for _, id := range affected {
		if _, ok := counts[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) > 0 {
		return nil, missing, nil
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, nil, fmt.Errorf("commit batch transaction: %w", err)
	}
	return counts, nil, nil
}

// insertBatchRows pipelines the inserts for a batch onto the given transaction.
func insertBatchRows(ctx context.Context, tx pgx.Tx, items []*Interaction) error {
	batch := &pgx.Batch{}
	query := `
		INSERT INTO interactions (
			id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12
		) ON CONFLICT (id) DO NOTHING;
	`

	for _, item := range items {
		version := item.Version
		if version <= 0 {
			version = 1
		}
		batch.Queue(query,
			item.ID,
			item.GroupID,
			item.RootID,
			item.ParentID,
			item.Title,
			item.Body,
			item.Author,
			item.CreatedAt,
			item.UpdatedAt,
			item.ReplyCount,
			item.Depth,
			version,
		)
	}

	br := tx.SendBatch(ctx, batch)
	for range items {
		if _, err := br.Exec(); err != nil {
			_ = br.Close()
			return fmt.Errorf("execute batch item: %w", err)
		}
	}
	if err := br.Close(); err != nil {
		return fmt.Errorf("close batch results: %w", err)
	}
	return nil
}

// affectedParentIDs returns the distinct set of interactions whose reply count a batch can
// change: every reply contributes to its immediate parent and to the thread root.
func affectedParentIDs(items []*Interaction) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(items)*2)
	ids := make([]uuid.UUID, 0, len(items)*2)

	add := func(id *uuid.UUID) {
		if id == nil {
			return
		}
		if _, dup := seen[*id]; dup {
			return
		}
		seen[*id] = struct{}{}
		ids = append(ids, *id)
	}

	for _, item := range items {
		add(item.ParentID)
		if item.RootID != nil && (item.ParentID == nil || *item.RootID != *item.ParentID) {
			add(item.RootID)
		}
	}
	return ids
}

// rowQuerier is the subset of pgx shared by a pool and a transaction, so the descendant
// count can run either standalone or inside an open transaction.
type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// CountDescendants returns the total descendant count for each supplied id, counted
// outside any transaction and therefore only over committed rows.
func (r *InteractionRepository) CountDescendants(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int, error) {
	return countDescendants(ctx, r.pool, ids)
}

// countDescendants returns the reply count for each supplied id.
//
// A thread root counts every reply in its conversation; an intermediate reply counts only
// its direct replies. That split is not arbitrary: the drainer only ever recounts the
// immediate parent and the thread root of each drained batch, so those are exactly the
// nodes whose counts a new reply can change. Deriving a uniform "all descendants at every
// depth" figure would require recounting every ancestor too, and any ancestor the drainer
// does not name keeps a stale number forever.
//
// When q is a transaction, rows inserted by that same transaction are visible to the count.
// An id whose row is absent returns a NULL total, which the caller treats as "not ready".
func countDescendants(ctx context.Context, q rowQuerier, ids []uuid.UUID) (map[uuid.UUID]int, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]int{}, nil
	}

	query := `
		WITH targets AS (
			SELECT unnest($1::uuid[]) AS id
		),
		thread_total AS (
			SELECT root_id AS id, COUNT(*)::bigint AS n
			FROM interactions
			WHERE root_id = ANY($1)
			GROUP BY root_id
		),
		direct_replies AS (
			SELECT parent_id AS id, COUNT(*)::bigint AS n
			FROM interactions
			WHERE parent_id = ANY($1)
			GROUP BY parent_id
		)
		SELECT t.id,
		       CASE
		           WHEN i.id IS NULL THEN NULL
		           WHEN i.root_id IS NULL THEN COALESCE(tt.n, 0)
		           ELSE COALESCE(dr.n, 0)
		       END AS n
		FROM targets t
		LEFT JOIN interactions i ON i.id = t.id
		LEFT JOIN thread_total tt ON tt.id = t.id
		LEFT JOIN direct_replies dr ON dr.id = t.id;
	`

	rows, err := q.Query(ctx, query, ids)
	if err != nil {
		return nil, fmt.Errorf("count descendants: %w", err)
	}
	defer rows.Close()

	counts := make(map[uuid.UUID]int, len(ids))
	for rows.Next() {
		var (
			id    uuid.UUID
			total *int
		)
		if err := rows.Scan(&id, &total); err != nil {
			return nil, fmt.Errorf("scan descendant count: %w", err)
		}
		if total == nil {
			// The row is not visible yet; the caller retries once it commits.
			continue
		}
		counts[id] = *total
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("descendant count rows: %w", err)
	}
	return counts, nil
}

// ApplyReplyCounts writes absolute reply counts in a single statement and returns the ids
// whose row does not exist yet.
//
// The missing ids matter: a reply's parent may be inserted by a different worker whose
// batch commits later, so the parent row can be absent when this runs. The UPDATE would
// then match nothing and the count would be lost for good, because the batch that creates
// the parent emits counts for the parent's own parent, not for the parent itself. Reporting
// those ids lets the caller redeliver the event once the row is committed.
//
// Storing absolute totals rather than a delta is what makes that redelivery safe: replaying
// a batch recomputes the same number instead of incrementing twice. The monotonic guard
// (reply_count < new) additionally prevents a delayed, older event from moving an
// already-larger count backwards.
//
// Ids are sorted before binding so row locks are always acquired in a deterministic
// order across concurrent counter workers, which is what prevents deadlocks.
func (r *InteractionRepository) ApplyReplyCounts(ctx context.Context, counts map[uuid.UUID]int) ([]uuid.UUID, error) {
	if len(counts) == 0 {
		return nil, nil
	}

	sortedIDs := make([]uuid.UUID, 0, len(counts))
	for id := range counts {
		sortedIDs = append(sortedIDs, id)
	}
	sort.Slice(sortedIDs, func(i, j int) bool {
		return bytes.Compare(sortedIDs[i][:], sortedIDs[j][:]) < 0
	})

	totals := make([]int64, len(sortedIDs))
	for i, id := range sortedIDs {
		totals[i] = int64(counts[id])
	}

	// The final SELECT reports targets that matched no row. An id whose row exists but is
	// already at or above the incoming total is deliberately excluded: there is nothing to
	// retry in that case, and retrying it would never clear.
	query := `
		WITH target AS (
			SELECT unnest($1::uuid[]) AS id, unnest($2::bigint[]) AS n
		),
		updated AS (
			UPDATE interactions AS i
			SET reply_count = target.n
			FROM target
			WHERE i.id = target.id AND i.reply_count < target.n
			RETURNING i.id
		)
		SELECT target.id
		FROM target
		WHERE NOT EXISTS (SELECT 1 FROM interactions i WHERE i.id = target.id);
	`

	rows, err := r.pool.Query(ctx, query, sortedIDs, totals)
	if err != nil {
		return nil, fmt.Errorf("apply reply counts: %w", err)
	}
	defer rows.Close()

	var missing []uuid.UUID
	for rows.Next() {
		var id uuid.UUID
		if err := rows.Scan(&id); err != nil {
			return nil, fmt.Errorf("scan missing reply count id: %w", err)
		}
		missing = append(missing, id)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("missing reply count rows: %w", err)
	}
	return missing, nil
}

// Update updates an interaction body and title with optimistic concurrency version control.
func (r *InteractionRepository) Update(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*Interaction, error) {
	item, _, err := r.UpdateWithIdempotency(ctx, id, expectedVersion, title, body, "")
	return item, err
}

// UpdateWithIdempotency applies an update and records its outcome under a client-supplied
// key, so a retry can replay that outcome instead of failing the version check.
//
// The record is written in the same transaction as the update. That ordering is what makes
// the replay safe: if the record committed, the update did too, so replaying it is honest.
// The reverse order would let a retry see a key whose update had rolled back.
//
// Returns (item, replayed, error). replayed is true when the key was already applied and
// the stored outcome is being returned without re-running the update.
//
// A nil or empty key skips recording entirely and behaves exactly like Update.
func (r *InteractionRepository) UpdateWithIdempotency(
	ctx context.Context,
	id uuid.UUID,
	expectedVersion int,
	title *string,
	body string,
	idempotencyKey string,
) (*Interaction, bool, error) {
	// A retry arrives with the original expectedVersion, which no longer holds because the
	// first attempt already incremented it. Check for a recorded outcome first so the
	// client gets its original result rather than an unexplainable conflict.
	if idempotencyKey != "" {
		recorded, err := r.replayUpdate(ctx, id, idempotencyKey)
		if err != nil {
			return nil, false, err
		}
		if recorded != nil {
			return recorded, true, nil
		}
	}

	// COALESCE preserves the existing title when the client omits it, so a
	// body-only edit can never silently wipe a root post's title.
	updateQuery := `
		UPDATE interactions
		SET title = COALESCE($3, title), body = $4, version = version + 1, updated_at = NOW()
		WHERE id = $1 AND version = $2
		RETURNING id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version;
	`

	if idempotencyKey == "" {
		item, err := r.runUpdate(ctx, updateQuery, id, expectedVersion, title, body)
		if err != nil {
			return nil, false, err
		}
		return item, false, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("begin update transaction: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	item, err := r.runUpdate(ctx, updateQuery, id, expectedVersion, title, body, tx)
	if err != nil {
		// A concurrent retry on the same key may have won between the check above and this
		// UPDATE. Look for its recorded outcome before reporting a conflict, so a retried
		// edit still replays rather than failing with an unexplainable 409.
		replayed, replayErr := r.replayAfterConflict(ctx, tx, id, idempotencyKey, err)
		if replayErr != nil {
			return nil, false, replayErr
		}
		if replayed != nil {
			if err := tx.Commit(ctx); err != nil {
				return nil, false, fmt.Errorf("commit update transaction: %w", err)
			}
			return replayed, true, nil
		}
		return nil, false, err
	}

	recordQuery := `
		INSERT INTO interaction_update_keys (interaction_id, idempotency_key, version, title, body)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (interaction_id, idempotency_key) DO NOTHING;
	`
	if _, err := tx.Exec(ctx, recordQuery, id, idempotencyKey, item.Version, item.Title, item.Body); err != nil {
		return nil, false, fmt.Errorf("record update idempotency key: %w", err)
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, false, fmt.Errorf("commit update transaction: %w", err)
	}
	return item, false, nil
}

// replayUpdate returns the recorded outcome for a previously applied key, or nil if the key
// has not been seen.
func (r *InteractionRepository) replayUpdate(ctx context.Context, id uuid.UUID, idempotencyKey string) (*Interaction, error) {
	return replayUpdateWith(ctx, r.pool, id, idempotencyKey)
}

// replayAfterConflict returns the recorded outcome for updateErr's key when updateErr is a
// version conflict and another writer already applied that key, or nil when the conflict is
// genuine. It reads inside the transaction so it sees a racing winner's commit.
func (*InteractionRepository) replayAfterConflict(
	ctx context.Context,
	tx pgx.Tx,
	id uuid.UUID,
	idempotencyKey string,
	updateErr error,
) (*Interaction, error) {
	if !errors.Is(updateErr, ErrVersionConflict) {
		return nil, nil
	}
	return replayUpdateWith(ctx, tx, id, idempotencyKey)
}

func replayUpdateWith(ctx context.Context, q execer, id uuid.UUID, idempotencyKey string) (*Interaction, error) {
	query := `
		SELECT i.id, i.group_id, i.root_id, i.parent_id, i.title, i.body, i.author,
		       i.created_at, i.updated_at, i.reply_count, i.depth, i.version
		FROM interaction_update_keys k
		JOIN interactions i ON i.id = k.interaction_id
		WHERE k.interaction_id = $1 AND k.idempotency_key = $2;
	`

	item, err := scanInteraction(q.QueryRow(ctx, query, id, idempotencyKey))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, nil
		}
		return nil, fmt.Errorf("replay update: %w", err)
	}
	return item, nil
}

// rowScanner is satisfied by pgx.Row.
type rowScanner interface {
	Scan(dest ...any) error
}

func scanInteraction(row rowScanner) (*Interaction, error) {
	var item Interaction
	err := row.Scan(
		&item.ID,
		&item.GroupID,
		&item.RootID,
		&item.ParentID,
		&item.Title,
		&item.Body,
		&item.Author,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.ReplyCount,
		&item.Depth,
		&item.Version,
	)
	if err != nil {
		return nil, err
	}
	return &item, nil
}

// execer is satisfied by the pool and by a transaction, so runUpdate can serve both paths.
type execer interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

func (r *InteractionRepository) runUpdate(
	ctx context.Context,
	query string,
	id uuid.UUID,
	expectedVersion int,
	title *string,
	body string,
	txs ...execer,
) (*Interaction, error) {
	var q execer = r.pool
	if len(txs) > 0 {
		q = txs[0]
	}

	item, err := scanInteraction(q.QueryRow(ctx, query, id, expectedVersion, title, body))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, r.updateConflictError(ctx, id, txs...)
		}
		return nil, fmt.Errorf("update interaction: %w", err)
	}
	return item, nil
}

// updateConflictError distinguishes a missing row from a genuine version conflict. When a
// transaction is supplied the lookup runs inside it so it sees the same snapshot.
func (r *InteractionRepository) updateConflictError(ctx context.Context, id uuid.UUID, txs ...execer) error {
	var q execer = r.pool
	if len(txs) > 0 {
		q = txs[0]
	}

	var currentVersion int
	err := q.QueryRow(ctx, "SELECT version FROM interactions WHERE id = $1;", id).Scan(&currentVersion)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		return fmt.Errorf("check interaction version: %w", err)
	}
	return ErrVersionConflict
}

// GetByID retrieves a single interaction by its UUID.
func (r *InteractionRepository) GetByID(ctx context.Context, id uuid.UUID) (*Interaction, error) {
	query := `
		SELECT id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
		FROM interactions
		WHERE id = $1;
	`
	row := r.pool.QueryRow(ctx, query, id)

	var item Interaction
	err := row.Scan(
		&item.ID,
		&item.GroupID,
		&item.RootID,
		&item.ParentID,
		&item.Title,
		&item.Body,
		&item.Author,
		&item.CreatedAt,
		&item.UpdatedAt,
		&item.ReplyCount,
		&item.Depth,
		&item.Version,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get interaction by id: %w", err)
	}

	return &item, nil
}

// GetThread fetches the root post and all replies in chronological order.
func (r *InteractionRepository) GetThread(ctx context.Context, rootID uuid.UUID) ([]*Interaction, error) {
	query := `
		SELECT id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
		FROM interactions
		WHERE id = $1 OR root_id = $1
		ORDER BY created_at ASC;
	`
	rows, err := r.pool.Query(ctx, query, rootID)
	if err != nil {
		return nil, fmt.Errorf("query thread: %w", err)
	}
	defer rows.Close()

	var results []*Interaction
	for rows.Next() {
		var item Interaction
		if err := rows.Scan(
			&item.ID,
			&item.GroupID,
			&item.RootID,
			&item.ParentID,
			&item.Title,
			&item.Body,
			&item.Author,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.ReplyCount,
			&item.Depth,
			&item.Version,
		); err != nil {
			return nil, fmt.Errorf("scan thread row: %w", err)
		}
		results = append(results, &item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("thread rows error: %w", err)
	}

	return results, nil
}

// ListFeed lists root posts for a given group ordered newest first.
func (r *InteractionRepository) ListFeed(ctx context.Context, groupID string, limit int, before *time.Time) ([]*Interaction, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	if groupID == "" {
		groupID = "root"
	}

	var (
		rows pgx.Rows
		err  error
	)

	if before != nil {
		query := `
			SELECT id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
			FROM interactions
			WHERE group_id = $1 AND parent_id IS NULL AND created_at < $2
			ORDER BY created_at DESC
			LIMIT $3;
		`
		rows, err = r.pool.Query(ctx, query, groupID, *before, limit)
	} else {
		query := `
			SELECT id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version
			FROM interactions
			WHERE group_id = $1 AND parent_id IS NULL
			ORDER BY created_at DESC
			LIMIT $2;
		`
		rows, err = r.pool.Query(ctx, query, groupID, limit)
	}

	if err != nil {
		return nil, fmt.Errorf("query feed: %w", err)
	}
	defer rows.Close()

	var results []*Interaction
	for rows.Next() {
		var item Interaction
		if err := rows.Scan(
			&item.ID,
			&item.GroupID,
			&item.RootID,
			&item.ParentID,
			&item.Title,
			&item.Body,
			&item.Author,
			&item.CreatedAt,
			&item.UpdatedAt,
			&item.ReplyCount,
			&item.Depth,
			&item.Version,
		); err != nil {
			return nil, fmt.Errorf("scan feed row: %w", err)
		}
		results = append(results, &item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("feed rows error: %w", err)
	}

	return results, nil
}
