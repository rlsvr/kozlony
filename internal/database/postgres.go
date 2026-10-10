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
	// count for every parent it touched. The count runs inside the insert
	// transaction so it observes the batch's own uncommitted rows.
	InsertBatchWithCounts(ctx context.Context, items []*Interaction) (map[uuid.UUID]int, error)
	// CountDescendants returns the total number of descendants for each supplied
	// interaction id, counted over committed rows only.
	CountDescendants(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]int, error)
	// ApplyReplyCounts writes absolute reply counts. Because it stores absolute
	// totals rather than deltas, replaying the same batch is a no-op, and the
	// monotonic guard stops an out-of-order event from walking the count backwards.
	ApplyReplyCounts(ctx context.Context, counts map[uuid.UUID]int) error
	Update(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*Interaction, error)
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
	_, err := r.InsertBatchWithCounts(ctx, items)
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
func (r *InteractionRepository) InsertBatchWithCounts(ctx context.Context, items []*Interaction) (map[uuid.UUID]int, error) {
	if len(items) == 0 {
		return map[uuid.UUID]int{}, nil
	}

	tx, err := r.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin batch transaction: %w", err)
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := insertBatchRows(ctx, tx, items); err != nil {
		return nil, err
	}

	counts, err := countDescendants(ctx, tx, affectedParentIDs(items))
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit batch transaction: %w", err)
	}
	return counts, nil
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

// countDescendants returns, for each supplied id, the number of replies in its subtree.
//
// The count walks parent_id recursively so reply_count carries a single meaning (total
// descendants) at every depth. A non-recursive shortcut is not equivalent: keying on
// root_id only works for thread roots and reports nothing for an intermediate reply, and
// adding a parent_id tally on top of it double-counts the top level of a subtree.
//
// UNION (not UNION ALL) deduplicates each level, which also makes the walk terminate on a
// malformed cycle in the data instead of looping forever.
//
// When q is a transaction, rows inserted by that same transaction are visible to the count.
func countDescendants(ctx context.Context, q rowQuerier, ids []uuid.UUID) (map[uuid.UUID]int, error) {
	if len(ids) == 0 {
		return map[uuid.UUID]int{}, nil
	}

	query := `
		WITH RECURSIVE targets AS (
			SELECT unnest($1::uuid[]) AS id
		),
		tree AS (
			SELECT c.id, t.id AS target
			FROM targets t
			JOIN interactions c ON c.parent_id = t.id
		UNION
			SELECT c.id, tr.target
			FROM tree tr
			JOIN interactions c ON c.parent_id = tr.id
		),
		totals AS (
			SELECT target, COUNT(*) AS n FROM tree GROUP BY target
		)
		SELECT targets.id, COALESCE(totals.n, 0)
		FROM targets
		LEFT JOIN totals ON totals.target = targets.id;
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
			total int
		)
		if err := rows.Scan(&id, &total); err != nil {
			return nil, fmt.Errorf("scan descendant count: %w", err)
		}
		counts[id] = total
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("descendant count rows: %w", err)
	}
	return counts, nil
}

// ApplyReplyCounts writes absolute reply counts in a single statement.
//
// Storing absolute totals rather than a delta is what makes this safe under JetStream's
// at-least-once delivery: replaying a batch recomputes the same number instead of
// incrementing twice. The monotonic guard (reply_count < new) additionally prevents a
// delayed, older event from moving an already-larger count backwards.
//
// Ids are sorted before binding so row locks are always acquired in a deterministic
// order across concurrent counter workers, which is what prevents deadlocks.
func (r *InteractionRepository) ApplyReplyCounts(ctx context.Context, counts map[uuid.UUID]int) error {
	if len(counts) == 0 {
		return nil
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

	query := `
		UPDATE interactions AS i
		SET reply_count = c.n
		FROM unnest($1::uuid[], $2::bigint[]) AS c(id, n)
		WHERE i.id = c.id AND i.reply_count < c.n;
	`

	if _, err := r.pool.Exec(ctx, query, sortedIDs, totals); err != nil {
		return fmt.Errorf("apply reply counts: %w", err)
	}
	return nil
}

// Update updates an interaction body and title with optimistic concurrency version control.
func (r *InteractionRepository) Update(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*Interaction, error) {
	// COALESCE preserves the existing title when the client omits it, so a
	// body-only edit can never silently wipe a root post's title.
	query := `
		UPDATE interactions
		SET title = COALESCE($3, title), body = $4, version = version + 1, updated_at = NOW()
		WHERE id = $1 AND version = $2
		RETURNING id, group_id, root_id, parent_id, title, body, author, created_at, updated_at, reply_count, depth, version;
	`
	row := r.pool.QueryRow(ctx, query, id, expectedVersion, title, body)

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
			return r.checkUpdateConflict(ctx, id)
		}
		return nil, fmt.Errorf("update interaction: %w", err)
	}

	return &item, nil
}

func (r *InteractionRepository) checkUpdateConflict(ctx context.Context, id uuid.UUID) (*Interaction, error) {
	var currentVersion int
	checkErr := r.pool.QueryRow(ctx, "SELECT version FROM interactions WHERE id = $1;", id).Scan(&currentVersion)
	if checkErr != nil {
		if errors.Is(checkErr, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("check interaction version: %w", checkErr)
	}
	return nil, ErrVersionConflict
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
