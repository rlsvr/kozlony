// Package database provides PostgreSQL persistence and connection management.
package database

import (
	"context"
	"errors"
	"fmt"
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

// Connect parses the database URL, configures connection pool parameters, and validates connectivity.
func Connect(ctx context.Context, databaseURL string) (*pgxpool.Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("parse database url: %w", err)
	}

	poolCfg.MaxConns = 50
	poolCfg.MinConns = 5
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
	Insert(ctx context.Context, item *Interaction) error
	InsertBatch(ctx context.Context, items []*Interaction) error
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

// InsertBatch writes a batch of interactions to PostgreSQL using a single transaction.
func (r *InteractionRepository) InsertBatch(ctx context.Context, items []*Interaction) error {
	if len(items) == 0 {
		return nil
	}

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

	br := r.pool.SendBatch(ctx, batch)
	defer func() {
		_ = br.Close()
	}()

	for range items {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("execute batch item: %w", err)
		}
	}

	return nil
}

// Update updates an interaction body and title with optimistic concurrency version control.
func (r *InteractionRepository) Update(ctx context.Context, id uuid.UUID, expectedVersion int, title *string, body string) (*Interaction, error) {
	query := `
		UPDATE interactions
		SET title = $3, body = $4, version = version + 1, updated_at = NOW()
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
