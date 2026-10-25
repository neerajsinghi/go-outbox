// Package dialect describes the small set of SQL differences go-outbox needs
// to care about across PostgreSQL, MySQL and SQLite: placeholder syntax, row
// locking (FOR UPDATE SKIP LOCKED), "insert if absent" statements and DDL.
//
// All queries inside go-outbox are written with '?' placeholders and passed
// through [Dialect.Rebind] before execution.
package dialect

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Dialect captures the database-specific SQL used by go-outbox. Use one of
// the predefined values [Postgres], [MySQL] or [SQLite].
type Dialect struct {
	name       string
	dollar     bool // $1, $2 placeholders instead of ?
	skipLocked bool // supports SELECT ... FOR UPDATE SKIP LOCKED
}

var (
	// Postgres targets PostgreSQL 9.5+ (SKIP LOCKED, ON CONFLICT).
	// Works with lib/pq, pgx/stdlib and compatible drivers.
	Postgres = Dialect{name: "postgres", dollar: true, skipLocked: true}

	// MySQL targets MySQL 8.0+ (SKIP LOCKED). Use the go-sql-driver/mysql
	// driver.
	MySQL = Dialect{name: "mysql", skipLocked: true}

	// SQLite targets SQLite 3.24+. SQLite has a single writer, so rows are
	// claimed with a single atomic UPDATE instead of SKIP LOCKED.
	SQLite = Dialect{name: "sqlite"}
)

// Name returns the dialect name ("postgres", "mysql" or "sqlite").
func (d Dialect) Name() string { return d.name }

// SupportsSkipLocked reports whether the database supports
// SELECT ... FOR UPDATE SKIP LOCKED.
func (d Dialect) SupportsSkipLocked() bool { return d.skipLocked }

// Rebind converts a query written with '?' placeholders into the dialect's
// native placeholder syntax. Queries must not contain literal '?' characters.
func (d Dialect) Rebind(query string) string {
	if !d.dollar {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	for i := 0; i < len(query); i++ {
		if query[i] == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteByte(query[i])
	}
	return b.String()
}

// Placeholders returns n comma-separated '?' placeholders, suitable for an
// IN (...) list. Pass the result through Rebind with the rest of the query.
func Placeholders(n int) string {
	if n <= 0 {
		return ""
	}
	return strings.TrimSuffix(strings.Repeat("?,", n), ",")
}

// InsertIgnore returns an INSERT statement (with '?' placeholders) that
// silently does nothing when a row with the same primary/unique key exists.
// Callers detect the conflict via RowsAffected() == 0.
func (d Dialect) InsertIgnore(table string, cols []string) string {
	vals := Placeholders(len(cols))
	list := strings.Join(cols, ", ")
	if d.name == MySQL.name {
		return fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES (%s)", table, list, vals)
	}
	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) ON CONFLICT DO NOTHING", table, list, vals)
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// ValidIdentifier reports whether name is a safe, unquoted SQL identifier
// (optionally schema-qualified, e.g. "public.outbox"). Table names are
// interpolated into SQL, so go-outbox rejects anything else.
func ValidIdentifier(name string) bool { return identRe.MatchString(name) }

// indexName derives an index name from a possibly schema-qualified table.
func indexName(table, suffix string) string {
	return strings.ReplaceAll(table, ".", "_") + "_" + suffix
}

// OutboxSchema returns the DDL statements that create the outbox table and
// its index. Timestamps are stored as Unix milliseconds (BIGINT) so that the
// same queries work unchanged on every dialect.
func (d Dialect) OutboxSchema(table string) []string {
	switch d.name {
	case Postgres.name:
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	seq             BIGSERIAL PRIMARY KEY,
	id              VARCHAR(64)  NOT NULL UNIQUE,
	topic           VARCHAR(255) NOT NULL,
	msg_key         VARCHAR(255) NOT NULL DEFAULT '',
	payload         BYTEA        NOT NULL,
	headers         TEXT         NOT NULL DEFAULT '{}',
	status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
	attempts        INTEGER      NOT NULL DEFAULT 0,
	next_attempt_at BIGINT       NOT NULL,
	locked_by       VARCHAR(64),
	locked_until    BIGINT,
	last_error      TEXT,
	created_at      BIGINT       NOT NULL,
	sent_at         BIGINT
)`, table),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (status, next_attempt_at, seq)`,
				indexName(table, "pending_idx"), table),
		}
	case MySQL.name:
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	seq             BIGINT       NOT NULL AUTO_INCREMENT PRIMARY KEY,
	id              VARCHAR(64)  NOT NULL UNIQUE,
	topic           VARCHAR(255) NOT NULL,
	msg_key         VARCHAR(255) NOT NULL DEFAULT '',
	payload         LONGBLOB     NOT NULL,
	headers         TEXT         NOT NULL,
	status          VARCHAR(16)  NOT NULL DEFAULT 'pending',
	attempts        INT          NOT NULL DEFAULT 0,
	next_attempt_at BIGINT       NOT NULL,
	locked_by       VARCHAR(64),
	locked_until    BIGINT,
	last_error      TEXT,
	created_at      BIGINT       NOT NULL,
	sent_at         BIGINT,
	INDEX %s (status, next_attempt_at, seq)
)`, table, indexName(table, "pending_idx")),
		}
	default:
		return []string{
			fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	seq             INTEGER PRIMARY KEY AUTOINCREMENT,
	id              TEXT    NOT NULL UNIQUE,
	topic           TEXT    NOT NULL,
	msg_key         TEXT    NOT NULL DEFAULT '',
	payload         BLOB    NOT NULL,
	headers         TEXT    NOT NULL DEFAULT '{}',
	status          TEXT    NOT NULL DEFAULT 'pending',
	attempts        INTEGER NOT NULL DEFAULT 0,
	next_attempt_at INTEGER NOT NULL,
	locked_by       TEXT,
	locked_until    INTEGER,
	last_error      TEXT,
	created_at      INTEGER NOT NULL,
	sent_at         INTEGER
)`, table),
			fmt.Sprintf(`CREATE INDEX IF NOT EXISTS %s ON %s (status, next_attempt_at, seq)`,
				indexName(table, "pending_idx"), table),
		}
	}
}

// IdempotencySchema returns the DDL statements that create the idempotency
// key table used by the SQL idempotency store.
func (d Dialect) IdempotencySchema(table string) []string {
	blob, text, bigint := "BLOB", "TEXT", "INTEGER"
	switch d.name {
	case Postgres.name:
		blob, bigint = "BYTEA", "BIGINT"
	case MySQL.name:
		blob, bigint = "LONGBLOB", "BIGINT"
	}
	return []string{
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	idem_key      VARCHAR(255) NOT NULL PRIMARY KEY,
	fingerprint   VARCHAR(64)  NOT NULL,
	completed     SMALLINT     NOT NULL DEFAULT 0,
	status_code   INTEGER      NOT NULL DEFAULT 0,
	headers       %s,
	body          %s,
	created_at    %s NOT NULL,
	expires_at    %s NOT NULL
)`, table, text, blob, bigint, bigint),
	}
}
