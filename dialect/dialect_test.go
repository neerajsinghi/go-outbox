package dialect

import (
	"strings"
	"testing"
)

func TestRebind(t *testing.T) {
	q := "UPDATE t SET a = ? WHERE b = ? AND c IN (?,?)"
	tests := []struct {
		d    Dialect
		want string
	}{
		{Postgres, "UPDATE t SET a = $1 WHERE b = $2 AND c IN ($3,$4)"},
		{MySQL, q},
		{SQLite, q},
	}
	for _, tt := range tests {
		t.Run(tt.d.Name(), func(t *testing.T) {
			if got := tt.d.Rebind(q); got != tt.want {
				t.Fatalf("Rebind = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestPlaceholders(t *testing.T) {
	tests := map[int]string{0: "", 1: "?", 3: "?,?,?"}
	for n, want := range tests {
		if got := Placeholders(n); got != want {
			t.Errorf("Placeholders(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestInsertIgnore(t *testing.T) {
	tests := []struct {
		d    Dialect
		want string
	}{
		{Postgres, "INSERT INTO k (a, b) VALUES (?,?) ON CONFLICT DO NOTHING"},
		{SQLite, "INSERT INTO k (a, b) VALUES (?,?) ON CONFLICT DO NOTHING"},
		{MySQL, "INSERT IGNORE INTO k (a, b) VALUES (?,?)"},
	}
	for _, tt := range tests {
		if got := tt.d.InsertIgnore("k", []string{"a", "b"}); got != tt.want {
			t.Errorf("%s: InsertIgnore = %q, want %q", tt.d.Name(), got, tt.want)
		}
	}
}

func TestSchemas(t *testing.T) {
	tests := []struct {
		d          Dialect
		skipLocked bool
		contains   []string
	}{
		{Postgres, true, []string{"BIGSERIAL", "BYTEA", "CREATE INDEX IF NOT EXISTS app_outbox_pending_idx"}},
		{MySQL, true, []string{"AUTO_INCREMENT", "LONGBLOB", "INDEX app_outbox_pending_idx"}},
		{SQLite, false, []string{"AUTOINCREMENT", "BLOB"}},
	}
	for _, tt := range tests {
		t.Run(tt.d.Name(), func(t *testing.T) {
			if tt.d.SupportsSkipLocked() != tt.skipLocked {
				t.Errorf("SupportsSkipLocked = %v", !tt.skipLocked)
			}
			ddl := strings.Join(tt.d.OutboxSchema("app.outbox"), "\n")
			for _, s := range tt.contains {
				if !strings.Contains(ddl, s) {
					t.Errorf("schema missing %q:\n%s", s, ddl)
				}
			}
			if !strings.Contains(strings.Join(tt.d.IdempotencySchema("idem"), ""), "idem_key") {
				t.Error("idempotency schema missing idem_key")
			}
		})
	}
}

func TestValidIdentifier(t *testing.T) {
	tests := map[string]bool{
		"outbox": true, "public.outbox": true, "_x1": true,
		"": false, "1abc": false, "a.b.c": false, "a-b": false, "a;drop": false, "a b": false,
	}
	for in, want := range tests {
		if got := ValidIdentifier(in); got != want {
			t.Errorf("ValidIdentifier(%q) = %v, want %v", in, got, want)
		}
	}
}
