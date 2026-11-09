-- Remote owns initialization. Applications only open this prepared database.
CREATE TABLE IF NOT EXISTS machine_stats (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    cpu_percent REAL NOT NULL,
    ram_used_mb REAL NOT NULL,
    ram_total_mb REAL NOT NULL,
    disk_used_gb REAL NOT NULL,
    disk_total_gb REAL NOT NULL,
    collected_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS error_logs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    app_name TEXT NOT NULL,
    environment TEXT NOT NULL,
    level TEXT NOT NULL CHECK(level IN ('INFO', 'WARN', 'ERROR', 'CRITICAL', 'PANIC')),
    message TEXT NOT NULL,
    stack_trace TEXT,
    metadata TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    record_type TEXT NOT NULL CHECK(record_type IN ('machine_stat', 'error_log')),
    record_id INTEGER NOT NULL,
    queued_at TEXT NOT NULL,
    sent_at TEXT,
    -- NULL means normal delivery; a reason quarantines the existing payload.
    quarantine_reason TEXT CHECK (
        quarantine_reason IS NULL OR
        (sent_at IS NULL AND length(quarantine_reason) BETWEEN 1 AND 512)
    ),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_attempt_at TEXT,
    UNIQUE(record_type, record_id)
);

CREATE INDEX IF NOT EXISTS idx_delivery_status_pending
    ON delivery_status(record_type, queued_at, id)
    WHERE sent_at IS NULL AND quarantine_reason IS NULL;

CREATE INDEX IF NOT EXISTS idx_delivery_status_sent
    ON delivery_status(sent_at, id) WHERE sent_at IS NOT NULL;
