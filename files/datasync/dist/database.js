"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Database = void 0;
const pg_1 = require("pg");
const logger_1 = require("./logger");
class Database {
    constructor(config) {
        this.pool = new pg_1.Pool({
            host: config.dbHost,
            port: config.dbPort,
            database: config.dbName,
            user: config.dbUser,
            password: config.dbPassword,
            max: config.dbPoolSize,
            idleTimeoutMillis: 30000,
            connectionTimeoutMillis: 10000,
        });
        this.pool.on('error', (err) => {
            logger_1.logger.error({ err }, 'Unexpected PostgreSQL pool error');
        });
    }
    async setup() {
        await this.pool.query(`
      CREATE TABLE IF NOT EXISTS events (
        id          UUID        PRIMARY KEY,
        session_id  UUID,
        user_id     UUID,
        type        VARCHAR(128),
        name        VARCHAR(256),
        properties  JSONB       DEFAULT '{}',
        ts          BIGINT,                    -- original unix ms timestamp
        created_at  TIMESTAMPTZ,
        raw         JSONB,
        ingested_at TIMESTAMPTZ DEFAULT NOW()
      )
    `);
        await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ingestion_shards (
        worker_id       INTEGER     PRIMARY KEY,
        start_ts        BIGINT      NOT NULL,
        end_ts          BIGINT      NOT NULL,
        cursor          TEXT,
        events_ingested INTEGER     DEFAULT 0,
        completed       BOOLEAN     DEFAULT FALSE,
        last_activity   TIMESTAMPTZ DEFAULT NOW(),
        error_count     INTEGER     DEFAULT 0
      )
    `);
        await this.pool.query(`
      CREATE TABLE IF NOT EXISTS ingestion_meta (
        key   TEXT PRIMARY KEY,
        value TEXT NOT NULL
      )
    `);
        // Partial indexes for common query patterns
        await this.pool.query(`
      CREATE INDEX IF NOT EXISTS idx_events_session  ON events(session_id);
      CREATE INDEX IF NOT EXISTS idx_events_user     ON events(user_id);
      CREATE INDEX IF NOT EXISTS idx_events_type     ON events(type);
      CREATE INDEX IF NOT EXISTS idx_events_ts       ON events(ts);
    `);
        logger_1.logger.info('Database schema ready');
    }
    /**
     * Bulk insert up to 5000 events in a single query.
     * ON CONFLICT DO NOTHING makes this fully idempotent.
     * Returns the number of rows actually inserted.
     */
    async bulkInsert(events) {
        if (events.length === 0)
            return 0;
        const COLS = 8;
        const values = [];
        const placeholders = [];
        let idx = 1;
        for (const e of events) {
            const tsMs = typeof e.timestamp === 'number'
                ? e.timestamp
                : new Date(e.timestamp).getTime();
            placeholders.push(`($${idx},$${idx + 1},$${idx + 2},$${idx + 3},$${idx + 4},$${idx + 5},$${idx + 6},$${idx + 7})`);
            values.push(e.id, e.sessionId ?? null, e.userId ?? null, e.type ?? null, e.name ?? null, JSON.stringify(e.properties ?? {}), tsMs, JSON.stringify(e));
            idx += COLS;
        }
        const sql = `
      INSERT INTO events (id, session_id, user_id, type, name, properties, ts, raw)
      VALUES ${placeholders.join(',')}
      ON CONFLICT (id) DO NOTHING
    `;
        const result = await this.pool.query(sql, values);
        return result.rowCount ?? 0;
    }
    async saveShard(workerId, shard) {
        await this.pool.query(`
      INSERT INTO ingestion_shards
        (worker_id, start_ts, end_ts, cursor, events_ingested, completed, last_activity, error_count)
      VALUES ($1,$2,$3,$4,$5,$6,NOW(),$7)
      ON CONFLICT (worker_id) DO UPDATE SET
        cursor          = EXCLUDED.cursor,
        events_ingested = EXCLUDED.events_ingested,
        completed       = EXCLUDED.completed,
        last_activity   = NOW(),
        error_count     = EXCLUDED.error_count
    `, [
            workerId,
            shard.startTs,
            shard.endTs,
            shard.cursor,
            shard.eventsIngested,
            shard.completed,
            shard.errorCount,
        ]);
    }
    async loadShards() {
        const result = await this.pool.query(`
      SELECT worker_id, start_ts, end_ts, cursor, events_ingested, completed, error_count
      FROM ingestion_shards
      ORDER BY worker_id
    `);
        return result.rows.map(r => ({
            workerId: r.worker_id,
            startTs: parseInt(r.start_ts),
            endTs: parseInt(r.end_ts),
            cursor: r.cursor,
            eventsIngested: r.events_ingested,
            completed: r.completed,
            errorCount: r.error_count,
        }));
    }
    async setMeta(key, value) {
        await this.pool.query(`
      INSERT INTO ingestion_meta (key, value) VALUES ($1, $2)
      ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value
    `, [key, value]);
    }
    async getMeta(key) {
        const result = await this.pool.query('SELECT value FROM ingestion_meta WHERE key = $1', [key]);
        return result.rows[0]?.value ?? null;
    }
    async getEventCount() {
        const result = await this.pool.query('SELECT COUNT(*)::int AS n FROM events');
        return result.rows[0].n;
    }
    async getAllEventIds(batchSize = 100000) {
        logger_1.logger.info('Fetching all event IDs from DB...');
        const result = await this.pool.query('SELECT id FROM events ORDER BY id');
        return result.rows.map((r) => r.id);
    }
    async end() {
        await this.pool.end();
    }
}
exports.Database = Database;
//# sourceMappingURL=database.js.map