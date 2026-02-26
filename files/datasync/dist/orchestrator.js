"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Orchestrator = void 0;
const apiClient_1 = require("./apiClient");
const worker_1 = require("./worker");
const cursor_1 = require("./cursor");
const rateLimiter_1 = require("./rateLimiter");
const progress_1 = require("./progress");
const logger_1 = require("./logger");
const TOTAL_EVENTS = 3000000;
class Orchestrator {
    constructor(config, db) {
        this.workers = [];
        this.config = config;
        this.db = db;
        // Single shared rate limiter across all workers
        this.rateLimiter = new rateLimiter_1.RateLimiter(config.rateLimitPerMinute);
    }
    async run() {
        const startMs = Date.now();
        // ── Determine shards ────────────────────────────────────────────────────
        const existingShards = await this.db.loadShards();
        let shards;
        if (existingShards.length > 0) {
            const incomplete = existingShards.filter(s => !s.completed);
            if (incomplete.length === 0) {
                logger_1.logger.info('All shards already completed — verifying count...');
                const count = await this.db.getEventCount();
                logger_1.logger.info({ count }, 'DB event count');
                if (count >= TOTAL_EVENTS) {
                    logger_1.logger.info('Already complete, skipping ingestion');
                    return;
                }
                logger_1.logger.warn({ count }, 'DB count below target, restarting incomplete shards');
            }
            logger_1.logger.info({ shards: existingShards.length, incomplete: incomplete.length }, 'Resuming from saved state');
            shards = existingShards
                .filter(s => !s.completed)
                .map(s => ({
                workerId: s.workerId,
                startTs: s.startTs,
                endTs: s.endTs,
                cursor: s.cursor,
                eventsIngested: s.eventsIngested,
                completed: s.completed,
                lastActivity: Date.now(),
                errorCount: s.errorCount,
            }));
        }
        else {
            shards = await this.buildShards();
        }
        if (shards.length === 0) {
            logger_1.logger.info('No shards to process');
            return;
        }
        // ── Boot workers ─────────────────────────────────────────────────────────
        this.progress = new progress_1.ProgressTracker(TOTAL_EVENTS);
        const workerInstances = shards.map(shard => {
            const client = new apiClient_1.ApiClient(this.config, this.rateLimiter);
            return new worker_1.Worker(shard, client, this.db, this.config.pageLimit);
        });
        this.workers = workerInstances;
        this.progress.start(() => workerInstances.map(w => w.getStats()));
        // ── Health check loop ────────────────────────────────────────────────────
        const healthInterval = setInterval(() => this.healthCheck(workerInstances), 30000);
        try {
            // Run all workers concurrently
            await Promise.all(workerInstances.map(w => w.run()));
        }
        finally {
            clearInterval(healthInterval);
            this.progress.stop();
        }
        // ── Final verification ───────────────────────────────────────────────────
        const count = await this.db.getEventCount();
        const elapsedMs = Date.now() - startMs;
        this.progress.printFinal(count, elapsedMs);
        if (count < TOTAL_EVENTS) {
            logger_1.logger.warn({ count, target: TOTAL_EVENTS }, '⚠️  Count below target — some events may be missing');
        }
        else {
            logger_1.logger.info({ count }, '🎉 All events ingested successfully');
        }
    }
    /**
     * Build shards based on workerCount.
     *
     * Strategy A — Single worker (workerCount=1): simple cursor pagination.
     * Strategy B — Multi-worker: probe event time range first, then shard by time.
     */
    async buildShards() {
        const workerCount = this.config.workerCount;
        if (workerCount === 1) {
            logger_1.logger.info('Single-worker mode — sequential pagination');
            const shard = {
                workerId: 0,
                startTs: 0,
                endTs: 0, // 0 = no time boundary
                cursor: null,
                eventsIngested: 0,
                completed: false,
                lastActivity: Date.now(),
                errorCount: 0,
            };
            await this.db.saveShard(0, shard);
            return [shard];
        }
        // Multi-worker: probe time range
        logger_1.logger.info({ workerCount }, 'Multi-worker mode — discovering event time range');
        const { minTs, maxTs, expMs } = await this.probeTimeRange();
        logger_1.logger.info({
            minTs: new Date(minTs).toISOString(),
            maxTs: new Date(maxTs).toISOString(),
        }, 'Event time range discovered');
        const shardDefs = (0, cursor_1.buildShardCursors)(minTs, maxTs, workerCount, expMs);
        const shards = shardDefs.map((def, i) => ({
            workerId: i,
            startTs: def.startTs,
            endTs: def.endTs,
            cursor: def.startCursor,
            eventsIngested: 0,
            completed: false,
            lastActivity: Date.now(),
            errorCount: 0,
        }));
        for (const shard of shards) {
            await this.db.saveShard(shard.workerId, shard);
        }
        return shards;
    }
    /**
     * Make 2 API calls to find the oldest and newest event timestamps.
     * - First page (no cursor) = newest events
     * - Forged cursor at ts=0 = oldest events
     */
    async probeTimeRange() {
        const client = new apiClient_1.ApiClient(this.config, this.rateLimiter);
        // Newest events
        const newest = await client.fetchEvents({ cursor: null, limit: 100 });
        const newestTs = Math.max(...newest.data
            .map(e => typeof e.timestamp === 'number' ? e.timestamp : new Date(e.timestamp).getTime()));
        // Oldest events — forge cursor at ts=0
        const forgedCursor = (0, cursor_1.forgeCursorAt)(0, Date.now() + 3 * 60 * 60 * 1000);
        const oldest = await client.fetchEvents({ cursor: forgedCursor, limit: 100 }).catch(() => null);
        let minTs;
        if (oldest && oldest.data.length > 0) {
            minTs = Math.min(...oldest.data
                .map(e => typeof e.timestamp === 'number' ? e.timestamp : new Date(e.timestamp).getTime()));
            logger_1.logger.info('Cursor forging confirmed — parallel sharding active');
        }
        else {
            // Forging not supported — fall back to single worker
            logger_1.logger.warn('Cursor forging not supported by API — falling back to single worker');
            this.config.workerCount = 1;
            return { minTs: 0, maxTs: newestTs, expMs: Date.now() + 3 * 60 * 60 * 1000 };
        }
        return {
            minTs,
            maxTs: newestTs + 1,
            expMs: Date.now() + 3 * 60 * 60 * 1000,
        };
    }
    healthCheck(workers) {
        const now = Date.now();
        for (const worker of workers) {
            const stats = worker.getStats();
            const staleSecs = (now - stats.lastActivity) / 1000;
            if (stats.status === 'running' && staleSecs > 120) {
                logger_1.logger.warn({ workerId: stats.workerId, staleSecs }, '⚠️  Worker appears stalled');
            }
            logger_1.logger.debug({
                workerId: stats.workerId,
                status: stats.status,
                ingested: stats.eventsIngested,
                rate: stats.eventsPerMinute,
                errors: stats.errorCount,
            }, 'Health check');
        }
    }
}
exports.Orchestrator = Orchestrator;
//# sourceMappingURL=orchestrator.js.map