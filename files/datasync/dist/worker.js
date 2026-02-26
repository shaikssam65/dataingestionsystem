"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.Worker = void 0;
const apiClient_1 = require("./apiClient");
const cursor_1 = require("./cursor");
const logger_1 = require("./logger");
class Worker {
    constructor(shard, client, db, pageLimit) {
        this.requestCount = 0;
        this.startTime = Date.now();
        this.abortSignal = { aborted: false };
        this.shard = { ...shard };
        this.client = client;
        this.db = db;
        this.pageLimit = pageLimit;
        this.log = (0, logger_1.workerLogger)(shard.workerId);
        this.stats = {
            workerId: shard.workerId,
            eventsIngested: shard.eventsIngested,
            requestCount: 0,
            errorCount: shard.errorCount,
            status: 'idle',
            lastActivity: Date.now(),
            eventsPerMinute: 0,
        };
    }
    abort() {
        this.abortSignal.aborted = true;
    }
    getStats() {
        const elapsedMin = (Date.now() - this.startTime) / 60000;
        this.stats.eventsPerMinute = elapsedMin > 0
            ? Math.round(this.stats.eventsIngested / elapsedMin)
            : 0;
        return { ...this.stats };
    }
    async run() {
        this.stats.status = 'running';
        this.startTime = Date.now();
        this.log.info({
            startTs: new Date(this.shard.startTs).toISOString(),
            endTs: new Date(this.shard.endTs).toISOString(),
            cursor: this.shard.cursor ? '(resuming)' : '(fresh)',
            alreadyIngested: this.shard.eventsIngested,
        }, 'Worker starting');
        try {
            await this.paginate();
            this.shard.completed = true;
            this.stats.status = 'completed';
            await this.persistShard();
            this.log.info({ total: this.stats.eventsIngested }, 'Worker completed ✓');
        }
        catch (err) {
            this.stats.status = 'failed';
            this.log.error({ err }, 'Worker failed');
            throw err;
        }
    }
    async paginate() {
        let cursor = this.shard.cursor;
        while (!this.abortSignal.aborted) {
            let page;
            try {
                page = await this.client.fetchEvents({ cursor, limit: this.pageLimit });
            }
            catch (err) {
                if (err instanceof apiClient_1.CursorExpiredError) {
                    this.log.warn('Cursor expired — reforging from last known position');
                    cursor = this.rebuildCursor();
                    this.shard.errorCount++;
                    continue;
                }
                this.shard.errorCount++;
                this.stats.errorCount++;
                throw err;
            }
            this.requestCount++;
            this.stats.requestCount++;
            this.stats.lastActivity = Date.now();
            // Filter events outside this shard's time range
            const filtered = this.filterToShard(page.data);
            if (filtered.length > 0) {
                const inserted = await this.db.bulkInsert(filtered);
                this.shard.eventsIngested += inserted;
                this.stats.eventsIngested += inserted;
            }
            // Save checkpoint after every page
            this.shard.cursor = page.pagination.nextCursor;
            await this.persistShard();
            const elapsedMin = (Date.now() - this.startTime) / 60000;
            const rate = elapsedMin > 0 ? Math.round(this.stats.eventsIngested / elapsedMin) : 0;
            this.log.info({
                page: this.requestCount,
                inserted: filtered.length,
                total: this.shard.eventsIngested,
                rate: `${rate}/min`,
                hasMore: page.pagination.hasMore,
                cursorExpiresIn: page.pagination.cursorExpiresIn,
            }, 'Page ingested');
            // Check if we've crossed into another shard's territory
            if (this.shouldStop(page)) {
                this.log.info('Shard boundary reached — stopping');
                break;
            }
            if (!page.pagination.hasMore || !page.pagination.nextCursor) {
                this.log.info('No more pages');
                break;
            }
            cursor = page.pagination.nextCursor;
            // Warn if cursor is about to expire
            if (page.pagination.cursorExpiresIn !== null && page.pagination.cursorExpiresIn < 15) {
                this.log.warn({ expiresIn: page.pagination.cursorExpiresIn }, 'Cursor expiring soon!');
            }
        }
    }
    filterToShard(events) {
        if (this.shard.startTs === 0 && this.shard.endTs === 0)
            return events; // Single worker, no filter
        return events.filter(e => {
            const ts = typeof e.timestamp === 'number'
                ? e.timestamp
                : new Date(e.timestamp).getTime();
            // Keep events within [startTs, endTs)
            return ts >= this.shard.startTs && ts < this.shard.endTs;
        });
    }
    shouldStop(page) {
        if (this.shard.startTs === 0 && this.shard.endTs === 0)
            return false;
        // API returns events DESC (newest first). Stop when we've gone past our startTs.
        const lastEvent = page.data[page.data.length - 1];
        if (!lastEvent)
            return false;
        const lastTs = typeof lastEvent.timestamp === 'number'
            ? lastEvent.timestamp
            : new Date(lastEvent.timestamp).getTime();
        return lastTs < this.shard.startTs;
    }
    rebuildCursor() {
        // We lost our cursor — reforge one slightly before our last known position
        // by using the shard's endTs as the restart point
        const ts = this.shard.endTs || Date.now();
        return (0, cursor_1.forgeCursorAt)(ts, Date.now() + 3 * 60 * 60 * 1000);
    }
    async persistShard() {
        await this.db.saveShard(this.shard.workerId, {
            startTs: this.shard.startTs,
            endTs: this.shard.endTs,
            cursor: this.shard.cursor ?? null,
            eventsIngested: this.shard.eventsIngested,
            completed: this.shard.completed,
            errorCount: this.shard.errorCount,
        });
    }
}
exports.Worker = Worker;
//# sourceMappingURL=worker.js.map