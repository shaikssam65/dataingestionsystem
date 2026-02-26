import { ApiClient, CursorExpiredError } from './apiClient';
import { Database } from './database';
import { WorkerShard, WorkerStats } from './types';
import { decodeCursor, forgeCursorAt } from './cursor';
import { sleep } from './rateLimiter';
import { workerLogger } from './logger';

export class Worker {
  private shard: WorkerShard;
  private readonly client: ApiClient;
  private readonly db: Database;
  private readonly pageLimit: number;
  private stats: WorkerStats;
  private requestCount = 0;
  private startTime = Date.now();
  private log: ReturnType<typeof workerLogger>;
  private abortSignal: { aborted: boolean } = { aborted: false };

  constructor(
    shard: WorkerShard,
    client: ApiClient,
    db: Database,
    pageLimit: number,
  ) {
    this.shard = { ...shard };
    this.client = client;
    this.db = db;
    this.pageLimit = pageLimit;
    this.log = workerLogger(shard.workerId);
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

  abort(): void {
    this.abortSignal.aborted = true;
  }

  getStats(): WorkerStats {
    const elapsedMin = (Date.now() - this.startTime) / 60_000;
    this.stats.eventsPerMinute = elapsedMin > 0
      ? Math.round(this.stats.eventsIngested / elapsedMin)
      : 0;
    return { ...this.stats };
  }

  async run(): Promise<void> {
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
    } catch (err) {
      this.stats.status = 'failed';
      this.log.error({ err }, 'Worker failed');
      throw err;
    }
  }

  private async paginate(): Promise<void> {
    let cursor = this.shard.cursor;

    while (!this.abortSignal.aborted) {
      let page;
      try {
        page = await this.client.fetchEvents({ cursor, limit: this.pageLimit });
      } catch (err) {
        if (err instanceof CursorExpiredError) {
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

      const elapsedMin = (Date.now() - this.startTime) / 60_000;
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

  private filterToShard(events: import('./types').ApiEvent[]): import('./types').ApiEvent[] {
    if (this.shard.startTs === 0 && this.shard.endTs === 0) return events; // Single worker, no filter

    return events.filter(e => {
      const ts = typeof e.timestamp === 'number'
        ? e.timestamp
        : new Date(e.timestamp as string).getTime();
      // Keep events within [startTs, endTs)
      return ts >= this.shard.startTs && ts < this.shard.endTs;
    });
  }

  private shouldStop(page: import('./types').ApiResponse): boolean {
    if (this.shard.startTs === 0 && this.shard.endTs === 0) return false;

    // API returns events DESC (newest first). Stop when we've gone past our startTs.
    const lastEvent = page.data[page.data.length - 1];
    if (!lastEvent) return false;

    const lastTs = typeof lastEvent.timestamp === 'number'
      ? lastEvent.timestamp
      : new Date(lastEvent.timestamp as string).getTime();

    return lastTs < this.shard.startTs;
  }

  private rebuildCursor(): string | null {
    // We lost our cursor — reforge one slightly before our last known position
    // by using the shard's endTs as the restart point
    const ts = this.shard.endTs || Date.now();
    return forgeCursorAt(ts, Date.now() + 3 * 60 * 60 * 1000);
  }

  private async persistShard(): Promise<void> {
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
