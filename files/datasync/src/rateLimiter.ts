import { logger } from './logger';

/**
 * Sliding-window rate limiter.
 * 
 * The API enforces 10 req/min globally per API key. All workers share
 * this single instance so they collectively never exceed the limit.
 * 
 * We use 9/min (1 buffer slot) to avoid occasional 429s at the boundary.
 */
export class RateLimiter {
  private timestamps: number[] = [];
  private readonly windowMs: number;
  private readonly maxRequests: number;
  private waiters: Array<() => void> = [];

  constructor(requestsPerMinute: number, bufferSlots = 1) {
    this.windowMs = 60_000;
    this.maxRequests = requestsPerMinute - bufferSlots;
  }

  async acquire(): Promise<void> {
    return new Promise(resolve => {
      this.waiters.push(resolve);
      this.flush();
    });
  }

  private flush(): void {
    if (this.waiters.length === 0) return;

    const now = Date.now();
    this.timestamps = this.timestamps.filter(t => now - t < this.windowMs);

    if (this.timestamps.length < this.maxRequests) {
      this.timestamps.push(now);
      const resolve = this.waiters.shift()!;
      resolve();
      // Immediately try to drain more waiters
      setImmediate(() => this.flush());
    } else {
      // Schedule retry when oldest slot expires
      const oldestExpiry = this.timestamps[0] + this.windowMs - now + 10;
      logger.debug({ waitMs: oldestExpiry, queued: this.waiters.length }, 'Rate limit — waiting');
      setTimeout(() => this.flush(), oldestExpiry);
    }
  }

  /** Force wait after a 429 response */
  async backoffAfter429(retryAfterSeconds = 60): Promise<void> {
    const waitMs = retryAfterSeconds * 1000 + 500;
    logger.warn({ waitMs }, '429 received — backing off');
    // Clear recorded timestamps so we don't pile on after the wait
    this.timestamps = [];
    await sleep(waitMs);
  }

  get queueDepth(): number {
    return this.waiters.length;
  }
}

export function sleep(ms: number): Promise<void> {
  return new Promise(resolve => setTimeout(resolve, ms));
}
