"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.RateLimiter = void 0;
exports.sleep = sleep;
const logger_1 = require("./logger");
/**
 * Sliding-window rate limiter.
 *
 * The API enforces 10 req/min globally per API key. All workers share
 * this single instance so they collectively never exceed the limit.
 *
 * We use 9/min (1 buffer slot) to avoid occasional 429s at the boundary.
 */
class RateLimiter {
    constructor(requestsPerMinute, bufferSlots = 1) {
        this.timestamps = [];
        this.waiters = [];
        this.windowMs = 60000;
        this.maxRequests = requestsPerMinute - bufferSlots;
    }
    async acquire() {
        return new Promise(resolve => {
            this.waiters.push(resolve);
            this.flush();
        });
    }
    flush() {
        if (this.waiters.length === 0)
            return;
        const now = Date.now();
        this.timestamps = this.timestamps.filter(t => now - t < this.windowMs);
        if (this.timestamps.length < this.maxRequests) {
            this.timestamps.push(now);
            const resolve = this.waiters.shift();
            resolve();
            // Immediately try to drain more waiters
            setImmediate(() => this.flush());
        }
        else {
            // Schedule retry when oldest slot expires
            const oldestExpiry = this.timestamps[0] + this.windowMs - now + 10;
            logger_1.logger.debug({ waitMs: oldestExpiry, queued: this.waiters.length }, 'Rate limit — waiting');
            setTimeout(() => this.flush(), oldestExpiry);
        }
    }
    /** Force wait after a 429 response */
    async backoffAfter429(retryAfterSeconds = 60) {
        const waitMs = retryAfterSeconds * 1000 + 500;
        logger_1.logger.warn({ waitMs }, '429 received — backing off');
        // Clear recorded timestamps so we don't pile on after the wait
        this.timestamps = [];
        await sleep(waitMs);
    }
    get queueDepth() {
        return this.waiters.length;
    }
}
exports.RateLimiter = RateLimiter;
function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}
//# sourceMappingURL=rateLimiter.js.map