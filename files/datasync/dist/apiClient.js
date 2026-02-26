"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.CursorExpiredError = exports.ApiClient = void 0;
const axios_1 = __importDefault(require("axios"));
const rateLimiter_1 = require("./rateLimiter");
const logger_1 = require("./logger");
const MAX_RETRIES = 6;
class ApiClient {
    constructor(config, rateLimiter) {
        this.rateLimiter = rateLimiter;
        this.http = axios_1.default.create({
            baseURL: config.apiBaseUrl,
            headers: {
                'X-API-Key': config.apiKey,
                'Accept': 'application/json',
                'Accept-Encoding': 'gzip', // compress transfer
                'Connection': 'keep-alive',
            },
            timeout: 45000,
            // Keep sockets alive across requests — reduces per-request overhead
            httpAgent: new (require('http').Agent)({ keepAlive: true, maxSockets: 20 }),
        });
    }
    async fetchEvents(params) {
        const query = { limit: params.limit };
        if (params.cursor)
            query.cursor = params.cursor;
        let attempt = 0;
        while (true) {
            await this.rateLimiter.acquire();
            try {
                const res = await this.http.get('/events', { params: query });
                return res.data;
            }
            catch (err) {
                const axiosErr = err;
                const status = axiosErr.response?.status;
                if (status === 429) {
                    const retryAfter = parseInt((axiosErr.response?.headers)['retry-after'] ?? '60');
                    await this.rateLimiter.backoffAfter429(retryAfter);
                    attempt = 0; // reset attempts after 429 backoff
                    continue;
                }
                if (status === 410) {
                    // Cursor expired — caller must handle restart
                    throw new CursorExpiredError('Cursor has expired (410 Gone)');
                }
                attempt++;
                if (attempt >= MAX_RETRIES) {
                    logger_1.logger.error({ status, attempt, url: '/events' }, 'Max retries exceeded');
                    throw err;
                }
                const backoff = Math.min(500 * Math.pow(2, attempt), 30000);
                logger_1.logger.warn({ status, attempt, backoff }, 'Request failed — retrying');
                await (0, rateLimiter_1.sleep)(backoff);
            }
        }
    }
    async submit(payload) {
        await this.rateLimiter.acquire();
        const res = await this.http.post('/submissions', payload, { timeout: 120000 });
        return res.data;
    }
}
exports.ApiClient = ApiClient;
class CursorExpiredError extends Error {
    constructor(message) {
        super(message);
        this.name = 'CursorExpiredError';
    }
}
exports.CursorExpiredError = CursorExpiredError;
//# sourceMappingURL=apiClient.js.map