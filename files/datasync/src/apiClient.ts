import axios, { AxiosInstance, AxiosError } from 'axios';
import { ApiResponse, Config } from './types';
import { RateLimiter, sleep } from './rateLimiter';
import { logger } from './logger';

const MAX_RETRIES = 6;

export class ApiClient {
  private readonly http: AxiosInstance;
  private readonly rateLimiter: RateLimiter;

  constructor(config: Config, rateLimiter: RateLimiter) {
    this.rateLimiter = rateLimiter;
    this.http = axios.create({
      baseURL: config.apiBaseUrl,
      headers: {
        'X-API-Key': config.apiKey,
        'Accept': 'application/json',
        'Accept-Encoding': 'gzip',        // compress transfer
        'Connection': 'keep-alive',
      },
      timeout: 45_000,
      // Keep sockets alive across requests — reduces per-request overhead
      httpAgent: new (require('http').Agent)({ keepAlive: true, maxSockets: 20 }),
    });
  }

  async fetchEvents(params: {
    cursor?: string | null;
    limit: number;
  }): Promise<ApiResponse> {
    const query: Record<string, unknown> = { limit: params.limit };
    if (params.cursor) query.cursor = params.cursor;

    let attempt = 0;
    while (true) {
      await this.rateLimiter.acquire();

      try {
        const res = await this.http.get<ApiResponse>('/events', { params: query });
        return res.data;
      } catch (err) {
        const axiosErr = err as AxiosError;
        const status = axiosErr.response?.status;

        if (status === 429) {
          const retryAfter = parseInt(
            (axiosErr.response?.headers as Record<string, string>)['retry-after'] ?? '60'
          );
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
          logger.error({ status, attempt, url: '/events' }, 'Max retries exceeded');
          throw err;
        }

        const backoff = Math.min(500 * Math.pow(2, attempt), 30_000);
        logger.warn({ status, attempt, backoff }, 'Request failed — retrying');
        await sleep(backoff);
      }
    }
  }

  async submit(payload: { eventIds: string[]; repositoryUrl: string }): Promise<unknown> {
    await this.rateLimiter.acquire();
    const res = await this.http.post('/submissions', payload, { timeout: 120_000 });
    return res.data;
  }
}

export class CursorExpiredError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'CursorExpiredError';
  }
}
