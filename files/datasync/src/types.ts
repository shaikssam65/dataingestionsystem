// ─── API Types ───────────────────────────────────────────────────────────────

export interface ApiEvent {
  id: string;
  sessionId: string;
  userId: string;
  type: string;
  name: string;
  properties: Record<string, unknown>;
  timestamp: number | string;
  session?: {
    id: string;
    deviceType?: string;
    browser?: string;
    os?: string;
  };
}

export interface ApiPagination {
  limit: number;
  hasMore: boolean;
  nextCursor: string | null;
  cursorExpiresIn: number | null;
}

export interface ApiMeta {
  total: number;
  returned: number;
  requestId: string;
}

export interface ApiResponse {
  data: ApiEvent[];
  pagination: ApiPagination;
  meta: ApiMeta;
}

// ─── Cursor Types ────────────────────────────────────────────────────────────

export interface CursorPayload {
  id: string;
  ts: number;
  v: number;
  exp: number;
}

// ─── Worker / Shard Types ────────────────────────────────────────────────────

export interface WorkerShard {
  workerId: number;
  startTs: number;      // epoch ms — start of this shard's time range
  endTs: number;        // epoch ms — end of this shard's time range (0 = open)
  cursor: string | null;
  eventsIngested: number;
  completed: boolean;
  lastActivity: number; // epoch ms
  errorCount: number;
}

export interface WorkerStats {
  workerId: number;
  eventsIngested: number;
  requestCount: number;
  errorCount: number;
  status: 'idle' | 'running' | 'completed' | 'failed';
  lastActivity: number;
  eventsPerMinute: number;
}

// ─── Ingestion State ─────────────────────────────────────────────────────────

export interface IngestionState {
  version: number;
  startedAt: number;
  totalTarget: number;
  shards: WorkerShard[];
  workerCount: number;
  eventTimeRangeMs: [number, number]; // [oldest_ts, newest_ts]
}

// ─── Config ──────────────────────────────────────────────────────────────────

export interface Config {
  apiKey: string;
  apiBaseUrl: string;
  pageLimit: number;
  workerCount: number;
  rateLimitPerMinute: number;    // global limit
  dbHost: string;
  dbPort: number;
  dbName: string;
  dbUser: string;
  dbPassword: string;
  dbPoolSize: number;
  stateFile: string;
  githubUrl: string;
}
