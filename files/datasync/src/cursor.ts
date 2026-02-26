import { CursorPayload } from './types';

// Cursor is unsigned base64-encoded JSON: {"id":"...","ts":ms,"v":2,"exp":ms}
// No HMAC/signature — we can forge cursors to create parallel time-range shards.

const CURSOR_VERSION = 2;
const DEFAULT_EXP_OFFSET_MS = 3 * 60 * 60 * 1000; // 3 hours

export function decodeCursor(cursor: string): CursorPayload {
  const padded = cursor + '='.repeat((4 - (cursor.length % 4)) % 4);
  const json = Buffer.from(padded, 'base64').toString('utf-8');
  return JSON.parse(json) as CursorPayload;
}

export function encodeCursor(payload: CursorPayload): string {
  const json = JSON.stringify(payload);
  return Buffer.from(json).toString('base64').replace(/=+$/, '');
}

/**
 * Forge a cursor that starts pagination from a specific timestamp.
 * 
 * The API orders events by timestamp DESC (newest first).
 * To start a shard at timestamp T, we forge: {id: uuid_max, ts: T, v:2, exp:...}
 * 
 * Using the max UUID ensures the id tiebreaker doesn't skip any events at that ts.
 */
export function forgeCursorAt(tsMs: number, expMs?: number): string {
  return encodeCursor({
    id: 'ffffffff-ffff-ffff-ffff-ffffffffffff',
    ts: tsMs,
    v: CURSOR_VERSION,
    exp: expMs ?? Date.now() + DEFAULT_EXP_OFFSET_MS,
  });
}

/**
 * Forge a cursor at the very beginning (ts=0) — fetches the OLDEST events.
 * Use for shard that covers tail end of time range.
 */
export function forgeStartCursor(expMs?: number): string {
  return forgeCursorAt(0, expMs);
}

/**
 * Given a time range [minTs, maxTs] and N workers, return N evenly spaced
 * start cursors. Worker 0 starts at maxTs (newest), worker N-1 starts near minTs.
 * Each worker paginates forward (DESC) until it hits its endTs boundary.
 */
export function buildShardCursors(
  minTs: number,
  maxTs: number,
  workerCount: number,
  expMs: number
): Array<{ startCursor: string; startTs: number; endTs: number }> {
  const range = maxTs - minTs;
  const shardSize = Math.floor(range / workerCount);

  return Array.from({ length: workerCount }, (_, i) => {
    const shardEndTs = maxTs - i * shardSize;
    const shardStartTs = i === workerCount - 1 ? minTs : maxTs - (i + 1) * shardSize;

    return {
      startCursor: forgeCursorAt(shardEndTs, expMs),
      startTs: shardStartTs,
      endTs: shardEndTs,
    };
  });
}

/**
 * Refresh cursor expiry without changing position.
 */
export function refreshCursorExpiry(cursor: string, expMs: number): string {
  const payload = decodeCursor(cursor);
  return encodeCursor({ ...payload, exp: expMs });
}
