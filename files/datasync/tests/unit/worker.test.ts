import { Worker } from '../../src/worker';
import { WorkerShard, ApiEvent, ApiResponse } from '../../src/types';

// Minimal mock helpers
function makeEvent(id: string, ts: number): ApiEvent {
  return {
    id,
    sessionId: 'sess-1',
    userId: 'user-1',
    type: 'click',
    name: 'test',
    properties: {},
    timestamp: ts,
  };
}

function makeApiResponse(events: ApiEvent[], hasMore: boolean, nextCursor: string | null = null): ApiResponse {
  return {
    data: events,
    pagination: { limit: 100, hasMore, nextCursor, cursorExpiresIn: 100 },
    meta: { total: 3_000_000, returned: events.length, requestId: 'req-1' },
  };
}

function makeShard(overrides: Partial<WorkerShard> = {}): WorkerShard {
  return {
    workerId: 0,
    startTs: 0,
    endTs: 0,
    cursor: null,
    eventsIngested: 0,
    completed: false,
    lastActivity: Date.now(),
    errorCount: 0,
    ...overrides,
  };
}

describe('Worker', () => {
  let mockClient: { fetchEvents: jest.Mock };
  let mockDb: { bulkInsert: jest.Mock; saveShard: jest.Mock };

  beforeEach(() => {
    mockClient = { fetchEvents: jest.fn() };
    mockDb = {
      bulkInsert: jest.fn().mockResolvedValue(5),
      saveShard: jest.fn().mockResolvedValue(undefined),
    };
  });

  it('should complete when hasMore is false', async () => {
    const events = [makeEvent('evt-1', 1000)];
    mockClient.fetchEvents.mockResolvedValueOnce(makeApiResponse(events, false));

    const worker = new Worker(
      makeShard(),
      mockClient as any,
      mockDb as any,
      100,
    );

    await worker.run();
    const stats = worker.getStats();
    expect(stats.status).toBe('completed');
    expect(mockClient.fetchEvents).toHaveBeenCalledTimes(1);
  });

  it('should paginate multiple pages', async () => {
    mockClient.fetchEvents
      .mockResolvedValueOnce(makeApiResponse([makeEvent('e1', 1000)], true, 'cursor-2'))
      .mockResolvedValueOnce(makeApiResponse([makeEvent('e2', 900)], true, 'cursor-3'))
      .mockResolvedValueOnce(makeApiResponse([makeEvent('e3', 800)], false));

    const worker = new Worker(makeShard(), mockClient as any, mockDb as any, 100);
    await worker.run();

    expect(mockClient.fetchEvents).toHaveBeenCalledTimes(3);
    expect(mockDb.bulkInsert).toHaveBeenCalledTimes(3);
  });

  it('should stop at shard boundary (time range)', async () => {
    const shard = makeShard({ startTs: 500, endTs: 1000 });

    // First page: event in range
    const p1 = makeApiResponse([makeEvent('e1', 800)], true, 'cursor-2');
    // Second page: event below startTs — should stop
    const p2 = makeApiResponse([makeEvent('e2', 300)], true, 'cursor-3');

    mockClient.fetchEvents
      .mockResolvedValueOnce(p1)
      .mockResolvedValueOnce(p2);

    const worker = new Worker(shard, mockClient as any, mockDb as any, 100);
    await worker.run();

    expect(mockClient.fetchEvents).toHaveBeenCalledTimes(2);
  });

  it('should track error count', async () => {
    const { CursorExpiredError } = require('../../src/apiClient');
    mockClient.fetchEvents
      .mockRejectedValueOnce(new CursorExpiredError('expired'))
      .mockResolvedValueOnce(makeApiResponse([makeEvent('e1', 1000)], false));

    const worker = new Worker(makeShard(), mockClient as any, mockDb as any, 100);
    await worker.run();

    const stats = worker.getStats();
    expect(stats.errorCount).toBeGreaterThan(0);
    expect(stats.status).toBe('completed');
  });

  it('should be abortable', async () => {
    mockClient.fetchEvents.mockImplementation(() =>
      new Promise(resolve => setTimeout(resolve, 10_000))
    );

    const worker = new Worker(makeShard(), mockClient as any, mockDb as any, 100);
    const runPromise = worker.run();
    worker.abort();

    // Worker should eventually resolve (abort flag checked between pages)
    // Since the first fetch is pending, it will still complete that one
    mockClient.fetchEvents.mockResolvedValue(makeApiResponse([], false));
    await expect(runPromise).resolves.toBeUndefined();
  });
});
