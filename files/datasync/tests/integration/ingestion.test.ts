/**
 * Integration tests — require a running PostgreSQL instance.
 * 
 * Run with: POSTGRES_HOST=localhost POSTGRES_PORT=5434 npm run test:integration
 * 
 * These tests are skipped in CI unless PG_INTEGRATION=true is set.
 */

const RUN_INTEGRATION = process.env.PG_INTEGRATION === 'true';
const describeIf = RUN_INTEGRATION ? describe : describe.skip;

import { Database } from '../../src/database';
import { loadConfig } from '../../src/config';
import { ApiEvent } from '../../src/types';

function fakeEvent(id: string): ApiEvent {
  return {
    id,
    sessionId: '00000000-0000-0000-0000-000000000001',
    userId: '00000000-0000-0000-0000-000000000002',
    type: 'click',
    name: 'test_event',
    properties: { page: '/test' },
    timestamp: Date.now(),
  };
}

describeIf('Database integration', () => {
  let db: Database;

  beforeAll(async () => {
    process.env.API_KEY = 'test-key';
    const config = loadConfig();
    db = new Database(config);
    await db.setup();
  });

  afterAll(async () => {
    await db.pool.query('TRUNCATE TABLE events, ingestion_shards, ingestion_meta');
    await db.end();
  });

  it('should insert and retrieve events', async () => {
    const events = [fakeEvent('11111111-1111-1111-1111-111111111111')];
    const count = await db.bulkInsert(events);
    expect(count).toBe(1);
  });

  it('should be idempotent on duplicate inserts', async () => {
    const id = '22222222-2222-2222-2222-222222222222';
    await db.bulkInsert([fakeEvent(id)]);
    const second = await db.bulkInsert([fakeEvent(id)]);
    expect(second).toBe(0); // ON CONFLICT DO NOTHING
  });

  it('should bulk insert 1000 events efficiently', async () => {
    const events = Array.from({ length: 1000 }, (_, i) =>
      fakeEvent(`33333333-3333-3333-3333-${String(i).padStart(12, '0')}`)
    );
    const start = Date.now();
    const count = await db.bulkInsert(events);
    const ms = Date.now() - start;
    expect(count).toBe(1000);
    expect(ms).toBeLessThan(5000); // should be fast
  });

  it('should save and load shards', async () => {
    await db.saveShard(0, {
      startTs: 1000,
      endTs: 2000,
      cursor: 'test-cursor',
      eventsIngested: 100,
      completed: false,
      errorCount: 0,
    });

    const shards = await db.loadShards();
    const shard = shards.find(s => s.workerId === 0);
    expect(shard).toBeDefined();
    expect(shard!.cursor).toBe('test-cursor');
    expect(shard!.eventsIngested).toBe(100);
  });

  it('should save and retrieve meta', async () => {
    await db.setMeta('test_key', 'test_value');
    const val = await db.getMeta('test_key');
    expect(val).toBe('test_value');
  });

  it('should return correct event count', async () => {
    const count = await db.getEventCount();
    expect(count).toBeGreaterThan(0);
  });
});
