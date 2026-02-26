import { decodeCursor, encodeCursor, forgeCursorAt, buildShardCursors, refreshCursorExpiry } from '../../src/cursor';

describe('cursor', () => {
  const NOW = 1_700_000_000_000;
  const EXP = NOW + 3 * 60 * 60 * 1000;

  describe('encode / decode roundtrip', () => {
    it('should roundtrip a cursor payload', () => {
      const payload = { id: 'abc-123', ts: NOW, v: 2, exp: EXP };
      const encoded = encodeCursor(payload);
      const decoded = decodeCursor(encoded);
      expect(decoded).toEqual(payload);
    });

    it('should handle real cursor from API', () => {
      const real = 'eyJpZCI6ImFmNWMzM2M4LThhYWMtNGFhMC05NDQ4LWIwN2I1MmRkYWY5ZiIsInRzIjoxNzY5NTQwNjU2MzMwLCJ2IjoyLCJleHAiOjE3NzIwNTU3MzQyNTJ9';
      const decoded = decodeCursor(real);
      expect(decoded.id).toBe('af5c33c8-8aac-4aa0-9448-b07b52ddaf9f');
      expect(decoded.ts).toBe(1769540656330);
      expect(decoded.v).toBe(2);
    });
  });

  describe('forgeCursorAt', () => {
    it('should create cursor at given timestamp', () => {
      const ts = 1_600_000_000_000;
      const cursor = forgeCursorAt(ts, EXP);
      const decoded = decodeCursor(cursor);
      expect(decoded.ts).toBe(ts);
      expect(decoded.v).toBe(2);
      expect(decoded.exp).toBe(EXP);
    });

    it('should use max UUID for tiebreaker', () => {
      const cursor = forgeCursorAt(NOW, EXP);
      const decoded = decodeCursor(cursor);
      expect(decoded.id).toBe('ffffffff-ffff-ffff-ffff-ffffffffffff');
    });
  });

  describe('buildShardCursors', () => {
    it('should create N non-overlapping shards', () => {
      const minTs = 1_000_000;
      const maxTs = 2_000_000;
      const shards = buildShardCursors(minTs, maxTs, 4, EXP);

      expect(shards).toHaveLength(4);

      // Shards should cover the full range
      const allEndTs = shards.map(s => s.endTs).sort((a, b) => a - b);
      const allStartTs = shards.map(s => s.startTs).sort((a, b) => a - b);

      expect(allStartTs[0]).toBeGreaterThanOrEqual(minTs);
      expect(allEndTs[allEndTs.length - 1]).toBeLessThanOrEqual(maxTs + 1);
    });

    it('should have valid start cursors', () => {
      const shards = buildShardCursors(1_000_000, 2_000_000, 3, EXP);
      for (const shard of shards) {
        expect(() => decodeCursor(shard.startCursor)).not.toThrow();
        const decoded = decodeCursor(shard.startCursor);
        expect(decoded.ts).toBe(shard.endTs);
      }
    });
  });

  describe('refreshCursorExpiry', () => {
    it('should update expiry while preserving position', () => {
      const original = forgeCursorAt(NOW, EXP);
      const newExp = EXP + 60_000;
      const refreshed = refreshCursorExpiry(original, newExp);
      const decoded = decodeCursor(refreshed);
      expect(decoded.ts).toBe(NOW);
      expect(decoded.exp).toBe(newExp);
    });
  });
});
