import { RateLimiter } from '../../src/rateLimiter';

describe('RateLimiter', () => {
  jest.useFakeTimers();

  it('should allow up to maxRequests immediately', async () => {
    const rl = new RateLimiter(10, 1); // 9 effective
    const results: number[] = [];

    const promises = Array.from({ length: 9 }, async (_, i) => {
      await rl.acquire();
      results.push(i);
    });

    await Promise.all(promises);
    expect(results).toHaveLength(9);
  });

  it('should queue requests beyond the limit', async () => {
    const rl = new RateLimiter(5, 0); // 5 effective
    let resolved = 0;

    // Start 10 requests
    const promises = Array.from({ length: 10 }, async () => {
      await rl.acquire();
      resolved++;
    });

    // After immediate execution, only 5 should be resolved
    await Promise.resolve();
    await Promise.resolve();
    // The first 5 resolve immediately
    expect(resolved).toBe(5);

    // Advance time past the window
    jest.advanceTimersByTime(61_000);
    await Promise.all(promises);
    expect(resolved).toBe(10);
  });

  it('should track queue depth', () => {
    const rl = new RateLimiter(3, 0); // 3 effective
    // Saturate the window
    rl.acquire();
    rl.acquire();
    rl.acquire();
    // Now queue 2 more
    rl.acquire();
    rl.acquire();
    expect(rl.queueDepth).toBeGreaterThan(0);
  });
});
