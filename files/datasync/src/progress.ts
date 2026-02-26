import { WorkerStats } from './types';
import { logger } from './logger';

export class ProgressTracker {
  private readonly total: number;
  private readonly startTime: number;
  private intervalHandle?: ReturnType<typeof setInterval>;

  constructor(total: number) {
    this.total = total;
    this.startTime = Date.now();
  }

  start(getStats: () => WorkerStats[]): void {
    this.intervalHandle = setInterval(() => {
      const stats = getStats();
      this.print(stats);
    }, 10_000); // print every 10s
  }

  stop(): void {
    if (this.intervalHandle) clearInterval(this.intervalHandle);
  }

  print(workerStats: WorkerStats[]): void {
    const totalIngested = workerStats.reduce((s, w) => s + w.eventsIngested, 0);
    const totalRate = workerStats.reduce((s, w) => s + w.eventsPerMinute, 0);
    const pct = this.total > 0 ? ((totalIngested / this.total) * 100).toFixed(1) : '?';
    const elapsedMin = (Date.now() - this.startTime) / 60_000;
    const remaining = totalRate > 0
      ? ((this.total - totalIngested) / totalRate).toFixed(1)
      : '?';

    const bar = this.makeBar(totalIngested, this.total);

    logger.info({
      progress: `${pct}%`,
      ingested: totalIngested.toLocaleString(),
      target: this.total.toLocaleString(),
      rate: `${totalRate.toLocaleString()}/min`,
      elapsed: `${elapsedMin.toFixed(1)}min`,
      eta: `${remaining}min`,
      bar,
      workers: workerStats.map(w => ({
        id: w.workerId,
        status: w.status,
        ingested: w.eventsIngested.toLocaleString(),
        rate: `${w.eventsPerMinute}/min`,
      })),
    }, '📊 Progress');
  }

  printFinal(totalIngested: number, elapsedMs: number): void {
    const elapsedMin = elapsedMs / 60_000;
    const rate = Math.round(totalIngested / elapsedMin);
    logger.info({
      totalIngested: totalIngested.toLocaleString(),
      elapsedMin: elapsedMin.toFixed(1),
      avgRate: `${rate.toLocaleString()}/min`,
    }, '✅ Ingestion complete');
  }

  private makeBar(current: number, total: number, width = 30): string {
    if (total === 0) return '[' + '?'.repeat(width) + ']';
    const filled = Math.round((current / total) * width);
    return '[' + '█'.repeat(filled) + '░'.repeat(width - filled) + ']';
  }
}
