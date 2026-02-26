"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.ProgressTracker = void 0;
const logger_1 = require("./logger");
class ProgressTracker {
    constructor(total) {
        this.total = total;
        this.startTime = Date.now();
    }
    start(getStats) {
        this.intervalHandle = setInterval(() => {
            const stats = getStats();
            this.print(stats);
        }, 10000); // print every 10s
    }
    stop() {
        if (this.intervalHandle)
            clearInterval(this.intervalHandle);
    }
    print(workerStats) {
        const totalIngested = workerStats.reduce((s, w) => s + w.eventsIngested, 0);
        const totalRate = workerStats.reduce((s, w) => s + w.eventsPerMinute, 0);
        const pct = this.total > 0 ? ((totalIngested / this.total) * 100).toFixed(1) : '?';
        const elapsedMin = (Date.now() - this.startTime) / 60000;
        const remaining = totalRate > 0
            ? ((this.total - totalIngested) / totalRate).toFixed(1)
            : '?';
        const bar = this.makeBar(totalIngested, this.total);
        logger_1.logger.info({
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
    printFinal(totalIngested, elapsedMs) {
        const elapsedMin = elapsedMs / 60000;
        const rate = Math.round(totalIngested / elapsedMin);
        logger_1.logger.info({
            totalIngested: totalIngested.toLocaleString(),
            elapsedMin: elapsedMin.toFixed(1),
            avgRate: `${rate.toLocaleString()}/min`,
        }, '✅ Ingestion complete');
    }
    makeBar(current, total, width = 30) {
        if (total === 0)
            return '[' + '?'.repeat(width) + ']';
        const filled = Math.round((current / total) * width);
        return '[' + '█'.repeat(filled) + '░'.repeat(width - filled) + ']';
    }
}
exports.ProgressTracker = ProgressTracker;
//# sourceMappingURL=progress.js.map