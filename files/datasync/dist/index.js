"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config_1 = require("./config");
const database_1 = require("./database");
const orchestrator_1 = require("./orchestrator");
const logger_1 = require("./logger");
async function main() {
    logger_1.logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    logger_1.logger.info('   DataSync Analytics — Ingestion Pipeline   ');
    logger_1.logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
    const config = (0, config_1.loadConfig)();
    logger_1.logger.info({
        apiBase: config.apiBaseUrl,
        pageLimit: config.pageLimit,
        workerCount: config.workerCount,
        rateLimitPerMin: config.rateLimitPerMinute,
        db: `${config.dbHost}:${config.dbPort}/${config.dbName}`,
    }, 'Config loaded');
    const db = new database_1.Database(config);
    // Graceful shutdown
    const shutdown = async (signal) => {
        logger_1.logger.info({ signal }, 'Shutting down gracefully...');
        await db.end();
        process.exit(0);
    };
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));
    try {
        await db.setup();
        const orchestrator = new orchestrator_1.Orchestrator(config, db);
        await orchestrator.run();
    }
    catch (err) {
        logger_1.logger.error({ err }, 'Fatal error');
        await db.end();
        process.exit(1);
    }
    await db.end();
    logger_1.logger.info('Done.');
}
main();
//# sourceMappingURL=index.js.map