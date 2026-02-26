import { loadConfig } from './config';
import { Database } from './database';
import { Orchestrator } from './orchestrator';
import { logger } from './logger';

async function main(): Promise<void> {
  logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');
  logger.info('   DataSync Analytics — Ingestion Pipeline   ');
  logger.info('━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━');

  const config = loadConfig();

  logger.info({
    apiBase: config.apiBaseUrl,
    pageLimit: config.pageLimit,
    workerCount: config.workerCount,
    rateLimitPerMin: config.rateLimitPerMinute,
    db: `${config.dbHost}:${config.dbPort}/${config.dbName}`,
  }, 'Config loaded');

  const db = new Database(config);

  // Graceful shutdown
  const shutdown = async (signal: string) => {
    logger.info({ signal }, 'Shutting down gracefully...');
    await db.end();
    process.exit(0);
  };
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  process.on('SIGINT',  () => shutdown('SIGINT'));

  try {
    await db.setup();
    const orchestrator = new Orchestrator(config, db);
    await orchestrator.run();
  } catch (err) {
    logger.error({ err }, 'Fatal error');
    await db.end();
    process.exit(1);
  }

  await db.end();
  logger.info('Done.');
}

main();
