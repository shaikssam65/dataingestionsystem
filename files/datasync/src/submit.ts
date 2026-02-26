import { loadConfig } from './config';
import { Database } from './database';
import { ApiClient } from './apiClient';
import { RateLimiter } from './rateLimiter';
import { logger } from './logger';

async function main(): Promise<void> {
  const config = loadConfig();
  if (!config.githubUrl) {
    logger.error('GITHUB_URL is required for submission');
    process.exit(1);
  }

  const db = new Database(config);

  try {
    const count = await db.getEventCount();
    logger.info({ count }, 'Event count in DB');

    if (count < 3_000_000) {
      logger.warn({ count, target: 3_000_000 }, 'Warning: count below 3M — submitting anyway');
    }

    logger.info('Loading event IDs from DB...');
    const eventIds = await db.getAllEventIds();
    logger.info({ count: eventIds.length }, 'Loaded event IDs');

    const rateLimiter = new RateLimiter(config.rateLimitPerMinute);
    const client = new ApiClient(config, rateLimiter);

    logger.info({ githubUrl: config.githubUrl }, 'Submitting...');
    const result = await client.submit({
      eventIds,
      repositoryUrl: config.githubUrl,
    });

    logger.info({ result }, '🎉 Submission successful!');
  } finally {
    await db.end();
  }
}

main().catch(err => {
  logger.error({ err }, 'Submit failed');
  process.exit(1);
});
