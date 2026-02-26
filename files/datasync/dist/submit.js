"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
const config_1 = require("./config");
const database_1 = require("./database");
const apiClient_1 = require("./apiClient");
const rateLimiter_1 = require("./rateLimiter");
const logger_1 = require("./logger");
async function main() {
    const config = (0, config_1.loadConfig)();
    if (!config.githubUrl) {
        logger_1.logger.error('GITHUB_URL is required for submission');
        process.exit(1);
    }
    const db = new database_1.Database(config);
    try {
        const count = await db.getEventCount();
        logger_1.logger.info({ count }, 'Event count in DB');
        if (count < 3000000) {
            logger_1.logger.warn({ count, target: 3000000 }, 'Warning: count below 3M — submitting anyway');
        }
        logger_1.logger.info('Loading event IDs from DB...');
        const eventIds = await db.getAllEventIds();
        logger_1.logger.info({ count: eventIds.length }, 'Loaded event IDs');
        const rateLimiter = new rateLimiter_1.RateLimiter(config.rateLimitPerMinute);
        const client = new apiClient_1.ApiClient(config, rateLimiter);
        logger_1.logger.info({ githubUrl: config.githubUrl }, 'Submitting...');
        const result = await client.submit({
            eventIds,
            repositoryUrl: config.githubUrl,
        });
        logger_1.logger.info({ result }, '🎉 Submission successful!');
    }
    finally {
        await db.end();
    }
}
main().catch(err => {
    logger_1.logger.error({ err }, 'Submit failed');
    process.exit(1);
});
//# sourceMappingURL=submit.js.map