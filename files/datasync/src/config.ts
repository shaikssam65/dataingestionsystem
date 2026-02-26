import { Config } from './types';

function required(name: string): string {
  const val = process.env[name];
  if (!val) throw new Error(`Missing required environment variable: ${name}`);
  return val;
}

function optional(name: string, fallback: string): string {
  return process.env[name] || fallback;
}

export function loadConfig(): Config {
  return {
    apiKey: required('API_KEY'),
    apiBaseUrl: optional('API_BASE_URL', 'http://datasync-dev-alb-101078500.us-east-1.elb.amazonaws.com/api/v1'),
    pageLimit: parseInt(optional('PAGE_LIMIT', '5000')),
    workerCount: parseInt(optional('WORKER_COUNT', '1')),  // Start at 1 until parallel confirmed
    rateLimitPerMinute: parseInt(optional('RATE_LIMIT_PER_MINUTE', '10')),
    dbHost: optional('POSTGRES_HOST', 'localhost'),
    dbPort: parseInt(optional('POSTGRES_PORT', '5434')),
    dbName: optional('POSTGRES_DB', 'datasync'),
    dbUser: optional('POSTGRES_USER', 'postgres'),
    dbPassword: optional('POSTGRES_PASSWORD', 'postgres'),
    dbPoolSize: parseInt(optional('DB_POOL_SIZE', '20')),
    stateFile: optional('STATE_FILE', '/tmp/ingestion_state.json'),
    githubUrl: optional('GITHUB_URL', ''),
  };
}
