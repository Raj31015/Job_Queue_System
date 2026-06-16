
import { Pool } from 'pg'
import { logger } from '../logger'

export const pool = new Pool({
  connectionString: process.env.DATABASE_URL || 'postgresql://taskflow:taskflow@localhost:5432/taskflow',
})

pool.on('error', (err) => {
  logger.error('Postgres pool error:', err)
})

export async function migrate(): Promise<void> {
  const client = await pool.connect()
  try {
    await client.query(`
      CREATE TABLE IF NOT EXISTS job_log (
        id           TEXT PRIMARY KEY,
        type         TEXT NOT NULL,
        payload      JSONB NOT NULL DEFAULT '{}',
        priority     TEXT NOT NULL DEFAULT 'normal',
        status       TEXT NOT NULL,
        attempts     INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        error        TEXT,
        result       JSONB,
        worker_id    TEXT,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at   TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        failed_at    TIMESTAMPTZ
      );

      CREATE INDEX IF NOT EXISTS idx_job_log_status   ON job_log(status);
      CREATE INDEX IF NOT EXISTS idx_job_log_type     ON job_log(type);
      CREATE INDEX IF NOT EXISTS idx_job_log_created  ON job_log(created_at DESC);

      CREATE TABLE IF NOT EXISTS throughput_snapshots (
        id           SERIAL PRIMARY KEY,
        snapshot_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        enqueued     INTEGER NOT NULL DEFAULT 0,
        completed    INTEGER NOT NULL DEFAULT 0,
        failed       INTEGER NOT NULL DEFAULT 0,
        dead         INTEGER NOT NULL DEFAULT 0
      );

      CREATE INDEX IF NOT EXISTS idx_throughput_time ON throughput_snapshots(snapshot_at DESC);
    `)
    logger.info('[db] migrations complete')
  } finally {
    client.release()
  }
}

// Persist job state to Postgres for history / analytics
export async function persistJob(job: {
  id: string; type: string; payload: Record<string, unknown>
  priority: string; status: string; attempts: number; maxAttempts: number
  error?: string; result?: unknown; workerId?: string
  createdAt: number; startedAt?: number; completedAt?: number; failedAt?: number
}): Promise<void> {
  await pool.query(`
    INSERT INTO job_log (id, type, payload, priority, status, attempts, max_attempts,
      error, result, worker_id, created_at, started_at, completed_at, failed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
      to_timestamp($11/1000.0),
      to_timestamp($12/1000.0),
      to_timestamp($13/1000.0),
      to_timestamp($14/1000.0))
    ON CONFLICT (id) DO UPDATE SET
      status       = EXCLUDED.status,
      attempts     = EXCLUDED.attempts,
      error        = EXCLUDED.error,
      result       = EXCLUDED.result,
      worker_id    = EXCLUDED.worker_id,
      started_at   = EXCLUDED.started_at,
      completed_at = EXCLUDED.completed_at,
      failed_at    = EXCLUDED.failed_at
  `, [
    job.id, job.type, job.payload, job.priority, job.status,
    job.attempts, job.maxAttempts, job.error || null,
    job.result ? JSON.stringify(job.result) : null,
    job.workerId || null,
    job.createdAt, job.startedAt || null, job.completedAt || null, job.failedAt || null,
  ])
}

export async function getJobHistory(limit = 100, status?: string): Promise<object[]> {
  const query = status
    ? `SELECT * FROM job_log WHERE status = $1 ORDER BY created_at DESC LIMIT $2`
    : `SELECT * FROM job_log ORDER BY created_at DESC LIMIT $1`
  const params = status ? [status, limit] : [limit]
  const res = await pool.query(query, params)
  return res.rows
}

export async function snapshotThroughput(stats: Record<string, number>): Promise<void> {
  await pool.query(`
    INSERT INTO throughput_snapshots (enqueued, completed, failed, dead)
    VALUES ($1, $2, $3, $4)
  `, [
    stats.total_enqueued || 0,
    stats.total_completed || 0,
    stats.total_failed || 0,
    stats.total_dead || 0,
  ])
}
