
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
        session_id   TEXT NOT NULL DEFAULT 'anonymous',
        type         TEXT NOT NULL,
        payload      JSONB NOT NULL DEFAULT '{}',
        priority     TEXT NOT NULL DEFAULT 'normal',
        status       TEXT NOT NULL,
        attempts     INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 3,
        error        TEXT,
        result       JSONB,
        worker_id    TEXT,
        run_at       TIMESTAMPTZ,
        created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at   TIMESTAMPTZ,
        completed_at TIMESTAMPTZ,
        failed_at    TIMESTAMPTZ
      );

      ALTER TABLE job_log ADD COLUMN IF NOT EXISTS session_id TEXT NOT NULL DEFAULT 'anonymous';
      ALTER TABLE job_log ADD COLUMN IF NOT EXISTS run_at TIMESTAMPTZ;

      CREATE INDEX IF NOT EXISTS idx_job_log_status   ON job_log(status);
      CREATE INDEX IF NOT EXISTS idx_job_log_session  ON job_log(session_id);
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
  id: string; sessionId: string; type: string; payload: Record<string, unknown>
  priority: string; status: string; attempts: number; maxAttempts: number
  error?: string; result?: unknown; workerId?: string
  runAt: number; createdAt: number; startedAt?: number; completedAt?: number; failedAt?: number
}): Promise<void> {
  await pool.query(`
    INSERT INTO job_log (id, session_id, type, payload, priority, status, attempts, max_attempts,
      error, result, worker_id, run_at, created_at, started_at, completed_at, failed_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,
      to_timestamp($12/1000.0),
      to_timestamp($13/1000.0),
      to_timestamp($14/1000.0),
      to_timestamp($15/1000.0),
      to_timestamp($16/1000.0))
    ON CONFLICT (id) DO UPDATE SET
      session_id    = EXCLUDED.session_id,
      status       = EXCLUDED.status,
      attempts     = EXCLUDED.attempts,
      error        = EXCLUDED.error,
      result       = EXCLUDED.result,
      worker_id    = EXCLUDED.worker_id,
      run_at       = EXCLUDED.run_at,
      started_at   = EXCLUDED.started_at,
      completed_at = EXCLUDED.completed_at,
      failed_at    = EXCLUDED.failed_at
  `, [
    job.id, job.sessionId, job.type, job.payload, job.priority, job.status,
    job.attempts, job.maxAttempts, job.error || null,
    job.result ? JSON.stringify(job.result) : null,
    job.workerId || null,
    job.runAt, job.createdAt, job.startedAt || null, job.completedAt || null, job.failedAt || null,
  ])
}

export async function getJobHistory(sessionId: string, limit = 100, status?: string): Promise<object[]> {
  const query = status
    ? `SELECT * FROM job_log WHERE session_id = $1 AND status = $2 ORDER BY created_at DESC LIMIT $3`
    : `SELECT * FROM job_log WHERE session_id = $1 ORDER BY created_at DESC LIMIT $2`
  const params = status ? [sessionId, status, limit] : [sessionId, limit]
  const res = await pool.query(query, params)
  return res.rows
}

export async function getSessionStats(sessionId: string): Promise<Record<string, number>> {
  const res = await pool.query(`
    SELECT
      COUNT(*)::int AS total_enqueued,
      COUNT(*) FILTER (WHERE status = 'completed')::int AS total_completed,
      COUNT(*) FILTER (WHERE status = 'dead')::int AS total_dead,
      COUNT(*) FILTER (WHERE status = 'active')::int AS queue_active,
      COUNT(*) FILTER (WHERE status = 'dead')::int AS queue_dead,
      COUNT(*) FILTER (WHERE status = 'waiting' AND priority = 'critical' AND (run_at IS NULL OR run_at <= NOW()))::int AS queue_critical,
      COUNT(*) FILTER (WHERE status = 'waiting' AND priority = 'high' AND (run_at IS NULL OR run_at <= NOW()))::int AS queue_high,
      COUNT(*) FILTER (WHERE status = 'waiting' AND priority = 'normal' AND (run_at IS NULL OR run_at <= NOW()))::int AS queue_normal,
      COUNT(*) FILTER (WHERE status = 'waiting' AND priority = 'low' AND (run_at IS NULL OR run_at <= NOW()))::int AS queue_low,
      COUNT(*) FILTER (WHERE status = 'waiting' AND run_at > NOW())::int AS queue_scheduled,
      COUNT(*) FILTER (WHERE status = 'waiting' AND error IS NOT NULL)::int AS total_failed
    FROM job_log
    WHERE session_id = $1
  `, [sessionId])

  const row = res.rows[0] ?? {}
  return Object.fromEntries(Object.entries(row).map(([key, value]) => [key, Number(value) || 0]))
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
