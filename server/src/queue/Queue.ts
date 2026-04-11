import Redis from 'ioredis'
import { v4 as uuid } from 'uuid'
import {
  Job, JobPriority, JobStatus, EnqueueOptions,
  PRIORITY_QUEUES, PRIORITY_ORDER, KEYS
} from './types'
import { logger } from '../logger'

export class Queue {
  private redis: Redis

  constructor(redis: Redis) {
    this.redis = redis
  }

  // ─── Enqueue ────────────────────────────────────────────────────────────────

  async enqueue(
    type: string,
    payload: Record<string, unknown>,
    options: EnqueueOptions = {}
  ): Promise<Job> {
    const {
      priority = 'normal',
      maxAttempts = 3,
      delay = 0,
      runAt,
    } = options

    const now = Date.now()
    const eligibleAt = runAt ? runAt.getTime() : now + delay

    const job: Job = {
      id: uuid(),
      type,
      payload,
      priority,
      status: 'waiting',
      attempts: 0,
      maxAttempts,
      delay,
      runAt: eligibleAt,
      createdAt: now,
    }

    // Store job data as Redis hash
    await this.redis.hset(KEYS.jobData(job.id), this.serialize(job))

    if (eligibleAt > now) {
      // Delayed job — add to scheduled sorted set (score = run timestamp)
      await this.redis.zadd(KEYS.scheduled, eligibleAt, job.id)
      logger.info(`[queue] scheduled job ${job.id} (${type}) in ${delay}ms`)
    } else {
      // Immediate job — push to priority queue (LPUSH = FIFO when RPOP)
      await this.redis.lpush(PRIORITY_QUEUES[priority], job.id)
      logger.info(`[queue] enqueued job ${job.id} (${type}) priority=${priority}`)
    }

    // Publish event for dashboard SSE
    await this.redis.publish(KEYS.events, JSON.stringify({
      event: 'job:enqueued', job
    }))

    await this.redis.hincrby(KEYS.stats, 'total_enqueued', 1)
    return job
  }

  // ─── Dequeue ─────────────────────────────────────────────────────────────────
  // Returns next available job, respecting priority order.
  // Uses a Redis lock to prevent double-processing by concurrent workers.

  async dequeue(workerId: string): Promise<Job | null> {
    // Promote any scheduled jobs that are now due
    await this.promoteScheduled()

    for (const priority of PRIORITY_ORDER) {
      const queueKey = PRIORITY_QUEUES[priority]

      // RPOP from the right (oldest item) — non-blocking
      const jobId = await this.redis.rpop(queueKey)
      if (!jobId) continue

      // Acquire a lock for this job (NX = only set if not exists, PX = TTL ms)
      // This prevents two workers from processing the same job if rpop races
      const lockKey = KEYS.lock(jobId)
      const locked = await this.redis.set(lockKey, workerId, 'PX', 30_000, 'NX')

      if (!locked) {
        // Another worker grabbed it — skip and try next
        logger.warn(`[queue] failed to lock job ${jobId}, skipping`)
        continue
      }

      const job = await this.getJob(jobId)
      if (!job) {
        await this.redis.del(lockKey)
        continue
      }

      // Mark as active
      const now = Date.now()
      const updates: Partial<Job> = {
        status: 'active',
        workerId,
        startedAt: now,
        attempts: job.attempts + 1,
      }

      await this.redis.hset(KEYS.jobData(jobId), this.serialize(updates))
      await this.redis.sadd(KEYS.active, jobId)

      const updatedJob = { ...job, ...updates }

      await this.redis.publish(KEYS.events, JSON.stringify({
        event: 'job:started', job: updatedJob
      }))

      logger.info(`[queue] worker ${workerId} dequeued job ${jobId} (${job.type})`)
      return updatedJob
    }

    return null
  }

  // ─── Acknowledge (success) ────────────────────────────────────────────────

  async ack(jobId: string, result: unknown = null): Promise<void> {
    const now = Date.now()
    const updates: Partial<Job> = {
      status: 'completed',
      completedAt: now,
      result: result as Record<string, unknown>,
    }

    await this.redis.hset(KEYS.jobData(jobId), this.serialize(updates))
    await this.redis.srem(KEYS.active, jobId)
    await this.redis.del(KEYS.lock(jobId))
    await this.redis.hincrby(KEYS.stats, 'total_completed', 1)

    const job = await this.getJob(jobId)
    await this.redis.publish(KEYS.events, JSON.stringify({
      event: 'job:completed', job: { ...job, ...updates }
    }))

    logger.info(`[queue] job ${jobId} completed`)
  }

  // ─── Fail (with retry logic) ────────────────────────────────────────────────

  async fail(jobId: string, error: Error): Promise<void> {
    const job = await this.getJob(jobId)
    if (!job) return

    const now = Date.now()
    await this.redis.srem(KEYS.active, jobId)
    await this.redis.del(KEYS.lock(jobId))

    if (job.attempts >= job.maxAttempts) {
      // Permanently failed — move to dead letter queue
      const updates: Partial<Job> = {
        status: 'dead',
        failedAt: now,
        error: error.message,
      }
      await this.redis.hset(KEYS.jobData(jobId), this.serialize(updates))
      await this.redis.lpush(KEYS.dead, jobId)
      await this.redis.hincrby(KEYS.stats, 'total_dead', 1)

      await this.redis.publish(KEYS.events, JSON.stringify({
        event: 'job:dead', job: { ...job, ...updates }
      }))

      logger.error(`[queue] job ${jobId} moved to dead letter queue after ${job.attempts} attempts`)
    } else {
      // Retry with exponential backoff: 2^attempts * 1000ms (+ jitter)
      const backoffMs = Math.pow(2, job.attempts) * 1000 + Math.random() * 500
      const retryAt = now + backoffMs

      const updates: Partial<Job> = {
        status: 'waiting',
        error: error.message,
        runAt: retryAt,
      }
      await this.redis.hset(KEYS.jobData(jobId), this.serialize(updates))

      // Schedule retry via sorted set
      await this.redis.zadd(KEYS.scheduled, retryAt, jobId)
      await this.redis.hincrby(KEYS.stats, 'total_failed', 1)

      await this.redis.publish(KEYS.events, JSON.stringify({
        event: 'job:retry', job: { ...job, ...updates }, retryIn: backoffMs
      }))

      logger.warn(`[queue] job ${jobId} will retry in ${(backoffMs / 1000).toFixed(1)}s (attempt ${job.attempts}/${job.maxAttempts})`)
    }
  }

  // ─── Scheduled job promotion ──────────────────────────────────────────────
  // Moves jobs from the scheduled sorted set into their priority queues
  // when their runAt timestamp has passed.

  async promoteScheduled(): Promise<number> {
    const now = Date.now()
    // ZRANGEBYSCORE: get all jobs with score <= now (i.e. due to run)
    const dueJobIds = await this.redis.zrangebyscore(KEYS.scheduled, 0, now)

    if (dueJobIds.length === 0) return 0

    for (const jobId of dueJobIds) {
      const job = await this.getJob(jobId)
      if (!job) {
        await this.redis.zrem(KEYS.scheduled, jobId)
        continue
      }

      await this.redis.lpush(PRIORITY_QUEUES[job.priority], jobId)
      await this.redis.zrem(KEYS.scheduled, jobId)
      logger.info(`[scheduler] promoted job ${jobId} to ${job.priority} queue`)
    }

    return dueJobIds.length
  }

  // ─── Stall detection ─────────────────────────────────────────────────────
  // Finds jobs that have been "active" too long (worker died mid-job)
  // and re-queues them.

  async recoverStalledJobs(stallThresholdMs = 60_000): Promise<number> {
    const activeIds = await this.redis.smembers(KEYS.active)
    let recovered = 0

    for (const jobId of activeIds) {
      const job = await this.getJob(jobId)
      if (!job || !job.startedAt) continue

      const elapsed = Date.now() - job.startedAt
      if (elapsed < stallThresholdMs) continue

      // Check if worker is still alive
      if (job.workerId) {
        const hb = await this.redis.get(KEYS.workerHB(job.workerId))
        if (hb && Date.now() - parseInt(hb) < stallThresholdMs) continue
      }

      logger.warn(`[queue] recovering stalled job ${jobId} (active for ${(elapsed / 1000).toFixed(0)}s)`)

      await this.redis.srem(KEYS.active, jobId)
      await this.redis.del(KEYS.lock(jobId))

      // Re-queue to front of its priority queue
      await this.redis.lpush(PRIORITY_QUEUES[job.priority], jobId)
      await this.redis.hset(KEYS.jobData(jobId), 'status', 'waiting')
      recovered++
    }

    return recovered
  }

  // ─── Helpers ──────────────────────────────────────────────────────────────

  async getJob(id: string): Promise<Job | null> {
    const data = await this.redis.hgetall(KEYS.jobData(id))
    if (!data || Object.keys(data).length === 0) return null
    return this.deserialize(data)
  }

  async getStats(): Promise<Record<string, number>> {
    const raw = await this.redis.hgetall(KEYS.stats)
    const stats: Record<string, number> = {}
    for (const [k, v] of Object.entries(raw)) {
      stats[k] = parseInt(v)
    }

    // Queue depths
    for (const priority of PRIORITY_ORDER) {
      stats[`queue_${priority}`] = await this.redis.llen(PRIORITY_QUEUES[priority])
    }
    stats['queue_scheduled'] = await this.redis.zcard(KEYS.scheduled)
    stats['queue_active'] = await this.redis.scard(KEYS.active)
    stats['queue_dead'] = await this.redis.llen(KEYS.dead)

    return stats
  }

  async getRecentJobs(limit = 50): Promise<Job[]> {
    // Get from dead + active
    const deadIds = await this.redis.lrange(KEYS.dead, 0, limit - 1)
    const activeIds = await this.redis.smembers(KEYS.active)
    const ids = [...new Set([...activeIds, ...deadIds])].slice(0, limit)

    const jobs = await Promise.all(ids.map(id => this.getJob(id)))
    return jobs.filter(Boolean) as Job[]
  }

  // Serialize Job object to Redis hash (all values must be strings)
  private serialize(obj: Partial<Job>): Record<string, string> {
    const result: Record<string, string> = {}
    for (const [key, value] of Object.entries(obj)) {
      if (value === undefined) continue
      result[key] = typeof value === 'object' ? JSON.stringify(value) : String(value)
    }
    return result
  }

  // Deserialize Redis hash back to Job
  private deserialize(data: Record<string, string>): Job {
    return {
      id: data.id,
      type: data.type,
      payload: JSON.parse(data.payload || '{}'),
      priority: data.priority as JobPriority,
      status: data.status as JobStatus,
      attempts: parseInt(data.attempts || '0'),
      maxAttempts: parseInt(data.maxAttempts || '3'),
      delay: parseInt(data.delay || '0'),
      runAt: parseInt(data.runAt || '0'),
      createdAt: parseInt(data.createdAt || '0'),
      startedAt: data.startedAt ? parseInt(data.startedAt) : undefined,
      completedAt: data.completedAt ? parseInt(data.completedAt) : undefined,
      failedAt: data.failedAt ? parseInt(data.failedAt) : undefined,
      error: data.error,
      workerId: data.workerId,
      result: data.result ? JSON.parse(data.result) : undefined,
    }
  }
}