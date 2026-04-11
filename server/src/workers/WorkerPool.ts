import Redis from 'ioredis'
import { v4 as uuid } from 'uuid'
import { Queue } from '../queue/Queue'
import { KEYS } from '../queue/types'
import { jobHandlers } from '../jobs/handlers'
import { logger } from '../logger'

export class WorkerPool {
  private queue: Queue
  private redis: Redis
  private workers: Map<string, Worker> = new Map()
  private concurrency: number
  private running = false

  constructor(queue: Queue, redis: Redis, concurrency = 3) {
    this.queue = queue
    this.redis = redis
    this.concurrency = concurrency
  }

  start(): void {
    this.running = true
    logger.info(`[pool] starting ${this.concurrency} workers`)

    for (let i = 0; i < this.concurrency; i++) {
      const worker = new Worker(this.queue, this.redis, i)
      this.workers.set(worker.id, worker)
      worker.start()
    }

    // Stall detector — runs every 30s
    setInterval(async () => {
      const recovered = await this.queue.recoverStalledJobs()
      if (recovered > 0) logger.info(`[pool] recovered ${recovered} stalled jobs`)
    }, 30_000)
  }

  stop(): void {
    this.running = false
    for (const worker of this.workers.values()) {
      worker.stop()
    }
  }

  getStatus(): object[] {
    return Array.from(this.workers.values()).map(w => w.getStatus())
  }
}

class Worker {
  readonly id: string
  private queue: Queue
  private redis: Redis
  private index: number
  private running = false
  private busy = false
  private processedCount = 0
  private lastJobType?: string
  private heartbeatInterval?: ReturnType<typeof setInterval>
  private pollInterval?: ReturnType<typeof setInterval>

  constructor(queue: Queue, redis: Redis, index: number) {
    this.id = `worker-${index}-${uuid().slice(0, 8)}`
    this.queue = queue
    this.redis = redis
    this.index = index
  }

  start(): void {
    this.running = true

    // Register worker
    this.redis.sadd(KEYS.workers, this.id)

    // Heartbeat every 10s — proof of life for stall detection
    this.heartbeatInterval = setInterval(() => {
      this.redis.set(KEYS.workerHB(this.id), Date.now().toString(), 'EX', 60)
    }, 10_000)
    // Initial heartbeat
    this.redis.set(KEYS.workerHB(this.id), Date.now().toString(), 'EX', 60)

    // Poll loop — check for jobs every 500ms when idle
    this.pollInterval = setInterval(() => {
      if (!this.busy) this.poll()
    }, 500)

    logger.info(`[worker:${this.id}] started`)
  }

  stop(): void {
    this.running = false
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval)
    if (this.pollInterval) clearInterval(this.pollInterval)
    this.redis.srem(KEYS.workers, this.id)
    logger.info(`[worker:${this.id}] stopped`)
  }

  private async poll(): Promise<void> {
    if (!this.running || this.busy) return

    try {
      const job = await this.queue.dequeue(this.id)
      if (!job) return

      this.busy = true
      this.lastJobType = job.type

      try {
        const handler = jobHandlers[job.type]
        if (!handler) throw new Error(`No handler registered for job type: ${job.type}`)

        logger.info(`[worker:${this.id}] processing ${job.type} (${job.id})`)
        const result = await handler(job)
        await this.queue.ack(job.id, result)
        this.processedCount++

      } catch (err) {
        const error = err instanceof Error ? err : new Error(String(err))
        logger.error(`[worker:${this.id}] job ${job.id} failed: ${error.message}`)
        await this.queue.fail(job.id, error)
      } finally {
        this.busy = false
      }

    } catch (err) {
      logger.error(`[worker:${this.id}] poll error: ${err}`)
      this.busy = false
    }
  }

  getStatus(): object {
    return {
      id: this.id,
      index: this.index,
      busy: this.busy,
      processedCount: this.processedCount,
      lastJobType: this.lastJobType,
    }
  }
}