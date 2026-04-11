import dotenv from 'dotenv'
dotenv.config({path:'../.env'})
import express from 'express'
import cors from 'cors'
import Redis from 'ioredis'
import { Queue } from './queue/Queue'
import { WorkerPool } from './workers/WorkerPool'
import { createRouter } from './routes'
import { migrate, pool as pgPool, snapshotThroughput, persistJob } from './db'
import { KEYS } from './queue/types'
import { logger } from './logger'

async function main() {
  // ─── Connections ────────────────────────────────────────────────────────────
  const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    lazyConnect: true,
  })

  await redis.connect()
  logger.info('[boot] Redis connected')

  await migrate()
  logger.info('[boot] Postgres ready')

  // ─── Core services ──────────────────────────────────────────────────────────
  const queue = new Queue(redis)
  const workerPool = new WorkerPool(
    queue,
    redis,
    parseInt(process.env.WORKER_CONCURRENCY || '3')
  )

  // ─── Persist completed/failed jobs to Postgres ──────────────────────────────
  const subscriber = redis.duplicate()
  await subscriber.connect()
  await subscriber.subscribe(KEYS.events)

  subscriber.on('message', async (_channel, message) => {
    try {
      const { event, job } = JSON.parse(message)
      if (['job:completed', 'job:dead', 'job:retry', 'job:started', 'job:enqueued'].includes(event)) {
        await persistJob(job)
      }
    } catch {}
  })

  // ─── Throughput snapshots every minute ─────────────────────────────────────
  setInterval(async () => {
    const stats = await queue.getStats()
    await snapshotThroughput(stats)
  }, 60_000)

  // ─── HTTP API ───────────────────────────────────────────────────────────────
  const app = express()
  app.use(cors())
  app.use(express.json())
  app.use('/api', createRouter(queue, redis, workerPool))

  app.get('/health', (_req, res) => res.json({ ok: true, ts: Date.now() }))

  const PORT = parseInt(process.env.PORT || '4000')
  app.listen(PORT, () => {
    logger.info(`[boot] API listening on :${PORT}`)
  })

  // ─── Start workers ──────────────────────────────────────────────────────────
  workerPool.start()
  logger.info('[boot] TaskFlow ready')

  // ─── Graceful shutdown ──────────────────────────────────────────────────────
  process.on('SIGTERM', async () => {
    logger.info('[boot] shutting down...')
    workerPool.stop()
    await redis.quit()
    await subscriber.quit()
    await pgPool.end()
    process.exit(0)
  })
}

main().catch(err => {
  console.error('Fatal boot error:', err)
  process.exit(1)
})