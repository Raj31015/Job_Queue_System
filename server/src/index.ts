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
  const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379', {
    maxRetriesPerRequest: null,
    enableReadyCheck: false,
    lazyConnect: true,
  })

  redis.on('error', (err) => {
    logger.warn(`[redis] error: ${err.message}`)
  })
  redis.on('close', () => {
    logger.warn('[redis] connection closed')
  })
  redis.on('reconnecting', () => {
    logger.warn('[redis] reconnecting')
  })
  redis.on('end', () => {
    logger.warn('[redis] connection ended')
  })

  await redis.connect()
  logger.info('[boot] Redis connected')

  await migrate()
  logger.info('[boot] Postgres ready')

  const queue = new Queue(redis)
  const workerPool = new WorkerPool(
    queue,
    redis,
    parseInt(process.env.WORKER_CONCURRENCY || '3')
  )

  const subscriber = redis.duplicate()
  subscriber.on('error', (err) => {
    logger.warn(`[redis:subscriber] error: ${err.message}`)
  })
  subscriber.on('close', () => {
    logger.warn('[redis:subscriber] connection closed')
  })
  subscriber.on('reconnecting', () => {
    logger.warn('[redis:subscriber] reconnecting')
  })
  subscriber.on('end', () => {
    logger.warn('[redis:subscriber] connection ended')
  })

  await subscriber.connect()
  await subscriber.subscribe(KEYS.events)

  subscriber.on('message', async (_channel, message) => {
    try {
      const { event, job } = JSON.parse(message)
      if (['job:completed', 'job:dead', 'job:retry', 'job:started', 'job:enqueued'].includes(event)) {
        await persistJob(job)
      }
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err)
      logger.warn(`[events] failed to persist job event: ${error}`)
    }
  })

  setInterval(async () => {
    try {
      const stats = await queue.getStats()
      await snapshotThroughput(stats)
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err)
      logger.warn(`[metrics] throughput snapshot skipped: ${error}`)
    }
  }, 60_000)

  setInterval(async () => {
    try {
      const promoted = await queue.promoteScheduled()
      if (promoted > 0) logger.info(`[scheduler] promoted ${promoted} due jobs`)
    } catch (err) {
      const error = err instanceof Error ? err.message : String(err)
      logger.warn(`[scheduler] promotion skipped: ${error}`)
    }
  }, 5_000)

  const app = express()
  app.use(cors())
  app.use(express.json())
  app.use('/api', createRouter(queue, redis, workerPool))

  app.get('/health', (_req, res) => res.json({ ok: true, ts: Date.now() }))

  const PORT = parseInt(process.env.PORT || '4000')
  app.listen(PORT, () => {
    logger.info(`[boot] API listening on :${PORT}`)
  })

  workerPool.start()
  logger.info('[boot] TaskFlow ready')

  process.on('SIGTERM', async () => {
    logger.info('[boot] shutting down...')
    workerPool.stop()
    await redis.quit()
    await subscriber.quit()
    await pgPool.end()
    process.exit(0)
  })
}

process.on('unhandledRejection', (reason) => {
  const error = reason instanceof Error ? reason.stack || reason.message : String(reason)
  logger.error(`[process] unhandled rejection: ${error}`)
})

main().catch(err => {
  console.error('Fatal boot error:', err)
  process.exit(1)
})
