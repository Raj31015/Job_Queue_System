import { Router, Request, Response } from 'express'
import Redis from 'ioredis'
import { Queue } from '../queue/Queue'
import { KEYS, JobPriority } from '../queue/types'
import { jobHandlers } from '../jobs/handlers'
import { getJobHistory } from '../db'
import { WorkerPool } from '../workers/WorkerPool'
import { logger } from '../logger'

export function createRouter(queue: Queue, redis: Redis, pool: WorkerPool): Router {
  const router = Router()

  // ─── Enqueue a job ────────────────────────────────────────────────────────
  router.post('/jobs', async (req: Request, res: Response) => {
    try {
      const { type, payload = {}, priority = 'normal', maxAttempts, delay, runAt } = req.body

      if (!type) return res.status(400).json({ error: 'type is required' })
      if (!jobHandlers[type]) {
        return res.status(400).json({
          error: `Unknown job type: ${type}`,
          validTypes: Object.keys(jobHandlers),
        })
      }

      const job = await queue.enqueue(type, payload, {
        priority: priority as JobPriority,
        maxAttempts,
        delay,
        runAt: runAt ? new Date(runAt) : undefined,
      })

      res.status(201).json({ ok: true, job })
    } catch (err) {
      logger.error('POST /jobs error:', err)
      res.status(500).json({ error: 'Internal server error' })
    }
  })

  // ─── Get job by ID ────────────────────────────────────────────────────────
  router.get('/jobs/:id', async (req: Request, res: Response) => {
    const job = await queue.getJob(req.params.id)
    if (!job) return res.status(404).json({ error: 'Job not found' })
    res.json(job)
  })

  // ─── Job history from Postgres ────────────────────────────────────────────
  router.get('/jobs', async (req: Request, res: Response) => {
    const { status, limit = '50' } = req.query
    const jobs = await getJobHistory(parseInt(limit as string), status as string)
    res.json(jobs)
  })

  // ─── Queue stats ──────────────────────────────────────────────────────────
  router.get('/stats', async (_req: Request, res: Response) => {
    const stats = await queue.getStats()
    const workers = pool.getStatus()
    res.json({ stats, workers })
  })

  // ─── Worker status ────────────────────────────────────────────────────────
  router.get('/workers', (_req: Request, res: Response) => {
    res.json(pool.getStatus())
  })

  // ─── Available job types ──────────────────────────────────────────────────
  router.get('/job-types', (_req: Request, res: Response) => {
    res.json(Object.keys(jobHandlers))
  })

  // ─── Bulk enqueue (for demo/testing) ─────────────────────────────────────
  router.post('/jobs/bulk', async (req: Request, res: Response) => {
    const { jobs } = req.body as {
      jobs: Array<{ type: string; payload?: Record<string, unknown>; priority?: string }>
    }
    if (!Array.isArray(jobs)) return res.status(400).json({ error: 'jobs must be an array' })

    const results = await Promise.all(
      jobs.map(j => queue.enqueue(j.type, j.payload ?? {}, { priority: j.priority as JobPriority }))
    )
    res.status(201).json({ ok: true, count: results.length, jobs: results })
  })

  // ─── SSE stream — real-time events to dashboard ───────────────────────────
  router.get('/events', (req: Request, res: Response) => {
    res.setHeader('Content-Type', 'text/event-stream')
    res.setHeader('Cache-Control', 'no-cache')
    res.setHeader('Connection', 'keep-alive')
    res.setHeader('Access-Control-Allow-Origin', '*')
    res.flushHeaders()

    // Subscribe to Redis pub/sub
    const subscriber = redis.duplicate()
    subscriber.subscribe(KEYS.events)

    subscriber.on('message', (_channel, message) => {
      res.write(`data: ${message}\n\n`)
    })

    // Send stats every 2s
    const statsInterval = setInterval(async () => {
      const stats = await queue.getStats()
      const workers = pool.getStatus()
      res.write(`data: ${JSON.stringify({ event: 'stats', stats, workers })}\n\n`)
    }, 2000)

    // Keepalive ping every 15s
    const ping = setInterval(() => {
      res.write(': ping\n\n')
    }, 15_000)

    req.on('close', () => {
      subscriber.unsubscribe()
      subscriber.disconnect()
      clearInterval(statsInterval)
      clearInterval(ping)
    })
  })

  return router
}
