export type JobPriority = 'critical' | 'high' | 'normal' | 'low'
export type JobStatus = 'waiting' | 'active' | 'completed' | 'failed' | 'dead'

export interface Job {
  id: string
  type: string
  payload: Record<string, unknown>
  priority: JobPriority
  status: JobStatus
  attempts: number
  maxAttempts: number
  delay: number          // ms delay before first run (0 = immediate)
  runAt: number          // Unix ms timestamp — when eligible to run
  createdAt: number
  startedAt?: number
  completedAt?: number
  failedAt?: number
  error?: string
  workerId?: string
  result?: unknown
}

export interface EnqueueOptions {
  priority?: JobPriority
  maxAttempts?: number
  delay?: number         // ms from now
  runAt?: Date           // absolute time
}

// Priority maps to separate Redis keys — workers drain critical first
export const PRIORITY_QUEUES: Record<JobPriority, string> = {
  critical: 'tq:queue:critical',
  high:     'tq:queue:high',
  normal:   'tq:queue:normal',
  low:      'tq:queue:low',
}

export const PRIORITY_ORDER: JobPriority[] = ['critical', 'high', 'normal', 'low']

// Redis key constants
export const KEYS = {
  jobData:    (id: string) => `tq:job:${id}`,           // Hash: full job data
  active:     'tq:active',                               // Set: currently running job IDs
  dead:       'tq:dead',                                 // List: permanently failed jobs
  scheduled:  'tq:scheduled',                            // Sorted set: delayed jobs (score = runAt)
  workerHB:   (id: string) => `tq:worker:${id}:hb`,     // String: last heartbeat timestamp
  workers:    'tq:workers',                              // Set: active worker IDs
  lock:       (jobId: string) => `tq:lock:${jobId}`,    // String: job processing lock
  stats:      'tq:stats',                                // Hash: counters
  events:     'tq:events',                               // Pub/sub channel
}