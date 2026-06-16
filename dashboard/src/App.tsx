import { useEffect, useMemo, useState } from 'react'

type JobPriority = 'critical' | 'high' | 'normal' | 'low'
type JobStatus = 'waiting' | 'active' | 'completed' | 'failed' | 'dead'

type Job = {
  id: string
  session_id?: string
  sessionId?: string
  type: string
  payload: Record<string, unknown>
  priority: JobPriority
  status: JobStatus
  attempts: number
  maxAttempts: number
  error?: string | null
  worker_id?: string | null
  workerId?: string | null
  created_at?: string
  createdAt?: number
  started_at?: string | null
  startedAt?: number
  completed_at?: string | null
  completedAt?: number
  failed_at?: string | null
  failedAt?: number
}

type WorkerStatus = {
  id: string
  index: number
  busy: boolean
  processedCount: number
  lastJobType?: string
}

type Stats = Record<string, number>

type StatsPayload = {
  stats: Stats
  workers: WorkerStatus[]
}

type StatsEvent = { event: 'stats'; stats: Stats; workers: WorkerStatus[] }
type JobEvent = { event: string; job?: Job; retryIn?: number }
type StreamEvent = StatsEvent | JobEvent

const API_BASE = import.meta.env.VITE_API_BASE ?? 'http://localhost:4000/api'
const SESSION_STORAGE_KEY = 'taskflow.session.id'

const statusTone: Record<JobStatus, string> = {
  waiting: 'bg-[#1e293b] text-[#93c5fd]',     // slate blue
  active: 'bg-[#0f172a] text-[#60a5fa]',      // deep navy
  completed: 'bg-[#022c22] text-[#34d399]',   // subtle success green-blue
  failed: 'bg-[#3f1d2e] text-[#f87171]',      // muted red (kept readable)
  dead: 'bg-[#111827] text-[#9ca3af]',        // neutral dark
}

const priorityTone: Record<JobPriority, string> = {
  critical: 'text-[#ef4444]',  
  high: 'text-[#38bdf8]',      
  normal: 'text-[#93c5fd]',   
  low: 'text-[#64748b]',       
}

const seedJobs = [
  { type: 'send_email', note: 'Campaign and transactional mailers' },
  { type: 'generate_pdf', note: 'Reports, invoices, and exports' },
  { type: 'resize_image', note: 'Media cleanup and responsive assets' },
  { type: 'sync_data', note: 'Back-office sync jobs' },
  { type: 'webhook', note: 'Partner callbacks and event fan-out' },
]

function App() {
  const [sessionId] = useState(() => getOrCreateSessionId())
  const [stats, setStats] = useState<Stats>({})
  const [workers, setWorkers] = useState<WorkerStatus[]>([])
  const [jobs, setJobs] = useState<Job[]>([])
  const [jobTypes, setJobTypes] = useState<string[]>([])
  const [activity, setActivity] = useState<Array<{ label: string; at: string }>>([])
  const [connected, setConnected] = useState(false)
  const [isGeneratingDemo, setIsGeneratingDemo] = useState(false)
  const [demoMessage, setDemoMessage] = useState<string | null>(null)

  useEffect(() => {
    let mounted = true

    const load = async () => {
      try {
        const [statsRes, jobsRes, typesRes] = await Promise.all([
          fetch(`${API_BASE}/stats`, { headers: sessionHeaders(sessionId) }),
          fetch(`${API_BASE}/jobs?limit=8`, { headers: sessionHeaders(sessionId) }),
          fetch(`${API_BASE}/job-types`),
        ])

        const statsData = (await statsRes.json()) as StatsPayload
        const jobsData = (await jobsRes.json()) as Job[]
        const typesData = (await typesRes.json()) as string[]

        if (!mounted) return

        setStats(statsData.stats ?? {})
        setWorkers(statsData.workers ?? [])
        setJobs(jobsData)
        setJobTypes(typesData)
        setConnected(true)
      } catch {
        if (!mounted) return
        setConnected(false)
      }
    }

    load()

    const source = new EventSource(`${API_BASE}/events?sessionId=${encodeURIComponent(sessionId)}`)

    source.onopen = () => setConnected(true)
    source.onerror = () => setConnected(false)
    source.onmessage = (message) => {
      try {
        const payload = JSON.parse(message.data) as StreamEvent

        if (isStatsEvent(payload)) {
          setStats(payload.stats)
          setWorkers(payload.workers)
          return
        }

        if (payload.job) {
          const incomingJob = payload.job
          setJobs(current => [incomingJob, ...current.filter(job => job.id !== incomingJob.id)].slice(0, 8))
          setActivity(current => [
            {
              label: formatActivity(payload),
              at: new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
            },
            ...current,
          ].slice(0, 6))
        }
      } catch {
        setConnected(false)
      }
    }

    return () => {
      mounted = false
      source.close()
    }
  }, [sessionId])

  const totals = useMemo(() => {
    const enqueued = stats.total_enqueued ?? 0
    const completed = stats.total_completed ?? 0
    const failed = stats.total_failed ?? 0
    const dead = stats.total_dead ?? 0
    const active = stats.queue_active ?? 0

    const completionRate = enqueued > 0 ? Math.round((completed / enqueued) * 100) : 0
    const errorPressure = enqueued > 0 ? Math.round(((failed + dead) / enqueued) * 100) : 0

    return { enqueued, completed, failed, dead, active, completionRate, errorPressure }
  }, [stats])

  const queueCards = [
  { label: 'Critical lane', value: stats.queue_critical ?? 0, tone: 'text-[#ef4444]' },
  { label: 'High touch', value: stats.queue_high ?? 0, tone: 'text-[#38bdf8]' },
  { label: 'Steady flow', value: stats.queue_normal ?? 0, tone: 'text-[#93c5fd]' },
  { label: 'Background work', value: stats.queue_low ?? 0, tone: 'text-[#64748b]' },
  { label: 'Scheduled ahead', value: stats.queue_scheduled ?? 0, tone: 'text-[#475569]' },
]

  const visibleTypes = jobTypes.length > 0 ? jobTypes : seedJobs.map(item => item.type)

  const generateDemoJobs = async () => {
    setIsGeneratingDemo(true)
    setDemoMessage(null)

    try {
      const response = await fetch(`${API_BASE}/jobs/bulk`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
          ...sessionHeaders(sessionId),
        },
        body: JSON.stringify({
          jobs: buildDemoJobs(),
        }),
      })

      if (!response.ok) {
        throw new Error('Could not enqueue demo jobs')
      }

      const result = (await response.json()) as { count?: number }
      setDemoMessage(`Queued ${result.count ?? 0} demo jobs`)
    } catch {
      setDemoMessage('Could not reach the API for demo jobs')
    } finally {
      setIsGeneratingDemo(false)
    }
  }

  return (
    <main className="min-h-screen px-4 py-6 text-[#e5e7eb] bg-[#020617] sm:px-6 lg:px-10">
      <div className="mx-auto max-w-7xl">
        <section className="relative overflow-hidden rounded-[34px] border border-[#1f2937] bg-[#0f172a] p-6 shadow-card sm:p-8">
  

          <div className="relative grid gap-8 lg:grid-cols-[1.55fr_0.95fr]">
            <div className="space-y-6">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div className="max-w-2xl">
                  <h1 className="max-w-xl text-4xl font-semibold leading-tight text-[#e5e7eb] sm:text-5xl">
                    Queue dashboard
                  </h1>
                  <p className="mt-4 max-w-2xl text-sm leading-7 text-[#94a3b8] sm:text-base">
                    Watch work move through the system, spot pressure before it turns noisy, and keep a human
                    sense of pace across workers, retries, and finished jobs.
                  </p>
                  <div className="mt-5 flex flex-wrap items-center gap-3">
                    <button
                      type="button"
                      onClick={generateDemoJobs}
                      disabled={isGeneratingDemo}
                      className="rounded-full border border-[#1e40af] bg-[#2563eb] px-5 py-3 text-sm font-medium text-white transition hover:-translate-y-0.5 hover:bg-[#1d4ed8] disabled:cursor-not-allowed disabled:opacity-70"
                    >
                      {isGeneratingDemo ? 'Generating demo jobs...' : 'Generate demo jobs'}
                    </button>
                    <p className="text-sm text-[#93a4c2]">
                      Drops a mixed set of email, PDF, image, sync, and webhook jobs into the queue.
                    </p>
                  </div>
                  <p className="mt-3 text-xs uppercase tracking-[0.18em] text-[#7f94b8]">
                    session {sessionId.slice(0, 8)}
                  </p>
                  {demoMessage ? (
                    <p className="mt-3 text-sm text-[#bfd4ff]">{demoMessage}</p>
                  ) : null}
                </div>

                <div className="w-full max-w-xs rounded-[28px] border border-[#1f2937] bg-[#0f172a] p-4 shadow-card">
                  <p className="text-xs uppercase tracking-[0.22em] text-[#7f94b8]">Today&apos;s read</p>
                  <div className="mt-4 flex items-end justify-between gap-3">
                    <div>
                      <p className="text-4xl font-semibold text-[#e2e8f0]">{totals.completionRate}%</p>
                      <p className="text-sm text-[#8fa3c4]">completion ratio</p>
                    </div>
                    <div className="rounded-full border border-[#31415f] bg-[#16243a] px-3 py-2 text-xs text-[#aec4eb]">
                      error pressure {totals.errorPressure}%
                    </div>
                  </div>
                  <div className="mt-4 h-2 overflow-hidden rounded-full bg-[#1f2937]">
                    <div
                      className="h-full rounded-full bg-[#3b82f6] transition-all duration-500"
                      style={{ width: `${Math.min(100, Math.max(8, totals.completionRate || 8))}%` }}
                    />
                  </div>
                </div>
              </div>

              <div className="grid gap-4 sm:grid-cols-2 xl:grid-cols-4">
                <StatCard label="Total queued" value={totals.enqueued} tone="bg-[#111827]" accent="#3b82f6" />
                <StatCard label="Running now" value={totals.active} tone="bg-[#0f172a]" accent="#60a5fa" />
                <StatCard label="Retries logged" value={totals.failed} tone="bg-[#111827]" accent="#f87171" />
                <StatCard label="Dead letters" value={totals.dead} tone="bg-[#020617]" accent="#64748b" />
              </div>
            </div>

            <aside className="grid gap-4">
              <div className="rounded-[28px] border border-[#1f2937] bg-[#0f172a]/90 p-5">
                <div className="flex items-center justify-between">
                  <div>
                    <p className="text-xs uppercase tracking-[0.22em] text-[#7f94b8]">Worker mood</p>
                    <h2 className="mt-2 text-2xl font-semibold text-[#e2e8f0]">Quietly busy</h2>
                  </div>
                  <div className="rounded-full border border-[#31415f] bg-[#16243a] px-3 py-1 text-xs text-[#aec4eb]">
                    {workers.filter(worker => worker.busy).length}/{workers.length || 0} active
                  </div>
                </div>
                <div className="mt-5 space-y-3">
                  {workers.length > 0 ? (
                    workers.map(worker => <WorkerCard key={worker.id} worker={worker} />)
                  ) : (
                    <p className="rounded-2xl border border-[#31415f] bg-[#16243a] px-4 py-5 text-sm text-[#9bb0d1]">
                      Worker presence will appear here once the pool starts reporting.
                    </p>
                  )}
                </div>
              </div>

              <div className="rounded-[26px] border border-[#1f2937] bg-[#020617] p-5">
                <p className="text-xs uppercase tracking-[0.24em] text-[#7f94b8]">Queue spread</p>
                <div className="mt-4 grid grid-cols-2 gap-3">
                  {queueCards.map(card => (
                    <div key={card.label} className="rounded-2xl border border-[#1f2937] bg-[#0f172a] p-4">
                      <p className="text-xs uppercase tracking-[0.16em] text-[#7f94b8]">{card.label}</p>
                      <p className={`mt-2 text-2xl font-semibold ${card.tone}`}>{card.value}</p>
                    </div>
                  ))}
                </div>
              </div>
            </aside>
          </div>
        </section>

        <section className="mt-6 grid gap-6 xl:grid-cols-[1.15fr_0.85fr]">
          <div className="rounded-[30px] border border-[#1f2937] bg-[#0f172a]/90 p-6 shadow-card">
            <div className="flex flex-wrap items-center justify-between gap-3">
              <div>
                <p className="text-xs uppercase tracking-[0.22em] text-[#7f94b8]">Recent job history</p>
                <h2 className="mt-2 text-3xl font-semibold text-[#e2e8f0]">What just happened</h2>
              </div>
              <div className="rounded-full border border-[#31415f] bg-[#16243a] px-4 py-2 text-xs text-[#aec4eb]">
                synced from `/api/jobs`
              </div>
            </div>

            <div className="mt-5 space-y-3">
              {jobs.length > 0 ? (
                jobs.map(job => <JobRow key={job.id} job={job} />)
              ) : (
                <EmptyState copy="No job history yet. Once work lands in Postgres, the latest items will stack here." />
              )}
            </div>
          </div>

          <div className="grid gap-6">
            <div className="rounded-[28px] border border-[#1f2937] bg-[#0f172a]/90 p-6 shadow-card">
              <p className="text-xs uppercase tracking-[0.22em] text-[#7f94b8]">Live notes</p>
              <h2 className="mt-2 text-3xl font-semibold text-[#e2e8f0]">Desk scribbles</h2>
              <div className="mt-5 space-y-3">
                {activity.length > 0 ? (
                  activity.map((item, index) => (
                    <div
                      key={`${item.at}-${index}`}
                      className="flex items-start gap-3 rounded-[22px] border border-[#1f2937] bg-[#020617] px-4 py-3"
                    >
                      <div className="mt-1 h-2.5 w-2.5 rounded-full bg-[#3b82f6]" />
                      <div>
                        <p className="text-sm leading-6 text-[#cdd8ec]">{item.label}</p>
                        <p className="text-xs uppercase tracking-[0.18em] text-[#7f94b8]">{item.at}</p>
                      </div>
                    </div>
                  ))
                ) : (
                  <EmptyState copy="When SSE events arrive, this area turns into a simple running notebook instead of a noisy event feed." />
                )}
              </div>
            </div>

            <div className="rounded-[30px] border border-[#1f2937] bg-[#0b1220] p-6 shadow-card">
              <div className="flex items-center justify-between gap-3">
                <div>
                  <p className="text-xs uppercase tracking-[0.22em] text-[#7f94b8]">Coverage</p>
                  <h2 className="mt-2 text-3xl font-semibold text-[#e2e8f0]">Job catalog</h2>
                </div>
                <span className="rounded-full border border-[#31415f] bg-[#16243a] px-3 py-1 text-xs text-[#aec4eb]">
                  {visibleTypes.length} handlers
                </span>
              </div>

              <div className="mt-5 grid gap-3">
                {visibleTypes.map(type => {
                  const seeded = seedJobs.find(item => item.type === type)
                  return (
                    <div
                      key={type}
                      className="flex items-center justify-between rounded-[22px] border border-[#1f2937] bg-[#0f172a] px-4 py-4"
                    >
                      <div>
                        <p className="font-medium text-[#e2e8f0]">{prettyType(type)}</p>
                        <p className="text-sm text-[#8fa3c4]">{seeded?.note ?? 'Registered worker task type'}</p>
                      </div>
                      <span className="text-xs uppercase tracking-[0.18em] text-[#7f94b8]">ready</span>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>
        </section>
      </div>
    </main>
  )
}

function StatCard({
  label,
  value,
  tone,
  accent,
}: {
  label: string
  value: number
  tone: string
  accent: string
}) {
  return (
    <article className={`rounded-[26px] border border-[#1f2937] p-5 shadow-sm ${tone}`}>
      <div className="flex items-start justify-between gap-3">
        <div>
          <p className="text-xs uppercase tracking-[0.2em] text-[#7f94b8]">{label}</p>
          <p className="mt-3 text-4xl font-semibold text-[#e2e8f0]">{value}</p>
        </div>
        <div className="mt-1 h-11 w-11 rounded-full opacity-80" style={{ backgroundColor: accent }} />
      </div>
    </article>
  )
}

function WorkerCard({ worker }: { worker: WorkerStatus }) {
  return (
    <div className="flex items-center justify-between rounded-[22px] border border-[#1f2937] bg-[#16243a] px-4 py-3">
      <div>
        <p className="font-medium text-[#e2e8f0]">Worker {worker.index + 1}</p>
        <p className="text-sm text-[#9bb0d1]">
          {worker.lastJobType ? prettyType(worker.lastJobType) : 'Waiting for a claim'}
        </p>
      </div>
      <div className="text-right">
        <p className={`text-sm font-medium ${worker.busy ? 'text-[#93c5fd]' : 'text-[#7f94b8]'}`}>
          {worker.busy ? 'On task' : 'Idle'}
        </p>
        <p className="text-xs uppercase tracking-[0.18em] text-[#7f94b8]">{worker.processedCount} finished</p>
      </div>
    </div>
  )
}

function JobRow({ job }: { job: Job }) {
  return (
    <article className="rounded-[24px] border border-[#1f2937] bg-[#0b1220] p-4 transition-transform duration-300 hover:-translate-y-0.5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="font-medium text-[#e2e8f0]">{prettyType(job.type)}</h3>
            <span className={`rounded-full px-2.5 py-1 text-xs font-medium ${statusTone[job.status]}`}>
              {job.status}
            </span>
            <span className={`text-xs uppercase tracking-[0.18em] ${priorityTone[job.priority]}`}>
              {job.priority}
            </span>
          </div>
          <p className="mt-2 text-sm leading-6 text-[#8fa3c4]">
            {summarizePayload(job.payload)}
          </p>
        </div>

        <div className="text-right text-xs uppercase tracking-[0.18em] text-[#7f94b8]">
          <p>{formatTime(job)}</p>
          <p className="mt-2 normal-case tracking-normal text-[#9bb0d1]">
            attempt {job.attempts}/{job.maxAttempts}
          </p>
        </div>
      </div>

      <div className="mt-3 flex flex-wrap items-center justify-between gap-3 border-t border-[#1f2937] pt-3 text-sm text-[#94a3b8]">
        <p className="font-mono text-xs text-[#7f94b8]">{job.id.slice(0, 8)}</p>
        <p>{job.worker_id ?? job.workerId ?? 'Awaiting assignment'}</p>
      </div>

      {job.error ? (
        <p className="mt-3 rounded-2xl bg-[#3f1d2e] px-3 py-2 text-sm text-[#fecaca]">
          {job.error}
        </p>
      ) : null}
    </article>
  )
}

function EmptyState({ copy }: { copy: string }) {
  return <div className="rounded-[24px] border border-dashed border-[#31415f] bg-[#16243a] px-4 py-8 text-sm text-[#9bb0d1]">{copy}</div>
}

function prettyType(value: string) {
  return value.replace(/_/g, ' ')
}

function summarizePayload(payload: Record<string, unknown>) {
  const entries = Object.entries(payload).slice(0, 3)
  if (entries.length === 0) return 'No payload details were attached.'
  return entries.map(([key, value]) => `${key}: ${String(value)}`).join('  •  ')
}

function formatActivity(payload: StreamEvent) {
  if (isStatsEvent(payload) || !payload.job) return 'Stats updated'
  const action = payload.event.replace('job:', '').replace(/_/g, ' ')
  return `${prettyType(payload.job.type)} ${action}`
}

function isStatsEvent(payload: StreamEvent): payload is StatsEvent {
  return payload.event === 'stats'
}

function formatTime(job: Job) {
  const value =
    job.completed_at ??
    job.completedAt ??
    job.failed_at ??
    job.failedAt ??
    job.started_at ??
    job.startedAt ??
    job.created_at ??
    job.createdAt

  if (!value) return 'No timestamp'

  const date = typeof value === 'string' ? new Date(value) : new Date(value)
  return date.toLocaleString([], {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function buildDemoJobs() {
  const stamp = Date.now()

  return [
    {
      type: 'send_email',
      priority: 'high',
      payload: {
        to: 'new-user@example.com',
        subject: 'Welcome aboard',
        template: 'onboarding',
      },
    },
    {
      type: 'generate_pdf',
      priority: 'normal',
      payload: {
        documentId: `invoice-${stamp}`,
        title: 'Weekly Ops Summary',
        pages: 6,
      },
    },
    {
      type: 'resize_image',
      priority: 'low',
      payload: {
        imageUrl: 'https://images.unsplash.com/photo-1497366754035-f200968a6e72',
        width: 1280,
        height: 720,
        format: 'webp',
      },
    },
    {
      type: 'sync_data',
      priority: 'critical',
      payload: {
        source: 'crm',
        recordCount: 220,
      },
    },
    {
      type: 'webhook',
      priority: 'normal',
      payload: {
        url: 'https://example.org/hooks/order-updated',
        event: 'order.updated',
        data: {
          orderId: `ord-${stamp}`,
          status: 'packed',
        },
      },
    },
    {
      type: 'send_email',
      priority: 'normal',
      payload: {
        to: 'finance@example.com',
        subject: 'Billing digest',
        template: 'digest',
      },
    },
  ]
}

export default App

function getOrCreateSessionId() {
  const existing = window.localStorage.getItem(SESSION_STORAGE_KEY)
  if (existing) return existing

  const created = window.crypto.randomUUID()
  window.localStorage.setItem(SESSION_STORAGE_KEY, created)
  return created
}

function sessionHeaders(sessionId: string) {
  return { 'x-session-id': sessionId }
}
