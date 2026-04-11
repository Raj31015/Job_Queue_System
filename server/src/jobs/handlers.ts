import { Job } from '../queue/types'
import { logger } from '../logger'

type JobHandler = (job: Job) => Promise<unknown>

// Simulate async work with realistic processing time
const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))

const sendEmailHandler: JobHandler = async (job) => {
  const { to, subject = 'Notification', template = 'default' } = job.payload as {
    to: string; subject?: string; template?: string
  }

  logger.info(`[job:send_email] sending to ${to}`)
  await sleep(800 + Math.random() * 1200) // Simulate SMTP call

  // Simulate occasional failures (10% chance) for demo purposes
  if (Math.random() < 0.1) throw new Error('SMTP connection timeout')

  return {
    messageId: `msg-${Date.now()}`,
    to,
    subject,
    template,
    sentAt: new Date().toISOString(),
  }
}

const generatePdfHandler: JobHandler = async (job) => {
  const { documentId, title = 'Report', pages = 5 } = job.payload as {
    documentId: string; title?: string; pages?: number
  }

  logger.info(`[job:generate_pdf] generating "${title}" (${pages} pages)`)
  await sleep(1500 + pages * 300) // More pages = more time

  if (Math.random() < 0.05) throw new Error('PDF renderer out of memory')

  return {
    documentId,
    filename: `${title.toLowerCase().replace(/\s+/g, '-')}-${Date.now()}.pdf`,
    pages,
    sizeKb: pages * 42 + Math.floor(Math.random() * 100),
    generatedAt: new Date().toISOString(),
  }
}

const resizeImageHandler: JobHandler = async (job) => {
  const { imageUrl, width = 800, height = 600, format = 'webp' } = job.payload as {
    imageUrl: string; width?: number; height?: number; format?: string
  }

  logger.info(`[job:resize_image] resizing to ${width}x${height} (${format})`)
  await sleep(500 + Math.random() * 1000)

  if (Math.random() < 0.08) throw new Error('Image decoding failed: unsupported format')

  return {
    originalUrl: imageUrl,
    outputUrl: `https://cdn.example.com/images/resized/${Date.now()}.${format}`,
    width,
    height,
    format,
    processedAt: new Date().toISOString(),
  }
}

const syncDataHandler: JobHandler = async (job) => {
  const { source, recordCount = 100 } = job.payload as {
    source: string; recordCount?: number
  }

  logger.info(`[job:sync_data] syncing ${recordCount} records from ${source}`)
  await sleep(2000 + recordCount * 10)

  return {
    source,
    synced: recordCount,
    skipped: Math.floor(recordCount * 0.05),
    errors: 0,
    completedAt: new Date().toISOString(),
  }
}

const webhookHandler: JobHandler = async (job) => {
  const { url, event, data } = job.payload as {
    url: string; event: string; data: unknown
  }

  logger.info(`[job:webhook] delivering ${event} to ${url}`)
  await sleep(300 + Math.random() * 700)

  if (Math.random() < 0.15) throw new Error('Webhook endpoint returned 503')

  return {
    url,
    event,
    statusCode: 200,
    deliveredAt: new Date().toISOString(),
  }
}

// Registry — add new job types here
export const jobHandlers: Record<string, JobHandler> = {
  send_email:    sendEmailHandler,
  generate_pdf:  generatePdfHandler,
  resize_image:  resizeImageHandler,
  sync_data:     syncDataHandler,
  webhook:       webhookHandler,
}

export type JobType = keyof typeof jobHandlers