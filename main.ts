import { Job, Queue, QueueScheduler, Worker } from 'bullmq'


(async () => {
    // Make sure Redis is running
    await verifyRedis()
    
    // Create a queue
    const queueName = 'process-queue'
    const queueOptions = { defaultJobOptions: {
      removeOnComplete: true
    }}
    const queue = new Queue(queueName, queueOptions)

    // Add a few jobs to the queue
    const outDir = createOutputDirectory()
    await addJobToQueue({ command: 'youtube-dl', args: ['--output', `${outDir}/first`, '-f', '"bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"', 'https://www.youtube.com/watch?v=8XAHY3braUA'] }, queue)
    await addJobToQueue({ command: 'youtube-dl', args: ['--output', `${outDir}/second`, '-f', '"bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"', 'https://www.youtube.com/watch?v=S24KpEHXdEw'] }, queue)
    await addJobToQueue({ command: 'youtube-dl', args: ['--output', `${outDir}/third`, '-f', '"bestvideo[ext=mp4]+bestaudio[ext=m4a]/best"', 'https://www.youtube.com/watch?v=RoPANT6bYAw'] }, queue)

    // The docs say:
    // Jobs that get rate limited will actually end as delayed jobs, so you need at least one QueueScheduler 
    // somewhere in your deployment so that jobs are put back to the wait status.
    const scheduler = new QueueScheduler(queueName)

    // Process the jobs
    const worker = new Worker(queueName, async (job: Job) => {
      // Do something with job
      console.log("Processing job: ", job.id)
      return execProcess(job.data.command, job.data.args)
    })
    
    // Listen for results
    let lastJob = false
    worker.on("progress", async (job: Job, progress: number | object) => {
      // Do something with the return value.
      console.log("In progress: ", progress)
      console.log("Jobs completed: ", await queue.getCompletedCount())
      console.log("Jobs waiting to be processed: ", await queue.count())
    })
    worker.on("completed", async (job: Job, returnvalue: any) => {
      // Do something with the return value.
      console.log("Completed: ", returnvalue)
      console.log("Jobs completed: ", await queue.getCompletedCount())
      const jobsToBeProcessed = await queue.count()
      console.log("Jobs waiting to be processed: ", jobsToBeProcessed)

      // If there are no more jobs, exit OK
      // TODO this is quite hacky, improve
      if (jobsToBeProcessed === 0) {
        if (lastJob) {
          // I've finished processing, exit
          process.exit(0)
        } else {
          // Mark that the next sucessful job should be the last one
          lastJob = true
        }
      }
    })
    worker.on("failed", async (job: Job, failedReason: string) => {
      // Do something with the return value.
      console.error("Failed: ", failedReason)
      console.log("Jobs completed: ", await queue.getCompletedCount())
      console.log("Jobs waiting to be processed: ", await queue.count())
    })
  }
)()

async function verifyRedis() {
  const { exec } = require("child_process")
  exec('redis-cli ping', (error, stdout, stderr) => {
    if (error || stderr) {
      console.error("Something went wrong with Redis")
      process.exit(1)
    } else if (stdout && !stdout.toString().includes("PONG")) {
      console.error("PONG not received from Redis")
      process.exit(2)
    }
  })
}

function createOutputDirectory() {
  const fs = require('fs')
  const outputDirName = `output/${Date.now()}`
  if (!fs.existsSync(outputDirName)){
    fs.mkdirSync(outputDirName, { recursive: true })
  }
  return outputDirName
}

async function addJobToQueue(job, queue: Queue) {
  await queue.add(queue.name, job)
}

async function execProcess(command: string, args: Array<string>): Promise<string> {
  return new Promise((resolve, reject) => {
      const { exec } = require("child_process")
      exec(`${command} ${args.join(" ")}`, (error, stdout, stderr) => {
      if (error) {
          reject(error)
      } else if (stderr) {
          reject(stderr)
      } else {
          resolve(stdout)
      }
      })
  })
}

