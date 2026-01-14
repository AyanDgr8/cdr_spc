// jobManager.js - Background job processing system

import { Worker } from 'worker_threads';
import path from 'path';
import { fileURLToPath } from 'url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

class JobManager {
  constructor() {
    this.jobs = new Map(); // jobId -> job details
    this.workers = new Map(); // jobId -> worker instance
  }

  // Start a background job
  startJob(jobId, jobType, params) {
    if (this.jobs.has(jobId)) {
      console.log(`⚠️ Job ${jobId} already exists. Canceling existing job and starting a new one.`);
      this.cancelJob(jobId);
    }

    const job = {
      id: jobId,
      type: jobType,
      status: 'running',
      progress: 0,
      startTime: new Date(),
      params: params,
      result: null,
      error: null
    };

    this.jobs.set(jobId, job);

    // Select appropriate worker based on job type
    let workerPath;
    
    if (jobType === 'finalReport' || jobType === 'populateFinalReport') {
      // Use new finalReportWorker for final report generation
      workerPath = path.join(__dirname, 'finalReportWorker.js');
      console.log(`🚀 Starting finalReportWorker for job ${jobId}`);
    } else {
      // Use legacy worker for other job types (if needed)
      workerPath = path.join(__dirname, 'dataFetchWorker.js');
      console.log(`🚀 Starting dataFetchWorker for job ${jobId}`);
    }
    
    const worker = new Worker(workerPath, {
      workerData: { jobId, jobType, params }
    });

    this.workers.set(jobId, worker);

    // Handle worker messages
    worker.on('message', (message) => {
      const job = this.jobs.get(jobId);
      if (!job) return;

      switch (message.type) {
        case 'progress':
          job.progress = message.progress;
          job.status = 'running';
          console.log(`📊 Job ${jobId} progress: ${message.progress}%`);
          break;
        
        case 'success':
          job.status = 'completed';
          job.progress = 100;
          job.result = message.result;
          job.endTime = new Date();
          console.log(`✅ Job ${jobId} completed successfully`);
          this.cleanupWorker(jobId);
          break;
        
        case 'error':
          job.status = 'failed';
          job.error = message.error;
          job.endTime = new Date();
          console.error(`❌ Job ${jobId} failed:`, message.error);
          this.cleanupWorker(jobId);
          break;
      }
    });

    worker.on('error', (error) => {
      const job = this.jobs.get(jobId);
      if (job) {
        job.status = 'failed';
        job.error = error.message;
        job.endTime = new Date();
      }
      console.error(`❌ Worker error for job ${jobId}:`, error);
      this.cleanupWorker(jobId);
    });

    worker.on('exit', (code) => {
      if (code !== 0) {
        const job = this.jobs.get(jobId);
        if (job && job.status === 'running') {
          job.status = 'failed';
          job.error = `Worker exited with code ${code}`;
          job.endTime = new Date();
        }
      }
      this.cleanupWorker(jobId);
    });

    return job;
  }

  // Get job status
  getJob(jobId) {
    return this.jobs.get(jobId);
  }

  // Get all jobs
  getAllJobs() {
    return Array.from(this.jobs.values());
  }

  // Cancel a job
  cancelJob(jobId) {
    const worker = this.workers.get(jobId);
    const job = this.jobs.get(jobId);
    
    if (worker) {
      worker.terminate();
    }
    
    if (job) {
      job.status = 'cancelled';
      job.endTime = new Date();
    }
    
    this.cleanupWorker(jobId);
    return job;
  }

  // Cleanup completed/failed jobs older than 1 hour
  cleanupOldJobs() {
    const oneHourAgo = new Date(Date.now() - 60 * 60 * 1000);
    
    for (const [jobId, job] of this.jobs.entries()) {
      if (job.endTime && job.endTime < oneHourAgo) {
        this.jobs.delete(jobId);
        console.log(`🧹 Cleaned up old job: ${jobId}`);
      }
    }
  }

  // Private method to cleanup worker
  cleanupWorker(jobId) {
    const worker = this.workers.get(jobId);
    if (worker) {
      this.workers.delete(jobId);
    }
  }

  // Check if job exists and is running
  isJobRunning(jobId) {
    const job = this.jobs.get(jobId);
    return job && job.status === 'running';
  }
}

// Singleton instance
const jobManager = new JobManager();

// Cleanup old jobs every 30 minutes
setInterval(() => {
  jobManager.cleanupOldJobs();
}, 30 * 60 * 1000);

export default jobManager;
