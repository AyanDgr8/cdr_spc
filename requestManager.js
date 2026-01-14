// requestManager.js
// Handles concurrent request management and user session isolation

import crypto from 'crypto';

class RequestManager {
  constructor() {
    this.activeRequests = new Map(); // sessionId -> requestInfo
    this.requestQueue = [];
    this.isProcessing = false;
    this.maxConcurrentRequests = 1; // Only allow 1 API fetch at a time
    this.sessionTimeout = 30 * 60 * 1000; // 30 minutes
  }

  // Generate unique session ID for each request
  generateSessionId(userId, timestamp) {
    return crypto.createHash('md5')
      .update(`${userId}_${timestamp}_${Math.random()}`)
      .digest('hex')
      .substring(0, 12);
  }

  // Check if user already has an active request
  hasActiveRequest(userId) {
    for (const [sessionId, requestInfo] of this.activeRequests) {
      if (requestInfo.userId === userId && requestInfo.status === 'processing') {
        return { active: true, sessionId, startTime: requestInfo.startTime };
      }
    }
    return { active: false };
  }

  // Add request to queue or start immediately
  async queueRequest(userId, params, fetchFunction) {
    const sessionId = this.generateSessionId(userId, Date.now());
    
    // Check for existing active request
    const existingRequest = this.hasActiveRequest(userId);
    if (existingRequest.active) {
      const waitTime = Math.round((Date.now() - existingRequest.startTime) / 1000);
      throw new Error(`User already has an active request (Session: ${existingRequest.sessionId}, Running for: ${waitTime}s). Please wait for completion.`);
    }

    const requestInfo = {
      sessionId,
      userId,
      params,
      fetchFunction,
      status: 'queued',
      queuedAt: Date.now(),
      startTime: null,
      endTime: null
    };

    this.activeRequests.set(sessionId, requestInfo);
    this.requestQueue.push(sessionId);

    console.log(`📋 Request queued - Session: ${sessionId}, User: ${userId}, Queue position: ${this.requestQueue.length}`);

    // Start processing if not already processing
    if (!this.isProcessing) {
      this.processQueue();
    }

    return sessionId;
  }

  // Process requests from queue
  async processQueue() {
    if (this.isProcessing || this.requestQueue.length === 0) {
      return;
    }

    this.isProcessing = true;

    while (this.requestQueue.length > 0) {
      const sessionId = this.requestQueue.shift();
      const requestInfo = this.activeRequests.get(sessionId);

      if (!requestInfo) {
        continue; // Request was cancelled
      }

      try {
        console.log(`🚀 Processing request - Session: ${sessionId}, User: ${requestInfo.userId}`);
        
        requestInfo.status = 'processing';
        requestInfo.startTime = Date.now();

        // Execute the actual fetch function with session isolation
        const result = await requestInfo.fetchFunction(requestInfo.params, sessionId);

        requestInfo.status = 'completed';
        requestInfo.endTime = Date.now();
        requestInfo.result = result;

        const duration = Math.round((requestInfo.endTime - requestInfo.startTime) / 1000);
        console.log(`✅ Request completed - Session: ${sessionId}, Duration: ${duration}s`);

      } catch (error) {
        console.error(`❌ Request failed - Session: ${sessionId}:`, error.message);
        
        requestInfo.status = 'failed';
        requestInfo.endTime = Date.now();
        requestInfo.error = error.message;
      }
    }

    this.isProcessing = false;
  }

  // Get request status
  getRequestStatus(sessionId) {
    const requestInfo = this.activeRequests.get(sessionId);
    if (!requestInfo) {
      return { status: 'not_found' };
    }

    const response = {
      sessionId: requestInfo.sessionId,
      userId: requestInfo.userId,
      status: requestInfo.status,
      queuedAt: requestInfo.queuedAt
    };

    if (requestInfo.startTime) {
      response.startTime = requestInfo.startTime;
      response.duration = Math.round((Date.now() - requestInfo.startTime) / 1000);
    }

    if (requestInfo.endTime) {
      response.endTime = requestInfo.endTime;
      response.totalDuration = Math.round((requestInfo.endTime - requestInfo.startTime) / 1000);
    }

    if (requestInfo.error) {
      response.error = requestInfo.error;
    }

    if (requestInfo.result) {
      response.result = requestInfo.result;
    }

    return response;
  }

  // Clean up old completed/failed requests
  cleanup() {
    const now = Date.now();
    for (const [sessionId, requestInfo] of this.activeRequests) {
      const age = now - requestInfo.queuedAt;
      
      if (age > this.sessionTimeout && 
          (requestInfo.status === 'completed' || requestInfo.status === 'failed')) {
        this.activeRequests.delete(sessionId);
        console.log(`🧹 Cleaned up old session: ${sessionId}`);
      }
    }
  }

  // Get queue status
  getQueueStatus() {
    return {
      isProcessing: this.isProcessing,
      queueLength: this.requestQueue.length,
      activeRequests: this.activeRequests.size,
      requests: Array.from(this.activeRequests.values()).map(req => ({
        sessionId: req.sessionId,
        userId: req.userId,
        status: req.status,
        queuedAt: req.queuedAt,
        startTime: req.startTime
      }))
    };
  }

  // Cancel a request (if still queued)
  cancelRequest(sessionId, userId) {
    const requestInfo = this.activeRequests.get(sessionId);
    
    if (!requestInfo) {
      return { success: false, message: 'Request not found' };
    }

    if (requestInfo.userId !== userId) {
      return { success: false, message: 'Unauthorized to cancel this request' };
    }

    if (requestInfo.status === 'processing') {
      return { success: false, message: 'Cannot cancel request that is already processing' };
    }

    if (requestInfo.status === 'queued') {
      // Remove from queue
      const queueIndex = this.requestQueue.indexOf(sessionId);
      if (queueIndex > -1) {
        this.requestQueue.splice(queueIndex, 1);
      }
    }

    this.activeRequests.delete(sessionId);
    return { success: true, message: 'Request cancelled successfully' };
  }

  // Simple session-based request tracking methods
  isRequestInProgress(sessionId) {
    const requestInfo = this.activeRequests.get(sessionId);
    return requestInfo && (requestInfo.status === 'processing' || requestInfo.status === 'queued');
  }

  startRequest(sessionId, requestType, params) {
    this.activeRequests.set(sessionId, {
      sessionId,
      requestType,
      params,
      status: 'processing',
      startTime: Date.now()
    });
    console.log(`🚀 Started ${requestType} request for session: ${sessionId}`);
  }

  completeRequest(sessionId) {
    const requestInfo = this.activeRequests.get(sessionId);
    if (requestInfo) {
      requestInfo.status = 'completed';
      requestInfo.endTime = Date.now();
      const duration = Math.round((requestInfo.endTime - requestInfo.startTime) / 1000);
      console.log(`✅ Completed request for session: ${sessionId}, Duration: ${duration}s`);
      
      // Clean up after 15 minutes
      setTimeout(() => {
        this.activeRequests.delete(sessionId);
      }, 15 * 60 * 1000);
    }
  }
}

// Singleton instance
const requestManager = new RequestManager();

// Cleanup old sessions every 5 minutes
setInterval(() => {
  requestManager.cleanup();
}, 5 * 60 * 1000);

export default requestManager;
