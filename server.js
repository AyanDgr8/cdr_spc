// server.js

import express from 'express';
import path from 'path';
import { fileURLToPath } from 'url';
import dotenv from 'dotenv';
import https from 'https';
import fs from 'fs';
// import { fetchAgentStatus } from './agentStatus.js';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import { createUnifiedReportFromDB, fetchReport } from './reportFetcher.js';
import { getPortalToken, httpsAgent } from './tokenService.js';
import requestManager from './requestManager.js';
// import sessionDbService from './sessionDbService.js';
import jobManager from './jobManager.js';
import axios from 'axios';
import { parseBuffer } from 'music-metadata';
import cookieParser from 'cookie-parser';
import jwt from 'jsonwebtoken';
import bcrypt from 'bcrypt';
import mysql from 'mysql2/promise';
import dbService, { checkDataExists, clearCache } from './dbService.js';
import finalReportService from './finalReportService.js';
import { DateTime } from 'luxon';

dotenv.config();

// Debug: Log environment variables to verify they're loaded correctly
console.log(' Environment variables loaded:');
console.log(`   PORT: ${process.env.PORT}`);
console.log(`   HOST: ${process.env.HOST}`);
console.log(`   PUBLIC_URL: ${process.env.PUBLIC_URL}`);

const app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(cookieParser());

// Session management middleware - commented out as requested
// app.use((req, res, next) => {
//   // Generate session ID if not exists
//   if (!req.cookies.sessionId) {
//     const sessionId = 'sess_' + Math.random().toString(36).substr(2, 9) + '_' + Date.now();
//     res.cookie('sessionId', sessionId, { 
//       httpOnly: true, 
//       secure: process.env.NODE_ENV === 'production',
//       maxAge: 30 * 60 * 1000 // 30 minutes
//     });
//     req.sessionId = sessionId;
//     console.log(`🆕 New session created: ${sessionId}`);
//   } else {
//     req.sessionId = req.cookies.sessionId;
//     console.log(`🔄 Existing session: ${req.sessionId}`);
//   }
//   next();
// });

// Add a simple middleware to ensure req.sessionId exists for backward compatibility
// app.use((req, res, next) => {
//   req.sessionId = 'no_session_' + Date.now();
//   next();
// });

const PORT = process.env.PORT || 9567;
const HOST = process.env.HOST || '0.0.0.0'; // 0.0.0.0 ensures the server binds to all network interfaces
const PUBLIC_URL = process.env.PUBLIC_URL || `https://${HOST}:${PORT}`;

console.log(` Server will start on: ${PUBLIC_URL}`);

// Helper to resolve __dirname in ES modules
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

app.use(express.static(path.join(__dirname, 'public')));

// --- Authentication setup ---
const JWT_SECRET = process.env.JWT_SECRET || 'dev_secret_key';

// Create MySQL connection pool
// const pool = mysql.createPool({
//   host:  'localhost',
//   user: 'root',
//   password: 'Ayan@1012',
//   database: 'spc_main_cdr',
//   port: 3306,
//   waitForConnections: true,
//   connectionLimit: 10,
//   queueLimit: 0
// });

const pool = mysql.createPool({
  host:"0.0.0.0",
  user: 'multycomm',
  password: 'WELcome@123',
  database: 'spc_main_cdr',
  port: 3306,
  waitForConnections: true,
  connectionLimit: 20,
  queueLimit: 0
});

// Login
app.post('/api/login', async (req, res) => {
  const { username, password } = req.body || {};
  if (!username || !password) return res.status(400).json({ error: 'Username and password required' });
  try {
    const [rows] = await pool.query(
      'SELECT id, username, email, password FROM users WHERE username = ? OR email = ? LIMIT 1',
      [username, username]
    );
    if (!rows.length) return res.status(401).json({ error: 'Invalid credentials' });
    const user = rows[0];
    const ok = await bcrypt.compare(password, user.password);
    if (!ok) return res.status(401).json({ error: 'Invalid credentials' });

    await pool.query('UPDATE users SET last_login = NOW() WHERE id = ?', [user.id]);

    const token = jwt.sign({ id: user.id, username: user.username, email: user.email }, JWT_SECRET, { expiresIn: '2h' });
    res.cookie('token', token, { httpOnly: true, sameSite: 'lax', maxAge: 2 * 60 * 60 * 1000 });
    res.json({ success: true, token }); // Include token in response body for iframe scenarios
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: 'Server error' });
  }
});

// Auth check
app.get('/api/auth/check', (req, res) => {
  // Check for token in cookie first
  const cookieToken = req.cookies?.token;
  
  // Then check for Authorization header (Bearer token)
  const authHeader = req.headers.authorization;
  const bearerToken = authHeader && authHeader.startsWith('Bearer ') ? authHeader.substring(7) : null;
  
  // Use either token source
  const token = cookieToken || bearerToken;
  
  if (!token) return res.json({ authenticated: false });
  
  try {
    const decoded = jwt.verify(token, JWT_SECRET);
    res.json({ authenticated: true, user: { id: decoded.id, username: decoded.username } });
  } catch {
    res.json({ authenticated: false });
  }
});

// Logout
app.post('/api/logout', (req, res) => {
  res.clearCookie('token');
  res.json({ success: true });
});

// Database population endpoints

// Populate all raw tables with data from all API endpoints
app.post('/api/db/populate', async (req, res) => {
  try {
    const { account, start_date, end_date } = req.body;
    
    if (!account) {
      return res.status(400).json({ error: 'Missing account parameter' });
    }
    
    // Parse dates and convert to epoch milliseconds if provided
    const params = {};
    if (start_date) {
      params.startDate = Date.parse(start_date);
    }
    if (end_date) {
      params.endDate = Date.parse(end_date);
    }
    
    console.log(`🚀 Starting comprehensive database population for account ${account}`, params);
    
    // Use the new comprehensive data fetcher for better performance
    const results = await fetchAndStoreAllDataSequentially(account, params);
    
    res.json({
      success: true,
      message: 'Comprehensive database population completed successfully',
      results,
      summary: {
        totalEndpoints: Object.keys(results).length,
        totalFetched: Object.values(results).reduce((sum, r) => sum + r.fetched, 0),
        totalStored: Object.values(results).reduce((sum, r) => sum + r.stored, 0),
        errors: Object.values(results).filter(r => r.error).length
      }
    });
  } catch (error) {
    console.error('❌ Error in comprehensive database population:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Populate database with incremental data since last update
app.post('/api/db/populate/incremental', async (req, res) => {
  try {
    const { account } = req.body;
    
    if (!account) {
      return res.status(400).json({ error: 'Missing account parameter' });
    }
    
    console.log(`Starting incremental database population for account ${account}`);
    
    // Start the incremental population process
    const results = await dataPopulator.populateIncrementalData();
    
    res.json({
      success: true,
      message: 'Incremental database population completed',
      results
    });
  } catch (error) {
    console.error('Error in incremental database population:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Populate specific table with data from API
app.post('/api/db/populate/:type', async (req, res) => {
  try {
    const { type } = req.params;
    const { account, start_date, end_date } = req.body;
    
    if (!account) {
      return res.status(400).json({ error: 'Missing account parameter' });
    }
    
    // Parse dates if provided
    let startDate = start_date ? new Date(start_date) : null;
    let endDate = end_date ? new Date(end_date) : null;
    
    console.log(`Starting database population for ${type} with account ${account} from ${startDate} to ${endDate}`);
    
    let result;
    switch (type) {
      case 'queue_calls':
        result = await dataPopulator.populateQueueCalls({ account, start_date: startDate, end_date: endDate });
        break;
      case 'queue_outbound_calls':
        result = await dataPopulator.populateQueueOutboundCalls({ account, start_date: startDate, end_date: endDate });
        break;
      case 'campaign_activities':
        result = await dataPopulator.populateCampaignActivities({ account, start_date: startDate, end_date: endDate });
        break;
      default:
        return res.status(400).json({ error: `Invalid type: ${type}` });
    }
    
    res.json({
      success: true,
      message: `Database population for ${type} completed`,
      count: result.length
    });
  } catch (error) {
    console.error(`Error in database population for ${req.params.type}:`, error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Force populate specific endpoint
app.post('/api/db/populate/:endpoint', async (req, res) => {
  try {
    const { endpoint } = req.params;
    const { account, start_date, end_date } = req.body;
    
    if (!account) {
      return res.status(400).json({ error: 'Missing account parameter' });
    }
    
    const validEndpoints = ['queueCalls', 'queueOutboundCalls', 'campaignsActivity'];
    if (!validEndpoints.includes(endpoint)) {
      return res.status(400).json({ error: `Invalid endpoint. Must be one of: ${validEndpoints.join(', ')}` });
    }
    
    // Parse dates and convert to epoch milliseconds if provided
    const params = {};
    if (start_date) {
      params.startDate = Date.parse(start_date);
    }
    if (end_date) {
      params.endDate = Date.parse(end_date);
    }
    
    console.log(`🎯 Force populating ${endpoint} for account ${account}`, params);
    
    // Use the apiDataFetcher for the specific endpoint
    const { fetchAllRecordsFromEndpoint } = await import('./apiDataFetcher.js');
    const result = await fetchAllRecordsFromEndpoint(endpoint, account, params);
    
    res.json({
      success: true,
      message: 'Unified report generated successfully',
      data: result.data,
      summary: result.summary,
      fetchResults: {
        fromCache: false,
        totalTime: 0,
        results: result
      },
      fromCache: false,
      cacheStatus: 'MISS',
      params
    });
  } catch (error) {
    console.error(`❌ Error populating ${req.params.endpoint}:`, error);
    res.status(500).json({
      success: false,
      error: error.message,
      endpoint: req.params.endpoint
    });
  }
});

app.post('/api/db/clear', async (req, res) => {
  try {
    const result = await clearCache();
    
    res.json({
      success: true,
      message: 'Cache memory cleared successfully',
      totalCleared: result.totalCleared,
      breakdown: result.breakdown
    });
  } catch (error) {
    console.error('❌ Error clearing cache:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Database statistics endpoint
app.get('/api/db/stats', async (req, res) => {
  try {
    const stats = {};
    
    // Get counts from each raw table
    // const [rawCdrsCount] = await dbService.query('SELECT COUNT(*) as count FROM raw_cdrs');
    // const [rawCdrsAllCount] = await dbService.query('SELECT COUNT(*) as count FROM raw_cdrs_all');
    const [rawQueueInboundCount] = await dbService.query('SELECT COUNT(*) as count FROM raw_queue_inbound');
    const [rawQueueOutboundCount] = await dbService.query('SELECT COUNT(*) as count FROM raw_queue_outbound');
    const [rawCampaignsCount] = await dbService.query('SELECT COUNT(*) as count FROM raw_campaigns');
    
    // Get latest timestamps from each raw table
    // const [rawCdrsLatest] = await dbService.query('SELECT MAX(timestamp) as latest FROM raw_cdrs');
    // const [rawCdrsAllLatest] = await dbService.query('SELECT MAX(timestamp) as latest FROM raw_cdrs_all');
    const [rawQueueInboundLatest] = await dbService.query('SELECT MAX(called_time) as latest FROM raw_queue_inbound');
    const [rawQueueOutboundLatest] = await dbService.query('SELECT MAX(called_time) as latest FROM raw_queue_outbound');
    const [rawCampaignsLatest] = await dbService.query('SELECT MAX(timestamp) as latest FROM raw_campaigns');
    
    // Get earliest timestamps for data range info
    // const [rawCdrsEarliest] = await dbService.query('SELECT MIN(timestamp) as earliest FROM raw_cdrs');
    // const [rawCdrsAllEarliest] = await dbService.query('SELECT MIN(timestamp) as earliest FROM raw_cdrs_all');
    const [rawQueueInboundEarliest] = await dbService.query('SELECT MIN(called_time) as earliest FROM raw_queue_inbound');
    const [rawQueueOutboundEarliest] = await dbService.query('SELECT MIN(called_time) as earliest FROM raw_queue_outbound');
    const [rawCampaignsEarliest] = await dbService.query('SELECT MIN(timestamp) as earliest FROM raw_campaigns');
    
    stats.counts = {
      // raw_cdrs: rawCdrsCount[0].count,
      // raw_cdrs_all: rawCdrsAllCount[0].count,
      raw_queue_inbound: rawQueueInboundCount[0].count,
      raw_queue_outbound: rawQueueOutboundCount[0].count,
      raw_campaigns: rawCampaignsCount[0].count
    };
    
    stats.latest = {
      // raw_cdrs: rawCdrsLatest[0].latest ? new Date(Number(rawCdrsLatest[0].latest)).toISOString() : null,
      // raw_cdrs_all: rawCdrsAllLatest[0].latest ? new Date(Number(rawCdrsAllLatest[0].latest)).toISOString() : null,
      raw_queue_inbound: rawQueueInboundLatest[0].latest ? new Date(Number(rawQueueInboundLatest[0].latest)).toISOString() : null,
      raw_queue_outbound: rawQueueOutboundLatest[0].latest ? new Date(Number(rawQueueOutboundLatest[0].latest)).toISOString() : null,
      raw_campaigns: rawCampaignsLatest[0].latest ? new Date(Number(rawCampaignsLatest[0].latest)).toISOString() : null
    };
    
    stats.earliest = {
      // raw_cdrs: rawCdrsEarliest[0].earliest ? new Date(Number(rawCdrsEarliest[0].earliest)).toISOString() : null,
      // raw_cdrs_all: rawCdrsAllEarliest[0].earliest ? new Date(Number(rawCdrsAllEarliest[0].earliest)).toISOString() : null,
      raw_queue_inbound: rawQueueInboundEarliest[0].earliest ? new Date(Number(rawQueueInboundEarliest[0].earliest)).toISOString() : null,
      raw_queue_outbound: rawQueueOutboundEarliest[0].earliest ? new Date(Number(rawQueueOutboundEarliest[0].earliest)).toISOString() : null,
      raw_campaigns: rawCampaignsEarliest[0].earliest ? new Date(Number(rawCampaignsEarliest[0].earliest)).toISOString() : null
    };
    
    // Calculate total records across all tables
    const totalRecords = Object.values(stats.counts).reduce((sum, count) => sum + count, 0);
    
    stats.summary = {
      totalRecords,
      tablesWithData: Object.values(stats.counts).filter(count => count > 0).length,
      isEmpty: totalRecords === 0
    };
    
    res.json(stats);
  } catch (error) {
    console.error('Error getting database statistics:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Job status endpoint
app.get('/api/jobs/:jobId/status', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.getJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json({
    id: job.id,
    status: job.status,
    progress: job.progress,
    startTime: job.startTime,
    endTime: job.endTime,
    error: job.error
  });
});

// Endpoint to manually trigger final report population
app.post('/api/reports/final-report/populate', async (req, res) => {
  const { account, startDate, endDate, filters } = req.body;
  
  if (!startDate || !endDate) {
    return res.status(400).json({ error: 'Missing required startDate and endDate parameters' });
  }
  
  try {
    // Generate a unique job ID
    const jobId = `final_report_manual_${Date.now()}`;
    
    // Start a background job to populate the final_report table
    const jobParams = {
      tenant: account || 'default',
      startDate,
      endDate,
      filters
    };
    
    const job = jobManager.startJob(jobId, 'finalReport', jobParams);
    
    res.json({
      success: true,
      message: 'Final report population job started',
      jobId: job.id,
      status: job.status
    });
  } catch (error) {
    console.error('Error starting final report population job:', error);
    res.status(500).json({
      success: false,
      error: error.message
    });
  }
});

// Get job result endpoint
app.get('/api/jobs/:jobId/result', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.getJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  if (job.status === 'running') {
    return res.status(202).json({ 
      message: 'Job still in progress',
      status: job.status,
      progress: job.progress
    });
  }
  
  if (job.status === 'failed') {
    return res.status(500).json({ 
      error: 'Job failed',
      details: job.error
    });
  }
  
  if (job.status === 'completed') {
    return res.json({
      success: true,
      ...job.result
    });
  }
  
  res.status(400).json({ error: 'Invalid job status' });
});

// Cancel job endpoint
app.delete('/api/jobs/:jobId', (req, res) => {
  const { jobId } = req.params;
  const job = jobManager.cancelJob(jobId);
  
  if (!job) {
    return res.status(404).json({ error: 'Job not found' });
  }
  
  res.json({ 
    message: 'Job cancelled successfully',
    jobId: job.id,
    status: job.status
  });
});

// List all jobs endpoint
app.get('/api/jobs', (req, res) => {
  const jobs = jobManager.getAllJobs().map(job => ({
    id: job.id,
    type: job.type,
    status: job.status,
    progress: job.progress,
    startTime: job.startTime,
    endTime: job.endTime
  }));
  
  res.json({ jobs });
});


// Simple in-memory cache: recordingId ⇒ duration (seconds)
const durationCache = new Map();

// Lightweight endpoint to expose recording duration without downloading full file
app.get('/api/recordings/:id/meta', async (req, res) => {
  const { id } = req.params;
  const { account } = req.query;

  if (!account) {
    return res.status(400).json({ error: 'Missing account query param' });
  }

  // Return cached value if present
  if (durationCache.has(id)) {
    res.setHeader('Cache-Control', 'public, max-age=3600');
    return res.json({ duration: durationCache.get(id) });
  }

  try {
    const token = await getPortalToken(account);
    const url = `${process.env.BASE_URL}/api/v2/reports/recordings/${id}`;

    // Fetch first 128 KB – enough for metadata / VBR TOC
    const upstreamRes = await axios.get(url, {
      responseType: 'arraybuffer',
      httpsAgent,
      headers: {
        Authorization: `Bearer ${token}`,
        'X-User-Agent': 'portal',
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? account,
        Range: 'bytes=0-131071',
        'Accept-Encoding': 'identity'
      },
      decompress: false
    });

    const { format } = await parseBuffer(Buffer.from(upstreamRes.data), 'audio/mpeg');
    if (!format.duration) throw new Error('Unable to determine duration');

    durationCache.set(id, format.duration);
    res.json({ duration: format.duration });
  } catch (err) {
    const status = err.response?.status || 500;
    if (status !== 404) {
      console.error(err.response?.data || err.stack || err.message);
    }
    res.status(status).json({ error: err.message });
  }
});

// Proxy: GET /api/recordings/:id?account=<tenant>
// Streams the MP3 recording from the upstream UC backend while adding the required auth token.
app.get('/api/recordings/:id', async (req, res) => {
  const { id } = req.params;
  const { account } = req.query;

  if (!account) {
    return res.status(400).json({ error: 'Missing account query param' });
  }

  try {
    // Obtain (cached) JWT for this tenant
    const token = await getPortalToken(account);

    const upstreamUrl = `${process.env.BASE_URL}/api/v2/reports/recordings/${id}`;
    // Ensure we get Content-Range/Length: if browser didn't request a range, request the full file starting from byte 0
    let rangeHdr = req.headers.range;
    if (!rangeHdr) {
      rangeHdr = 'bytes=0-';
    }

    const upstreamRes = await axios.get(upstreamUrl, {
      responseType: 'stream',
      httpsAgent,
      headers: {
        Authorization: `Bearer ${token}`,
        'X-User-Agent': 'portal',
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? account,
        Range: rangeHdr,
        'Accept-Encoding': 'identity'
      },
      // Ensure axios does not decompress so byte positions stay intact
      decompress: false
    });

    // Mirror upstream status (200 or 206 for range requests) and critical headers
    res.status(upstreamRes.status);

    // Pass through essential headers required for proper playback & seeking
    const forwardHeaders = [
      'content-type',
      'content-disposition',
      'content-length',
      'content-range',
      'accept-ranges'
    ];

    forwardHeaders.forEach(h => {
      if (upstreamRes.headers[h]) {
        res.setHeader(h, upstreamRes.headers[h]);
      }
    });

    // If we have cached duration, advertise it so browsers can show timeline immediately
    if (durationCache.has(id)) {
      const dur = durationCache.get(id);
      // Non-standard but understood by Chrome/Firefox
      res.setHeader('X-Content-Duration', dur.toFixed(3));
      // RFC 3803 (used by QuickTime / Safari)
      res.setHeader('Content-Duration', dur.toFixed(3));
    }

    // FIX 5: Add Cache-Control for browser caching (1 hour)
    res.setHeader('Cache-Control', 'public, max-age=3600');

    // Stream data
    upstreamRes.data.pipe(res);
  } catch (err) {
    const status = err.response?.status || 500;
    if (status !== 404) {
      console.error(err.response?.data || err.stack || err.message);
    }
    res.status(status).json({ error: err.message });
  }
});

// Cache for recordings by call_id (1 hour TTL)
const recordingsByCallIdCache = new Map();
const RECORDING_CACHE_TTL = 3600000;
// Track in-flight requests to prevent duplicate fetches
const pendingRequests = new Map();

// Proxy: GET /api/recordings/by-call-id/:yearMonthCallId
// Fetches recordings by call_id from the upstream API
app.get('/api/recordings/by-call-id/:yearMonthCallId', async (req, res) => {
  const { yearMonthCallId } = req.params;
  const account = req.query.account || 'default';
  const cacheKey = `${account}:${yearMonthCallId}`;

  // Return cached if available
  const cached = recordingsByCallIdCache.get(cacheKey);
  if (cached && (Date.now() - cached.ts < RECORDING_CACHE_TTL)) {
    console.log(`⚡ Cache HIT: ${yearMonthCallId}`);
    return res.json(cached.data);
  }

  // Check if request is already in-flight (deduplication)
  if (pendingRequests.has(cacheKey)) {
    console.log(`⏳ Waiting for pending request: ${yearMonthCallId}`);
    try {
      const result = await pendingRequests.get(cacheKey);
      return res.json(result);
    } catch (err) {
      return res.status(500).json({ error: err.message });
    }
  }

  // Create promise for this request and store for deduplication
  const fetchPromise = (async () => {
    const startTime = Date.now();
    console.log(`📞 Fetching recordings by call_id: ${yearMonthCallId}`);
    
    const tokenStart = Date.now();
    const token = await getPortalToken(account);
    console.log(`⏱️  Token fetch took: ${Date.now() - tokenStart}ms`);
    
    const upstreamUrl = `${process.env.BASE_URL}/api/v2/reports/recordings/by_call_id/${yearMonthCallId}`;
    console.log(`🔗 Upstream URL: ${upstreamUrl}`);

    const apiStart = Date.now();
    const upstreamRes = await axios.get(upstreamUrl, {
      httpsAgent,
      headers: {
        Authorization: `Bearer ${token}`,
        'X-User-Agent': 'portal',
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER ?? account,
        'Accept': 'application/json'
      },
      timeout: 30000  // Reduced from 900000ms to 30s
    });
    console.log(`⏱️  Upstream API took: ${Date.now() - apiStart}ms`);

    console.log(`✅ Recordings fetched successfully for call_id: ${yearMonthCallId} (Total: ${Date.now() - startTime}ms)`);
    
    // Cache the result
    recordingsByCallIdCache.set(cacheKey, { data: upstreamRes.data, ts: Date.now() });
    
    return upstreamRes.data;
  })();

  // Store pending request for deduplication
  pendingRequests.set(cacheKey, fetchPromise);

  try {
    const data = await fetchPromise;
    res.json(data);
  } catch (err) {
    const status = err.response?.status || 500;
    console.error(`❌ Error fetching recordings by call_id ${yearMonthCallId}:`, err.response?.data || err.message);
    res.status(status).json({ 
      error: err.message,
      details: err.response?.data 
    });
  } finally {
    // Clean up pending request
    pendingRequests.delete(cacheKey);
  }
});

// SSL Certificate Management
const loadSSLCertificates = () => {
  try {
    const sslOptions = {
      key: fs.readFileSync('ssl/privkey.pem'),
      cert: fs.readFileSync('ssl/fullchain.pem')
    };
    
    console.log("🔒 SSL certificates loaded successfully");
    return sslOptions;
  } catch (error) {
    console.error("❌ Error loading SSL certificates:", error.message);
    
    // Check if SSL files exist
    const sslFiles = ['ssl/privkey.pem', 'ssl/fullchain.pem'];
    sslFiles.forEach(file => {
      if (!fs.existsSync(file)) {
        console.error(`❌ SSL file not found: ${file}`);
      }
    });
    
    console.log("⚠️  Falling back to HTTP server");
    return null;
  }
};

const sslOptions = loadSSLCertificates();

// Store active queries for progressive loading
const activeQueries = new Map();

// Helper function to generate a unique query ID
function generateQueryId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

// Progressive loading endpoint
app.get('/api/reports/progressive', async (req, res) => {
  const { queryId, page = '1' } = req.query;
  const pageNum = parseInt(page) || 1;
  const pageSize = 1000; // Page size set to 1000 for consistent batch loading
  const debugHtmlContent = true; // Define debugHtmlContent here
  try {
    // If no queryId provided, this is a new query request
    if (!queryId) {
      return res.status(400).json({
        success: false,
        error: 'Missing queryId parameter'
      });
    }
    
    // Check if this is an active query
    if (!activeQueries.has(queryId)) {
      return res.status(404).json({
        success: false,
        error: 'Query not found or expired'
      });
    }
    
    const queryData = activeQueries.get(queryId);
    const { sql, values, totalRecords } = queryData;
    
    // Calculate offset based on page number
    const offset = (pageNum - 1) * pageSize;
    
    // Check if we're requesting beyond available records
    if (offset >= totalRecords) {
      return res.json({
        success: true,
        data: [],
        page: pageNum,
        totalPages: Math.ceil(totalRecords / pageSize),
        totalRecords,
        isLastPage: true
      });
    }
    
    // Execute query for this page
    // Fix any potential SQL syntax issues with the ORDER BY clause
    let fixedSql = sql;
    
    // Check if there's a duplicate transfer_event filter after ORDER BY
    const orderByIndex = fixedSql.indexOf('ORDER BY');
    if (orderByIndex > 0) {
      // Split the query into parts before and after ORDER BY
      const beforeOrderBy = fixedSql.substring(0, orderByIndex).trim();
      let orderByClause = fixedSql.substring(orderByIndex);
      
      // Check if there's an AND condition after ORDER BY (which is invalid SQL)
      const andAfterOrderBy = orderByClause.indexOf('AND');
      if (andAfterOrderBy > 0) {
        // Remove everything after AND in the ORDER BY clause
        orderByClause = orderByClause.substring(0, andAfterOrderBy).trim();
        fixedSql = beforeOrderBy + ' ' + orderByClause;
      }
    }
    
    // Ensure the SQL query doesn't have any trailing AND or WHERE
    if (fixedSql.trim().endsWith('WHERE')) {
      fixedSql = fixedSql.trim().slice(0, -5); // Remove the trailing 'WHERE'
    }
    if (fixedSql.trim().endsWith('AND')) {
      fixedSql = fixedSql.trim().slice(0, -3); // Remove the trailing 'AND'
    }
    
    const pageSql = `${fixedSql} LIMIT ${pageSize} OFFSET ${offset}`;
    
    const results = await dbService.query(pageSql, values);
    
    // Debug: Log the first result to see if hold_duration_intervals is included
    console.log(`Total records matching query: ${totalRecords}`);
    console.log('First result keys:', Object.keys(results[0] || {}));
    console.log('hold_duration_intervals in first result:', results[0]?.hold_duration_intervals);
    
    // Debug: Show first few records' timestamp and formatted time data
    if (results.length > 0) {
      console.log('=== DEBUG: First 3 records timestamp data ===');
      results.slice(0, 3).forEach((record, index) => {
        console.log(`Record ${index + 1}:`);
        console.log(`  called_time: ${record.called_time}`);
        console.log(`  called_time_formatted: ${record.called_time_formatted}`);
      });
      console.log('=== END DEBUG ===');
    }
    
    // Calculate if this is the last page
    const totalPages = Math.ceil(totalRecords / pageSize);
    const isLastPage = pageNum >= totalPages;
    
    // If this is the last page, clean up the query data
    if (isLastPage) {
      activeQueries.delete(queryId);
    }
    
    return res.json({
      success: true,
      data: results,
      page: pageNum,
      totalPages,
      totalRecords,
      isLastPage
    });
    
  } catch (error) {
    console.error(`Error processing progressive query: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Error processing query',
      details: error.message
    });
  }
});

// Initialize a progressive query
app.post('/api/reports/progressive/init', async (req, res) => {
  const requestId = Math.random().toString(36).substring(2, 10);
  
  try {
    // Extract query parameters from request body
    const {
      start,
      end,      
      contact_number,
      agent_name,
      extension,
      queue_campaign_name,
      record_type,
      agent_disposition,
      sub_disp_1,
      sub_disp_2,
      status,
      campaign_type,
      country,
      transfer_event,
      sort_by = 'called_time',
      sort_order = 'desc'
    } = req.body;
    
    // Validate required parameters
    if (!start || !end) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: start and end dates are required',
        request_id: requestId
      });
    }
    
    // Parse and validate date parameters
    let startTimestamp, endTimestamp, startDateFormatted, endDateFormatted;
    try {
      // Use standard JavaScript Date for initial parsing
      const startDateJS = new Date(start);
      const endDateJS = new Date(end);
      
      if (isNaN(startDateJS.getTime()) || isNaN(endDateJS.getTime())) {
        throw new Error('Invalid date format');
      }
      
      // Convert to timestamps (seconds since epoch)
      startTimestamp = Math.floor(startDateJS.getTime() / 1000);
      endTimestamp = Math.floor(endDateJS.getTime() / 1000);
      
      // Format dates in DD/MM/YYYY format for database queries
      const formatDateForDB = (date) => {
        const day = String(date.getDate()).padStart(2, '0');
        const month = String(date.getMonth() + 1).padStart(2, '0');
        const year = date.getFullYear();
        return `${day}/${month}/${year}`;
      };
      
      startDateFormatted = formatDateForDB(startDateJS);
      endDateFormatted = formatDateForDB(endDateJS);
      
      // For logs/debugging
      console.log(`Date range: ${startDateFormatted} to ${endDateFormatted}`);
      console.log(`Timestamp range: ${startTimestamp} to ${endTimestamp}`);
    } catch (error) {
      return res.status(400).json({
        success: false,
        message: 'Invalid date format. Please use ISO format (YYYY-MM-DDTHH:MM:SS)',
        request_id: requestId
      });
    }
    // --- timezone-robust parsing (requires luxon) ---
    const startEpoch = Math.floor(
      DateTime.fromISO(start, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );
    const endEpochExclusive = Math.floor(
      DateTime.fromISO(end, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );

    // Build SQL using epoch (indexed) column
    let sql = `
      SELECT call_id, record_type, type, agent_name, extension, queue_campaign_name,
             called_time, called_time_formatted, caller_id_number, caller_id_name,
             callee_id_number, answered_time, hangup_time, wait_duration, talk_duration,
             hold_duration, hold_duration_intervals, agent_hangup, agent_disposition,
             disposition, sub_disp_1, sub_disp_2, status, campaign_type, abandoned,
             country, follow_up_notes, agent_history, queue_history, lead_history,
             recording, transfer_event, transfer_extension, transfer_type
      FROM final_report USE INDEX (idx_called_time)
      WHERE called_time >= ? AND called_time < ?
    `;
    const values = [startEpoch, endEpochExclusive];
    
    // Add optional filters
    if (contact_number) {
      sql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      values.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    
    if (agent_name) {
      sql += ' AND agent_name LIKE ?';
      values.push(`%${agent_name}%`);
    }
    
    if (queue_campaign_name) {
      sql += ' AND queue_campaign_name LIKE ?';
      values.push(`%${queue_campaign_name}%`);
    }
    
    if (record_type) {
      sql += ' AND record_type = ?';
      values.push(record_type);
    }
    
    if (agent_disposition) {
      sql += ' AND agent_disposition LIKE ?';
      values.push(`%${agent_disposition}%`);
    }
    
    if (sub_disp_1) {
      sql += ' AND sub_disp_1 LIKE ?';
      values.push(`%${sub_disp_1}%`);
    }
    
    if (sub_disp_2) {
      sql += ' AND sub_disp_2 LIKE ?';
      values.push(`%${sub_disp_2}%`);
    }
    
    if (status) {
      sql += ' AND status LIKE ?';
      values.push(`%${status}%`);
    }
    
    if (campaign_type) {
      sql += ' AND campaign_type LIKE ?';
      values.push(`%${campaign_type}%`);
    }
    
    if (country) {
      sql += ' AND country LIKE ?';
      values.push(`%${country}%`);
    }
    
    if (extension) {
      sql += ' AND extension LIKE ?';
      values.push(`%${extension}%`);
    }
    
    if (transfer_event === '1' || transfer_event === 1 || transfer_event === 'Yes' || transfer_event === true) {
      sql += ' AND transfer_event = 1';
      // countSql will be set later: ' AND transfer_event = 1';
    } else if (transfer_event === '0' || transfer_event === 0 || transfer_event === 'No' || transfer_event === false) {
      sql += ' AND (transfer_event = 0 OR transfer_event IS NULL)';
      // countSql will be set later: ' AND (transfer_event = 0 OR transfer_event IS NULL)';
    }
    
    // Validate sort_by to prevent SQL injection
    const validSortColumns = [
      'call_id', 'record_type', 'type', 'agent_name', 'extension', 'queue_campaign_name',
      'called_time', 'called_time_formatted', 'caller_id_number', 'caller_id_name', 'callee_id_number',
      'answered_time', 'hangup_time', 'wait_duration', 'talk_duration', 'hold_duration',
      'hold_duration_intervals',
      'agent_hangup', 'agent_disposition', 'sub_disp_1', 'sub_disp_2',
      'status', 'campaign_type', 'abandoned', 'country', 
      'transfer_event', 'transfer_extension', 'transfer_type', 'created_at', 'updated_at'
    ];
    
    const sortColumn = validSortColumns.includes(sort_by) ? sort_by : 'called_time';
    const sortDir = sort_order?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    
    // Add sorting
    sql += ` ORDER BY ${sortColumn} ${sortDir}`;

    // First, get the total count without pagination
    let countSql = `SELECT COUNT(*) as total FROM final_report USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    let countValues = [startEpoch, endEpochExclusive];
    
    if (contact_number) {
      countSql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      countValues.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (agent_name) {
      countSql += ' AND agent_name LIKE ?';
      countValues.push(`%${agent_name}%`);
    }
    if (queue_campaign_name) {
      countSql += ' AND queue_campaign_name LIKE ?';
      countValues.push(`%${queue_campaign_name}%`);
    }
    if (record_type) {
      countSql += ' AND record_type = ?';
      countValues.push(record_type);
    }
    if (agent_disposition) {
      countSql += ' AND agent_disposition LIKE ?';
      countValues.push(`%${agent_disposition}%`);
    }
    if (sub_disp_1) {
      countSql += ' AND sub_disp_1 LIKE ?';
      countValues.push(`%${sub_disp_1}%`);
    }
    if (sub_disp_2) {
      countSql += ' AND sub_disp_2 LIKE ?';
      countValues.push(`%${sub_disp_2}%`);
    }
    if (status) {
      countSql += ' AND status LIKE ?';
      countValues.push(`%${status}%`);
    }
    if (campaign_type) {
      countSql += ' AND campaign_type LIKE ?';
      countValues.push(`%${campaign_type}%`);
    }
    if (country) {
      countSql += ' AND country LIKE ?';
      countValues.push(`%${country}%`);
    }
    if (extension) {
      countSql += ' AND extension LIKE ?';
      countValues.push(`%${extension}%`);
    }
    if (transfer_event === '1' || transfer_event === 1 || transfer_event === 'Yes' || transfer_event === true) {
      sql += ' AND transfer_event = 1';
      // countSql will be set later: ' AND transfer_event = 1';
    } else if (transfer_event === '0' || transfer_event === 0 || transfer_event === 'No' || transfer_event === false) {
      sql += ' AND (transfer_event = 0 OR transfer_event IS NULL)';
      // countSql will be set later: ' AND (transfer_event = 0 OR transfer_event IS NULL)';
    }
    
    
    // Get total count
    const countResult = await dbService.query(countSql, countValues);
    const totalRecords = countResult[0].total;
    
    console.log(`Total records matching query: ${totalRecords}`);
    
    // Generate a unique query ID
    const queryId = generateQueryId();
    
    // Store the query data for future page requests
    activeQueries.set(queryId, {
      sql,
      values,
      totalRecords,
      createdAt: Date.now(),
      lastAccessed: Date.now()
    });
    
    // Set a timeout to clean up this query after 30 minutes
    setTimeout(() => {
      if (activeQueries.has(queryId)) {
        console.log(`Cleaning up expired query ${queryId}`);
        activeQueries.delete(queryId);
      }
    }, 30 * 60 * 1000); // 30 minutes
    
    // Return the query ID and metadata
    return res.json({
      success: true,
      queryId,
      totalRecords,
      totalPages: Math.ceil(totalRecords / 1000), // Using page size of 1000
      message: 'Query initialized successfully. Use this queryId to fetch pages of results.',
      request_id: requestId
    });
    
  } catch (error) {
    console.error(`Error initializing progressive query: ${error.message}`);
    return res.status(500).json({
      success: false,
      error: 'Error initializing query',
      details: error.message,
      request_id: requestId
    });
  }
});

app.get('/api/reports/search', async (req, res) => {
  const requestId = Math.random().toString(36).substring(2, 10);
  
  try {
    // Extract query parameters
    const {
      start, // start timestamp or date
      end, // end timestamp or date
      contact_number,
      agent_name,
      extension,
      queue_campaign_name,
      record_type,
      agent_disposition,
      sub_disp_1,
      sub_disp_2,
      status,
      campaign_type,
      country,
      transfer_event,
      sort_by = 'called_time',
      sort_order = 'desc',
      page,
      limit,
      fetchAll = 'false'
    } = req.query;
    
    // Validate required parameters
    if (!start || !end) {
      return res.status(400).json({
        success: false,
        message: 'Missing required parameters: start and end dates are required',
        request_id: requestId
      });
    }
    
    // Convert dates to appropriate format
    let startTimestamp, endTimestamp;
    let startDateFormatted, endDateFormatted;
    
    // Handle start date conversion
    if (typeof start === 'string') {
      if (start.includes('-') || start.includes('/')) {
        // ISO format or date string
        const startDate = new Date(start);
        startTimestamp = Math.floor(startDate.getTime() / 1000);
        startDateFormatted = `${startDate.getDate().toString().padStart(2, '0')}/${(startDate.getMonth() + 1).toString().padStart(2, '0')}/${startDate.getFullYear()}`;
      } else {
        // Numeric string
        const numValue = Number(start);
        startTimestamp = numValue > 10000000000 ? Math.floor(numValue / 1000) : numValue;
        const startDate = new Date(startTimestamp * 1000);
        startDateFormatted = `${startDate.getDate().toString().padStart(2, '0')}/${(startDate.getMonth() + 1).toString().padStart(2, '0')}/${startDate.getFullYear()}`;
      }
    }
    
    // Handle end date conversion
    if (typeof end === 'string') {
      if (end.includes('-') || end.includes('/')) {
        // ISO format or date string
        const endDate = new Date(end);
        endTimestamp = Math.ceil(endDate.getTime() / 1000);
        endDateFormatted = `${endDate.getDate().toString().padStart(2, '0')}/${(endDate.getMonth() + 1).toString().padStart(2, '0')}/${endDate.getFullYear()}`;
      } else {
        // Numeric string
        const numValue = Number(end);
        endTimestamp = numValue > 10000000000 ? Math.ceil(numValue / 1000) : numValue;
        const endDate = new Date(endTimestamp * 1000);
        endDateFormatted = `${endDate.getDate().toString().padStart(2, '0')}/${(endDate.getMonth() + 1).toString().padStart(2, '0')}/${endDate.getFullYear()}`;
      }
    }
    
    // --- timezone-robust parsing (requires luxon) ---
    const startEpoch = Math.floor(
      DateTime.fromISO(start, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );
    const endEpochExclusive = Math.floor(
      DateTime.fromISO(end, { zone: 'Asia/Dubai' }).toUTC().toSeconds()
    );

    // Build SQL using epoch (indexed) column
    let sql = `
      SELECT call_id, record_type, type, agent_name, extension, queue_campaign_name,
             called_time, called_time_formatted, caller_id_number, caller_id_name,
             callee_id_number, answered_time, hangup_time, wait_duration, talk_duration,
             hold_duration, hold_duration_intervals, agent_hangup, agent_disposition,
             disposition, sub_disp_1, sub_disp_2, status, campaign_type, abandoned,
             country, follow_up_notes, agent_history, queue_history, lead_history,
             recording, transfer_event, transfer_extension, transfer_type
      FROM final_report USE INDEX (idx_called_time)
      WHERE called_time >= ? AND called_time < ?
    `;
    const values = [startEpoch, endEpochExclusive];
    
    // Add optional filters
    if (contact_number) {
      sql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      values.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    
    if (agent_name) {
      sql += ' AND agent_name LIKE ?';
      values.push(`%${agent_name}%`);
    }
    
    if (queue_campaign_name) {
      sql += ' AND queue_campaign_name LIKE ?';
      values.push(`%${queue_campaign_name}%`);
    }
    
    if (record_type) {
      sql += ' AND record_type = ?';
      values.push(record_type);
    }
    
    if (agent_disposition) {
      sql += ' AND agent_disposition LIKE ?';
      values.push(`%${agent_disposition}%`);
    }
    
    if (sub_disp_1) {
      sql += ' AND sub_disp_1 LIKE ?';
      values.push(`%${sub_disp_1}%`);
    }
    
    if (sub_disp_2) {
      sql += ' AND sub_disp_2 LIKE ?';
      values.push(`%${sub_disp_2}%`);
    }
    
    if (status) {
      sql += ' AND status LIKE ?';
      values.push(`%${status}%`);
    }
    
    if (campaign_type) {
      sql += ' AND campaign_type LIKE ?';
      values.push(`%${campaign_type}%`);
    }
    
    if (country) {
      sql += ' AND country LIKE ?';
      values.push(`%${country}%`);
    }
    
    if (extension) {
      sql += ' AND extension LIKE ?';
      values.push(`%${extension}%`);
    }
    
    // Add transfer_event filter if provided
    // Make sure we're not adding it if the SQL query doesn't have a WHERE clause yet
    if (transfer_event) {
      // Make sure we have a WHERE clause before adding AND conditions
      if (!sql.includes('WHERE')) {
        sql += ' WHERE ';
      } else if (!sql.trim().endsWith('WHERE')) {
        sql += ' AND ';
      }
      
      if (transfer_event === '1' || transfer_event === 1 || transfer_event === 'Yes' || transfer_event === true) {
        sql += 'transfer_event = 1';
      } else if (transfer_event === '0' || transfer_event === 0 || transfer_event === 'No' || transfer_event === false) {
        sql += '(transfer_event = 0 OR transfer_event IS NULL)';
      }
    }
    
    // Validate sort_by to prevent SQL injection
    const validSortColumns = [
      'call_id', 'record_type', 'type', 'agent_name', 'extension', 'queue_campaign_name',
      'called_time', 'called_time_formatted', 'caller_id_number', 'caller_id_name', 'callee_id_number',
      'answered_time', 'hangup_time', 'wait_duration', 'talk_duration', 'hold_duration',
      'hold_duration_intervals',
      'agent_hangup', 'agent_disposition', 'disposition', 'sub_disp_1', 'sub_disp_2',
      'status', 'campaign_type', 'abandoned', 'country', 'created_at', 'updated_at'
    ];
    
    const sortColumn = validSortColumns.includes(sort_by) ? sort_by : 'called_time';
    const sortDir = sort_order?.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    
    // Add sorting
    sql += ` ORDER BY ${sortColumn} ${sortDir}`;

    const shouldFetchAll = fetchAll === 'true';
    
    console.log(`Request parameters: fetchAll=${fetchAll}, shouldFetchAll=${shouldFetchAll}`);

    // First, get the total count without pagination
    let countSql = `SELECT COUNT(*) as total FROM final_report USE INDEX (idx_called_time) WHERE called_time >= ? AND called_time < ?`;
    let countValues = [startEpoch, endEpochExclusive];
    
    if (contact_number) {
      countSql += ' AND (caller_id_number LIKE ? OR callee_id_number LIKE ?)';
      countValues.push(`%${contact_number}%`, `%${contact_number}%`);
    }
    if (agent_name) {
      countSql += ' AND agent_name LIKE ?';
      countValues.push(`%${agent_name}%`);
    }
    if (queue_campaign_name) {
      countSql += ' AND queue_campaign_name LIKE ?';
      countValues.push(`%${queue_campaign_name}%`);
    }
    if (record_type) {
      countSql += ' AND record_type = ?';
      countValues.push(record_type);
    }
    if (agent_disposition) {
      countSql += ' AND agent_disposition LIKE ?';
      countValues.push(`%${agent_disposition}%`);
    }
    if (sub_disp_1) {
      countSql += ' AND sub_disp_1 LIKE ?';
      countValues.push(`%${sub_disp_1}%`);
    }
    if (sub_disp_2) {
      countSql += ' AND sub_disp_2 LIKE ?';
      countValues.push(`%${sub_disp_2}%`);
    }
    if (status) {
      countSql += ' AND status LIKE ?';
      countValues.push(`%${status}%`);
    }
    if (campaign_type) {
      countSql += ' AND campaign_type LIKE ?';
      countValues.push(`%${campaign_type}%`);
    }
    if (country) {
      countSql += ' AND country LIKE ?';
      countValues.push(`%${country}%`);
    }
    if (extension) {
      countSql += ' AND extension LIKE ?';
      countValues.push(`%${extension}%`);
    }
    
    if (transfer_event) {
      if (transfer_event === '1' || transfer_event === 1 || transfer_event === 'Yes' || transfer_event === true) {
        countSql += ' AND transfer_event = 1';
      } else if (transfer_event === '0' || transfer_event === 0 || transfer_event === 'No' || transfer_event === false) {
        countSql += ' AND (transfer_event = 0 OR transfer_event IS NULL)';
      }
    }
    
    // Get total count
    const countResult = await dbService.query(countSql, countValues);
    const totalRecords = countResult[0].total;
    
    // Execute the query with timeout protection
    const queryStartTime = Date.now();
    // Use a much longer timeout when fetching all records
    const timeoutDuration = shouldFetchAll ? 1200000 : 180000; // 20 minutes for fetchAll, 3 minutes otherwise
    const timeoutPromise = new Promise((_, reject) => {
      setTimeout(() => reject(new Error('Query timeout exceeded')), timeoutDuration);
    });
    
    // Log that we're starting a potentially large query
    if (shouldFetchAll) {
      console.log(`Starting large query with fetchAll=true, timeout set to ${timeoutDuration/60000} minutes`);
    }
    
    // Execute query with race against timeout and chunked processing for large result sets
    let results;
    try {
      if (shouldFetchAll) {
        console.log('Using chunked processing for large result set');
        results = await Promise.race([
          processInChunks(sql, values, totalRecords, 5000), // Process in chunks of 5000 records
          timeoutPromise
        ]);
      } else {
        // For regular queries, use standard query with a higher limit to ensure all records are returned
        // Use a much higher limit to ensure we get all records
        const limitedSql = `${sql} LIMIT 50000`;
        results = await Promise.race([
          dbService.query(limitedSql, values),
          timeoutPromise
        ]);
      }
    } catch (error) {
      console.error(`❌ Query error: ${error.message}`);
      return res.status(504).json({ error: 'Query timeout or database error', details: error.message });
    }
    
    const queryDuration = Date.now() - queryStartTime;
    
    // Debug: Log the first result to see if hold_duration_intervals is included
    console.log(`Total records matching query: ${totalRecords}`);
    console.log('First result keys:', Object.keys(results[0] || {}));
    console.log('hold_duration_intervals in first result:', results[0]?.hold_duration_intervals);
    
    // Debug: Show first few records' timestamp and formatted time data
    if (results.length > 0) {
      console.log('=== DEBUG: First 3 records timestamp data ===');
      results.slice(0, 3).forEach((record, index) => {
        console.log(`Record ${index + 1}:`);
        console.log(`  called_time: ${record.called_time}`);
        console.log(`  called_time_formatted: ${record.called_time_formatted}`);
      });
      console.log('=== END DEBUG ===');
    }
    
    // Calculate totals for each record type
    const totals = {
      Campaign: 0,
      Inbound: 0,
      Outbound: 0,
      Total: totalRecords // Use the total count from the database query
    };
    
    results.forEach(record => {
      if (record.record_type && totals[record.record_type] !== undefined) {
        totals[record.record_type]++;
      }
    });
    
    // Return the results with totals and pagination info
    return res.json({
      success: true,
      data: results,
      totals: totals,
      fetchAll: shouldFetchAll,
      query_time_ms: queryDuration,
      request_id: requestId
    });
  } catch (error) {
    // Determine appropriate status code based on error type
    let statusCode = 500;
    let errorMessage = 'Internal server error';
    let errorDetails = null;
    
    // Handle validation errors
    if (error.message && error.message.includes('Missing required parameter')) {
      statusCode = 400;
      errorMessage = error.message;
    } 
    // Handle timeout errors
    else if (error.message && (error.message.includes('timeout') || error.message.includes('Query timeout'))) {
      statusCode = 504;
      errorMessage = 'Query timed out. Please try with a smaller date range or more specific filters.';
    }
    // Handle connection errors
    else if (error.code === 'ECONNRESET' || error.code === 'ETIMEDOUT' || error.code === 'EPIPE' || error.code === 'EMFILE') {
      statusCode = 503;
      errorMessage = `Database connection error (${error.code}). Please try again in a few moments.`;
      errorDetails = { code: error.code };
    }
    // Handle database errors
    else if (error.code && error.code.startsWith('ER_')) {
      statusCode = 500;
      errorMessage = 'Database error. Please check your query parameters.';
      errorDetails = { code: error.code };
    }
    
    // Send error response with request ID for tracking
    res.status(statusCode).json({ 
      error: errorMessage,
      request_id: requestId,
      ...(errorDetails && { details: errorDetails })
    });
  }
});

// Only use HTTPS if PUBLIC_URL starts with https://
const useHTTPS = PUBLIC_URL.startsWith('https://');

if (sslOptions && useHTTPS) {
  const server = https.createServer(sslOptions, app);
  server.listen(PORT, HOST, () => {
    console.log(`🔐 HTTPS server running at ${PUBLIC_URL}`);
    console.log(`🌐 Server accessible on all network interfaces (${HOST}:${PORT})`);
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTPS Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
} else {
  const server = app.listen(PORT, HOST, () => {
    console.log(`🌐 HTTP server running at ${PUBLIC_URL}`);
    if (!useHTTPS) {
      console.log(`⚠️  Running in HTTP mode (PUBLIC_URL is set to HTTP)`);
    } else {
      console.log(`⚠️  Running in HTTP mode (no SSL certificates found)`);
    }
  });
  
  server.on('error', (err) => {
    console.error('❌ HTTP Server error:', err);
    if (err.code === 'EADDRINUSE') {
      console.error(`❌ Port ${PORT} is already in use. Try a different port.`);
    } else if (err.code === 'EACCES') {
      console.error(`❌ Permission denied. Port ${PORT} might require sudo privileges.`);
    }
    process.exit(1);
  });
}