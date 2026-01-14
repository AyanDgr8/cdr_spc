// dataFetchWorker.js - Worker thread for background data fetching

import { parentPort, workerData } from 'worker_threads';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import sessionDbService from './sessionDbService.js';

const { jobId, jobType, params } = workerData;

async function processJob() {
  try {
    parentPort.postMessage({ type: 'progress', progress: 10 });
    
    if (jobType === 'fetch_unified_data') {
      const { account, sessionId, apiParams } = params;
      
      // Step 0: Initialize session tables
      console.log(`🔧 Worker ${jobId}: Initializing session tables for ${sessionId}...`);
      await sessionDbService.initializeSessionTables(sessionId);
      
      // Step 1: Fetch data from APIs
      parentPort.postMessage({ type: 'progress', progress: 20 });
      console.log(`🔄 Worker ${jobId}: Starting API data fetch...`);
      
      const fetchResults = await fetchAllAPIsAndPopulateDB(account, apiParams);
      
      parentPort.postMessage({ type: 'progress', progress: 60 });
      
      // Step 2: Store data in session-specific tables
      if (fetchResults.records) {
        console.log(`📋 Worker ${jobId}: Storing data in session tables...`);
        
        await Promise.all([
          sessionDbService.batchInsertRawCdrs(fetchResults.records.cdrs || [], sessionId),
          sessionDbService.batchInsertRawCdrsAll(fetchResults.records.cdrs_all || [], sessionId),
          sessionDbService.batchInsertRawQueueInbound(fetchResults.records.queueCalls || [], sessionId),
          sessionDbService.batchInsertRawQueueOutbound(fetchResults.records.queueOutboundCalls || [], sessionId),
          sessionDbService.batchInsertRawCampaigns(fetchResults.records.campaignsActivity || [], sessionId)
        ]);
        
        parentPort.postMessage({ type: 'progress', progress: 80 });
      }
      
      // Step 3: Create unified report
      console.log(`📊 Worker ${jobId}: Creating unified report...`);
      const unifiedReport = await sessionDbService.createUnifiedReportFromSession(sessionId, account, apiParams);
      
      parentPort.postMessage({ type: 'progress', progress: 100 });
      
      // Send success result
      parentPort.postMessage({
        type: 'success',
        result: {
          data: unifiedReport.rows,
          summary: unifiedReport.summary,
          fetchResults: {
            ...fetchResults,
            source: 'api_fresh'
          },
          fromCache: false,
          cacheStatus: 'MISS'
        }
      });
      
    } else {
      throw new Error(`Unknown job type: ${jobType}`);
    }
    
  } catch (error) {
    console.error(`❌ Worker ${jobId} error:`, error);
    parentPort.postMessage({
      type: 'error',
      error: error.message
    });
  }
}

// Start processing
processJob();
