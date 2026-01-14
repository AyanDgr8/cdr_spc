// apiDataFetcher.js
// Streamlined API data fetcher for SPC CDR reporting system
// Fetches all 5 APIs simultaneously and populates database

// Import and configure dotenv to load environment variables
import dotenv from 'dotenv';
dotenv.config();

// Using Node.js built-in fetch (Node.js 18+)
import dbService from './dbService.js';
import { getPortalToken } from './tokenService.js';

const BASE_URL = process.env.BASE_URL;

// API endpoint configurations
const API_ENDPOINTS = {
  // cdrs: {
  //   url: '/api/v2/reports/cdrs',
  //   table: 'raw_cdrs',
  //   insertFunction: 'batchInsertRawCdrs'
  // },
  // cdrs_all: {
  //   url: '/api/v2/reports/cdrs/all', 
  //   table: 'raw_cdrs_all',
  //   insertFunction: 'batchInsertRawCdrsAll'
  // },
  queueCalls: {
    url: '/api/v2/reports/queues_cdrs',
    table: 'raw_queue_inbound',
    insertFunction: 'batchInsertRawQueueInbound'
  },
  queueOutboundCalls: {
    url: '/api/v2/reports/queues_outbound_cdrs',
    table: 'raw_queue_outbound', 
    insertFunction: 'batchInsertRawQueueOutbound'
  },
  campaignsActivity: {
    url: '/api/v2/reports/campaigns/leads/history',
    table: 'raw_campaigns',
    insertFunction: 'batchInsertRawCampaigns'
  }
};

// Main function to fetch all APIs simultaneously and populate database
export async function fetchAllAPIsAndPopulateDB(tenant, params) {
  console.log('🚀 Starting simultaneous fetch of all 5 APIs...');
  
  const startTime = Date.now();
  
  // NOTE: NOT clearing existing data to preserve global cache
  console.log('📊 Preserving existing global cache data...');
  // Start all 5 APIs simultaneously
  const promises = Object.keys(API_ENDPOINTS).map(endpointKey => 
    fetchEndpointWithPagination(endpointKey, tenant, params)
  );
  
  try {
    const results = await Promise.allSettled(promises);
    
    const summary = {
      totalTime: Date.now() - startTime,
      results: {}
    };
    
    results.forEach((result, index) => {
      const endpointKey = Object.keys(API_ENDPOINTS)[index];
      if (result.status === 'fulfilled') {
        summary.results[endpointKey] = result.value;
        console.log(`✅ ${endpointKey}: ${result.value.totalRecords} records in ${result.value.totalPages} pages`);
      } else {
        summary.results[endpointKey] = { error: result.reason.message };
        console.error(`❌ ${endpointKey}: Failed -`, result.reason.message);
      }
    });
    
    console.log(`🎉 All APIs completed in ${summary.totalTime}ms`);
    return summary;
    
  } catch (error) {
    console.error('❌ Error in simultaneous fetch:', error);
    throw error;
  }
}

// Fetch single endpoint with complete pagination and database storage
async function fetchEndpointWithPagination(endpointKey, tenant, params) {
  const config = API_ENDPOINTS[endpointKey];
  if (!config) {
    throw new Error(`Unknown endpoint: ${endpointKey}`);
  }
  
  // Handle virtual endpoints (like transferredCalls)
  if (config.isVirtual) {
    console.log(`🔍 ${endpointKey} is a virtual endpoint, skipping API call`);
    // For virtual endpoints, we don't actually fetch from an API
    // Instead, we process data that's already in the database
    
    // For other virtual endpoints, return empty result
    return {
      endpoint: endpointKey,
      totalRecords: 0,
      totalPages: 0
    };
  }
  
  let totalRecords = 0;
  let nextStartKey = null;
  let pageCount = 0;
  
  // Pagination settings
  const MAX_PAGES = 25000; // Hard safety limit for 8400+ records
  
  console.log(`🚀 Starting ${endpointKey} pagination...`);
  
  // Continue fetching until next_start_key becomes null or empty string
  while (pageCount < MAX_PAGES) {
    const requestParams = { ...params };
    if (nextStartKey) {
      requestParams.startKey = nextStartKey;
    }
    
    try {
      const response = await fetchPage(config.url, tenant, requestParams);
      
      // Extract data array and pagination info
      // For cdrs_all, data is in response.cdrs, for others it's in response.data
      const data = response.cdrs || response.data || [];
      const newStartKey = response.next_start_key;
      const pageSize = response.page_size || data.length;
      
      console.log(`📝 ${endpointKey}: Page ${pageCount + 1}, Records: ${data.length}, next_start_key: "${newStartKey}", Total: ${totalRecords + data.length}`);
      
      // If no data returned, we're done
      if (data.length === 0) {
        console.log(`✅ ${endpointKey}: No more data - ${totalRecords} total records in ${pageCount + 1} pages`);
        break;
      }
      
      // Store batch in database immediately for memory efficiency
      try {
        // Validate disposition data before insertion
        const recordsWithDisposition = data.filter(record => record.agent_disposition);
        const recordsWithSubdisposition = data.filter(record => record.agent_subdisposition);
        const recordsWithoutDisposition = data.length - recordsWithDisposition.length;
        
        console.log(`🔍 ${endpointKey} Disposition Analysis: ${recordsWithDisposition.length}/${data.length} have agent_disposition, ${recordsWithSubdisposition.length}/${data.length} have agent_subdisposition`);
        
        if (recordsWithoutDisposition > 0) {
          console.warn(`⚠️ ${endpointKey}: ${recordsWithoutDisposition}/${data.length} records missing agent_disposition`);
          
          // Log a sample record without disposition for debugging
          const sampleRecord = data.find(record => !record.agent_disposition);
          if (sampleRecord) {
            console.log(`🔍 Sample record without disposition:`, {
              call_id: sampleRecord.call_id || sampleRecord.callid,
              available_fields: Object.keys(sampleRecord),
              agent_disposition: sampleRecord.agent_disposition,
              agent_subdisposition: sampleRecord.agent_subdisposition,
              disposition: sampleRecord.disposition,
              status: sampleRecord.status,
              hangup_cause: sampleRecord.hangup_cause
            });
          }
        } else {
          console.log(`✅ ${endpointKey}: All ${data.length} records have agent_disposition field`);
        }
        
        // Log sample records WITH disposition for comparison
        if (recordsWithDisposition.length > 0) {
          const sampleWithDisposition = recordsWithDisposition[0];
          console.log(`🔍 Sample record WITH disposition:`, {
            call_id: sampleWithDisposition.call_id || sampleWithDisposition.callid,
            agent_disposition: sampleWithDisposition.agent_disposition,
            agent_subdisposition: sampleWithDisposition.agent_subdisposition?.name || sampleWithDisposition.agent_subdisposition,
            disposition: sampleWithDisposition.disposition
          });
        }
        
        const result = await dbService[config.insertFunction](data);
        totalRecords += data.length;
        
        console.log(`💾 ${endpointKey}: DB Inserted: ${result?.affectedRows || 'OK'} records`);
      } catch (dbError) {
        console.error(`❌ ${endpointKey}: Database error on page ${pageCount + 1}:`, dbError.message);
        console.error(`❌ ${endpointKey}: Sample data that failed:`, data.slice(0, 2));
        // Continue with pagination even if DB insert fails
        totalRecords += data.length;
      }
      
      pageCount++;
      
      // CRITICAL: Stop if next_start_key is null, undefined, or empty string
      // This is the natural end of pagination as defined by the API
      if (!newStartKey || newStartKey === '' || newStartKey === null) {
        console.log(`✅ ${endpointKey}: Pagination complete - next_start_key is "${newStartKey}". Total: ${totalRecords} records in ${pageCount} pages`);
        break;
      }
      
      // Continue fetching with new token - no cycle detection needed
      // The API will naturally terminate when next_start_key becomes null/empty
      console.log(`   ➡️  ${endpointKey}: Continuing with next_start_key: "${newStartKey}"`);
      
      // Set next token for next iteration
      nextStartKey = newStartKey;
      
    } catch (error) {
      console.error(`❌ ${endpointKey}: Error on page ${pageCount + 1}:`, error.message);
      console.error(`❌ ${endpointKey}: Full error details:`, error);
      
      // Retry logic for transient errors and authentication issues
      if (error.message.includes('timeout') || error.message.includes('ECONNRESET') || 
          error.message.includes('502') || error.message.includes('503') ||
          error.message.includes('RETRY_AUTH')) {
        console.log(`🔄 ${endpointKey}: Retrying page ${pageCount + 1} due to ${error.message.includes('RETRY_AUTH') ? 'authentication' : 'transient'} error...`);
        await new Promise(resolve => setTimeout(resolve, 2000)); // Wait 2 seconds
        continue; // Retry the same page
      }
      
      break;
    }
  }
  
  if (pageCount >= MAX_PAGES) {
    console.log(`⚠️ ${endpointKey}: Reached maximum page limit (${MAX_PAGES}). Total: ${totalRecords} records`);
  }
  
  return {
    endpoint: endpointKey,
    totalRecords,
    totalPages: pageCount
  };
}

// Helper function to fetch a single page from an API endpoint
// Format date for API compatibility - API expects Unix timestamp in seconds
function formatDateForApi(dateString) {
  if (!dateString) return null;
  
  // If it's already a number, assume it's already formatted correctly
  if (typeof dateString === 'number') return dateString;
  
  try {
    // Parse the date string to a Date object
    const date = dateString instanceof Date ? dateString : new Date(dateString);
    
    // Convert to Unix timestamp in seconds (not milliseconds)
    return Math.floor(date.getTime() / 1000);
  } catch (e) {
    console.error('Error formatting date:', e);
    return dateString; // Return original if parsing fails
  }
}

async function fetchPage(url, tenant, params) {
  // Get JWT token using the existing token service with retry logic
  let token;
  try {
    token = await getPortalToken(tenant);
    if (!token) {
      throw new Error('Failed to obtain authentication token');
    }
  } catch (tokenError) {
    console.error(`❌ Token acquisition failed:`, tokenError.message);
    throw new Error(`Authentication failed: ${tokenError.message}`);
  }
  
  // Format date parameters for API compatibility
  const formattedParams = { ...params };
  
  // Convert start_date/end_date to startDate/endDate as required by the API
  if (formattedParams.start_date) {
    formattedParams.startDate = formatDateForApi(formattedParams.start_date);
    delete formattedParams.start_date;
  }
  if (formattedParams.end_date) {
    formattedParams.endDate = formatDateForApi(formattedParams.end_date);
    delete formattedParams.end_date;
  }
  
  // Add pageSize=2000 parameter to all API requests
  formattedParams.pageSize = 2000;
  
  // Use the actual account ID from env for API calls, not the tenant name
  const queryParams = new URLSearchParams({
    account: process.env.ACCOUNT_ID_HEADER,
    ...formattedParams
  });
  
  const fullUrl = `${BASE_URL}${url}?${queryParams}`;
  
  // Debug logging
  console.log(`🔍 API Request: ${fullUrl}`);
  console.log(`🔑 Auth: JWT Token (${tenant})`);
  
  try {
    const response = await fetch(fullUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER
      },
      timeout: 30000 // 30 second timeout
    });
    
    console.log(`📡 Response: ${response.status} ${response.statusText}`);
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error(`❌ API Error Response Body:`, errorText);
      
      // Handle 401 (unauthorized) - token might be expired
      if (response.status === 401) {
        console.warn(`⚠️ 401 Unauthorized - token may be expired, will retry with fresh token`);
        throw new Error(`RETRY_AUTH:API request failed: ${response.status} ${response.statusText} - ${errorText}`);
      }
      
      throw new Error(`API request failed: ${response.status} ${response.statusText} - ${errorText}`);
    }
    
    const responseData = await response.json();
    
    // Validate response structure
    if (!responseData) {
      throw new Error('API returned null/undefined response');
    }
    
    // Log sample of first record to verify field presence
    const data = responseData.cdrs || responseData.data || [];
    if (data.length > 0) {
      const firstRecord = data[0];
      console.log(`🔍 Sample record fields:`, Object.keys(firstRecord));
      
      // Check for disposition fields specifically
      if (firstRecord.agent_disposition !== undefined) {
        console.log(`✅ agent_disposition field present:`, firstRecord.agent_disposition);
      } else {
        console.warn(`⚠️ agent_disposition field missing in API response`);
      }
      
      if (firstRecord.agent_subdisposition !== undefined) {
        console.log(`✅ agent_subdisposition field present:`, typeof firstRecord.agent_subdisposition);
      } else {
        console.warn(`⚠️ agent_subdisposition field missing in API response`);
      }
    }
    
    return responseData;
    
  } catch (error) {
    console.error(`❌ Fetch error details:`, {
      message: error.message,
      url: fullUrl,
      headers: {
        'Authorization': `Bearer ${token ? token.substring(0, 20) + '...' : 'missing'}`,
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER
      }
    });
    throw error;
  }
}

/**
 * Fetch transferred calls for a specific agent extension from agent_history
 * This function has been disabled as it's no longer needed
 * @returns {Promise<Array>} - Empty array
 */
export async function fetchTransferredCallsForExtension() {
  console.log('⚠️ fetchTransferredCallsForExtension has been disabled');
  return [];
}

/**
 * Process agent_history to detect transfers (no longer fetches CDR records)
 * @param {Array} agentHistory - Agent history array from a call record
 * @returns {Promise<Array>} - Empty array (functionality disabled)
 */
export async function processAgentHistoryForTransfers(agentHistory) {
  if (!Array.isArray(agentHistory) || agentHistory.length === 0) {
    return [];
  }
  
  console.log(`ℹ️ processAgentHistoryForTransfers has been disabled - no longer fetching CDR records`);
  return [];
}

/**
 * Fetch specific calls from API to check for recording updates
 * @param {string} endpoint - API endpoint to fetch from
 * @param {Array} callIds - Array of call IDs to fetch
 * @returns {Promise<Array>} - Array of updated call records
 */
export async function refetchCallsForRecordingUpdates(endpoint, callIds) {
  if (!callIds || callIds.length === 0) {
    return [];
  }
  
  try {
    // Get JWT token
    const token = await getPortalToken('default');
    if (!token) {
      throw new Error('Failed to obtain authentication token');
    }
    
    // Calculate time range for the last 4 hours to ensure we capture the calls
    const endDate = Math.floor(Date.now() / 1000);
    const startDate = endDate - (4 * 60 * 60); // 4 hours ago
    
    const queryParams = new URLSearchParams({
      account: process.env.ACCOUNT_ID_HEADER,
      startDate: startDate,
      endDate: endDate,
      pageSize: 2000
    });
    
    const fullUrl = `${BASE_URL}${endpoint}?${queryParams}`;
    
    console.log(`🎵 Re-fetching calls for recording updates from: ${endpoint}`);
    
    const response = await fetch(fullUrl, {
      method: 'GET',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': `Bearer ${token}`,
        'X-Account-ID': process.env.ACCOUNT_ID_HEADER
      },
      timeout: 30000
    });
    
    if (!response.ok) {
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const responseData = await response.json();
    const data = responseData.cdrs || responseData.data || [];
    
    // Filter to only the calls we're interested in
    const targetCalls = data.filter(call => {
      const callId = call.call_id || call.callid;
      return callIds.includes(callId);
    });
    
    console.log(`🎵 Found ${targetCalls.length}/${callIds.length} target calls in API response`);
    
    // Log recording status
    const callsWithRecording = targetCalls.filter(call => 
      (call.media_recording_id && call.media_recording_id !== '') || 
      (call.recording_filename && call.recording_filename !== '')
    );
    console.log(`🎵 ${callsWithRecording.length}/${targetCalls.length} calls now have recording information`);
    
    return targetCalls;
    
  } catch (error) {
    console.error(`Error re-fetching calls for recording updates: ${error.message}`);
    return [];
  }
}

/**
 * Fetch all records from a specific endpoint with full pagination
 * This function has been disabled as it's no longer needed
 * @returns {Promise<Object>} - Empty result object
 */
export async function fetchAllRecordsFromEndpoint() {
  console.log('⚠️ fetchAllRecordsFromEndpoint has been disabled');
  return {
    endpoint: 'disabled',
    totalRecords: 0,
    totalPages: 0,
    message: 'API fetching has been disabled'
  };
}

export default {
  fetchAllAPIsAndPopulateDB,
  fetchTransferredCallsForExtension,
  processAgentHistoryForTransfers,
  fetchAllRecordsFromEndpoint,
  refetchCallsForRecordingUpdates
};