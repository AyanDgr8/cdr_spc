// db-populate-service.js
// A robust service script to automatically populate the database at regular intervals
// Usage: node db-populate-service.js [intervalMinutes] [lookbackHours]
// Example: node db-populate-service.js 5 24

import dotenv from 'dotenv';
import { fetchAllAPIsAndPopulateDB, refetchCallsForRecordingUpdates } from './apiDataFetcher.js';
import finalReportService, { updateRecordingIds } from './finalReportService.js';
import dbService from './dbService.js';
import { getPortalToken } from './tokenService.js';
import fs from 'fs';
import path from 'path';

dotenv.config();

// Default configuration
const DEFAULT_INTERVAL_MINUTES = 5;
const DEFAULT_LOOKBACK_HOURS = 24;
const LOG_FILE = path.join(process.cwd(), 'db-populate-service.log');
const STATUS_FILE = path.join(process.cwd(), 'db-populate-status.json');

// Parse command line arguments
function parseArgs() {
  const args = process.argv.slice(2);
  
  // Get interval in minutes (default: 5 minutes)
  const intervalMinutes = args.length >= 1 ? parseInt(args[0], 10) : DEFAULT_INTERVAL_MINUTES;
  
  // Get lookback period in hours (default: 24 hours)
  const lookbackHours = args.length >= 2 ? parseInt(args[1], 10) : DEFAULT_LOOKBACK_HOURS;
  
  return {
    intervalMinutes: isNaN(intervalMinutes) ? DEFAULT_INTERVAL_MINUTES : intervalMinutes,
    lookbackHours: isNaN(lookbackHours) ? DEFAULT_LOOKBACK_HOURS : lookbackHours
  };
}

// Custom logging function that writes to both console and log file
function log(message, type = 'info') {
  const timestamp = new Date().toISOString();
  let formattedMessage;
  
  switch (type) {
    case 'error':
      formattedMessage = `[${timestamp}] ❌ ERROR: ${message}`;
      console.error(formattedMessage);
      break;
    case 'warning':
      formattedMessage = `[${timestamp}] ⚠️ WARNING: ${message}`;
      console.warn(formattedMessage);
      break;
    case 'success':
      formattedMessage = `[${timestamp}] ✅ SUCCESS: ${message}`;
      console.log(formattedMessage);
      break;
    default:
      formattedMessage = `[${timestamp}] ℹ️ INFO: ${message}`;
      console.log(formattedMessage);
  }
  
  // Append to log file
  fs.appendFileSync(LOG_FILE, formattedMessage + '\n');
  
  return formattedMessage;
}

// Function to check raw tables for data
async function checkRawTables() {
  log('Checking raw tables for data...');
  
  const tables = [
    'raw_queue_inbound',
    'raw_queue_outbound',
    'raw_campaigns'
  ];
  
  const counts = {};
  
  for (const table of tables) {
    try {
      const result = await dbService.query(`SELECT COUNT(*) as count FROM ${table}`);
      counts[table] = result[0].count;
      log(`${table}: ${result[0].count} records`);
    } catch (error) {
      log(`Error checking ${table}: ${error.message}`, 'error');
    }
  }
  
  return counts;
}

// Function to verify agent_disposition fields are present in stored raw data
async function verifyAgentDispositionFields() {
  log('Verifying agent_disposition fields in raw tables...');
  
  const tables = [
    { name: 'raw_queue_inbound', type: 'Inbound' },
    { name: 'raw_queue_outbound', type: 'Outbound' },
    { name: 'raw_campaigns', type: 'Campaign' }
  ];
  
  for (const table of tables) {
    try {
      // Get a sample of recent records (campaigns table uses call_id, others use callid)
      const idField = table.name === 'raw_campaigns' ? 'call_id' : 'callid';
      const sampleQuery = `
        SELECT ${idField} as callid, raw_data 
        FROM ${table.name} 
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 1 HOUR)
        ORDER BY created_at DESC 
        LIMIT 5
      `;
      
      const sampleRecords = await dbService.query(sampleQuery);
      log(`${table.type} (${table.name}): Found ${sampleRecords.length} recent records`);
      
      if (sampleRecords.length > 0) {
        let recordsWithDisposition = 0;
        let recordsWithSubdisposition = 0;
        
        for (const record of sampleRecords) {
          try {
            let rawData;
            
            // Handle both string and object types (MySQL auto-parses JSON fields)
            if (typeof record.raw_data === 'object') {
              rawData = record.raw_data;
            } else if (typeof record.raw_data === 'string') {
              rawData = JSON.parse(record.raw_data);
            } else {
              log(`  ❌ ${record.callid}: Unexpected raw_data type: ${typeof record.raw_data}`, 'error');
              continue;
            }
            
            // Check for agent_disposition
            if (rawData.agent_disposition !== undefined) {
              recordsWithDisposition++;
              log(`  ✅ ${record.callid}: agent_disposition = "${rawData.agent_disposition}"`);
            } else {
              log(`  ❌ ${record.callid}: agent_disposition MISSING`);
              log(`  🔍 Available fields: ${Object.keys(rawData).slice(0, 15).join(', ')}...`);
            }
            
            // Check for agent_subdisposition
            if (rawData.agent_subdisposition !== undefined) {
              recordsWithSubdisposition++;
              
              // Extract subdisposition using same logic as reportFetcher.js
              const [subDisp1, subDisp2] = (() => {
                let sd = rawData.agent_subdisposition ?? null;
                if (Array.isArray(sd)) sd = sd[0];
                if (!sd || typeof sd !== 'object') return ['', ''];
                const first = sd.name ?? '';
                
                // Handle both old and new subdisposition formats
                let second = '';
                if (sd.subdisposition) {
                  const subDisp = sd.subdisposition;
                  
                  // New format: subdisposition has key-value pairs
                  if (subDisp.key && subDisp.value) {
                    second = `${subDisp.key} = ${subDisp.value}`;
                  }
                  // Old format: subdisposition has name
                  else if (subDisp.name) {
                    second = subDisp.name;
                  }
                }
                
                return [first, second];
              })();
              
              log(`  ✅ ${record.callid}: agent_subdisposition = "${subDisp1}" | "${subDisp2}"`);
            } else {
              log(`  ❌ ${record.callid}: agent_subdisposition MISSING`);
            }
            
          } catch (parseError) {
            log(`  ❌ ${record.callid}: Error parsing raw_data - ${parseError.message}`, 'error');
          }
        }
        
        log(`${table.type} Summary: ${recordsWithDisposition}/${sampleRecords.length} have agent_disposition, ${recordsWithSubdisposition}/${sampleRecords.length} have agent_subdisposition`);
        
        if (recordsWithDisposition === 0) {
          log(`⚠️ WARNING: No ${table.type} records have agent_disposition field!`, 'warning');
        }
      }
      
    } catch (error) {
      log(`Error verifying ${table.name}: ${error.message}`, 'error');
    }
  }
}

// Function to update missing disposition fields, follow-up notes, and recording IDs for recent calls
async function updateMissingDispositionFields() {
  log('Updating missing disposition fields, follow-up notes, and recording IDs for recent calls...');
  
  const tables = [
    { name: 'raw_queue_inbound', type: 'Inbound', endpoint: '/api/v2/reports/queues_cdrs' },
    { name: 'raw_queue_outbound', type: 'Outbound', endpoint: '/api/v2/reports/queues_outbound_cdrs' },
    { name: 'raw_campaigns', type: 'Campaign', endpoint: '/api/v2/reports/campaigns/leads/history' }
  ];
  
  let totalUpdated = 0;
  let totalRecordingUpdates = 0;
  
  for (const table of tables) {
    try {
      log(`Processing ${table.type} calls for disposition and follow-up notes updates...`);
      
      // Find recent calls missing agent_disposition, follow_up_notes, or needing recording updates
      const idField = table.name === 'raw_campaigns' ? 'call_id' : 'callid';
      const missingFieldsQuery = `
        SELECT ${idField} as callid, raw_data, created_at
        FROM ${table.name} 
        WHERE created_at >= DATE_SUB(NOW(), INTERVAL 2 HOUR)
        AND (
          (JSON_EXTRACT(raw_data, '$.agent_disposition') IS NULL OR JSON_EXTRACT(raw_data, '$.agent_disposition') = '')
          OR (JSON_EXTRACT(raw_data, '$.follow_up_notes') IS NULL OR JSON_EXTRACT(raw_data, '$.follow_up_notes') = '')
          OR (JSON_EXTRACT(raw_data, '$.media_recording_id') IS NOT NULL OR JSON_EXTRACT(raw_data, '$.recording_filename') IS NOT NULL)
        )
        ORDER BY created_at DESC 
        LIMIT 50
      `;
      
      const missingRecords = await dbService.query(missingFieldsQuery);
      log(`${table.type}: Found ${missingRecords.length} calls for disposition, follow-up notes, or recording updates`);
      
      if (missingRecords.length === 0) {
        continue;
      }
      
      // Re-fetch these calls from the API to get updated information
      const callIds = missingRecords.map(record => record.callid);
      const updatedCalls = await refetchCallsFromAPI(table.endpoint, callIds);
      
      // Also re-fetch specifically for recording updates
      const recordingUpdatedCalls = await refetchCallsForRecordingUpdates(table.endpoint, callIds);
      
      log(`${table.type}: Re-fetched ${updatedCalls.length} calls from API`);
      log(`${table.type}: Re-fetched ${recordingUpdatedCalls.length} calls specifically for recording updates`);
      
      // Update records that now have disposition, follow-up notes, or recording information
      let updatedCount = 0;
      const callsToUpdateInFinalReport = [];
      const callsWithRecordingUpdates = [];
      
      for (const updatedCall of updatedCalls) {
        // Check if call has new disposition, follow-up notes, or recording information
        const hasDisposition = updatedCall.agent_disposition && updatedCall.agent_disposition !== '';
        const hasFollowUpNotes = updatedCall.follow_up_notes && updatedCall.follow_up_notes !== '';
        const hasRecording = (updatedCall.media_recording_id && updatedCall.media_recording_id !== '') || 
                            (updatedCall.recording_filename && updatedCall.recording_filename !== '');
        
        if (hasDisposition || hasFollowUpNotes || hasRecording) {
          try {
            const callId = updatedCall.call_id || updatedCall.callid;
            
            // Update raw table
            const updateQuery = `
              UPDATE ${table.name} 
              SET raw_data = ?, updated_at = NOW() 
              WHERE ${idField} = ?
            `;
            
            await dbService.query(updateQuery, [
              JSON.stringify(updatedCall),
              callId
            ]);
            
            // Track calls that need final_report update
            callsToUpdateInFinalReport.push({
              callId: callId,
              agent_disposition: updatedCall.agent_disposition,
              agent_subdisposition: updatedCall.agent_subdisposition,
              follow_up_notes: updatedCall.follow_up_notes
            });
            
            // Track calls with recording updates
            if (hasRecording) {
              callsWithRecordingUpdates.push({
                callId: callId,
                media_recording_id: updatedCall.media_recording_id,
                recording_filename: updatedCall.recording_filename
              });
            }
            
            updatedCount++;
            const dispositionInfo = hasDisposition ? `agent_disposition = "${updatedCall.agent_disposition}"` : '';
            const followUpInfo = hasFollowUpNotes ? `follow_up_notes = "${updatedCall.follow_up_notes}"` : '';
            const recordingInfo = hasRecording ? `recording = "${updatedCall.media_recording_id || updatedCall.recording_filename}"` : '';
            const updateInfo = [dispositionInfo, followUpInfo, recordingInfo].filter(info => info).join(', ');
            log(`  ✅ Updated ${callId}: ${updateInfo}`);
          } catch (updateError) {
            log(`  ❌ Failed to update ${updatedCall.call_id || updatedCall.callid}: ${updateError.message}`, 'error');
          }
        }
      }
      
      // Process recording updates from the dedicated recording fetch
      const additionalRecordingUpdates = [];
      for (const recordingCall of recordingUpdatedCalls) {
        const callId = recordingCall.call_id || recordingCall.callid;
        const hasRecording = (recordingCall.media_recording_id && recordingCall.media_recording_id !== '') || 
                            (recordingCall.recording_filename && recordingCall.recording_filename !== '');
        
        if (hasRecording) {
          additionalRecordingUpdates.push({
            callId: callId,
            media_recording_id: recordingCall.media_recording_id,
            recording_filename: recordingCall.recording_filename
          });
        }
      }
      
      // Combine all recording updates
      const allRecordingUpdates = [...callsWithRecordingUpdates, ...additionalRecordingUpdates];
      
      // Update final_report table with new disposition information
      if (callsToUpdateInFinalReport.length > 0) {
        await updateFinalReportDispositions(callsToUpdateInFinalReport);
        log(`${table.type}: Updated ${callsToUpdateInFinalReport.length} records in final_report table`);
      }
      
      // Update final_report table with new recording information using the dedicated function
      if (allRecordingUpdates.length > 0) {
        const recordingUpdates = await updateRecordingIds(allRecordingUpdates);
        totalRecordingUpdates += recordingUpdates;
        log(`${table.type}: Updated ${recordingUpdates} recording IDs in final_report table`);
      }
      
      log(`${table.type}: Successfully updated ${updatedCount} calls with disposition, follow-up notes, and recordings`);
      totalUpdated += updatedCount;
      
    } catch (error) {
      log(`Error updating disposition fields for ${table.name}: ${error.message}`, 'error');
    }
  }
  
  log(`Total calls updated with disposition and follow-up notes: ${totalUpdated}`);
  log(`Total recording IDs updated: ${totalRecordingUpdates}`);
  return { dispositionUpdates: totalUpdated, recordingUpdates: totalRecordingUpdates };
}

// Function to update final_report table with new disposition information
async function updateFinalReportDispositions(callsToUpdate) {
  if (!callsToUpdate || callsToUpdate.length === 0) {
    return;
  }
  
  try {
    log(`Updating ${callsToUpdate.length} records in final_report table with disposition and follow-up notes...`);
    
    for (const call of callsToUpdate) {
      try {
        // Extract disposition and follow-up notes information
        const agentDisposition = call.agent_disposition || '';
        
        // Extract subdisposition data using the same logic as reportFetcher.js
        const [subDisp1, subDisp2] = (() => {
          let sd = call.agent_subdisposition ?? null;
          if (Array.isArray(sd)) sd = sd[0];
          if (!sd || typeof sd !== 'object') return ['', ''];
          const first = sd.name ?? '';
          
          // Handle both old and new subdisposition formats
          let second = '';
          if (sd.subdisposition) {
            const subDisp = sd.subdisposition;
            
            // New format: subdisposition has key-value pairs
            if (subDisp.key && subDisp.value) {
              second = `${subDisp.key} = ${subDisp.value}`;
            }
            // Old format: subdisposition has name
            else if (subDisp.name) {
              second = subDisp.name;
            }
          }
          
          return [first, second];
        })();
        
        const followUpNotes = call.follow_up_notes || '';
        
        // Build dynamic update query based on available fields
        const fieldsToUpdate = [];
        const values = [];
        
        if (agentDisposition) {
          fieldsToUpdate.push('agent_disposition = ?');
          values.push(agentDisposition);
        }
        
        if (subDisp1 || subDisp2) {
          fieldsToUpdate.push('sub_disp_1 = ?', 'sub_disp_2 = ?');
          values.push(subDisp1, subDisp2);
        }
        
        if (followUpNotes) {
          fieldsToUpdate.push('follow_up_notes = ?');
          values.push(followUpNotes);
        }
        
        if (fieldsToUpdate.length > 0) {
          fieldsToUpdate.push('updated_at = NOW()');
          values.push(call.callId);
          
          const updateFinalReportQuery = `
            UPDATE final_report 
            SET ${fieldsToUpdate.join(', ')}
            WHERE call_id = ?
          `;
          
          await dbService.query(updateFinalReportQuery, values);
          
          const updateInfo = [];
          if (agentDisposition) updateInfo.push(`agent_disposition="${agentDisposition}"`);
          if (subDisp1 || subDisp2) updateInfo.push(`sub_disp_1="${subDisp1}", sub_disp_2="${subDisp2}"`);
          if (followUpNotes) updateInfo.push(`follow_up_notes="${followUpNotes}"`);
          
          log(`  ✅ Updated final_report for ${call.callId}: ${updateInfo.join(', ')}`);
        }
        
      } catch (updateError) {
        log(`  ❌ Failed to update final_report for ${call.callId}: ${updateError.message}`, 'error');
      }
    }
    
  } catch (error) {
    log(`Error updating final_report dispositions: ${error.message}`, 'error');
  }
}

// Note: Recording updates are now handled by the updateRecordingIds function from finalReportService.js

// Function to re-fetch specific calls from API
async function refetchCallsFromAPI(endpoint, callIds) {
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
    
    const fullUrl = `${process.env.BASE_URL}${endpoint}?${queryParams}`;
    
    log(`🔍 Re-fetching calls from: ${endpoint}`);
    
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
    
    log(`🔍 Found ${targetCalls.length}/${callIds.length} target calls in API response`);
    
    // Log disposition status
    const callsWithDisposition = targetCalls.filter(call => call.agent_disposition && call.agent_disposition !== '');
    log(`📊 ${callsWithDisposition.length}/${targetCalls.length} calls now have disposition information`);
    
    return targetCalls;
    
  } catch (error) {
    log(`Error re-fetching calls from API: ${error.message}`, 'error');
    return [];
  }
}

// Main function to fetch data and populate tables
async function populateDBWithTimeRange() {
  const startTime = Date.now();
  const runId = `run-${Date.now()}`;
  log(`Starting database population process (ID: ${runId})...`);
  
  try {
    // Calculate time range: from [lookbackHours] ago until now
    const endDate = Math.floor(Date.now() / 1000); // Current time in seconds
    const startDate = endDate - (config.lookbackHours * 60 * 60); // [lookbackHours] ago in seconds
    
    log(`Using date range: ${new Date(startDate * 1000).toISOString()} to ${new Date(endDate * 1000).toISOString()}`);
    log(`Lookback period: ${config.lookbackHours} hours`);
    
    // Step 1: Check current state of raw tables
    log('Step 1: Checking current state of raw tables...');
    const beforeCounts = await checkRawTables();
    
    // Step 2: Fetch data from APIs and populate raw tables
    log('Step 2: Fetching data from APIs and populating raw tables...');
    
    const apiParams = {
      start_date: startDate,
      end_date: endDate
    };
    
    // Fetch data from APIs and populate raw tables
    const fetchResults = await fetchAllAPIsAndPopulateDB('default', apiParams);
    log(`API fetch results: ${JSON.stringify(fetchResults)}`);
    
    // Step 2.1: Verify agent_disposition fields are present in stored data
    log('Step 2.1: Verifying agent_disposition fields in stored raw data...');
    await verifyAgentDispositionFields();
    
    // Step 2.2: Update missing disposition fields and recording IDs for recent calls
    log('Step 2.2: Updating missing disposition fields and recording IDs for recent calls...');
    const updateResults = await updateMissingDispositionFields();
    const dispositionUpdates = updateResults.dispositionUpdates || 0;
    const recordingUpdates = updateResults.recordingUpdates || 0;
    
    // Step 3: Check raw tables after population
    log('Step 3: Checking raw tables after population...');
    const afterCounts = await checkRawTables();
    
    // Calculate new records added
    const newRecords = {};
    let totalNewRecords = 0;
    
    for (const table in afterCounts) {
      if (beforeCounts[table] !== undefined) {
        newRecords[table] = afterCounts[table] - beforeCounts[table];
        totalNewRecords += newRecords[table];
      }
    }
    
    log(`Total new raw records added: ${totalNewRecords}`);
    
    // Step 4: Populate final_report table from raw tables
    log('Step 4: Populating final_report table from raw tables...');
    
    // Skip clearing existing data - we want to add to existing records
    log('Adding new data without clearing existing records...');
    
    // Populate final_report table with enhanced data
    log('Using enhanced final report population with CDR matching and follow-up notes...');
    const populateResult = await finalReportService.populateFinalReportEnhanced('default', {
      startDate,
      endDate
    });
    
    log(`Enhanced final report population result: ${JSON.stringify(populateResult)}`, 'success');
    
    // Step 5: Verify final_report table has data
    log('Step 5: Verifying final_report table has data...');
    const finalReportCount = await dbService.query('SELECT COUNT(*) as count FROM final_report');
    log(`final_report: ${finalReportCount[0].count} records`);
    
    // Check CDR records specifically
    const cdrCountQuery = "SELECT COUNT(*) as count FROM final_report WHERE record_type = 'CDR'";
    const cdrCount = await dbService.query(cdrCountQuery);
    log(`CDR records in final_report: ${cdrCount[0].count} records`);
    
    // Check follow-up notes
    const notesQuery = 'SELECT COUNT(*) as count FROM final_report WHERE follow_up_notes IS NOT NULL';
    const notesCount = await dbService.query(notesQuery);
    log(`Records with follow-up notes: ${notesCount[0].count} records`);
    
    // Check record type distribution
    const distributionQuery = `
      SELECT record_type, COUNT(*) as count 
      FROM final_report 
      GROUP BY record_type 
      ORDER BY count DESC
    `;
    const distribution = await dbService.query(distributionQuery);
    log('Record type distribution in final_report:');
    distribution.forEach(row => {
      log(`  ${row.record_type}: ${row.count} records`);
    });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    log(`Process completed successfully in ${duration}s!`, 'success');
    
    // Update status file with last successful run
    updateStatusFile({
      lastRun: new Date().toISOString(),
      status: 'success',
      duration: `${duration}s`,
      recordsAdded: populateResult.standardPopulation?.recordCount || 0,
      followUpNotesUpdated: populateResult.followUpNotesUpdated || 0,
      dispositionUpdates: dispositionUpdates || 0,
      recordingUpdates: recordingUpdates || 0,
      nextRun: new Date(Date.now() + (config.intervalMinutes * 60 * 1000)).toISOString()
    });
    
    return true;
  } catch (error) {
    log(`Error in database population process (ID: ${runId}): ${error.message}`, 'error');
    log(error.stack, 'error');
    
    // Update status file with error information
    updateStatusFile({
      lastRun: new Date().toISOString(),
      status: 'error',
      error: error.message,
      nextRun: new Date(Date.now() + (config.intervalMinutes * 60 * 1000)).toISOString()
    });
    
    return false;
  }
}

// Function to update status file
function updateStatusFile(status) {
  const statusFile = path.join(process.cwd(), 'db-populate-status.json');
  
  try {
    // Read existing status if available
    let currentStatus = {};
    if (fs.existsSync(statusFile)) {
      currentStatus = JSON.parse(fs.readFileSync(statusFile, 'utf8'));
    }
    
    // Update with new status
    const updatedStatus = {
      ...currentStatus,
      ...status,
      lastUpdated: new Date().toISOString()
    };
    
    // Write updated status
    fs.writeFileSync(statusFile, JSON.stringify(updatedStatus, null, 2));
  } catch (error) {
    log(`Error updating status file: ${error.message}`, 'error');
  }
}

// Function to run the service with error handling and recovery
async function runService() {
  try {
    await populateDBWithTimeRange();
  } catch (error) {
    log(`Critical service error: ${error.message}`, 'error');
    log(error.stack, 'error');
  }
  
  // Schedule next run regardless of success or failure
  scheduleNextRun();
}

// Function to schedule the next run
function scheduleNextRun() {
  const intervalMs = config.intervalMinutes * 60 * 1000;
  const nextRunTime = new Date(Date.now() + intervalMs);
  log(`Scheduling next run at ${nextRunTime.toISOString()} (in ${config.intervalMinutes} minutes)`);
  
  // Update status file with next run time
  updateStatusFile({
    nextRun: nextRunTime.toISOString()
  });
  
  setTimeout(() => {
    runService();
  }, intervalMs);
}

// Initialize log file
function initializeLogFile() {
  const header = `
=========================================
DB POPULATE SERVICE STARTED
=========================================
Date: ${new Date().toISOString()}
Interval: ${config.intervalMinutes} minutes
Lookback: ${config.lookbackHours} hours
=========================================
`;
  
  // Create or truncate log file
  fs.writeFileSync(LOG_FILE, header);
  log('Log file initialized');
}

// Parse configuration from command line arguments
const config = parseArgs();
log(`Service configured with: ${config.intervalMinutes} minute intervals, ${config.lookbackHours} hour lookback`);

// Initialize
(async () => {
  // Initialize log file
  initializeLogFile();
  
  // Create initial status file
  updateStatusFile({
    serviceStarted: new Date().toISOString(),
    status: 'starting',
    config: {
      intervalMinutes: config.intervalMinutes,
      lookbackHours: config.lookbackHours
    }
  });
  
  // Start the service
  log('Starting initial run...');
  await runService();
})();

// Handle graceful shutdown
process.on('SIGINT', () => {
  log('Received SIGINT. Shutting down gracefully...', 'warning');
  updateStatusFile({
    status: 'stopped',
    reason: 'SIGINT received'
  });
  process.exit(0);
});

process.on('SIGTERM', () => {
  log('Received SIGTERM. Shutting down gracefully...', 'warning');
  updateStatusFile({
    status: 'stopped',
    reason: 'SIGTERM received'
  });
  process.exit(0);
});

// Handle uncaught exceptions
process.on('uncaughtException', (error) => {
  log(`Uncaught exception: ${error.message}`, 'error');
  log(error.stack, 'error');
  updateStatusFile({
    status: 'crashed',
    error: error.message,
    stack: error.stack
  });
  
  // Give time for logs to be written before exiting
  setTimeout(() => {
    process.exit(1);
  }, 1000);
});
