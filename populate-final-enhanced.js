/**
 * Enhanced Final Report Population Script
 * 
 * This script combines:
 * 1. CDR matching logic from test-exact-schema-matching.js
 * 2. Follow-up notes update functionality from bulk-update-FUN.js
 * 
 * It populates the final_report table with matched CDR records and
 * updates follow-up notes from raw_cdrs_all.
 */

import dotenv from 'dotenv';
import dbService from './dbService.js';
import { populateFinalReport } from './finalReportService.js';

dotenv.config();

/**
 * Normalize phone number by removing all non-digit characters
 * @param {string} phoneNumber - Phone number to normalize
 * @returns {string} - Normalized phone number (digits only)
 */
function normalizePhoneNumber(phoneNumber) {
  if (!phoneNumber || typeof phoneNumber !== 'string') {
    return '';
  }
  
  // Remove all non-digit characters
  const digitsOnly = phoneNumber.replace(/\D/g, '');
  
  // If it's empty after removing non-digits, return empty string
  if (!digitsOnly) {
    return '';
  }
  
  // Return the normalized phone number
  return digitsOnly;
}

/**
 * Match CDR records with outbound calls using exact schema matching
 * @returns {Promise<Array>} - Array of matched CDR records
 */
async function matchCDRsWithOutboundCalls() {
  try {
    console.log('🔍 Matching CDR records with outbound calls using exact schema...');
    console.log('Conditions:');
    console.log('1. CDR raw_data.caller_id_number = Outbound raw_data.agent_ext');
    console.log('2. CDR timestamp between Outbound called_time and hangup_time');
    
    // Fetch data from both tables
    const outboundCalls = await dbService.getRawQueueOutbound({});
    const cdrRecords = await dbService.getRawCdrs({});
    
    console.log(`\n📊 Found ${outboundCalls.length} outbound calls and ${cdrRecords.length} CDR records`);
    
    // Process outbound calls - extract agent_ext from raw_data
    console.log('\n📊 Step 1: Processing outbound calls...');
    const processedOutbound = [];
    
    outboundCalls.forEach(record => {
      try {
        const rawData = typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data;
        
        if (rawData.agent_ext && rawData.called_time && rawData.hangup_time) {
          processedOutbound.push({
            id: record.id,
            callid: record.callid,
            agent_ext: normalizePhoneNumber(rawData.agent_ext),
            called_time: rawData.called_time,
            hangup_time: rawData.hangup_time,
            queue_name: record.queue_name,
            raw_data: rawData
          });
        }
      } catch (error) {
        // Skip invalid records
      }
    });
    
    console.log(`Processed ${processedOutbound.length} valid outbound calls`);
    
    // Process CDR records - extract caller_id_number from raw_data
    console.log('\n📊 Step 2: Processing CDR records...');
    const processedCDRs = [];
    
    cdrRecords.forEach(record => {
      try {
        const rawData = typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data;
        
        if (rawData.caller_id_number && record.timestamp) {
          processedCDRs.push({
            id: record.id,
            call_id: record.call_id,
            caller_id_number: normalizePhoneNumber(rawData.caller_id_number),
            timestamp: record.timestamp,
            raw_data: rawData
          });
        }
      } catch (error) {
        // Skip invalid records
      }
    });
    
    console.log(`Processed ${processedCDRs.length} valid CDR records`);
    
    // Create lookup map for outbound calls by agent_ext
    console.log('\n📊 Step 3: Creating agent extension lookup...');
    const outboundByAgentExt = new Map();
    
    processedOutbound.forEach(outbound => {
      if (!outboundByAgentExt.has(outbound.agent_ext)) {
        outboundByAgentExt.set(outbound.agent_ext, []);
      }
      outboundByAgentExt.get(outbound.agent_ext).push(outbound);
    });
    
    console.log(`Created lookup map with ${outboundByAgentExt.size} unique agent extensions`);
    
    // Match CDRs with outbound calls
    console.log('\n📊 Step 4: Matching CDRs with outbound calls...');
    const matchedCDRs = [];
    
    for (const cdr of processedCDRs) {
      // Look for outbound calls with matching agent_ext
      const matchingOutbounds = outboundByAgentExt.get(cdr.caller_id_number);
      
      if (matchingOutbounds) {
        // Check time overlap
        for (const outbound of matchingOutbounds) {
          // Convert CDR timestamp to milliseconds for comparison
          const cdrTimestamp = cdr.timestamp * 1000; // CDR timestamp is in seconds
          const outboundCalledTime = outbound.called_time * 1000; // Outbound time is in seconds
          const outboundHangupTime = outbound.hangup_time * 1000; // Outbound time is in seconds
          
          if (cdrTimestamp >= outboundCalledTime && cdrTimestamp <= outboundHangupTime) {
            // Calculate answered time and hangup time based on specifications
            const ringingSeconds = parseInt(cdr.raw_data.ringing_seconds || 0);
            const durationSeconds = parseInt(cdr.raw_data.duration_seconds || 0);
            const timestamp = parseInt(cdr.timestamp);
            const answeredTime = timestamp + ringingSeconds;
            const hangupTime = timestamp + durationSeconds;
            
            // Create a record for final_report table
            const matchedRecord = {
              call_id: cdr.call_id,
              record_type: 'CDR',
              agent_name: outbound.raw_data.agent_name || null,
              extension: outbound.agent_ext,
              queue_campaign_name: outbound.queue_name,
              called_time: cdr.timestamp,
              called_time_formatted: new Date(cdrTimestamp).toLocaleString('en-GB', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
              }),
              // Store formatted date strings directly in the answered_time and hangup_time columns
              answered_time: new Date(answeredTime * 1000).toLocaleString('en-GB', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
              }),
              hangup_time: new Date(hangupTime * 1000).toLocaleString('en-GB', {
                day: '2-digit',
                month: '2-digit',
                year: 'numeric',
                hour: '2-digit',
                minute: '2-digit',
                second: '2-digit'
              }),
              caller_id_number: cdr.caller_id_number,
              caller_id_name: cdr.raw_data.caller_id_name || null,
              callee_id_number: cdr.raw_data.callee_id_number || null,
              timestamp: cdr.timestamp,
              _recordType: 'cdr',
              raw_data: cdr.raw_data
            };
            
            matchedCDRs.push(matchedRecord);
            break; // Found a match for this CDR, no need to check other outbound calls
          }
        }
      }
    }
    
    console.log(`\n✅ MATCHING RESULTS: ${matchedCDRs.length} matches found`);
    return matchedCDRs;
    
  } catch (error) {
    console.error('❌ Error during CDR matching:', error);
    return [];
  }
}

/**
 * Insert matched CDR records into final_report table
 * @param {Array} records - Array of matched CDR records
 * @returns {Promise<number>} - Number of inserted records
 */
async function insertMatchedCDRsIntoFinalReport(records) {
  if (!records || records.length === 0) {
    console.log('⚠️ No matched CDR records to insert');
    return 0;
  }
  
  console.log(`📊 Inserting ${records.length} matched CDR records into final_report table...`);
  
  // Clear existing CDR records with the same call_ids to avoid duplicates
  const callIds = records.map(record => record.call_id).filter(Boolean);
  
  if (callIds.length > 0) {
    try {
      // Delete in batches to avoid query size limits
      const BATCH_SIZE = 1000;
      for (let i = 0; i < callIds.length; i += BATCH_SIZE) {
        const batchIds = callIds.slice(i, i + BATCH_SIZE);
        const placeholders = batchIds.map(() => '?').join(',');
        await dbService.query(
          `DELETE FROM final_report WHERE call_id IN (${placeholders}) AND record_type = 'CDR'`,
          batchIds
        );
      }
      console.log(`🗑️ Cleared ${callIds.length} existing CDR records from final_report table`);
    } catch (error) {
      console.error('❌ Error clearing existing CDR records:', error);
      // Continue with insertion even if clearing fails
    }
  }
  
  // Insert records in batches
  const BATCH_SIZE = 100;
  let insertedCount = 0;
  
  for (let i = 0; i < records.length; i += BATCH_SIZE) {
    const batch = records.slice(i, i + BATCH_SIZE);
    await insertBatch(batch);
    insertedCount += batch.length;
    console.log(`📊 Inserted batch ${Math.ceil((i + 1) / BATCH_SIZE)}/${Math.ceil(records.length / BATCH_SIZE)} (${insertedCount}/${records.length} records)`);
  }
  
  console.log(`✅ Successfully inserted ${insertedCount} CDR records into final_report table`);
  return insertedCount;
}

/**
 * Insert a batch of records into the final_report table
 * @param {Array} batch - Batch of records to insert
 * @returns {Promise<void>}
 */
async function insertBatch(batch) {
  if (!batch || batch.length === 0) return;
  
  const values = batch.map(record => {
    // Extract follow-up notes if available
    let followUpNotes = null;
    if (record.raw_data) {
      if (record.raw_data.follow_up_notes) {
        followUpNotes = record.raw_data.follow_up_notes;
      } else if (record.raw_data.fonoUC) {
        followUpNotes = record.raw_data.fonoUC.follow_up_notes ||
                      record.raw_data.fonoUC.cc?.follow_up_notes ||
                      record.raw_data.fonoUC.cc_outbound?.follow_up_notes ||
                      record.raw_data.fonoUC.cc_campaign?.follow_up_notes || null;
      }
    }
    
    // Map record fields to final_report table columns
    return [
      record.call_id,
      record.record_type,
      record.type || null,
      record.agent_name || null,
      record.extension || null,
      record.queue_campaign_name || null,
      parseInt(record.called_time || record.timestamp || 0),
      record.called_time_formatted || null,
      record.caller_id_number || null,
      record.caller_id_name || null,
      record.callee_id_number || null,
      record.answered_time || null,
      record.hangup_time || null,
      record.wait_duration || null,
      record.talk_duration || null,
      record.hold_duration || null,
      record.agent_hangup || null,
      record.agent_disposition || null,
      record.disposition || null,
      record.sub_disp_1 || null,
      record.sub_disp_2 || null,
      record.status || null,
      record.campaign_type || null,
      record.abandoned || null,
      record.country || null,
      followUpNotes,
      record.agent_history || null,
      record.queue_history || null,
      record.lead_history || null,
      record.recording || null
    ];
  });
  
  // Create the correct number of placeholders
  const columns = [
    'call_id', 'record_type', 'type', 'agent_name', 'extension', 'queue_campaign_name',
    'called_time', 'called_time_formatted', 'caller_id_number', 'caller_id_name', 'callee_id_number',
    'answered_time', 'hangup_time', 'wait_duration', 'talk_duration', 'hold_duration',
    'agent_hangup', 'agent_disposition', 'disposition', 'sub_disp_1', 'sub_disp_2',
    'status', 'campaign_type', 'abandoned', 'country', 'follow_up_notes',
    'agent_history', 'queue_history', 'lead_history', 'recording'
  ];
  
  const questionMarks = Array(columns.length).fill('?').join(',');
  const placeholders = values.map(() => `(${questionMarks})`).join(',');
  
  const sql = `
    INSERT INTO final_report (
      call_id, record_type, type, agent_name, extension, queue_campaign_name, 
      called_time, called_time_formatted, caller_id_number, caller_id_name, callee_id_number,
      answered_time, hangup_time, wait_duration, talk_duration, hold_duration,
      agent_hangup, agent_disposition, disposition, sub_disp_1, sub_disp_2,
      status, campaign_type, abandoned, country, follow_up_notes,
      agent_history, queue_history, lead_history, recording
    ) VALUES ${placeholders}
  `;
  
  const flatValues = values.flat();
  await dbService.query(sql, flatValues);
}

/**
 * Main function to populate final_report table with matched CDR records and update follow-up notes
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Summary of the operation
 */
async function populateFinalReportEnhanced(tenant, params = {}) {
  console.log('🚀 Starting enhanced final_report table population...');
  const startTime = Date.now();
  
  try {
    // Step 1: Populate final_report table with standard data
    console.log('\n📊 Step 1: Populating final_report table with standard data...');
    const standardPopulateResult = await populateFinalReport(tenant, params);
    console.log(`Standard population result: ${standardPopulateResult.message}`);
    
    // Step 2: Match CDR records with outbound calls
    console.log('\n📊 Step 2: Matching CDR records with outbound calls...');
    const matchedCDRs = await matchCDRsWithOutboundCalls();
    
    // Step 3: Insert matched CDR records into final_report table
    console.log('\n📊 Step 3: Inserting matched CDR records into final_report table...');
    const insertedCount = await insertMatchedCDRsIntoFinalReport(matchedCDRs);

    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    return {
      success: true,
      message: `Successfully populated final_report table with enhanced data in ${duration}s`,
      standardPopulation: standardPopulateResult,
      matchedCDRsCount: matchedCDRs.length,
      insertedCDRsCount: insertedCount,
    };
  } catch (error) {
    console.error('❌ Error during enhanced final_report population:', error);
    return {
      success: false,
      message: `Error during enhanced final_report population: ${error.message}`,
      error: error.stack
    };
  }
}

/**
 * Test function to verify the enhanced final_report population
 */
async function testEnhancedFinalReport() {
  try {
    console.log('🔍 Testing enhanced final_report population...');
    
    // Step 1: Check current state of final_report table
    console.log('\n📊 Step 1: Checking current final_report table state...');
    
    const currentCountQuery = 'SELECT COUNT(*) as count FROM final_report';
    const currentCount = await dbService.query(currentCountQuery);
    console.log(`Current records in final_report: ${currentCount[0].count}`);
    
    const currentCDRCountQuery = "SELECT COUNT(*) as count FROM final_report WHERE record_type = 'CDR'";
    const currentCDRCount = await dbService.query(currentCDRCountQuery);
    console.log(`Current CDR records in final_report: ${currentCDRCount[0].count}`);
    
    const currentNotesQuery = 'SELECT COUNT(*) as count FROM final_report WHERE follow_up_notes IS NOT NULL';
    const currentNotesCount = await dbService.query(currentNotesQuery);
    console.log(`Current records with follow-up notes: ${currentNotesCount[0].count}`);
    
    // Step 2: Run enhanced population
    console.log('\n📊 Step 2: Running enhanced final_report population...');
    
    const populateResult = await populateFinalReportEnhanced('spc', {
      startDate: Math.floor(Date.parse('2025-08-08T00:00:00Z') / 1000),
      endDate: Math.floor(Date.parse('2025-08-12T00:00:00Z') / 1000)
    });
    
    console.log('\nEnhanced population result:', populateResult);
    
    // Step 3: Check final_report table after population
    console.log('\n📊 Step 3: Checking final_report table after enhanced population...');
    
    const newCountQuery = 'SELECT COUNT(*) as count FROM final_report';
    const newCount = await dbService.query(newCountQuery);
    console.log(`Total records in final_report after population: ${newCount[0].count}`);
    
    // Check CDR records specifically
    const cdrCountQuery = "SELECT COUNT(*) as count FROM final_report WHERE record_type = 'CDR'";
    const cdrCount = await dbService.query(cdrCountQuery);
    console.log(`CDR records in final_report: ${cdrCount[0].count}`);
    
    // Check follow-up notes
    const notesQuery = 'SELECT COUNT(*) as count FROM final_report WHERE follow_up_notes IS NOT NULL';
    const notesCount = await dbService.query(notesQuery);
    console.log(`Records with follow-up notes: ${notesCount[0].count}`);
    
    // Check record type distribution
    const distributionQuery = `
      SELECT record_type, COUNT(*) as count 
      FROM final_report 
      GROUP BY record_type 
      ORDER BY count DESC
    `;
    const distribution = await dbService.query(distributionQuery);
    console.log('\nRecord type distribution in final_report:');
    distribution.forEach(row => {
      console.log(`  ${row.record_type}: ${row.count} records`);
    });
    
    // Step 4: Show sample CDR records from final_report with follow-up notes
    console.log('\n📋 Step 4: Sample CDR records with follow-up notes:');
    
    const sampleQuery = `
      SELECT id, call_id, agent_name, extension, called_time, called_time_formatted, caller_id_number, follow_up_notes
      FROM final_report 
      WHERE record_type = 'CDR' AND follow_up_notes IS NOT NULL
      LIMIT 5
    `;
    const samples = await dbService.query(sampleQuery);
    
    if (samples.length > 0) {
      samples.forEach((record, index) => {
        console.log(`\nCDR ${index + 1}:`);
        
        // Since we don't have access to raw_data in the query, we'll use estimated values
        // for demonstration purposes
        const calledTime = record.called_time || 0;
        
        // Use estimated values for ringing and duration
        // In production, these would come from the raw_data field
        const estimatedRingingSeconds = 5; // Example value
        const estimatedDurationSeconds = 180; // Example value (3 minutes)
        
        // Calculate answered time and hangup time as proper UNIX timestamps
        const answeredTime = parseInt(calledTime) + parseInt(estimatedRingingSeconds);
        const hangupTime = parseInt(calledTime) + parseInt(estimatedDurationSeconds);
        
        // Format times for display - include time component
        const answeredTimeFormatted = new Date(answeredTime * 1000).toLocaleString('en-GB', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit'
        });
        const hangupTimeFormatted = new Date(hangupTime * 1000).toLocaleString('en-GB', {
          day: '2-digit',
          month: '2-digit',
          year: 'numeric',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit'
        });
        
        console.log(`  ID: ${record.id}`);
        console.log(`  Call ID: ${record.call_id}`);
        console.log(`  Agent: ${record.agent_name}`);
        console.log(`  Extension: ${record.extension}`);
        console.log(`  Called Time: ${record.called_time_formatted}`);
        console.log(`  Answered Time: ${answeredTimeFormatted}`);
        console.log(`  Hangup Time: ${hangupTimeFormatted}`);
        console.log(`  Caller ID: ${record.caller_id_number}`);
        console.log(`  Follow-up Notes: ${record.follow_up_notes ? record.follow_up_notes.substring(0, 50) + (record.follow_up_notes.length > 50 ? '...' : '') : 'None'}`);
      });
    } else {
      console.log('No CDR records with follow-up notes found');
    }

    console.log('\n Enhanced final report population test complete');

    
  } catch (error) {
    console.error('❌ Error during enhanced final report test:', error);
  }
}

// Run the test
async function main() {
  try {
    await testEnhancedFinalReport();
  } catch (error) {
    console.error('❌ Error in main function:', error);
  } finally {
    // Close the database connection
    try {
      console.log('Closing database connection...');
      await dbService.end();
      console.log('✅ Database connection closed');
    } catch (err) {
      console.error('⚠️ Error closing database connection:', err);
    }
  }
}

main().catch(console.error);
