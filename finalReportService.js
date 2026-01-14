// finalReportService.js
// Service for managing the final_report table that contains pre-processed data
// for instant querying without complex processing

import dbService from './dbService.js';
import { createUnifiedReportFromDB, processRecordData } from './reportFetcher.js';

// /**
//  * Format timestamp to local time string
//  * @param {number|string} timestamp - Timestamp to format
//  * @returns {string} - Formatted timestamp string
//  */
// function formatTimestamp(timestamp) {
//   if (!timestamp) return '';
  
//   try {
//     // Convert to milliseconds if in seconds
//     const ts = typeof timestamp === 'number' && timestamp < 10000000000 ? timestamp * 1000 : timestamp;
//     return new Date(ts).toLocaleString('en-GB', { timeZone: 'Asia/Dubai' });
//   } catch (error) {
//     return '';
//   }
// }

// /**
//  * Format duration in seconds to HH:MM:SS format
//  * @param {number|string} seconds - Duration in seconds
//  * @returns {string} - Formatted duration string
//  */
// function formatDuration(seconds) {
//   if (!seconds) return '';
  
//   try {
//     const secs = parseInt(seconds, 10);
//     if (isNaN(secs)) return '';
    
//     const hours = Math.floor(secs / 3600);
//     const minutes = Math.floor((secs % 3600) / 60);
//     const remainingSeconds = secs % 60;
    
//     return [
//       hours.toString().padStart(2, '0'),
//       minutes.toString().padStart(2, '0'),
//       remainingSeconds.toString().padStart(2, '0')
//     ].join(':');
//   } catch (error) {
//     return '';
//   }
// }

/**
 * Populate the final_report table with processed data from raw tables
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Summary of the operation
 */
export async function populateFinalReport(tenant, params = {}) {
  console.log('🚀 Starting final_report table population...');
  const startTime = Date.now();
  
  try {
    // Step 1: Get unified report data (already processed)
    console.log('📊 Step 1: Fetching unified report data...');
    const unifiedReport = await createUnifiedReportFromDB(tenant, params);
    
    if (!unifiedReport || !unifiedReport.rows || unifiedReport.rows.length === 0) {
      return { success: false, message: 'No data available to populate final_report table' };
    }
    
    console.log(`📊 Retrieved ${unifiedReport.rows.length} records from unified report`);
    
    const combinedRecords = [...unifiedReport.rows];
    
    // Step 4: Insert combined data into final_report table
    console.log('📊 Step 4: Inserting combined data into final_report table...');
    await insertIntoFinalReport(combinedRecords);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    return {
      success: true,
      message: `Successfully populated final_report table with ${combinedRecords.length} records in ${duration}s`,
      recordCount: combinedRecords.length,
    };
  } catch (error) {
    console.error('❌ Error populating final_report table:', error);
    return {
      success: false,
      message: `Error populating final_report table: ${error.message}`,
      error: error.stack
    };
  }
}

/**
 * Insert processed records into the final_report table
 * @param {Array} records - Array of processed records
 * @returns {Promise<void>}
 */
async function insertIntoFinalReport(records) {
  if (!records || records.length === 0) {
    console.log('⚠️ No records to insert into final_report table');
    return;
  }
  
  console.log(`📊 Inserting ${records.length} records into final_report table...`);
  
  // Check for existing records with the same record_type and call_id to avoid duplicates
  let newRecords = [...records]; // Start with all records
  
  try {
    // Check in batches to avoid query size limits
    const BATCH_SIZE = 1000;
    const existingRecordKeys = new Set();
    
    // Group records by record_type for more efficient checking
    const recordsByType = {};
    records.forEach(record => {
      const recordType = record._recordType === 'campaign' ? 'Campaign' :
        record._recordType === 'inbound' ? 'Inbound' :
        record._recordType === 'outbound' ? 'Outbound' :
        record._recordType === 'outbound_transfer' ? 'Outbound' : 'Unknown';
      
      const callId = record.call_id || record.callid;
      if (!callId) return;
      
      if (!recordsByType[recordType]) {
        recordsByType[recordType] = [];
      }
      recordsByType[recordType].push(callId);
    });


    
    // Filter out records that already exist in the database
    newRecords = records.filter(record => {
      const recordType = record._recordType === 'campaign' ? 'Campaign' :
        record._recordType === 'inbound' ? 'Inbound' :
        record._recordType === 'outbound' ? 'Outbound' :
        record._recordType === 'outbound_transfer' ? 'Outbound' : 'Unknown';
      
      const callId = record.call_id || record.callid;
      if (!callId) return true; // Keep records without call_id
      
      const recordKey = `${recordType}:${callId}`;
      return !existingRecordKeys.has(recordKey);
    });
    
    console.log(`🔍 Found ${existingRecordKeys.size} existing records in final_report table`);
    console.log(`📊 Adding ${newRecords.length} new records (skipping ${records.length - newRecords.length} duplicates)`);
  } catch (error) {
    console.error('❌ Error checking existing records:', error);
    // Continue with insertion of all records if checking fails
  }
  
  // Insert records in batches
  const BATCH_SIZE = 100;
  let insertedCount = 0;
  
  for (let i = 0; i < newRecords.length; i += BATCH_SIZE) {
    const batch = newRecords.slice(i, i + BATCH_SIZE);
    await insertBatch(batch);
    insertedCount += batch.length;
    console.log(`📊 Inserted batch ${Math.ceil((i + 1) / BATCH_SIZE)}/${Math.ceil(newRecords.length / BATCH_SIZE)} (${insertedCount}/${newRecords.length} records)`);
  }
  
  console.log(`✅ Successfully inserted ${insertedCount} records into final_report table`);
}

/**
 * Insert a batch of records into the final_report table
 * @param {Array} batch - Batch of records to insert
 * @returns {Promise<void>}
 */
async function insertBatch(batch) {
  if (!batch || batch.length === 0) return;
  
  console.log(`Inserting batch of ${batch.length} records`);
  
  // Process CDR records to ensure all fields are properly extracted
  const processedBatch = batch.map((record, index) => {
    try {
      // Enhanced disposition validation and logging
      if (record['Agent Disposition'] && typeof record['Agent Disposition'] === 'object') {
        console.warn(`⚠️ Record ${index}: Agent Disposition is object:`, JSON.stringify(record['Agent Disposition']));
        record['Agent Disposition'] = JSON.stringify(record['Agent Disposition']);
      }
      
      if (record.Disposition && typeof record.Disposition === 'object') {
        console.warn(`⚠️ Record ${index}: Disposition is object:`, JSON.stringify(record.Disposition));
        record.Disposition = JSON.stringify(record.Disposition);
      }
      
      // Enhanced subdisposition validation
      if (record['Sub_disp_1'] && typeof record['Sub_disp_1'] === 'object') {
        console.warn(`⚠️ Record ${index}: Sub_disp_1 is object:`, JSON.stringify(record['Sub_disp_1']));
        record['Sub_disp_1'] = record['Sub_disp_1'].name || JSON.stringify(record['Sub_disp_1']);
      }
      
      if (record['Sub_disp_2'] && typeof record['Sub_disp_2'] === 'object') {
        console.warn(`⚠️ Record ${index}: Sub_disp_2 is object:`, JSON.stringify(record['Sub_disp_2']));
        record['Sub_disp_2'] = record['Sub_disp_2'].name || JSON.stringify(record['Sub_disp_2']);
      }
      
      // Validate field lengths to prevent truncation
      const maxLengths = {
        'Agent Disposition': 100,
        'Disposition': 100,
        'Sub_disp_1': 200,
        'Sub_disp_2': 200
      };
      
      Object.keys(maxLengths).forEach(field => {
        if (record[field] && record[field].length > maxLengths[field]) {
          console.warn(`⚠️ Record ${index}: ${field} exceeds max length (${record[field].length} > ${maxLengths[field]})`);
          record[field] = record[field].substring(0, maxLengths[field]);
        }
      });
      
      return record;
    } catch (error) {
      console.error(`❌ Error processing record ${index}:`, error);
      console.error(`❌ Problematic record:`, JSON.stringify(record, null, 2));
      return record; // Return original record to avoid losing data
    }
  });
  
  
  // Simple count of records with follow-up notes for logging purposes
  const recordsWithNotes = processedBatch.filter(record => 
    record['Follow up notes'] || record.follow_up_notes
  );
  
  if (recordsWithNotes.length > 0) {
    console.log(`📝 Found ${recordsWithNotes.length} records with follow-up notes in this batch`);
  }
  
  const values = processedBatch.map(record => {
    // Extract follow-up notes directly from the record
    const followUpNotes = record['Follow up notes'] || record.follow_up_notes || null;
    const callId = record.call_id || record.callid || null;
    
    // Map record fields to final_report table columns
    return [
      callId,
      record._recordType === 'campaign' ? 'Campaign' :
        record._recordType === 'inbound' ? 'Inbound' :
        record._recordType === 'outbound' ? 'Outbound' :
        record._recordType === 'outbound_transfer' ? 'Outbound' : 'Unknown',
      record.Type || null,
      record['Agent name'] || record.agent_name || null,
      record.Extension || record.extension || null,
      record['Queue / Campaign Name'] || null,
      record.called_time || record.timestamp || null,
      record['Called Time'] || null,
      record['Caller ID Number'] || record.caller_id_number || null,
      record['Caller ID / Lead Name'] || record.caller_id_name || null,
      record['Callee ID / Lead number'] || record.callee_id_number || null,
      record['Answered time'] || null,
      record['Hangup time'] || null,
      record['Wait Duration'] || null,
      record['Talk Duration'] || null,
      record['Hold Duration'] || null,
      record['Hold Duration Intervals'] || null,
      record['Agent Hangup'] || null,
      record['Agent Disposition'] || null,
      record['Disposition'] || record['System Disposition'] || null,
      record['Sub_disp_1'] || record['Sub Disp 1'] || null,
      record['Sub_disp_2'] || record['Sub Disp 2'] || null,
      record.Status || null,
      record['Campaign Type'] || null,
      record.Abandoned || null,
      record.Country || null,
      followUpNotes, // Use the extracted follow-up notes
      record['Agent History'] || record.agent_history || null,
      // For campaigns, use Lead History as Queue History, otherwise use Queue History
      record._recordType === 'campaign' ? (record['Lead History'] || record.lead_history || record['Queue History'] || record.queue_history || null) : (record['Queue History'] || record.queue_history || null),
      record['Lead History'] || record.lead_history || null,
      record.Recording || null,
      // Add transfer fields
      record.transfer_event || false,
      record.transfer_extension || null,
      record.transfer_type || null
    ];
  });
  
  
  // Let's count the columns in our SQL statement
  const columns = [
    'call_id', 'record_type', 'type', 'agent_name', 'extension', 'queue_campaign_name',
    'called_time', 'called_time_formatted', 'caller_id_number', 'caller_id_name', 'callee_id_number',
    'answered_time', 'hangup_time', 'wait_duration', 'talk_duration', 'hold_duration',
    'hold_duration_intervals', 'agent_hangup', 'agent_disposition', 'disposition', 'sub_disp_1', 'sub_disp_2',
    'status', 'campaign_type', 'abandoned', 'country', 'follow_up_notes',
    'agent_history', 'queue_history', 'lead_history', 'recording',
    'transfer_event', 'transfer_extension', 'transfer_type'
  ];
  
  console.log(`Number of columns in SQL statement: ${columns.length}`);
  
  // Count how many values we have per record (should match number of columns)
  if (values.length > 0) {
    console.log(`Number of values in first record: ${values[0].length}`);
  }
  
  // Create the correct number of placeholders
  const questionMarks = Array(columns.length).fill('?').join(',');
  const placeholders = values.map(() => `(${questionMarks})`).join(',');
  
  const sql = `
    INSERT IGNORE INTO final_report (
      call_id, record_type, type, agent_name, extension, queue_campaign_name, 
      called_time, called_time_formatted, caller_id_number, caller_id_name, callee_id_number,
      answered_time, hangup_time, wait_duration, talk_duration, hold_duration,
      hold_duration_intervals, agent_hangup, agent_disposition, disposition, sub_disp_1, sub_disp_2,
      status, campaign_type, abandoned, country, follow_up_notes,
      agent_history, queue_history, lead_history, recording,
      transfer_event, transfer_extension, transfer_type
    ) VALUES ${placeholders}
  `;
  
  const flatValues = values.flat();
  
  try {
    await dbService.query(sql, flatValues);
    console.log(`✅ Successfully inserted batch of ${batch.length} records`);
  } catch (error) {
    console.error(`❌ Database insertion error for batch:`, error);
    console.error(`❌ SQL:`, sql);
    console.error(`❌ First few values:`, flatValues.slice(0, 50));
    
    // Try inserting records one by one to identify problematic records
    console.log(`🔄 Attempting individual record insertion...`);
    let successCount = 0;
    let failCount = 0;
    
    for (let i = 0; i < values.length; i++) {
      try {
        const singleRecordSql = `
          INSERT IGNORE INTO final_report (
              call_id, record_type, type, agent_name, extension, queue_campaign_name, 
              called_time, called_time_formatted, caller_id_number, caller_id_name, callee_id_number,
              answered_time, hangup_time, wait_duration, talk_duration, hold_duration,
              hold_duration_intervals, agent_hangup, agent_disposition, disposition, sub_disp_1, sub_disp_2,
              status, campaign_type, abandoned, country, follow_up_notes,
              agent_history, queue_history, lead_history, recording,
              transfer_event, transfer_extension, transfer_type
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `;
        
        await dbService.query(singleRecordSql, values[i]);
        successCount++;
      } catch (singleError) {
        failCount++;
        console.error(`❌ Failed to insert record ${i}:`, singleError.message);
        console.error(`❌ Record data:`, JSON.stringify(processedBatch[i], null, 2));
        console.error(`❌ Record values:`, values[i]);
      }
    }
    
    console.log(`📊 Individual insertion results: ${successCount} success, ${failCount} failed`);
    
    if (failCount > 0) {
      throw new Error(`Failed to insert ${failCount} out of ${values.length} records`);
    }
  }
}

/**
 * Query the final_report table with optional filters
 * @param {Object} params - Query parameters (startDate, endDate, filters, etc.)
 * @returns {Promise<Object>} - Query results
 */
export async function queryFinalReport(params = {}) {
  // Reduce logging to improve performance
  const startTime = Date.now();
  
  try {
    
    // Extract parameters
    const { 
      startDate, endDate, 
      caller_id_number, callee_id_number, agent_name, queue_campaign_name,
      record_type, disposition, sub_disp_1, sub_disp_2, status, campaign_type, country,
      limit = 10000, offset = 0, sort_by = 'called_time', sort_order = 'desc'
    } = params;
    
    // Build WHERE clause
    const conditions = [];
    const values = [];
    
    // Time range filter (required)
    if (startDate && endDate) {
      // Debug the incoming date parameters
      console.log('🔍 Date filtering with:', { startDate, endDate });
      
      // Convert ISO dates to timestamps if needed
      let startTimestamp, endTimestamp;
      
      // Handle start date conversion
      if (typeof startDate === 'string') {
        if (startDate.includes('-') || startDate.includes('/')) {
          // ISO format or date string
          startTimestamp = Math.floor(new Date(startDate).getTime() / 1000);
        } else {
          // Numeric string
          const numValue = Number(startDate);
          startTimestamp = numValue > 10000000000 ? Math.floor(numValue / 1000) : numValue;
        }
      } else if (typeof startDate === 'number') {
        // Already a number
        startTimestamp = startDate > 10000000000 ? Math.floor(startDate / 1000) : startDate;
      } else {
        startTimestamp = 0;
      }
      
      // Handle end date conversion
      if (typeof endDate === 'string') {
        if (endDate.includes('-') || endDate.includes('/')) {
          // ISO format or date string
          endTimestamp = Math.ceil(new Date(endDate).getTime() / 1000);
        } else {
          // Numeric string
          const numValue = Number(endDate);
          endTimestamp = numValue > 10000000000 ? Math.ceil(numValue / 1000) : numValue;
        }
      } else if (typeof endDate === 'number') {
        // Already a number
        endTimestamp = endDate > 10000000000 ? Math.ceil(endDate / 1000) : endDate;
      } else {
        endTimestamp = Math.ceil(Date.now() / 1000);
      }
      
      // Format dates for string comparison (DD/MM/YYYY format)
      const startDateObj = new Date(startTimestamp * 1000);
      const endDateObj = new Date(endTimestamp * 1000);
      
      // Get the date parts in DD/MM/YYYY format
      const startDateFormatted = `${startDateObj.getDate().toString().padStart(2, '0')}/${(startDateObj.getMonth() + 1).toString().padStart(2, '0')}/${startDateObj.getFullYear()}`;
      const endDateFormatted = `${endDateObj.getDate().toString().padStart(2, '0')}/${(endDateObj.getMonth() + 1).toString().padStart(2, '0')}/${endDateObj.getFullYear()}`;
      
      console.log('🔍 Converted timestamps:', { startTimestamp, endTimestamp });
      console.log('🔍 Formatted dates:', { startDateFormatted, endDateFormatted });
      
      // Log the actual values we're using for debugging
      console.log('🔍 Final timestamp values for query:', { 
        startTimestamp, 
        endTimestamp, 
        startDateFormatted, 
        endDateFormatted,
        startDateObj: startDateObj.toISOString(),
        endDateObj: endDateObj.toISOString()
      });
      
      // Use timestamp-based condition as the primary filter
      conditions.push('called_time >= ? AND called_time <= ?');
      values.push(startTimestamp, endTimestamp);
      
      // Add a fallback for string-formatted dates if needed
      if (startDateFormatted && endDateFormatted) {
        conditions.push('OR (called_time_formatted >= ? AND called_time_formatted <= ?)');
        values.push(startDateFormatted, endDateFormatted);
      }
    }
    
    // Optional filters
    if (caller_id_number) {
      conditions.push('caller_id_number LIKE ?');
      values.push(`%${caller_id_number}%`);
    }
    
    if (callee_id_number) {
      conditions.push('callee_id_number LIKE ?');
      values.push(`%${callee_id_number}%`);
    }
    
    if (agent_name) {
      conditions.push('agent_name LIKE ?');
      values.push(`%${agent_name}%`);
    }
    
    if (queue_campaign_name) {
      conditions.push('queue_campaign_name LIKE ?');
      values.push(`%${queue_campaign_name}%`);
    }
    
    if (record_type) {
      conditions.push('record_type = ?');
      values.push(record_type);
    }
    
    // Additional filters
    if (disposition) {
      conditions.push('disposition LIKE ?');
      values.push(`%${disposition}%`);
    }
    
    if (sub_disp_1) {
      conditions.push('sub_disp_1 LIKE ?');
      values.push(`%${sub_disp_1}%`);
    }
    
    if (sub_disp_2) {
      conditions.push('sub_disp_2 LIKE ?');
      values.push(`%${sub_disp_2}%`);
    }
    
    if (status) {
      conditions.push('status LIKE ?');
      values.push(`%${status}%`);
    }
    
    if (campaign_type) {
      conditions.push('campaign_type LIKE ?');
      values.push(`%${campaign_type}%`);
    }
    
    if (country) {
      conditions.push('country LIKE ?');
      values.push(`%${country}%`);
    }
    
    // Build the SQL query
    let sql = 'SELECT * FROM final_report';
    if (conditions.length > 0) {
      // Separate standard AND conditions from OR conditions
      const andConditions = [];
      const orConditions = [];
      
      conditions.forEach(condition => {
        if (condition.startsWith('OR ')) {
          orConditions.push(condition.substring(3)); // Remove the 'OR ' prefix
        } else {
          andConditions.push(condition);
        }
      });
      
      // Build WHERE clause with proper grouping
      sql += ' WHERE ';
      
      // Add AND conditions first
      if (andConditions.length > 0) {
        sql += '(' + andConditions.join(' AND ') + ')';
      }
      
      // Add OR conditions if they exist
      if (orConditions.length > 0) {
        if (andConditions.length > 0) {
          sql += ' OR ';
        }
        sql += '(' + orConditions.join(' OR ') + ')';
      }
    }
    
    // Add sorting
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
    const sortDir = sort_order.toLowerCase() === 'asc' ? 'ASC' : 'DESC';
    
    sql += ` ORDER BY ${sortColumn} ${sortDir}`;
    
    // Add pagination
    // Convert limit and offset to integers and add them directly to the SQL query
    // MySQL expects numeric literals for LIMIT and OFFSET, not parameters
    const limitNum = parseInt(limit, 10) || 10000;
    const offsetNum = parseInt(offset, 10) || 0;
    sql += ` LIMIT ${limitNum} OFFSET ${offsetNum}`;
    
    console.log('🔍 Executing SQL query:', sql);
    console.log('🔍 With values:', values);
    
    // Execute the query with timing and retry logic
    const queryStartTime = Date.now();
    let results;
    
    // Use the optimized query function from dbService which already has retry logic
    try {
      // Use streamQuery for potentially large result sets to reduce memory usage
      if (limitNum > 1000) {
        results = await dbService.streamQuery(sql, values);
      } else {
        results = await dbService.query(sql, values);
      }
      
      const queryDuration = Date.now() - queryStartTime;
      if (queryDuration > 1000) { // Only log slow queries
        console.log(`Query execution completed in ${queryDuration}ms with ${results.length} results`);
      }
    } catch (queryError) {
      // Minimal error logging to reduce overhead
      console.error(`Query execution failed: ${queryError.message}`);
      throw queryError;
    }
    
    // Calculate totals for each record type efficiently
    const totals = {
      Campaign: 0,
      Inbound: 0,
      Outbound: 0,
      Total: results.length
    };
    
    // Use a more efficient loop for calculating totals
    for (let i = 0; i < results.length; i++) {
      const recordType = results[i].record_type;
      if (recordType && totals[recordType] !== undefined) {
        totals[recordType]++;
      }
    }
    
    // Get total count without pagination for accurate pagination info
    let totalCount = results.length;
    
    // Only execute count query if we're paginating and have a large dataset
    if ((limitNum < 10000 || offsetNum > 0) && (limitNum + offsetNum < 50000)) {
      // Use a more efficient COUNT query with LIMIT 1 optimization
      let countSql = 'SELECT SQL_NO_CACHE COUNT(*) as total FROM final_report';
      
      if (conditions.length > 0) {
        // Separate standard AND conditions from OR conditions
        const andConditions = [];
        const orConditions = [];
        
        for (let i = 0; i < conditions.length; i++) {
          const condition = conditions[i];
          if (condition.startsWith('OR ')) {
            orConditions.push(condition.substring(3)); // Remove the 'OR ' prefix
          } else {
            andConditions.push(condition);
          }
        }
        
        // Build WHERE clause with proper grouping
        countSql += ' WHERE ';
        
        // Add AND conditions first
        if (andConditions.length > 0) {
          countSql += '(' + andConditions.join(' AND ') + ')';
        }
        
        // Add OR conditions if they exist
        if (orConditions.length > 0) {
          if (andConditions.length > 0) {
            countSql += ' OR ';
          }
          countSql += '(' + orConditions.join(' OR ') + ')';
        }
      }
      
      try {
        const countResult = await dbService.query(countSql, values);
        totalCount = countResult[0]?.total || 0;
      } catch (countError) {
        // If count query fails, use results.length as fallback
        totalCount = results.length;
      }
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    return {
      success: true,
      message: `Retrieved ${results.length} records in ${duration}s`,
      rows: results,
      totals,
      totalCount,
      query: { sql, params: values }
    };
  } catch (error) {
    console.error('❌ Error querying final_report table:', error);
    console.error('❌ Error stack:', error.stack);
    console.error('❌ Query parameters:', JSON.stringify(params, null, 2));
    
    // Determine error type for better diagnostics
    let errorType = 'unknown';
    if (error.code === 'ER_PARSE_ERROR') {
      errorType = 'sql_syntax';
    } else if (error.code === 'ER_BAD_FIELD_ERROR') {
      errorType = 'invalid_column';
    } else if (error.code === 'ER_NO_SUCH_TABLE') {
      errorType = 'missing_table';
    } else if (error.message && error.message.includes('timeout')) {
      errorType = 'query_timeout';
    }
    
    return {
      success: false,
      message: `Error querying final_report table: ${error.message}`,
      errorType,
      error: error.stack,
      rows: []
    };
  }
}

/**
 * Check if final_report table has data for the given time range
 * @param {Object} params - Query parameters (startDate, endDate)
 * @returns {Promise<Object>} - Check result
 */
export async function checkFinalReportData(params = {}) {
  const { startDate, endDate } = params;
  
  if (!startDate || !endDate) {
    return { exists: false, count: 0 };
  }
  
  try {
    // Convert ISO dates to timestamps if needed
    const startTimestamp = typeof startDate === 'string' && startDate.includes('-') 
      ? new Date(startDate).getTime() / 1000 
      : Number(startDate) / 1000;
    const endTimestamp = typeof endDate === 'string' && endDate.includes('-') 
      ? new Date(endDate).getTime() / 1000 
      : Number(endDate) / 1000;
    
    // Format dates for string comparison (DD/MM/YYYY format)
    const startDateObj = new Date(startTimestamp * 1000);
    const endDateObj = new Date(endTimestamp * 1000);
    
    // Get the date parts in DD/MM/YYYY format
    const startDateFormatted = `${startDateObj.getDate().toString().padStart(2, '0')}/${(startDateObj.getMonth() + 1).toString().padStart(2, '0')}/${startDateObj.getFullYear()}`;
    const endDateFormatted = `${endDateObj.getDate().toString().padStart(2, '0')}/${(endDateObj.getMonth() + 1).toString().padStart(2, '0')}/${endDateObj.getFullYear()}`;
    
    console.log('🔍 checkFinalReportData with:', { startTimestamp, endTimestamp, startDateFormatted, endDateFormatted });
    
    const sql = `SELECT COUNT(*) as count FROM final_report WHERE 
      ((called_time >= ? AND called_time <= ?) OR 
      (called_time_formatted >= ? AND called_time_formatted <= ?))`;
    const values = [startTimestamp, endTimestamp, startDateFormatted, endDateFormatted];
    
    const result = await dbService.query(sql, values);
    
    const count = result[0]?.count || 0;
    return {
      exists: count > 0,
      count
    };
  } catch (error) {
    console.error('❌ Error checking final_report data:', error);
    return { exists: false, count: 0, error: error.message };
  }
}

/**
 * Clear the final_report table
 * @returns {Promise<Object>} - Clear result
 */
export async function clearFinalReport() {
  try {
    await dbService.query('TRUNCATE TABLE final_report');
    return { success: true, message: 'final_report table cleared successfully' };
  } catch (error) {
    console.error('❌ Error clearing final_report table:', error);
    return { success: false, message: `Error clearing final_report table: ${error.message}` };
  }
}

/**
 * Process and insert records directly from API data into final_report table
 * @param {Object} records - Raw API records (cdrs, cdrs_all, queueCalls, queueOutboundCalls, campaignsActivity)
 * @param {Object} params - Parameters (startDate, endDate, filters)
 * @returns {Promise<Object>} - Result of the operation
 */
export async function processAndInsertRecords(records, params = {}) {
  console.log('🔄 Processing API data for final_report table...');
  const startTime = Date.now();
  
  try {
    // Step 1: Process raw data into unified format
    console.log('📊 Step 1: Processing raw data into unified format...');
    
    // Use the existing processRecordData function from reportFetcher
    const processedRecords = [];
    
    // Process Queue Inbound
    if (records.queueCalls && records.queueCalls.length > 0) {
      console.log(`📊 Processing ${records.queueCalls.length} Queue Inbound records...`);
      records.queueCalls.forEach(record => {
        const processed = processRecordData(record, 'inbound');
        if (processed) processedRecords.push(processed);
      });
    }
    
    // Process Queue Outbound
    if (records.queueOutboundCalls && records.queueOutboundCalls.length > 0) {
      console.log(`📊 Processing ${records.queueOutboundCalls.length} Queue Outbound records...`);
      records.queueOutboundCalls.forEach(record => {
        const processed = processRecordData(record, 'outbound');
        if (processed) processedRecords.push(processed);
      });
    }
    
    // Process Campaigns
    if (records.campaignsActivity && records.campaignsActivity.length > 0) {
      console.log(`📊 Processing ${records.campaignsActivity.length} Campaign records...`);
      records.campaignsActivity.forEach(record => {
        const processed = processRecordData(record, 'campaign');
        if (processed) processedRecords.push(processed);
      });
    }
    
    console.log(`📊 Total processed records: ${processedRecords.length}`);
    
    // Step 2: Insert processed records into final_report table
    if (processedRecords.length > 0) {
      console.log('📊 Step 2: Inserting processed records into final_report table...');
      await insertIntoFinalReport(processedRecords);
    } else {
      console.log('⚠️ No records to insert after processing');
    }
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    return {
      success: true,
      message: `Successfully processed and inserted ${processedRecords.length} records in ${duration}s`,
      recordCount: processedRecords.length
    };
  } catch (error) {
    console.error('❌ Error processing and inserting records:', error);
    return {
      success: false,
      message: `Error processing and inserting records: ${error.message}`,
      error: error.stack
    };
  }
}

/**
 * Update recording IDs for records that may have new recordings available
 * @param {Array} callsToUpdate - Array of calls to check for recording updates
 * @returns {Promise<number>} - Number of records updated
 */
export async function updateRecordingIds(callsToUpdate) {
  if (!callsToUpdate || callsToUpdate.length === 0) {
    return 0;
  }
  
  try {
    console.log(`🎵 Updating recording IDs for ${callsToUpdate.length} records...`);
    
    let updatedCount = 0;
    
    for (const call of callsToUpdate) {
      try {
        // Extract recording information
        const newRecordingId = call.media_recording_id || call.recording_filename || '';
        
        if (newRecordingId) {
          // Build update query for final_report table
          const updateFinalReportQuery = `
            UPDATE final_report 
            SET recording = ?, updated_at = NOW()
            WHERE call_id = ?
          `;
          
          await dbService.query(updateFinalReportQuery, [newRecordingId, call.callId]);
          
          console.log(`  🎵 Updated recording for ${call.callId}: "${newRecordingId}"`);
          updatedCount++;
        }
        
      } catch (updateError) {
        console.log(`  ❌ Failed to update recording for ${call.callId}: ${updateError.message}`);
      }
    }
    
    console.log(`🎵 Successfully updated ${updatedCount} recording IDs`);
    return updatedCount;
    
  } catch (error) {
    console.error(`Error updating recording IDs: ${error.message}`);
    return 0;
  }
}

/**
 * Enhanced version of populateFinalReport that includes CDR matching and follow-up notes
 * @param {string} tenant - Tenant identifier
 * @param {Object} params - Query parameters (startDate, endDate, etc.)
 * @returns {Promise<Object>} - Summary of the operation
 */
export async function populateFinalReportEnhanced(tenant, params = {}) {
  console.log('🚀 Starting enhanced final_report table population...');
  const startTime = Date.now();
  
  try {
    // Step 1: Populate final_report table with standard data
    console.log('\n📊 Step 1: Populating final_report table with standard data...');
    const standardPopulateResult = await populateFinalReport(tenant, params);
    console.log(`Standard population result: ${standardPopulateResult.message}`);

    
    // Step 5: No need to update follow-up notes as they're now captured directly during insertion
    console.log('\n📊 Step 5: Skipping follow-up notes update as they are now captured directly during insertion');
    
    // Count records with follow-up notes for reporting purposes
    const followUpNotesQuery = `
      SELECT COUNT(*) as count
      FROM final_report
      WHERE follow_up_notes IS NOT NULL
    `;
    
    const followUpNotesResult = await dbService.query(followUpNotesQuery);
    const followUpNotesCount = followUpNotesResult[0]?.count || 0;
    
    console.log(`📊 Found ${followUpNotesCount} records with follow-up notes in final_report table`);
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    return {
      success: true,
      message: `Successfully populated final_report table with enhanced data in ${duration}s`,
      standardPopulation: standardPopulateResult,
      followUpNotesCount: followUpNotesCount
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

export default {
  populateFinalReport,
  populateFinalReportEnhanced,
  queryFinalReport,
  checkFinalReportData,
  clearFinalReport,
  processAndInsertRecords
};
