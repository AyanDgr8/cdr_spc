// // enhanced-cdr-matching.js

// /**
//  * Enhanced CDR Matching Module
//  * 
//  * This module implements optimized CDR matching logic based on exact schema matching
//  * from test-exact-schema-matching.js, enhanced with optimized filtering methods
//  * from reportFetcher.js.
//  *
//  * Exports:
//  * - getMatchedCDRsForFinalReport: Main function to fetch and match CDR records
//  * - normalizePhoneNumber: Helper function to normalize phone numbers
//  * - detectTransferEvents: Helper function to detect transfer events in agent_history
//  */

// import dbService from './dbService.js';

// /**
//  * Normalize phone number by removing all non-digit characters
//  * @param {string} phoneNumber - Phone number to normalize
//  * @returns {string} - Normalized phone number (digits only)
//  */
// function normalizePhoneNumber(phoneNumber) {
//   if (!phoneNumber) return '';
//   return String(phoneNumber).replace(/\D/g, '');
// }


// /**
//  * Optimized CDR filtering using the best approach based on dataset size
//  * @param {Array} cdrRecords - CDR records to filter
//  * @param {Array} processedOutboundCalls - Processed outbound calls with normalized data
//  * @returns {Promise<Array>} - Filtered CDR records
//  */
// async function getMatchingCdrsOptimized(cdrRecords, processedOutboundCalls) {
//   if (cdrRecords.length === 0 || processedOutboundCalls.length === 0) {
//     return [];
//   }
  
//   // For very large datasets, always use optimized in-memory approach
//   // Database approach has too much overhead for large datasets
//   const useDatabaseApproach = cdrRecords.length < 50000 && cdrRecords.length > 5000;

//   if (useDatabaseApproach) {
//     console.log('🔍 Using database-based filtering for medium-sized datasets...');
//     return await getMatchingCdrsFromDatabase(cdrRecords, processedOutboundCalls);
//   } else {
//     console.log('💾 Using optimized in-memory filtering...');
//     return await getMatchingCdrsInMemoryOptimized(cdrRecords, processedOutboundCalls);
//   }
// }

// /**
//  * Database-based CDR filtering using SQL joins
//  * @param {Array} cdrRecords - CDR records to filter
//  * @param {Array} processedOutboundCalls - Processed outbound calls
//  * @returns {Promise<Array>} - Filtered CDR records
//  */
// async function getMatchingCdrsFromDatabase(cdrRecords, processedOutboundCalls) {
//   console.log('🗄️ Using database-based CDR filtering with SQL joins...');
  
//   // Create temporary tables for efficient joining
//   const tempCdrTable = `temp_cdr_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
//   const tempOutboundTable = `temp_outbound_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  
//   try {
//     // Create temporary CDR table
//     await dbService.query(`
//       CREATE TEMPORARY TABLE ${tempCdrTable} (
//         id INT AUTO_INCREMENT PRIMARY KEY,
//         original_index INT,
//         caller_id_number VARCHAR(50),
//         normalized_caller_id VARCHAR(50),
//         datetime_ts BIGINT,
//         record_data JSON,
//         INDEX idx_caller_datetime (normalized_caller_id, datetime_ts)
//       )
//     `);
    
//     // Create temporary outbound table
//     await dbService.query(`
//       CREATE TEMPORARY TABLE ${tempOutboundTable} (
//         id INT AUTO_INCREMENT PRIMARY KEY,
//         agent_ext VARCHAR(50),
//         normalized_agent_ext VARCHAR(50),
//         called_time_ts BIGINT,
//         hangup_time_ts BIGINT,
//         call_data JSON,
//         INDEX idx_ext_timerange (normalized_agent_ext, called_time_ts, hangup_time_ts)
//       )
//     `);
    
//     // Prepare CDR data for batch insert
//     const cdrInsertData = cdrRecords.map((cdr, index) => {
//       const rawData = cdr.raw_data || cdr;
//       const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
//       const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
      
//       // Handle Gregorian timestamp conversion for all CDR record types
//       let convertedDateTime = dateTime;
//       if (typeof dateTime === 'number' && dateTime > 60000000000) {
//         // Gregorian timestamp: subtract 62167219200 seconds (correct offset from 0001-01-01 to 1970-01-01)
//         const unixSeconds = dateTime - 62167219200;
//         convertedDateTime = unixSeconds * 1000; // Convert to milliseconds
//       } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
//         // Handle Unix timestamp in seconds (convert to milliseconds)
//         convertedDateTime = dateTime * 1000;
//       }
      
//       const normalizedCallerId = normalizePhoneNumber(callerIdNumber);
//       const datetimeTs = new Date(convertedDateTime).getTime();
      
//       return [
//         index,
//         callerIdNumber || '',
//         normalizedCallerId || '',
//         isNaN(datetimeTs) ? 0 : datetimeTs,
//         JSON.stringify(cdr)
//       ];
//     }).filter(row => row[2] && row[3] > 0); // Filter out invalid records
    
//     // Prepare outbound data for batch insert
//     const outboundInsertData = processedOutboundCalls.map(outbound => [
//       outbound.originalAgentExt || '',
//       outbound.normalizedAgentExt || '',
//       outbound.calledTime.getTime(),
//       outbound.hangupTime.getTime(),
//       JSON.stringify(outbound)
//     ]).filter(row => row[1] && row[2] > 0 && row[3] > 0);
    
//     console.log(`📊 Preparing to insert ${cdrInsertData.length} CDRs and ${outboundInsertData.length} outbound calls into temp tables...`);
    
//     // Batch insert CDR data in chunks to avoid placeholder limit
//     if (cdrInsertData.length > 0) {
//       const BATCH_SIZE = 1000; // Safe batch size to avoid placeholder limit
//       for (let i = 0; i < cdrInsertData.length; i += BATCH_SIZE) {
//         const batch = cdrInsertData.slice(i, i + BATCH_SIZE);
//         const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ');
//         const flatParams = batch.flat();
//         await dbService.query(
//           `INSERT INTO ${tempCdrTable} (original_index, caller_id_number, normalized_caller_id, datetime_ts, record_data) VALUES ${placeholders}`,
//           flatParams
//         );
//       }
//       console.log(`📊 Inserted ${cdrInsertData.length} CDR records in ${Math.ceil(cdrInsertData.length / BATCH_SIZE)} batches`);
//     }
    
//     // Batch insert outbound data in chunks to avoid placeholder limit
//     if (outboundInsertData.length > 0) {
//       const BATCH_SIZE = 1000; // Safe batch size to avoid placeholder limit
//       for (let i = 0; i < outboundInsertData.length; i += BATCH_SIZE) {
//         const batch = outboundInsertData.slice(i, i + BATCH_SIZE);
//         const placeholders = batch.map(() => '(?, ?, ?, ?, ?)').join(', ');
//         const flatParams = batch.flat();
//         await dbService.query(
//           `INSERT INTO ${tempOutboundTable} (agent_ext, normalized_agent_ext, called_time_ts, hangup_time_ts, call_data) VALUES ${placeholders}`,
//           flatParams
//         );
//       }
//       console.log(`📊 Inserted ${outboundInsertData.length} outbound records in ${Math.ceil(outboundInsertData.length / BATCH_SIZE)} batches`);
//     }
    
//     // Execute optimized join query
//     const joinQuery = `
//       SELECT DISTINCT c.original_index, c.record_data
//       FROM ${tempCdrTable} c
//       INNER JOIN ${tempOutboundTable} o ON (
//         c.normalized_caller_id = o.normalized_agent_ext
//         AND c.datetime_ts >= o.called_time_ts
//         AND c.datetime_ts <= o.hangup_time_ts
//       )
//       ORDER BY c.original_index
//     `;
    
//     console.log('🔍 Executing optimized join query...');
//     const results = await dbService.query(joinQuery);
    
//     // Extract matched CDR records in original order
//     const matchedCdrs = results.map(row => {
//       try {
//         return typeof row.record_data === 'string' ? JSON.parse(row.record_data) : row.record_data;
//       } catch (error) {
//         console.error('Error parsing record_data:', error, 'Raw data:', row.record_data);
//         return row.record_data; // Return as-is if parsing fails
//       }
//     });
    
//     console.log(`✅ Database join completed: ${matchedCdrs.length} matching CDRs found`);
//     return matchedCdrs;
    
//   } finally {
//     // Clean up temporary tables
//     try {
//       await dbService.query(`DROP TEMPORARY TABLE IF EXISTS ${tempCdrTable}`);
//       await dbService.query(`DROP TEMPORARY TABLE IF EXISTS ${tempOutboundTable}`);
//     } catch (cleanupError) {
//       console.warn('⚠️ Error cleaning up temporary tables:', cleanupError.message);
//     }
//   }
// }

// /**
//  * Optimized in-memory CDR filtering using efficient lookup structures
//  * @param {Array} cdrRecords - CDR records to filter
//  * @param {Array} processedOutboundCalls - Processed outbound calls
//  * @returns {Promise<Array>} - Filtered CDR records
//  */
// async function getMatchingCdrsInMemoryOptimized(cdrRecords, processedOutboundCalls) {
//   console.log('💾 Using optimized in-memory CDR filtering...');
//   console.log(`⚠️ DIAGNOSTIC: Filtering ${cdrRecords.length} CDR records against ${processedOutboundCalls.length} outbound calls`);
  
//   // Create efficient lookup structures
//   const outboundByExt = new Map();
  
//   // Group outbound calls by normalized agent extension for O(1) lookup
//   processedOutboundCalls.forEach(outbound => {
//     const ext = outbound.normalizedAgentExt;
//     if (!outboundByExt.has(ext)) {
//       outboundByExt.set(ext, []);
//     }
//     outboundByExt.get(ext).push(outbound);
//   });
  
//   console.log(`📊 Created lookup map with ${outboundByExt.size} unique extensions`);
  
//   // DIAGNOSTIC: Log some sample extensions to help with debugging
//   if (outboundByExt.size > 0) {
//     console.log('⚠️ DIAGNOSTIC: Sample extensions in lookup map:');
//     let count = 0;
//     for (const [ext, calls] of outboundByExt.entries()) {
//       if (count < 5) {
//         console.log(`   - Extension: ${ext}, Calls: ${calls.length}`);
//         count++;
//       } else {
//         break;
//       }
//     }
//   } else {
//     console.log('⚠️ DIAGNOSTIC: No extensions in lookup map - this will result in no CDR matches');
//   }
  
//   const matchedCdrs = [];
//   let processedCount = 0;
  
//   for (const cdr of cdrRecords) {
//     processedCount++;
    
//     // Extract data from raw_data JSON or direct fields
//     const rawData = cdr.raw_data || cdr;
//     const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
//     const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
    
//     const cdrCallerIdNumber = normalizePhoneNumber(callerIdNumber);
    
//     // Handle Gregorian timestamp conversion for all CDR record types
//     let convertedDateTime = dateTime;
//     if (typeof dateTime === 'number' && dateTime > 60000000000) {
//       const unixSeconds = dateTime - 62167219200;
//       convertedDateTime = unixSeconds * 1000;
//     } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
//       // Handle Unix timestamp in seconds (convert to milliseconds)
//       convertedDateTime = dateTime * 1000;
//     }
    
//     const cdrDateTime = new Date(convertedDateTime);
    
//     // Skip invalid records
//     if (!cdrCallerIdNumber || isNaN(cdrDateTime.getTime())) {
//       continue;
//     }
    
//     // Quick lookup for matching extensions
//     const matchingOutbounds = outboundByExt.get(cdrCallerIdNumber);
//     if (!matchingOutbounds) {
//       continue; // No outbound calls for this extension
//     }
    
//     // Check time overlap with matching extensions
//     const matchFound = matchingOutbounds.some(outbound => {
//       return cdrDateTime >= outbound.calledTime && cdrDateTime <= outbound.hangupTime;
//     });
    
//     if (matchFound) {
//       matchedCdrs.push(cdr);
      
//       if (processedCount <= 3) {
//         const matchingOutbound = matchingOutbounds.find(outbound => 
//           cdrDateTime >= outbound.calledTime && cdrDateTime <= outbound.hangupTime
//         );
//         const cdrCallId = rawData.custom_channel_vars?.bridge_id || rawData.call_id || cdr.call_id || 'unknown';
//         const outboundCallId = matchingOutbound.call_id || matchingOutbound.callid || 'unknown';
//         console.log(`✅ CDR ${cdrCallId} matches outbound ${outboundCallId} (ext: ${cdrCallerIdNumber})`);
//       }
//     }
    
//     // Progress logging for large datasets
//     if (processedCount % 1000 === 0) {
//       console.log(`📊 Processed ${processedCount}/${cdrRecords.length} CDRs, found ${matchedCdrs.length} matches`);
//     }
//   }
  
//   console.log(`✅ In-memory matching completed: ${matchedCdrs.length} matching CDRs found`);
//   return matchedCdrs;
// }

// /**
//  * Fallback CDR filtering using original nested loop approach
//  * @param {Array} cdrRecords - CDR records to filter
//  * @param {Array} processedOutboundCalls - Processed outbound calls
//  * @returns {Promise<Array>} - Filtered CDR records
//  */
// async function fallbackCdrFiltering(cdrRecords, processedOutboundCalls) {
//   console.log('🔄 Using fallback filtering method...');
  
//   const filteredCDRs = cdrRecords.filter((cdr, index) => {
//     const rawData = cdr.raw_data || cdr;
//     const callerIdNumber = rawData.caller_id_number || rawData.Caller_ID_Number || cdr.caller_id_number;
//     const dateTime = rawData.datetime || rawData.timestamp || rawData.called_time || cdr.datetime || cdr.timestamp || cdr.called_time;
    
//     const cdrCallerIdNumber = normalizePhoneNumber(callerIdNumber);
    
//     let convertedDateTime = dateTime;
//     if (typeof dateTime === 'number' && dateTime > 60000000000) {
//       const unixSeconds = dateTime - 62167219200;
//       convertedDateTime = unixSeconds * 1000;
//     } else if (typeof dateTime === 'number' && dateTime < 10000000000) {
//       // Handle Unix timestamp in seconds (convert to milliseconds)
//       convertedDateTime = dateTime * 1000;
//     }
    
//     const cdrDateTime = new Date(convertedDateTime);
    
//     if (!cdrCallerIdNumber || isNaN(cdrDateTime.getTime())) {
//       return false;
//     }
    
//     return processedOutboundCalls.some(outboundCall => {
//       const extensionMatch = cdrCallerIdNumber === outboundCall.normalizedAgentExt;
//       const timeMatch = cdrDateTime >= outboundCall.calledTime && cdrDateTime <= outboundCall.hangupTime;
//       return extensionMatch && timeMatch;
//     });
//   });
  
//   return filteredCDRs;
// }

// /**
//  * Detect transfer events in agent_history or queue_history
//  * @param {Array} history - Agent history or queue history array
//  * @param {string} callType - Type of call ('inbound' or 'outbound')
//  * @returns {Object} - Transfer information object
//  */
// function detectTransferEvents(history, callType = 'outbound') {
//   if (!Array.isArray(history) || history.length === 0) {
//     return { transfer_event: false, transfer_extension: null, transfer_type: null, last_attempt: null };
//   }
  
//   // Default result
//   const result = { transfer_event: false, transfer_extension: null, transfer_type: null, last_attempt: null };
  
//   // Look for transfer events in history with multiple patterns
//   const transferEvents = history.filter(event => {
//     if (!event) return false;
    
//     // Pattern 1: Exact pattern matching as specified
//     if (event.type === 'transfer') {
//       if ((callType === 'outbound' && event.event === 'transfer') ||
//           (callType === 'inbound' && event.event === 'dial')) {
//         return true;
//       }
//     }
    
//     // Pattern 2: Check for any event with 'transfer' in type or event
//     if ((event.type && event.type.toLowerCase().includes('transfer')) ||
//         (event.event && event.event.toLowerCase().includes('transfer'))) {
//       return true;
//     }
    
//     // Pattern 3: Check for agent_action containing 'transfer'
//     if (event.agent_action && event.agent_action.toLowerCase().includes('transfer')) {
//       return true;
//     }
    
//     // Pattern 4: Check for ext field (indicates potential transfer)
//     if (event.ext && event.ext.length > 3) {
//       return true;
//     }
//     return false;
//   });
  
//   if (transferEvents.length > 0) {
//     // Use the last transfer event (in case there are multiple)
//     const lastTransferEvent = transferEvents[transferEvents.length - 1];
    
//     result.transfer_event = true;
    
//     // Look for extension in multiple fields
//     result.transfer_extension = lastTransferEvent.ext || 
//                                lastTransferEvent.extension || 
//                                lastTransferEvent.destination || 
//                                lastTransferEvent.dest_exten || 
//                                null;
                               
//     result.transfer_type = callType === 'inbound' ? 'inbound_transfer' : 'outbound_transfer';
    
//     // Look for timestamp in multiple fields
//     result.last_attempt = lastTransferEvent.last_attempt || 
//                          lastTransferEvent.timestamp || 
//                          lastTransferEvent.time || 
//                          Math.floor(Date.now() / 1000);
    
//     console.log(`🔄 Transfer detected in ${callType} call:`);
//     console.log(`   - Type: ${lastTransferEvent.type || 'unknown'}`);
//     console.log(`   - Event: ${lastTransferEvent.event || 'unknown'}`);
//     console.log(`   - Extension: ${result.transfer_extension || 'unknown'}`);
//     console.log(`   - Last Attempt: ${result.last_attempt ? new Date(result.last_attempt * 1000).toISOString() : 'unknown'}`);
//   }
  
//   return result;
// }

// // getTransferredCalls function has been removed as requested


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

// // processOutboundRecordsForTransfers function has been removed as requested

// export {
//   normalizePhoneNumber,
//   detectTransferEvents
// };
