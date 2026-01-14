/**
 * test-transferred-calls.js
 * 
 * Test script to verify the transferred calls functionality implemented in enhanced-cdr-matching.js
 * This script tests the getTransferredCalls function with sample transfer information.
 */

import { getTransferredCalls, detectTransferEvents } from './enhanced-cdr-matching.js';
import { processAgentHistoryForTransfers, fetchTransferredCallsForExtension } from './apiDataFetcher.js';
import dbService from './dbService.js';

// We'll find a real extension from the database
let sampleTransferInfo = {
  transfer_event: true,
  transfer_extension: '1068', // Default value, will be replaced if we find a real one
  transfer_type: 'outbound_transfer',
  last_attempt: Math.floor(Date.now() / 1000) - 360000 // 1 hour ago
};

// Find a real extension from the database
async function findRealExtension() {
  try {
    // Query to find the most common extensions in CDR records for the specific date range
    const sql = `
      SELECT 
        JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_number')) as extension,
        COUNT(*) as count
      FROM raw_cdrs
      WHERE JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_number')) IS NOT NULL
      AND timestamp BETWEEN ? AND ?
      GROUP BY extension
      ORDER BY count DESC
      LIMIT 5
    `;
    
    const results = await dbService.query(sql, [params.startDate, params.endDate]);
    
    if (results.length > 0) {
      console.log('📊 Found the following extensions in CDR records:');
      results.forEach(row => {
        console.log(`   - Extension: ${row.extension}, Count: ${row.count}`);
      });
      
      // Use the most common extension
      if (results[0].extension) {
        sampleTransferInfo.transfer_extension = results[0].extension;
        console.log(`✅ Using real extension for testing: ${sampleTransferInfo.transfer_extension}`);
        return true;
      }
    }
    
    console.log('⚠️ No extensions found in CDR records');
    return false;
  } catch (error) {
    console.error('❌ Error finding real extension:', error);
    return false;
  }
}

// Sample tenant and params
const tenant = 'spc';
const params = {
  // September 8-10, 2025 time range
  startDate: 1757275200, // September 8, 2025 00:00:00 UTC
  endDate: 1757447940,   // September 10, 2025 23:59:00 UTC
};

/**
 * Test the getTransferredCalls function
 */
async function testGetTransferredCalls() {
  console.log('🧪 Testing getTransferredCalls function...');
  console.log(`🔍 Using transfer extension: ${sampleTransferInfo.transfer_extension}`);
  
  try {
    // Get transferred calls
    const transferredCalls = await getTransferredCalls(sampleTransferInfo, tenant, params);
    
    console.log(`✅ Found ${transferredCalls.length} transferred calls using enhanced-cdr-matching.js`);
    
    // Now test the new fetchTransferredCallsForExtension function
    console.log('\n🧪 Testing fetchTransferredCallsForExtension function...');
    const apiTransferredCalls = await fetchTransferredCallsForExtension(
      sampleTransferInfo.transfer_extension,
      sampleTransferInfo.last_attempt,
      tenant,
      params
    );
    
    console.log(`✅ Found ${apiTransferredCalls.length} transferred calls using apiDataFetcher.js`);
    
    // Display sample data if available
    if (transferredCalls.length > 0) {
      console.log('\n📋 Sample transferred call data from enhanced-cdr-matching.js:');
      console.log(JSON.stringify(transferredCalls[0], null, 2).substring(0, 500) + '...');
      
      // Check if the transferred calls have the correct properties
      const hasCorrectProperties = transferredCalls.every(call => 
        call._recordType === 'transferred_cdr' && 
        call.record_type === 'transferred_cdr' &&
        call.Type === 'Transferred CDR' &&
        call.transfer_source_extension === sampleTransferInfo.transfer_extension
      );
      
      if (hasCorrectProperties) {
        console.log('✅ All transferred calls have the correct properties');
      } else {
        console.log('❌ Some transferred calls are missing required properties');
      }
    }
    
    if (apiTransferredCalls.length > 0) {
      console.log('\n📋 Sample transferred call data from apiDataFetcher.js:');
      console.log(JSON.stringify(apiTransferredCalls[0], null, 2).substring(0, 500) + '...');
    }
    
    if (transferredCalls.length === 0 && apiTransferredCalls.length === 0) {
      console.log('⚠️ No transferred calls found. Try with a different extension or time range.');
      
      // Let's check if there are any CDR records with this extension
      console.log('\n🔍 Checking if there are any CDR records with this extension...');
      
      const sql = `
        SELECT COUNT(*) as count FROM raw_cdrs 
        WHERE JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.caller_id_number')) = ? 
        OR JSON_UNQUOTE(JSON_EXTRACT(raw_data, '$.Caller_ID_Number')) = ?
      `;
      
      const result = await dbService.query(sql, [sampleTransferInfo.transfer_extension, sampleTransferInfo.transfer_extension]);
      console.log(`📊 Found ${result[0].count} CDR records with extension ${sampleTransferInfo.transfer_extension}`);
      
      if (result[0].count === 0) {
        console.log('💡 Suggestion: Try with a different extension that exists in your CDR records');
      }
    }
  } catch (error) {
    console.error('❌ Error testing getTransferredCalls:', error);
  }
}

/**
 * Test with a real transfer from the database
 */
async function testWithRealTransfer() {
  console.log('\n🧪 Testing with a real transfer from the database...');
  
  try {
    // Find a record with transfer_event = true in the specific date range
    const sql = `
      SELECT * FROM raw_queue_outbound 
      WHERE JSON_EXTRACT(raw_data, '$.agent_history') IS NOT NULL 
      AND called_time BETWEEN ? AND ?
      LIMIT 20
    `;
    
    const outboundRecords = await dbService.query(sql, [params.startDate, params.endDate]);
    console.log(`📊 Found ${outboundRecords.length} outbound records with agent_history`);
    
    if (outboundRecords.length === 0) {
      console.log('⚠️ No outbound records with agent_history found');
      return;
    }
    
    // Find a record with a transfer event
    let transferFound = false;
    
    for (const record of outboundRecords) {
      const rawData = typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data;
      
      if (Array.isArray(rawData.agent_history)) {
        // Use the imported detectTransferEvents function
        const transferInfo = detectTransferEvents(rawData.agent_history, 'outbound');
        
        if (transferInfo.transfer_event) {
          console.log(`✅ Found a record with transfer event: ${record.callid}`);
          console.log(`🔄 Transfer extension: ${transferInfo.transfer_extension}`);
          console.log(`🔄 Transfer type: ${transferInfo.transfer_type}`);
          
          // Add last_attempt from the first agent_history event with timestamp
          const eventWithTimestamp = rawData.agent_history.find(event => event.last_attempt);
          if (eventWithTimestamp) {
            transferInfo.last_attempt = eventWithTimestamp.last_attempt;
          } else {
            transferInfo.last_attempt = Math.floor(Date.now() / 1000) - 3600; // Fallback to 1 hour ago
          }
          
          // Test getTransferredCalls with this real transfer info
          console.log('\n🧪 Testing getTransferredCalls with real transfer info...');
          const transferredCalls = await getTransferredCalls(transferInfo, tenant, params);
          
          console.log(`✅ Found ${transferredCalls.length} transferred calls using enhanced-cdr-matching.js`);
          
          // Now test the new processAgentHistoryForTransfers function
          console.log('\n🧪 Testing processAgentHistoryForTransfers function...');
          const agentHistoryTransfers = await processAgentHistoryForTransfers(
            rawData.agent_history,
            tenant,
            params
          );
          
          console.log(`✅ Found ${agentHistoryTransfers.length} transferred calls using processAgentHistoryForTransfers`);
          
          if (transferredCalls.length > 0) {
            console.log('\n📋 Sample transferred call data from enhanced-cdr-matching.js:');
            console.log(JSON.stringify(transferredCalls[0], null, 2).substring(0, 500) + '...');
          }
          
          if (agentHistoryTransfers.length > 0) {
            console.log('\n📋 Sample transferred call data from processAgentHistoryForTransfers:');
            console.log(JSON.stringify(agentHistoryTransfers[0], null, 2).substring(0, 500) + '...');
          }
          
          transferFound = true;
          break;
        }
      }
    }
    
    if (!transferFound) {
      console.log('⚠️ No records with transfer events found in the sample');
    }
  } catch (error) {
    console.error('❌ Error testing with real transfer:', error);
  }
}

/**
 * Test for transferred calls directly in final_report table
 */
async function testTransferredCallsInFinalReport() {
  console.log('\n🧪 Testing for transferred calls directly in final_report table...');
  
  try {
    // Query to find records with transfer_extension in final_report table
    const sql = `
      SELECT *
      FROM final_report
      WHERE transfer_extension IS NOT NULL
        AND transfer_extension <> ''
        AND FROM_UNIXTIME(called_time) 
              BETWEEN FROM_UNIXTIME(?) AND FROM_UNIXTIME(?)
      LIMIT 10
    `;
    
    const results = await dbService.query(sql, [params.startDate, params.endDate]);
    
    console.log(`📊 Found ${results.length} records with transfer_extension in final_report table`);
    
    if (results.length > 0) {
      console.log('\n📋 Sample transferred call data from final_report:');
      results.forEach((record, index) => {
        console.log(`\nTransferred Call ${index + 1}:`);
        console.log(`  ID: ${record.id}`);
        console.log(`  Call ID: ${record.call_id}`);
        console.log(`  Record Type: ${record.record_type}`);
        console.log(`  Agent: ${record.agent_name}`);
        console.log(`  Extension: ${record.extension}`);
        console.log(`  Transfer Extension: ${record.transfer_extension}`);
        console.log(`  Transfer Type: ${record.transfer_type}`);
        console.log(`  Called Time: ${record.called_time_formatted}`);
      });
      
      // If we found records, use the first one's extension for further testing
      if (results[0].transfer_extension) {
        sampleTransferInfo.transfer_extension = results[0].transfer_extension;
        sampleTransferInfo.last_attempt = results[0].called_time || Math.floor(Date.now() / 1000) - 3600;
        console.log(`\n✅ Updated sample transfer info with extension ${sampleTransferInfo.transfer_extension}`);
      }
    } else {
      console.log('\n⚠️ No transferred calls found in final_report table for the specified date range.');
      console.log('💡 Suggestion: Try populating the final_report table with enhanced data first.');
    }
  } catch (error) {
    console.error('❌ Error testing for transferred calls in final_report:', error);
  }
}

/**
 * Test database connection limits
 */
async function testDatabaseConnectionLimits() {
  console.log('\n🧪 Testing database connection limits...');
  
  try {
    // Create multiple concurrent requests to test connection limits
    const concurrentRequests = 30;
    console.log(`🔄 Creating ${concurrentRequests} concurrent database requests...`);
    
    const startTime = Date.now();
    
    // Create an array of promises for concurrent requests
    const promises = Array(concurrentRequests).fill().map((_, i) => {
      return new Promise(async (resolve) => {
        try {
          // Use a simple query to test connection limits
          const result = await dbService.query('SELECT 1 as test', []);
          resolve({ success: true, index: i });
        } catch (error) {
          resolve({ success: false, index: i, error: error.message });
        }
      });
    });
    
    // Wait for all promises to resolve
    const results = await Promise.all(promises);
    
    const endTime = Date.now();
    const duration = endTime - startTime;
    
    // Count successes and failures
    const successes = results.filter(r => r.success).length;
    const failures = results.filter(r => !r.success).length;
    
    console.log(`✅ Test completed in ${duration}ms`);
    console.log(`📊 Successful requests: ${successes}`);
    console.log(`📊 Failed requests: ${failures}`);
    
    if (failures > 0) {
      console.log('\n⚠️ Some requests failed. Sample errors:');
      results.filter(r => !r.success).slice(0, 3).forEach(r => {
        console.log(`   - Request ${r.index}: ${r.error}`);
      });
      
      console.log('\n💡 This indicates that the connection pool limit is being reached.');
      console.log('   Our batch processing implementation should handle this gracefully.');
    } else {
      console.log('\n✅ All requests succeeded. The connection pool is handling the load well.');
    }
  } catch (error) {
    console.error('❌ Error testing database connection limits:', error);
  }
}

/**
 * Test the combined approach in reportFetcher.js
 */
async function testCombinedApproach() {
  console.log('\n🧪 Testing combined approach for transferred calls...');
  
  try {
    // Find a record with agent_history
    const sql = `
      SELECT * FROM raw_queue_outbound 
      WHERE JSON_EXTRACT(raw_data, '$.agent_history') IS NOT NULL 
      AND called_time BETWEEN ? AND ?
      LIMIT 5
    `;
    
    const outboundRecords = await dbService.query(sql, [params.startDate, params.endDate]);
    
    if (outboundRecords.length === 0) {
      console.log('⚠️ No outbound records with agent_history found');
      return;
    }
    
    // Process each record
    for (const record of outboundRecords) {
      const rawData = typeof record.raw_data === 'string' ? JSON.parse(record.raw_data) : record.raw_data;
      
      if (!Array.isArray(rawData.agent_history) || rawData.agent_history.length === 0) {
        continue;
      }
      
      console.log(`📝 Processing record ${record.callid} with ${rawData.agent_history.length} agent_history entries`);
      
      // First use detectTransferEvents
      const transferInfo = detectTransferEvents(rawData.agent_history, 'outbound');
      
      if (!transferInfo.transfer_event) {
        console.log('⚠️ No transfer event detected in this record');
        continue;
      }
      
      console.log(`✅ Found transfer to extension ${transferInfo.transfer_extension}`);
      
      // Add source_record to transferInfo
      transferInfo.source_record = rawData;
      
      // Add last_attempt from agent_history
      const eventWithTimestamp = rawData.agent_history.find(event => event.last_attempt);
      if (eventWithTimestamp) {
        transferInfo.last_attempt = eventWithTimestamp.last_attempt;
        console.log(`📅 Transfer timestamp: ${new Date(transferInfo.last_attempt * 1000).toISOString()}`);
      }
      
      // Get transferred calls using both methods
      const transferredCalls1 = await getTransferredCalls(transferInfo, tenant, params);
      const transferredCalls2 = await processAgentHistoryForTransfers(rawData.agent_history, tenant, params);
      
      console.log(`📊 Results comparison:`);
      console.log(`   - getTransferredCalls: ${transferredCalls1.length} calls`);
      console.log(`   - processAgentHistoryForTransfers: ${transferredCalls2.length} calls`);
      
      // Combine results
      const allCalls = [...transferredCalls1, ...transferredCalls2];
      
      // Remove duplicates
      const uniqueTransferredCalls = [];
      const seenCallIds = new Set();
      
      allCalls.forEach(call => {
        const callId = call.call_id || call.callid;
        if (callId && !seenCallIds.has(callId)) {
          seenCallIds.add(callId);
          uniqueTransferredCalls.push(call);
        }
      });
      
      console.log(`✅ Combined unique results: ${uniqueTransferredCalls.length} calls`);
      
      if (uniqueTransferredCalls.length > 0) {
        return; // We found and processed at least one transferred call
      }
    }
    
    console.log('⚠️ No records with transfer events found in the sample');
  } catch (error) {
    console.error('❌ Error testing combined approach:', error);
  }
}

// Run the tests
async function runTests() {
  console.log('🚀 Starting transferred calls tests for September 8-10, 2025...');
  console.log(`📅 Using date range: ${new Date(params.startDate * 1000).toISOString()} to ${new Date(params.endDate * 1000).toISOString()}`);
  
  // First check for transferred calls directly in the final_report table
  console.log('🔍 Checking for transferred calls in final_report table...');
  await testTransferredCallsInFinalReport();
  
  // Then find a real extension to use for testing
  console.log('🔍 Finding real extensions in the database...');
  await findRealExtension();
  
  // Now run the tests with the real extension
  await testGetTransferredCalls();
  await testWithRealTransfer();
  
  // Test the combined approach
  await testCombinedApproach();
  
  // Test database connection limits
  await testDatabaseConnectionLimits();
  
  console.log('\n🏁 All tests completed');
  
  // Close database connections
  await dbService.end();
}

runTests().catch(error => {
  console.error('❌ Error running tests:', error);
  process.exit(1);
});
