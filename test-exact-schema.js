/**
 * Test CDR matching using the exact schema provided by user
 * CDR schema: id, call_id, timestamp, raw_data (contains caller_id_number)
 * Outbound schema: id, callid, queue_name, called_time, raw_data (contains agent_ext)
 * 
 * Matching conditions:
 * 1. CDR raw_data.caller_id_number = Outbound raw_data.agent_ext
 * 2. CDR timestamp between Outbound called_time and hangup_time
 */

import dbService from './dbService.js';

// Define the normalizePhoneNumber function locally
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

async function testExactSchemaMatching() {
  try {
    console.log('🔍 Testing CDR matching using exact schema...');
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
    
    // Show sample agent extensions
    console.log('\nSample agent extensions from outbound calls:');
    let count = 0;
    for (const [ext, calls] of outboundByAgentExt.entries()) {
      if (count < 10) {
        console.log(`   - Agent Extension: ${ext}, Calls: ${calls.length}`);
        count++;
      } else {
        break;
      }
    }
    
    // Show sample CDR caller_id_numbers
    const sampleCdrCallers = processedCDRs.slice(0, 10).map(cdr => cdr.caller_id_number);
    console.log('\nSample CDR caller_id_numbers:');
    console.log(`   ${sampleCdrCallers.join(', ')}`);
    
    // Test matching algorithm
    console.log('\n📊 Step 4: Testing matching algorithm...');
    let matchCount = 0;
    const sampleMatches = [];
    
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
            matchCount++;
            
            if (sampleMatches.length < 10) {
              sampleMatches.push({
                cdr: {
                  id: cdr.id,
                  call_id: cdr.call_id,
                  caller_id_number: cdr.caller_id_number,
                  timestamp: new Date(cdrTimestamp).toISOString()
                },
                outbound: {
                  id: outbound.id,
                  callid: outbound.callid,
                  agent_ext: outbound.agent_ext,
                  called_time: new Date(outboundCalledTime).toISOString(),
                  hangup_time: new Date(outboundHangupTime).toISOString(),
                  queue_name: outbound.queue_name
                }
              });
            }
            
            break; // Found a match for this CDR, no need to check other outbound calls
          }
        }
      }
    }
    
    console.log(`\n✅ MATCHING RESULTS: ${matchCount} matches found`);
    
    if (matchCount > 0) {
      console.log('\n🎯 Sample Successful Matches:');
      sampleMatches.forEach((match, index) => {
        console.log(`\nMatch ${index + 1}:`);
        console.log(`  CDR: ID ${match.cdr.id}, Caller ${match.cdr.caller_id_number}, Time: ${match.cdr.timestamp}, Call ID: ${match.cdr.call_id}`);
        console.log(`  Outbound: ID ${match.outbound.id}, Agent ${match.outbound.agent_ext}, Time: ${match.outbound.called_time} - ${match.outbound.hangup_time}, Queue: ${match.outbound.queue_name}`);
      });
      
      console.log(`\n🎉 SUCCESS! Found ${matchCount} CDR records that match outbound calls!`);
      console.log('✅ These CDR records should appear in the final report.');
      
      // Show the matching CDR IDs for reference
      const matchingCdrIds = sampleMatches.map(match => match.cdr.id);
      console.log(`\n📋 Sample matching CDR IDs: ${matchingCdrIds.join(', ')}`);
      
    } else {
      console.log('\n❌ No matches found');
      console.log('Possible reasons:');
      console.log('1. No CDR caller_id_number matches any outbound agent_ext');
      console.log('2. CDR timestamps do not fall within outbound call time windows');
      
      // Show some potential matches by extension only (ignoring time)
      console.log('\n🔍 Checking for extension matches (ignoring time):');
      let extensionMatches = 0;
      for (const cdr of processedCDRs.slice(0, 100)) {
        if (outboundByAgentExt.has(cdr.caller_id_number)) {
          extensionMatches++;
          if (extensionMatches <= 5) {
            const matchingOutbound = outboundByAgentExt.get(cdr.caller_id_number)[0];
            console.log(`   Extension match: CDR caller ${cdr.caller_id_number} = Outbound agent ${matchingOutbound.agent_ext}`);
          }
        }
      }
      console.log(`Found ${extensionMatches} extension matches (ignoring time constraints)`);
    }
    
    console.log('\n🏁 Exact schema matching test complete');
    
  } catch (error) {
    console.error('❌ Error during exact schema matching test:', error);
  }
}

// Run the test
testExactSchemaMatching().catch(console.error);
