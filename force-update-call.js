// force-update-call.js
// Force update a specific call with disposition data from the API
// Usage: node force-update-call.js <callid>

import dotenv from 'dotenv';
import dbService from './dbService.js';
import { getPortalToken } from './tokenService.js';

dotenv.config();

const callId = process.argv[2];

if (!callId) {
  console.error('❌ Please provide a call ID');
  console.log('Usage: node force-update-call.js <callid>');
  process.exit(1);
}

async function forceUpdateCall() {
  try {
    console.log(`🔍 Force updating call: ${callId}`);
    
    // Get JWT token
    const token = await getPortalToken('default');
    if (!token) {
      throw new Error('Failed to obtain authentication token');
    }
    
    // Calculate time range for the last 24 hours
    const endDate = Math.floor(Date.now() / 1000);
    const startDate = endDate - (24 * 60 * 60);
    
    // Try all endpoints
    const endpoints = [
      '/api/v2/reports/queues_outbound_cdrs',
      '/api/v2/reports/queues_cdrs',
      '/api/v2/reports/campaigns/leads/history'
    ];
    
    let foundCall = null;
    
    for (const endpoint of endpoints) {
      console.log(`🔍 Checking endpoint: ${endpoint}`);
      
      const queryParams = new URLSearchParams({
        account: process.env.ACCOUNT_ID_HEADER,
        startDate: startDate,
        endDate: endDate,
        pageSize: 2000
      });
      
      const fullUrl = `${process.env.BASE_URL}${endpoint}?${queryParams}`;
      
      const response = await fetch(fullUrl, {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
          'Authorization': `Bearer ${token}`,
          'X-Account-ID': process.env.ACCOUNT_ID_HEADER
        }
      });
      
      if (!response.ok) {
        console.log(`⚠️ ${endpoint} failed: ${response.status}`);
        continue;
      }
      
      const responseData = await response.json();
      const data = responseData.cdrs || responseData.data || [];
      
      foundCall = data.find(call => {
        const id = call.call_id || call.callid;
        return id === callId;
      });
      
      if (foundCall) {
        console.log(`✅ Found call in ${endpoint}`);
        break;
      }
    }
    
    if (!foundCall) {
      console.error(`❌ Call ${callId} not found in any endpoint`);
      process.exit(1);
    }
    
    console.log('📊 Call data:', JSON.stringify(foundCall, null, 2));
    
    // Extract disposition info
    const agentDisposition = foundCall.agent_disposition || '';
    const followUpNotes = foundCall.follow_up_notes || '';
    
    // Extract subdispositions with new format support
    let subDisp1 = '';
    let subDisp2 = '';
    
    if (foundCall.agent_subdisposition) {
      if (typeof foundCall.agent_subdisposition === 'object' && foundCall.agent_subdisposition.name) {
        subDisp1 = foundCall.agent_subdisposition.name;
        
        if (foundCall.agent_subdisposition.subdisposition) {
          const subDisp = foundCall.agent_subdisposition.subdisposition;
          
          // New format: subdisposition has key-value pairs
          if (subDisp.key && subDisp.value) {
            subDisp2 = `${subDisp.key} = ${subDisp.value}`;
          }
          // Old format: subdisposition has name
          else if (subDisp.name) {
            subDisp2 = subDisp.name;
          }
        }
      }
    }
    
    console.log(`\n📋 Extracted data:`);
    console.log(`   Agent Disposition: "${agentDisposition}"`);
    console.log(`   Sub Disp 1: "${subDisp1}"`);
    console.log(`   Sub Disp 2: "${subDisp2}"`);
    console.log(`   Follow-up Notes: "${followUpNotes}"`);
    
    // Update final_report table
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
    
    if (fieldsToUpdate.length === 0) {
      console.log('⚠️ No fields to update');
      process.exit(0);
    }
    
    fieldsToUpdate.push('updated_at = NOW()');
    values.push(callId);
    
    const updateQuery = `
      UPDATE final_report 
      SET ${fieldsToUpdate.join(', ')}
      WHERE call_id = ?
    `;
    
    console.log(`\n🔄 Updating final_report table...`);
    const result = await dbService.query(updateQuery, values);
    
    if (result.affectedRows > 0) {
      console.log(`✅ Successfully updated ${result.affectedRows} record(s) in final_report`);
    } else {
      console.log(`⚠️ No records updated. Call ID might not exist in final_report table.`);
    }
    
  } catch (error) {
    console.error('❌ Error:', error.message);
    console.error(error.stack);
  } finally {
    await dbService.end();
  }
}

forceUpdateCall();
