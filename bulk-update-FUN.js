// bulk-update-FUN.js
// Script to bulk update follow-up notes in final_report table from raw_cdrs_all

import dotenv from 'dotenv';
import dbService from './dbService.js';

dotenv.config();

async function bulkUpdateFollowUpNotes() {
  try {
    console.log('🔄 Starting bulk follow-up notes update process...');
    
    // Step 1: Get all records from final_report that need updating
    console.log('📊 Step 1: Fetching records from final_report table...');
    const finalReportQuery = `
      SELECT id, call_id, record_type
      FROM final_report
      WHERE follow_up_notes IS NULL
    `;
    
    const finalReportRecords = await dbService.query(finalReportQuery);
    console.log(`Found ${finalReportRecords.length} records in final_report to update`);
    
    if (finalReportRecords.length === 0) {
      console.log('✅ No records need updating. All records already have follow-up notes or there are no records.');
      return;
    }
    
    // Step 2: For each record, check if there's a matching follow-up note in raw_cdrs_all
    console.log('📊 Step 2: Finding matching follow-up notes in raw_cdrs_all...');
    let updatedCount = 0;
    let notFoundCount = 0;
    let noNotesCount = 0;
    
    // Process records in batches to avoid memory issues
    const batchSize = 50;
    const totalBatches = Math.ceil(finalReportRecords.length / batchSize);
    
    for (let batchIndex = 0; batchIndex < totalBatches; batchIndex++) {
      const batchStart = batchIndex * batchSize;
      const batchEnd = Math.min((batchIndex + 1) * batchSize, finalReportRecords.length);
      const batch = finalReportRecords.slice(batchStart, batchEnd);
      
      console.log(`\n📦 Processing batch ${batchIndex + 1}/${totalBatches} (records ${batchStart + 1}-${batchEnd})`);
      
      for (const record of batch) {
        // Skip if no call_id
        if (!record.call_id) {
          console.log(`⚠️ Record ID ${record.id} has no call_id, skipping`);
          notFoundCount++;
          continue;
        }
        
        // Find matching raw_cdrs_all record
        const rawCdrsQuery = `
          SELECT call_id, raw_data
          FROM raw_cdrs_all
          WHERE call_id = ?
        `;
        
        const rawCdrsResults = await dbService.query(rawCdrsQuery, [record.call_id]);
        
        if (rawCdrsResults.length === 0) {
          console.log(`⚠️ No matching raw_cdrs_all record found for call_id ${record.call_id}`);
          notFoundCount++;
          continue;
        }
        
        // Extract follow-up notes from raw_cdrs_all
        let rawData;
        try {
          rawData = typeof rawCdrsResults[0].raw_data === 'string' 
            ? JSON.parse(rawCdrsResults[0].raw_data) 
            : rawCdrsResults[0].raw_data;
        } catch (e) {
          console.log(`⚠️ Error parsing raw_data for call_id ${record.call_id}: ${e.message}`);
          notFoundCount++;
          continue;
        }
        
        // Check all possible locations for follow-up notes
        let followUpNotes = null;
        
        // Direct follow_up_notes field
        if (rawData.follow_up_notes) {
          followUpNotes = rawData.follow_up_notes;
        }
        // In fonoUC structure
        else if (rawData.fonoUC) {
          followUpNotes = rawData.fonoUC.follow_up_notes ||
                        rawData.fonoUC.cc?.follow_up_notes ||
                        rawData.fonoUC.cc_outbound?.follow_up_notes ||
                        rawData.fonoUC.cc_campaign?.follow_up_notes || null;
        }
        
        if (followUpNotes) {
          // Update the final_report record with the follow-up notes
          const updateQuery = `
            UPDATE final_report
            SET follow_up_notes = ?
            WHERE id = ?
          `;
          
          await dbService.query(updateQuery, [followUpNotes, record.id]);
          updatedCount++;
          
          if (updatedCount % 10 === 0 || updatedCount === 1) {
            console.log(`✅ Updated ${updatedCount} records with follow-up notes so far`);
          }
        } else {
          noNotesCount++;
          if (noNotesCount % 50 === 0 || noNotesCount === 1) {
            console.log(`ℹ️ No follow-up notes found for ${noNotesCount} records so far`);
          }
        }
      }
    }
    
    console.log(`\n📊 Summary:`);
    console.log(`✅ Successfully updated ${updatedCount} records with follow-up notes`);
    console.log(`⚠️ No matching raw_cdrs_all record found for ${notFoundCount} records`);
    console.log(`ℹ️ No follow-up notes found for ${noNotesCount} records`);
    
    // Step 3: Verify the updates
    console.log('\n📊 Step 3: Verifying updates...');
    const verifyQuery = `
      SELECT COUNT(*) as count
      FROM final_report
      WHERE follow_up_notes IS NOT NULL
    `;
    
    const verifyResult = await dbService.query(verifyQuery);
    console.log(`Total records with follow-up notes in final_report: ${verifyResult[0].count}`);
    
    // Calculate percentage
    const totalQuery = `
      SELECT COUNT(*) as count
      FROM final_report
    `;
    
    const totalResult = await dbService.query(totalQuery);
    const percentage = (verifyResult[0].count / totalResult[0].count) * 100;
    console.log(`Percentage of records with follow-up notes: ${percentage.toFixed(2)}%`);
    
  } catch (error) {
    console.error('❌ Error during bulk update:', error);
  } finally {
    // Close the database connection
    try {
      await dbService.end();
      console.log('✅ Database connection closed');
    } catch (err) {
      console.error('⚠️ Error closing database connection:', err);
    }
  }
}

// Run the bulk update function
bulkUpdateFollowUpNotes();
