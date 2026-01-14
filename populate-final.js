// populate-final.js
// A robust script to populate the database and enhance final_report table
// Usage: node populate-final.js [startTimestamp] [endTimestamp]
// Example: node populate-final.js 1693526400 1693612800

/**
 * Combined Database Population and Final Report Enhancement Script
 * 
 * This script combines:
 * 1. populate-db-with-time.js - Populates raw tables from APIs with custom date range
 * 2. populate-final-enhanced.js - Enhances final_report with CDR matching using optimized algorithms
 * 
 * Usage: node populate-final.js [startTimestamp] [endTimestamp]
 * Example: node populate-final.js 1693526400 1693612800
 */

import dotenv from 'dotenv';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import dbService from './dbService.js';
import { populateFinalReportEnhanced } from './finalReportService.js';

dotenv.config();

/**
 * Function to check raw tables for data
 */
async function checkRawTables() {
  console.log('🔍 Checking raw tables for data...');
  
  const tables = [
    // 'raw_cdrs',
    'raw_queue_inbound',
    'raw_queue_outbound',
    'raw_campaigns'
  ];
  
  for (const table of tables) {
    try {
      const result = await dbService.query(`SELECT COUNT(*) as count FROM ${table}`);
      console.log(`📊 ${table}: ${result[0].count} records`);
    } catch (error) {
      console.error(`❌ Error checking ${table}:`, error.message);
    }
  }
}

/**
 * Function to parse command line arguments or use default values
 */
function parseArgs() {
  const args = process.argv.slice(2);
  
  // Default to last 24 hours if no arguments provided
  const endDate = Math.floor(Date.now() / 1000); // Current time in seconds
  const startDate = endDate - (24 * 60 * 60); // 24 hours ago in seconds
  
  if (args.length >= 2) {
    const parsedStartDate = parseInt(args[0], 10);
    const parsedEndDate = parseInt(args[1], 10);
    
    if (!isNaN(parsedStartDate) && !isNaN(parsedEndDate)) {
      return {
        startDate: parsedStartDate,
        endDate: parsedEndDate
      };
    }
  }
  
  return {
    startDate,
    endDate
  };
}


// Using functions from finalReportService.js instead

// Using populateFinalReportEnhanced from finalReportService.js instead

// demonstrateEnhancedCDRMatching function has been removed as it's no longer needed

/**
 * Main function to fetch data and populate tables
 */
async function populateDBWithTimeRange() {
  console.log('🚀 Starting database population process...');
  const startTime = Date.now();
  
  try {
    // Parse command line arguments
    const { startDate, endDate } = parseArgs();
    
    console.log(`📅 Using date range: ${new Date(startDate * 1000).toISOString()} to ${new Date(endDate * 1000).toISOString()}`);
    
    // Step 1: Check current state of raw tables
    console.log('📊 Step 1: Checking current state of raw tables...');
    await checkRawTables();
    
    // Step 2: Fetch data from APIs and populate raw tables
    console.log('📊 Step 2: Fetching data from APIs and populating raw tables...');
    
    const apiParams = {
      start_date: startDate,
      end_date: endDate
    };
    
    // Fetch data from APIs and populate raw tables
    const fetchResults = await fetchAllAPIsAndPopulateDB('default', apiParams);
    console.log('📊 API fetch results:', fetchResults);
    
    // Step 3: Check raw tables after population
    console.log('📊 Step 3: Checking raw tables after population...');
    await checkRawTables();
    
    // Step 5: Populate final_report table with enhanced data
    console.log('📊 Step 5: Populating final_report table with enhanced data...');
    
    // Skip clearing existing data - we want to add to existing records
    console.log('📊 Adding new data without clearing existing records...');
    
    // Use populateFinalReportEnhanced from finalReportService.js which already handles:
    // - CDR matching with optimized algorithms from enhanced-cdr-matching.js
    // - Follow-up notes extraction during record insertion
    // - Transferred calls detection and processing
    const enhancedPopulateResult = await populateFinalReportEnhanced('default', {
      startDate,
      endDate
    });
    
    console.log('✅ Enhanced final report population result:', enhancedPopulateResult);
    
    // Step 5: Verify final_report table has data
    console.log('📊 Step 5: Verifying final_report table has data...');
    const finalReportCount = await dbService.query('SELECT COUNT(*) as count FROM final_report');
    console.log(`📊 final_report: ${finalReportCount[0].count} records`);
    
    // Check follow-up notes
    const notesQuery = 'SELECT COUNT(*) as count FROM final_report WHERE follow_up_notes IS NOT NULL';
    const notesCount = await dbService.query(notesQuery);
    console.log(`📊 Records with follow-up notes: ${notesCount[0].count} records`);
    
    // Check record type distribution
    const distributionQuery = `
      SELECT record_type, COUNT(*) as count 
      FROM final_report 
      GROUP BY record_type 
      ORDER BY count DESC
    `;
    const distribution = await dbService.query(distributionQuery);
    console.log('\n📊 Record type distribution in final_report:');
    distribution.forEach(row => {
      console.log(`  ${row.record_type}: ${row.count} records`);
    });
    
    const duration = ((Date.now() - startTime) / 1000).toFixed(2);
    console.log(`\n✅ Process completed successfully in ${duration}s!`);
    
  } catch (error) {
    console.error('❌ Error in database population process:', error);
  } finally {
    // Close database connections
    try {
      console.log('Closing database connection...');
      await dbService.end();
      console.log('✅ Database connection closed');
    } catch (err) {
      console.error('⚠️ Error closing database connection:', err);
    }
  }
}

// Run the populate function
populateDBWithTimeRange().catch(console.error);
