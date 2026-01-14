// populate-db-with-timerange.js
// Script to populate raw tables and final_report table with a custom date range
// Usage: node populate-db-with-timerange.js <startTimestamp> <endTimestamp>
// Example: node populate-db-with-timerange.js 1693526400 1693612800

import dotenv from 'dotenv';
import { fetchAllAPIsAndPopulateDB } from './apiDataFetcher.js';
import finalReportService from './finalReportService.js';
import dbService from './dbService.js';

dotenv.config();

// Function to check raw tables for data
async function checkRawTables() {
  console.log('🔍 Checking raw tables for data...');
  
  const tables = [
    'raw_cdrs',
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

// Function to parse command line arguments or use default values
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

// Main function to fetch data and populate tables
async function populateDBWithTimeRange() {
  console.log('🚀 Starting database population process...');
  
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
    
    // Step 4: Populate final_report table from raw tables
    console.log('📊 Step 4: Populating final_report table from raw tables...');
    
    // Skip clearing existing data - we want to add to existing records
    console.log('📊 Adding new data without clearing existing records...');
    
    // Populate final_report table
    const populateResult = await finalReportService.populateFinalReport('default', {
      startDate,
      endDate
    });
    
    console.log('✅ Final report population result:', populateResult);
    
    // Step 5: Verify final_report table has data
    console.log('📊 Step 5: Verifying final_report table has data...');
    const finalReportCount = await dbService.query('SELECT COUNT(*) as count FROM final_report');
    console.log(`📊 final_report: ${finalReportCount[0].count} records`);
    
    if (finalReportCount[0].count > 0) {
      // Show a sample of the data
      const sampleResult = await dbService.query('SELECT * FROM final_report LIMIT 1');
      console.log('📋 Sample record:', JSON.stringify(sampleResult[0], null, 2));
    }
    
    console.log('✅ Process completed successfully!');
    
  } catch (error) {
    console.error('❌ Error in database population process:', error);
  } finally {
    // Close database connections
    // Note: We don't close the pool here as it might be used by other processes
    // The Node.js process will exit naturally when done
  }
}

// Run the populate function
populateDBWithTimeRange();
  