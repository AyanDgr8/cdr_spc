-- MySQL Database Schema for SPC Reporting System - Raw Data Storage
-- This schema supports storing raw API response data as JSON

-- Create the database if it doesn't exist
CREATE DATABASE IF NOT EXISTS spc_main_cdr CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

-- Use the database
USE spc_main_cdr;


-- Raw Campaigns data table
CREATE TABLE IF NOT EXISTS raw_campaigns (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100),
    campaign_name VARCHAR(100),
    timestamp BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_call_id (call_id),
    INDEX idx_campaign_name (campaign_name),
    INDEX idx_timestamp (timestamp),
    INDEX idx_timestamp_call_id (timestamp, call_id)
);

-- Raw Queue Inbound data table
CREATE TABLE IF NOT EXISTS raw_queue_inbound (
    id INT AUTO_INCREMENT PRIMARY KEY,
    callid VARCHAR(100),
    queue_name VARCHAR(100),
    called_time BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_callid (callid),
    INDEX idx_queue_name (queue_name),
    INDEX idx_called_time (called_time),
    INDEX idx_called_time_callid (called_time, callid)
);

-- Raw Queue Outbound data table
CREATE TABLE IF NOT EXISTS raw_queue_outbound (
    id INT AUTO_INCREMENT PRIMARY KEY,
    callid VARCHAR(100),
    queue_name VARCHAR(100),
    called_time BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_callid (callid),
    INDEX idx_queue_name (queue_name),
    INDEX idx_called_time (called_time),
    INDEX idx_called_time_callid (called_time, callid)
);

-- Raw CDRs All data table
CREATE TABLE IF NOT EXISTS raw_cdrs_all (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100),
    timestamp BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_call_id (call_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_timestamp_call_id (timestamp, call_id)
);

-- Raw CDRs data table
CREATE TABLE IF NOT EXISTS raw_cdrs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    call_id VARCHAR(100),
    timestamp BIGINT,
    raw_data JSON NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY unique_call_id (call_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_timestamp_call_id (timestamp, call_id)
);


-- final_report_schema.sql
-- Schema for the final_report table that will store all processed data

-- Create the final_report table
CREATE TABLE IF NOT EXISTS final_report (
    id INT AUTO_INCREMENT PRIMARY KEY,
    
    -- Record identification
    call_id VARCHAR(100),
    record_type ENUM('Campaign', 'Inbound', 'Outbound', 'CDR') NOT NULL,
    
    -- Common fields displayed in the frontend
    type VARCHAR(50),
    agent_name VARCHAR(100),
    extension VARCHAR(20),
    queue_campaign_name VARCHAR(200),
    called_time BIGINT,
    called_time_formatted VARCHAR(50),
    caller_id_number VARCHAR(50),
    caller_id_name VARCHAR(200),
    callee_id_number VARCHAR(50),
    answered_time VARCHAR(50),
    hangup_time VARCHAR(50),
    wait_duration VARCHAR(20),
    talk_duration VARCHAR(20),
    hold_duration VARCHAR(20),
    agent_hangup VARCHAR(10),
    agent_disposition VARCHAR(100),
    disposition VARCHAR(100),
    sub_disp_1 VARCHAR(100),
    sub_disp_2 VARCHAR(100),
    status VARCHAR(50),
    campaign_type VARCHAR(50),
    abandoned VARCHAR(10),
    country VARCHAR(50),
    follow_up_notes TEXT,
    
    -- HTML content fields (stored as text)
    agent_history TEXT,
    queue_history TEXT,
    lead_history TEXT,
    recording VARCHAR(200),
    
    -- Metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    
    -- Indexes for fast retrieval
    INDEX idx_call_id (call_id),
    INDEX idx_record_type (record_type),
    INDEX idx_called_time (called_time),
    INDEX idx_caller_id_number (caller_id_number),
    INDEX idx_callee_id_number (callee_id_number),
    INDEX idx_agent_name (agent_name),
    INDEX idx_queue_campaign_name (queue_campaign_name),
    INDEX idx_called_time_record_type (called_time, record_type)
);

-- Users table for authentication
CREATE TABLE IF NOT EXISTS users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    username VARCHAR(100) NOT NULL UNIQUE,
    email VARCHAR(255) NOT NULL UNIQUE,
    password VARCHAR(255) NOT NULL,
    last_login TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Insert default admin user (password: Ayan1012)
INSERT INTO users (username, email, password) 
VALUES ('Ayan Khan', 'ayan@multycomm.com', '$2b$10$8XpgD1hs3A5H5hOIGWnp6.lQMJY.xYy9.B9A1iRNJCwCJOY5pMTpO')
ON DUPLICATE KEY UPDATE username = VALUES(username);



ALTER TABLE final_report ADD INDEX idx_called_time_formatted (called_time_formatted);
ALTER TABLE final_report
  MODIFY COLUMN caller_id_number VARCHAR(200),
  MODIFY COLUMN caller_id_name   VARCHAR(200),
  MODIFY COLUMN callee_id_number VARCHAR(200);

ALTER TABLE final_report
   MODIFY COLUMN disposition VARCHAR(100),
   MODIFY COLUMN sub_disp_1 VARCHAR(200),
   MODIFY COLUMN sub_disp_2 VARCHAR(200);


-- ***************

ALTER TABLE final_report ADD COLUMN transfer_event BOOLEAN DEFAULT FALSE, 
ADD COLUMN transfer_extension VARCHAR(50) NULL, 
ADD COLUMN transfer_type VARCHAR(50) NULL;


  -- Add transfer_source_call_id column to final_report table
ALTER TABLE final_report ADD COLUMN transfer_source_call_id VARCHAR(100) DEFAULT NULL;

-- Add index for faster lookups
ALTER TABLE final_report ADD INDEX idx_transfer_source_call_id (transfer_source_call_id);

-- Add index for combined lookups
ALTER TABLE final_report ADD INDEX idx_transfer_ext_source (transfer_extension, transfer_source_call_id);


--  Final report: ensure uniqueness to avoid accidental duplicates
ALTER TABLE final_report
  ADD UNIQUE KEY uq_recordtype_callid (record_type, call_id); 


ALTER TABLE final_report 
MODIFY COLUMN record_type 
    ENUM('Campaign', 'Inbound', 'Outbound', 'CDR', 'Transferred CDR') NOT NULL;


-- ***************
-- Add hold_duration_intervals column to store formatted hold intervals
ALTER TABLE final_report ADD COLUMN hold_duration_intervals TEXT DEFAULT NULL;


-- 
ALTER TABLE final_report
MODIFY COLUMN agent_disposition VARCHAR(200),
MODIFY COLUMN disposition VARCHAR(200),
MODIFY COLUMN sub_disp_1 VARCHAR(200),
MODIFY COLUMN sub_disp_2 VARCHAR(200);
