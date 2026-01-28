// Dubai timezone for consistent date handling
const TIMEZONE = 'Asia/Dubai';

// Global state for results
const state = {
  currentResults: [],
  totalCount: 0,
  lastSearchParams: null,
  timeoutIds: [], // Array to store timeout IDs
  progressiveLoading: {
    active: false,
    queryId: null,
    currentPage: 1,
    totalPages: 0,
    loadedRecords: 0,
    totalRecords: 0,
    isComplete: false
  }
};

// DOM elements
const elements = {
  form: document.getElementById('filterForm'),
  startInput: document.getElementById('start'),
  endInput: document.getElementById('end'),
  fetchBtn: document.getElementById('fetchBtn'),
  csvBtn: document.getElementById('csvBtn'),
  loading: document.getElementById('loading'),
  errorBox: document.getElementById('errorBox'),
  stats: document.getElementById('stats'),
  resultTable: document.getElementById('resultTable')
};

// Initialize date inputs with current day (Dubai timezone)
function initializeDateInputs() {
  const now = luxon.DateTime.now().setZone(TIMEZONE);
  const startOfDay = now.startOf('day');
  const endOfDay = now.endOf('day');
  
  elements.startInput.value = startOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
  elements.endInput.value = endOfDay.toFormat("yyyy-MM-dd'T'HH:mm");
}

// Format duration in seconds to HH:MM:SS
function formatDuration(duration) {
  // Handle null, undefined, empty strings
  if (duration === null || duration === undefined || duration === '') {
    return '00:00:00';
  }
  
  // Handle string format "HH:MM:SS"
  if (typeof duration === 'string') {
    // If it's already in HH:MM:SS format, return as is
    if (duration.match(/^\d{2}:\d{2}:\d{2}$/)) {
      return duration;
    }
    
    // Handle MySQL TIME format
    if (duration.match(/^\d{2}:\d{2}:\d{2}\.\d+$/)) {
      return duration.split('.')[0]; // Remove microseconds
    }
    
    // Try to convert string to number
    if (!isNaN(duration)) {
      duration = Number(duration);
    } else {
      return '00:00:00';
    }
  }
  
  // Handle numeric values (seconds)
  if (typeof duration === 'number') {
    if (isNaN(duration) || duration < 0) {
      return '00:00:00';
    }
    
    const hours = Math.floor(duration / 3600);
    const minutes = Math.floor((duration % 3600) / 60);
    const seconds = Math.floor(duration % 60);
    
    return [
      hours.toString().padStart(2, '0'),
      minutes.toString().padStart(2, '0'),
      seconds.toString().padStart(2, '0')
    ].join(':');
  }
  
  // If all else fails
  return '00:00:00';
}

// Format date for display
function formatDate(dateString, formattedString) {
  // If we have a pre-formatted string from the database, use it
  if (formattedString && formattedString !== '0000-00-00 00:00:00' && formattedString !== 'undefined') return formattedString;
  
  // Handle empty values
  if (!dateString || dateString === null) return '';
  if (dateString === '0' || dateString === 0) return '';
  if (dateString === '0000-00-00 00:00:00') return '';
  if (dateString === 'null' || dateString === 'undefined') return '';
  
  // If dateString is already in the format DD/MM/YYYY, HH:MM:SS, return it directly
  if (typeof dateString === 'string' && dateString.match(/^\d{2}\/\d{2}\/\d{4}, \d{2}:\d{2}:\d{2}$/)) {
    return dateString;
  }
  
  try {
    // For Unix timestamps (seconds since epoch)
    if (typeof dateString === 'number' || !isNaN(parseInt(dateString))) {
      const timestamp = parseInt(dateString);
      
      // Check if this is a very small timestamp (likely invalid)
      // 86400 = seconds in a day, so anything less than 86400 is likely invalid
      if (timestamp < 86400) return '';
      
      // Convert to milliseconds if needed (Unix timestamps are in seconds)
      const timestampMs = timestamp > 10000000000 ? timestamp : timestamp * 1000;
      const date = new Date(timestampMs);
      
      // Validate the date is reasonable (not 1970)
      if (date.getFullYear() < 2000) return '';
      
      // Format the date manually
      const day = date.getDate().toString().padStart(2, '0');
      const month = (date.getMonth() + 1).toString().padStart(2, '0');
      const year = date.getFullYear();
      const hours = date.getHours().toString().padStart(2, '0');
      const minutes = date.getMinutes().toString().padStart(2, '0');
      const seconds = date.getSeconds().toString().padStart(2, '0');
      
      return `${day}/${month}/${year}, ${hours}:${minutes}:${seconds}`;
    }
    
    // For ISO strings or MySQL datetime strings
    if (typeof dateString === 'string') {
      // Check if it's a MySQL datetime string (YYYY-MM-DD HH:MM:SS)
      if (dateString.match(/^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$/)) {
        const [datePart, timePart] = dateString.split(' ');
        const [year, month, day] = datePart.split('-');
        const [hours, minutes, seconds] = timePart.split(':');
        
        return `${day}/${month}/${year}, ${hours}:${minutes}:${seconds}`;
      }
      
      // Try as ISO string
      const date = new Date(dateString);
      if (!isNaN(date.getTime())) {
        // Validate the date is reasonable (not 1970)
        if (date.getFullYear() < 2000) return '';
        
        // Format the date manually
        const day = date.getDate().toString().padStart(2, '0');
        const month = (date.getMonth() + 1).toString().padStart(2, '0');
        const year = date.getFullYear();
        const hours = date.getHours().toString().padStart(2, '0');
        const minutes = date.getMinutes().toString().padStart(2, '0');
        const seconds = date.getSeconds().toString().padStart(2, '0');
        
        return `${day}/${month}/${year}, ${hours}:${minutes}:${seconds}`;
      }
    }
    
    // If all else fails, return empty string
    return '';
  } catch (e) {
    console.error('Error formatting date:', e, 'dateString:', dateString);
    return '';
  }
}

// Get all form values as an object
function getFormValues() {
  // ❌ remove Luxon conversion
  // const startDate = elements.startInput.value ? luxon.DateTime.fromISO(elements.startInput.value).toISO() : '';
  // const endDate = elements.endInput.value ? luxon.DateTime.fromISO(elements.endInput.value).toISO() : '';

  // ✅ just use the raw values directly (exact strings as in DB)
  const startDate = elements.startInput.value || '';
  const endDate = elements.endInput.value || '';
  
  console.log('Original start date:', elements.startInput.value);
  console.log('Formatted start date for API:', startDate);
  console.log('Original end date:', elements.endInput.value);
  console.log('Formatted end date for API:', endDate);
  
  const formData = {
    start: startDate,
    end: endDate,
    // caller_id_number: document.getElementById('caller_id_number').value,
    // callee_id_number: document.getElementById('callee_id_number').value,
    contact_number: document.getElementById('contact_number').value,
    agent_name: document.getElementById('agent_name').value,
    extension: document.getElementById('extension').value,
    queue_campaign_name: document.getElementById('queue_campaign_name').value,
    record_type: document.getElementById('record_type').value,
    agent_disposition: document.getElementById('agent_disposition').value,
    sub_disp_1: document.getElementById('sub_disp_1').value,
    sub_disp_2: document.getElementById('sub_disp_2').value,
    status: document.getElementById('status').value,
    campaign_type: document.getElementById('campaign_type').value,
    country: document.getElementById('country').value,
    transfer_event: document.getElementById('transferred').value, // Changed from 'transferred' to 'transfer_event' to match server-side parameter
    // Always use called_time in descending order
    sort_by: 'called_time',
    sort_order: 'desc'
  };
  
  return formData;
}



async function loadQueueCampaignDropdown(fromTs, toTs) {
  try {
    const res = await fetch(
      `/api/filters/queue-campaign?from_ts=${fromTs}&to_ts=${toTs}`
    );
    const data = await res.json();

    if (!data.success) throw new Error(data.error);

    const select = document.getElementById('queue_campaign_name');

    // ðŸ”‘ Preserve current selection
    const selectedValue = select.value;

    // Reset dropdown
    select.innerHTML = '<option value="">All</option>';

    data.data.forEach(name => {
      const opt = document.createElement('option');
      opt.value = name;
      opt.textContent = name;

      // ðŸ”‘ Restore selected option
      if (name === selectedValue) {
        opt.selected = true;
      }

      select.appendChild(opt);
    });

  } catch (err) {
    console.error('Failed to load queue/campaign dropdown:', err);
  }
}


async function loadAgentDispositionDropdown(fromTs, toTs) {
  try {
    const res = await fetch(
      `/api/filters/agent-disposition?from_ts=${fromTs}&to_ts=${toTs}`
    );

    const data = await res.json();
    if (!data.success) throw new Error(data.error);

    const select = document.getElementById('agent_disposition');

    // ðŸ”‘ Preserve currently selected value
    const selectedValue = select.value;

    // Reset dropdown
    select.innerHTML = '<option value="">All</option>';

    data.data.forEach(value => {
      const opt = document.createElement('option');
      opt.value = value;
      opt.textContent = value;

      // ðŸ”‘ Restore selection if it existed
      if (value === selectedValue) {
        opt.selected = true;
      }

      select.appendChild(opt);
    });

  } catch (err) {
    console.error('Failed to load agent disposition dropdown:', err);
  }
}


// Show error message
function showError(message) {
  // Replace newlines with HTML line breaks for proper display
  const formattedMessage = message.replace(/\n/g, '<br>');
  elements.errorBox.innerHTML = formattedMessage;
  elements.errorBox.classList.remove('is-hidden');
  
  // For multi-line errors, show them longer
  const displayTime = message.includes('\n') ? 10000 : 5000;
  
  setTimeout(() => {
    elements.errorBox.classList.add('is-hidden');
  }, displayTime);
}

// Show loading indicator
function toggleLoading(show) {
  if (show) {
    elements.loading.classList.remove('is-hidden');
    elements.fetchBtn.disabled = true;
  } else {
    elements.loading.classList.add('is-hidden');
    elements.fetchBtn.disabled = false;
  }
}

// Update record count display
function updateRecordCount() {
  elements.stats.textContent = `Found ${state.totalCount} records`;
  elements.stats.classList.remove('is-hidden');
}

// Create table headers
function createTableHeaders() {
  const headers = [
    'S.No.',
    'Record Type',
    'Agent Name',
    'Extension',
    'Queue/Campaign',
    'Called Time (dd:mm:yyyy, hh:mm:ss)',
    'Caller ID Number',
    'Callee ID Number',
    'Answered Time (dd:mm:yyyy, hh:mm:ss)',
    'Hangup Time (dd:mm:yyyy, hh:mm:ss)',
    'Wait Duration (hh:mm:ss)',
    'Talk Duration (hh:mm:ss)',
    'Hold Duration (hh:mm:ss)',
    'Hold Duration Intervals',
    'Agent Disposition',
    'Sub Disp 1',
    'Sub Disp 2',
    'Follow-up Notes',
    'Agent Hangup',
    'Status',
    'Campaign Type',
    'Abandoned',
    'Country',
    'Transfer',
    'Transfer To Extension',
    'Transfer Type',
    'Agent History',
    'Queue History',
    'Recording',
    'All Recordings',
    'Call ID',
    'System Disposition',
  ];
  
  const thead = document.createElement('thead');
  const headerRow = document.createElement('tr');
  
  headers.forEach(header => {
    const th = document.createElement('th');
    th.textContent = header;
    headerRow.appendChild(th);
  });
  
  thead.appendChild(headerRow);
  return thead;
}

// Create table rows from data
function createTableRows(data) {
  const tbody = document.createElement('tbody');
  
  data.forEach(row => {
    const tr = document.createElement('tr');
    
    // Add class based on record type for styling
    const recordTypeClass = row.record_type ? `row-${row.record_type.toLowerCase()}` : '';
    if (recordTypeClass) {
      tr.classList.add(recordTypeClass);
    }
    
    // Process agent_history and queue_history if they're JSON strings
    let agentHistoryContent = '';
    if (row.agent_history) {
      if (typeof row.agent_history === 'string') {
        try {
          // Try to parse if it's a JSON string
          const parsedHistory = JSON.parse(row.agent_history);
          agentHistoryContent = historyToHtml(parsedHistory);
        } catch (e) {
          // If not valid JSON, use as-is (might already be HTML)
          agentHistoryContent = row.agent_history;
        }
      } else if (Array.isArray(row.agent_history)) {
        // If it's already an array, convert to HTML
        agentHistoryContent = historyToHtml(row.agent_history);
      }
    }
    
    let queueHistoryContent = '';
    if (row.queue_history) {
      if (typeof row.queue_history === 'string') {
        try {
          // Try to parse if it's a JSON string
          const parsedHistory = JSON.parse(row.queue_history);
          queueHistoryContent = queueHistoryToHtml(parsedHistory);
        } catch (e) {
          // If not valid JSON, use as-is (might already be HTML)
          queueHistoryContent = row.queue_history;
        }
      } else if (Array.isArray(row.queue_history)) {
        // If it's already an array, convert to HTML
        queueHistoryContent = queueHistoryToHtml(row.queue_history);
      }
    }
    
    // Log the raw data for debugging
    console.log('Row data:', {
      answered_time: row.answered_time,
      hangup_time: row.hangup_time,
      wait_duration: row.wait_duration,
      talk_duration: row.talk_duration,
      hold_duration: row.hold_duration,
      hold_duration_intervals: row.hold_duration_intervals,
    });
    
    // Debug: Log all available keys in the row object
    console.log('All row keys:', Object.keys(row));
    
    // Debug: Check if hold_duration_intervals exists with different casing
    console.log('Hold duration intervals check:', {
      'hold_duration_intervals': row.hold_duration_intervals,
      'Hold Duration Intervals': row['Hold Duration Intervals'],
      'holdDurationIntervals': row.holdDurationIntervals,
      'hold_duration_intervals_exists': 'hold_duration_intervals' in row
    });
    
    // Debug: Check for any field that might contain hold intervals
    const possibleHoldFields = Object.keys(row).filter(key => 
      key.toLowerCase().includes('hold') && key.toLowerCase().includes('interval')
    );
    console.log('Possible hold interval fields:', possibleHoldFields);
    possibleHoldFields.forEach(field => {
      console.log(`${field}:`, row[field]);
    });
    
    // Create row cells array
    const cells = [
      // S.No. - already added separately
      row.record_type || '',
      row.agent_name || '',
      row.extension || '',
      row.queue_campaign_name || '',
      // Use string timestamps directly when available
      typeof row.called_time_formatted === 'string' ? row.called_time_formatted : 
        (typeof row.called_time === 'string' && row.called_time.includes('/') ? row.called_time : formatDate(row.called_time)),
      row.caller_id_number || '',
      row.callee_id_number || '',
      // Handle answered_time - use string directly or empty string for null
      row.answered_time ? 
        (typeof row.answered_time === 'string' && row.answered_time.includes('/') ? 
          row.answered_time : formatDate(row.answered_time)) : '',
      
      // Handle hangup_time - use string directly or empty string for null
      row.hangup_time ? 
        (typeof row.hangup_time === 'string' && row.hangup_time.includes('/') ? 
          row.hangup_time : formatDate(row.hangup_time)) : '',
      
      // Handle wait_duration - use string directly or default format for null
      row.wait_duration ? 
        (typeof row.wait_duration === 'string' && row.wait_duration.includes(':') ? 
          row.wait_duration : formatDuration(row.wait_duration)) : '00:00:00',
      
      // Handle talk_duration - use string directly or default format for null
      row.talk_duration ? 
        (typeof row.talk_duration === 'string' && row.talk_duration.includes(':') ? 
          row.talk_duration : formatDuration(row.talk_duration)) : '00:00:00',
      
      // Handle hold_duration - use string directly or default format for null
      row.hold_duration ? 
        (typeof row.hold_duration === 'string' && row.hold_duration.includes(':') ? 
          row.hold_duration : formatDuration(row.hold_duration)) : '00:00:00',
      
      // Handle hold_duration_intervals - display as formatted text with line breaks
      // Check multiple possible field names for hold duration intervals
      (() => {
        const holdIntervals = row.hold_duration_intervals || 
                            row['Hold Duration Intervals'] || 
                            row.holdDurationIntervals || 
                            row['hold_duration_intervals'] ||
                            '';
        return holdIntervals ? holdIntervals.replace(/\n/g, '<br>') : '';
      })(),
      
      row.agent_disposition || '',
      row.sub_disp_1 || '',
      row.sub_disp_2 || '',
      row.follow_up_notes || '',
      row.agent_hangup || '',
      row.status || '',
      row.campaign_type || '',
      row.abandoned || '',
      row.country || '',
      row.transfer_event ? 'Yes' : 'No',
      row.transfer_extension || '',
      row.transfer_type || '',
      processHistoryData(row.agent_history, 'agent'),
      processHistoryData(row.queue_history, 'queue'),
      row.recording ? createRecordingLink(row.recording) : '',
      (row.call_id && row.called_time) ? createAllRecordingsButton(row.call_id, row.called_time) : '',
      row.call_id || '',
      row.system_disposition || row.disposition || '',
    ];
    
    // Add S.No. column first
    const indexCell = document.createElement('td');
    indexCell.textContent = (data.indexOf(row) + 1).toString();
    tr.appendChild(indexCell);
    
    // Add all other cells
    cells.forEach(cellContent => {
      const td = document.createElement('td');
      
      if (typeof cellContent === 'string') {
        td.innerHTML = cellContent;
      } else {
        td.appendChild(cellContent);
      }
      
      tr.appendChild(td);
    });
    
    tbody.appendChild(tr);
  });
  
  return tbody;
}

// Create recording link/button - plays single recording directly (no API call)
function createRecordingLink(recordingUrl) {
  if (!recordingUrl) return '';
  
  const button = document.createElement('button');
  button.className = 'button is-small is-info is-rounded play-btn';
  button.innerHTML = '<span class="icon is-small"><i class="material-icons">play_arrow</i></span>';
  button.title = 'Play this recording';
  
  // Construct direct recording URL
  let cleanUrl = recordingUrl;
  if (!recordingUrl.includes('/') && !recordingUrl.includes('http')) {
    cleanUrl = `/api/recordings/${recordingUrl}?account=default`;
  }
  
  button.dataset.src = cleanUrl;
  
  return button;
}

// Create "All Recordings" button - fetches all recordings for this call via API
function createAllRecordingsButton(callId, calledTime) {
  if (!callId || !calledTime) return '';
  
  const button = document.createElement('button');
  button.className = 'button is-small is-warning is-rounded all-recordings-btn';
  button.innerHTML = '<span class="icon is-small"><i class="material-icons">library_music</i></span>';
  button.title = 'View all recordings for this call';
  
  button.dataset.callId = callId;
  button.dataset.calledTime = calledTime;
  
  return button;
}

// Fetch recordings by call_id
async function fetchRecordingsByCallId(callId, calledTime) {
  try {
    console.log('fetchRecordingsByCallId called with:', { callId, calledTime, type: typeof calledTime });
    
    // Extract year-month from called_time (format: YYYYMM)
    let yearMonth = '';
    
    if (!calledTime) {
      throw new Error('called_time is required but was not provided');
    }
    
    // Handle different date formats
    // First, check if it's a numeric string (timestamp as string)
    if (typeof calledTime === 'string' && /^\d+$/.test(calledTime)) {
      console.log('Converting numeric string to number:', calledTime);
      calledTime = parseInt(calledTime, 10);
    }
    
    if (typeof calledTime === 'number') {
      // Unix timestamp (seconds or milliseconds)
      console.log('Processing numeric timestamp:', calledTime);
      const timestampMs = calledTime > 10000000000 ? calledTime : calledTime * 1000;
      const dateObj = new Date(timestampMs);
      
      if (isNaN(dateObj.getTime())) {
        throw new Error(`Invalid timestamp: ${calledTime}`);
      }
      
      const year = dateObj.getFullYear();
      const month = (dateObj.getMonth() + 1).toString().padStart(2, '0');
      yearMonth = `${year}${month}`;
      console.log('Extracted yearMonth from timestamp:', yearMonth, 'Date:', dateObj.toISOString());
      
    } else if (typeof calledTime === 'string') {
      console.log('Processing string date:', calledTime);
      
      // String format (DD/MM/YYYY, HH:MM:SS or YYYY-MM-DD HH:MM:SS)
      if (calledTime.includes('/')) {
        // DD/MM/YYYY format
        const parts = calledTime.split(',')[0].split('/');
        if (parts.length >= 3) {
          yearMonth = parts[2] + parts[1]; // YYYYMM
          console.log('Extracted yearMonth from DD/MM/YYYY:', yearMonth);
        }
      } else if (calledTime.includes('-')) {
        // YYYY-MM-DD format
        const parts = calledTime.split(' ')[0].split('-');
        if (parts.length >= 3) {
          yearMonth = parts[0] + parts[1]; // YYYYMM
          console.log('Extracted yearMonth from YYYY-MM-DD:', yearMonth);
        }
      }
      
      // If string parsing failed, try to parse as date
      if (!yearMonth) {
        const dateObj = new Date(calledTime);
        if (!isNaN(dateObj.getTime())) {
          const year = dateObj.getFullYear();
          const month = (dateObj.getMonth() + 1).toString().padStart(2, '0');
          yearMonth = `${year}${month}`;
          console.log('Extracted yearMonth from parsed string:', yearMonth);
        }
      }
    }
    
    if (!yearMonth) {
      throw new Error(`Could not determine year-month from called_time: ${calledTime} (type: ${typeof calledTime})`);
    }
    
    // Use backend proxy endpoint to avoid CORS issues
    const apiUrl = `/api/recordings/by-call-id/${yearMonth}-${callId}?account=default`;
    
    console.log('Fetching recordings from backend proxy:', apiUrl);
    
    // Make the API request through the backend proxy
    const response = await fetch(apiUrl, {
      method: 'GET',
      headers: {
        'Accept': 'application/json'
      }
    });
    
    if (!response.ok) {
      const errorText = await response.text();
      console.error('API error response:', errorText);
      throw new Error(`API request failed: ${response.status} ${response.statusText}`);
    }
    
    const data = await response.json();
    console.log('Recordings response:', data);
    console.log('Number of recordings:', data.recordings ? data.recordings.length : 0);
    console.log('Recordings array:', data.recordings);
    
    return data;
  } catch (error) {
    console.error('Error fetching recordings by call_id:', error);
    throw error;
  }
}

// Display multiple recordings in a modal
function showRecordingsModal(recordings) {
  if (!recordings || recordings.length === 0) {
    alert('No recordings found for this call');
    return;
  }
  
  // Create modal
  const modal = document.createElement('div');
  modal.className = 'modal is-active recordings-list-modal';
  
  // Build recordings list HTML
  let recordingsHtml = '';
  recordings.forEach((recording, index) => {
    recordingsHtml += `
      <div class="box" style="margin-bottom: 1rem;">
        <div style="display: flex; align-items: center; justify-content: space-between;">
          <div>
            <strong>Recording ${index + 1}</strong>
            <br>
            <small style="font-family: monospace; color: #666;">${recording.id}</small>
          </div>
          <button class="button is-info is-rounded play-recording-btn" 
                  data-recording-id="${recording.id}" 
                  data-recording-name="${recording.id}">
            <span class="icon"><i class="material-icons">play_arrow</i></span>
            <span>Play</span>
          </button>
        </div>
      </div>
    `;
  });
  
  modal.innerHTML = `
    <div class="modal-background"></div>
    <div class="modal-card">
      <header class="modal-card-head">
        <p class="modal-card-title">Recordings (${recordings.length})</p>
        <button class="delete" aria-label="close"></button>
      </header>
      <section class="modal-card-body">
        ${recordingsHtml}
      </section>
    </div>
  `;
  
  document.body.appendChild(modal);
  
  // Close modal handlers
  const closeModal = () => {
    document.body.removeChild(modal);
  };
  
  modal.querySelector('.modal-background').onclick = closeModal;
  modal.querySelector('.delete').onclick = closeModal;
  document.addEventListener('keydown', function escHandler(e) {
    if (e.key === 'Escape') {
      closeModal();
      document.removeEventListener('keydown', escHandler);
    }
  });
  
  // Add click handlers for play buttons
  modal.querySelectorAll('.play-recording-btn').forEach(btn => {
    btn.addEventListener('click', function() {
      const recordingId = this.dataset.recordingId;
      const recordingName = this.dataset.recordingName;
      
      // Use backend proxy endpoint to avoid CORS issues
      const recordingUrl = `/api/recordings/${recordingId}?account=default`;
      
      console.log('Playing recording:', recordingUrl);
      playRecording(recordingUrl, recordingName);
    });
  });
}


// Play recording in a modal with waveform 
function playRecording(url, fileName) {
  console.log('Playing recording:', url);

  if (!url) {
    console.error('Invalid recording URL');
    alert('Error: Invalid recording URL');
    return;
  }

  // Use provided fileName or extract from URL
  if (!fileName) {
    fileName = url.split('/').pop().split('?')[0];
  }

  // FIX 2: Create HTML5 Audio FIRST for instant playback (0.2-0.5s start)
  const audio = new Audio(url);
  audio.preload = 'metadata';
  
  // Create modal
  const modal = document.createElement('div');
  modal.className = 'modal is-active recording-modal';
  modal.innerHTML = `
    <div class="modal-background"></div>
    <div class="modal-card">
      <header class="modal-card-head font-size-xs">
        <p class="modal-card-title font-size-0.5rem">Recording: ${fileName}</p>
        <button class="delete" aria-label="close"></button>
      </header>
      <section class="modal-card-body">
        <div id="waveform" style="margin-bottom:0.5rem;"></div>
        <div class="compact-player-controls">
          <button class="button is-small" id="rewBtn" title="Rewind 10s"><i class="material-icons">fast_rewind</i></button>
          <button class="button is-primary compact-play-btn" style="border-radius:50%;" id="playBtn" title="Play"><i class="material-icons" id="playIcon">play_arrow</i></button>
          <button class="button is-small" id="fwdBtn" title="Forward 10s"><i class="material-icons">fast_forward</i></button>
          <span id="timeLbl" style="font-size:0.8rem;">0:00 / --:--</span>
          <button class="button is-small is-light" id="speedBtn" title="Switch speed">1x</button>
        </div>
        <div style="position:absolute; bottom:10px; right:5px;" title="Download Recording">
          <a id="downloadBtn" href="${url}" download="${fileName.endsWith('.mp3') ? fileName : fileName + '.mp3'}"><i class="material-icons">file_download</i></a>
        </div>
      </section>
    </div>
  `;
  document.body.appendChild(modal);

  // FIX 6: WaveSurfer should already be loaded at page load
  if (window.WaveSurfer) {
    initPlayer();
  } else {
    // Fallback: load dynamically if not pre-loaded
    const script = document.createElement('script');
    script.src = "https://unpkg.com/wavesurfer.js@7/dist/wavesurfer.min.js";
    script.onload = () => initPlayer();
    document.head.appendChild(script);
  }

  function initPlayer() {
    // FIX 3: Use MediaElement backend with existing audio element
    const wavesurfer = WaveSurfer.create({
      container: '#waveform',
      waveColor: '#90caf9',
      progressColor: '#1976d2',
      cursorColor: '#ef5350',
      barWidth: 2,
      responsive: true,
      backend: 'MediaElement',
      media: audio, // Attach to existing audio element
      height: 60,
    });

    const playBtn = document.getElementById('playBtn');
    const rewBtn = document.getElementById('rewBtn');
    const fwdBtn = document.getElementById('fwdBtn');
    const speedBtn = document.getElementById('speedBtn');
    const timeLbl = document.getElementById('timeLbl');

    // Play/pause
    playBtn.onclick = () => {
      wavesurfer.playPause();
      document.getElementById('playIcon').textContent =
        wavesurfer.isPlaying() ? 'pause' : 'play_arrow';
    };

    // Rewind/forward
    rewBtn.onclick = () => wavesurfer.skip(-10);
    fwdBtn.onclick = () => wavesurfer.skip(10);

    // Speed control
    let speeds = [1, 1.25, 1.5, 2];
    speedBtn.onclick = () => {
      const cur = wavesurfer.getPlaybackRate();
      const idx = (speeds.indexOf(cur) + 1) % speeds.length;
      const next = speeds[idx];
      wavesurfer.setPlaybackRate(next);
      speedBtn.textContent = next + 'x';
    };

    // Update time label
    wavesurfer.on('audioprocess', () => {
      const cur = wavesurfer.getCurrentTime();
      const dur = wavesurfer.getDuration();
      timeLbl.textContent = `${fmt(cur)} / ${dur ? fmt(dur) : '--:--'}`;
    });
    wavesurfer.on('ready', () => {
      const dur = wavesurfer.getDuration();
      timeLbl.textContent = `0:00 / ${fmt(dur)}`;
    });

    // ✅ Closing modal (X, background, Esc)
    const closeModal = () => {
      wavesurfer.destroy();
      audio.pause();
      audio.src = '';
      document.body.removeChild(modal);
    };
    modal.querySelector('.modal-background').onclick = closeModal;
    modal.querySelector('.delete').onclick = closeModal;
    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') closeModal();
    });
  }

  function fmt(sec) {
    if (!isFinite(sec)) return '--:--';
    const m = Math.floor(sec / 60);
    const s = Math.floor(sec % 60).toString().padStart(2, '0');
    return `${m}:${s}`;
  }
}


// Fetch data from API
async function fetchData(params) {
  // Validate required parameters
  if (!params.start || !params.end) {
    showError('Start and end dates are required');
    return;
  }

  // Convert start/end to unix timestamps (seconds)
  const fromTs = Math.floor(new Date(params.start).getTime() / 1000);
  const toTs   = Math.floor(new Date(params.end).getTime() / 1000);

  // Load Queue/Campaign dropdown dynamically
  loadQueueCampaignDropdown(fromTs, toTs);
  loadAgentDispositionDropdown(fromTs, toTs);


  try {
    // Reset state for new query
    state.currentResults = [];
    state.progressiveLoading.active = false;
    state.progressiveLoading.queryId = null;
    state.progressiveLoading.currentPage = 1;
    state.progressiveLoading.loadedRecords = 0;
    state.progressiveLoading.isComplete = false;
    
    // Show user feedback that request is in progress
    elements.stats.textContent = 'Initializing query...';
    elements.stats.classList.remove('is-hidden');
    
    // Clear any existing timeouts
    if (state.timeoutIds && state.timeoutIds.length) {
      state.timeoutIds.forEach(id => clearTimeout(id));
      state.timeoutIds = [];
    }
    
    // Set timeouts for progressive user feedback
    state.timeoutIds.push(setTimeout(() => {
      if (!state.progressiveLoading.active) {
        elements.stats.textContent = 'Request is taking longer than expected. Please wait...';
      }
    }, 5000));
    
    // Initialize the progressive loading query
    console.log('Initializing progressive loading query');
    const initResponse = await axios.post('/api/reports/progressive/init', params);
    
    if (!initResponse.data || !initResponse.data.success) {
      throw new Error(initResponse.data?.error || 'Failed to initialize query');
    }
    
    // Store query information
    state.progressiveLoading.active = true;
    state.progressiveLoading.queryId = initResponse.data.queryId;
    state.progressiveLoading.totalPages = initResponse.data.totalPages;
    state.progressiveLoading.totalRecords = initResponse.data.totalRecords;
    
    // Clear initialization timeouts
    if (state.timeoutIds && state.timeoutIds.length) {
      state.timeoutIds.forEach(id => clearTimeout(id));
      state.timeoutIds = [];
    }
    
    // Update stats with total record count
    elements.stats.textContent = `Found ${initResponse.data.totalRecords} records. Loading data...`;
    
    // Start loading pages
    await loadNextPage();
    
    // Save last search params
    state.lastSearchParams = params;
  } catch (error) {
    console.error('Error fetching data:', error);
    
    // Clear all timeout IDs if they exist
    if (state.timeoutIds && state.timeoutIds.length) {
      state.timeoutIds.forEach(id => clearTimeout(id));
      state.timeoutIds = [];
    }
    
    // Reset progressive loading state
    state.progressiveLoading.active = false;
    
    // Provide more specific error messages based on error type
    let errorMessage = 'Failed to fetch data';
    
    if (error.response) {
      // The request was made and the server responded with a status code
      // that falls out of the range of 2xx
      if (error.response.status === 400) {
        errorMessage = 'Invalid request: ' + (error.response.data.error || 'Please check your inputs');
      } else if (error.response.status === 404) {
        errorMessage = 'API endpoint not found. Server may need to be restarted.';
      } else if (error.response.status === 500) {
        errorMessage = 'Server error: ' + (error.response.data.error || 'Internal server error');
      } else if (error.response.status === 504) {
        errorMessage = 'Request timed out. The dataset may be too large. Try a smaller date range or more specific filters.';
        
        // Suggest using more filters for large datasets
        errorMessage += '\n\nTry adding more filters such as agent name, queue name, or record type to reduce the result size.';
      }
      
      // Add request ID if available for troubleshooting
      if (error.response.data && error.response.data.request_id) {
        errorMessage += `\n\nRequest ID: ${error.response.data.request_id}`;
      }
    } else if (error.request) {
      // The request was made but no response was received
      errorMessage = 'No response received from server. Please check your network connection.';
    } else {
      // Something happened in setting up the request that triggered an Error
      errorMessage = 'Error: ' + error.message;
    }
    
    showError(errorMessage);
    elements.resultTable.innerHTML = '';
    elements.csvBtn.disabled = true;
  } finally {
    toggleLoading(false);
  }
}

// Function to load the next pages of results in parallel with improved performance
async function loadNextPage() {
  if (!state.progressiveLoading.active || state.progressiveLoading.isComplete) {
    return;
  }
  
  try {
    // Update status message with more detailed information
    const percentComplete = Math.round((state.progressiveLoading.loadedRecords / state.progressiveLoading.totalRecords) * 100);
    elements.stats.textContent = `Loading data: ${state.progressiveLoading.loadedRecords.toLocaleString()} of ${state.progressiveLoading.totalRecords.toLocaleString()} records (${percentComplete}%)`;
    
    // Determine how many pages to load in parallel (optimized for performance)
    const parallelPages = 10; // Increased to 10 pages at once for faster loading
    const pagesToLoad = [];
    
    // Prepare multiple page requests
    for (let i = 0; i < parallelPages; i++) {
      const pageToLoad = state.progressiveLoading.currentPage + i;
      if (pageToLoad <= state.progressiveLoading.totalPages) {
        pagesToLoad.push(pageToLoad);
      }
    }
    
    if (pagesToLoad.length === 0) {
      // No more pages to load
      state.progressiveLoading.isComplete = true;
      finishProgressiveLoading();
      return;
    }
    
    // Create an array of promises for each page request
    const pagePromises = pagesToLoad.map(page => {
      return axios.get('/api/reports/progressive', {
        params: {
          queryId: state.progressiveLoading.queryId,
          page: page
        }
      });
    });
    
    // Wait for all page requests to complete
    const responses = await Promise.all(pagePromises);
    
    // Process all responses
    let allNewRecords = [];
    let lastPageReached = false;
    
    responses.forEach((response, index) => {
      if (!response.data || !response.data.success) {
        console.error(`Error loading page ${pagesToLoad[index]}:`, response.data?.error || 'Unknown error');
        return;
      }
      
      // Add records from this page
      allNewRecords = [...allNewRecords, ...response.data.data];
      
      // Check if this was the last page
      if (response.data.isLastPage) {
        lastPageReached = true;
      }
    });
    
    // Add all new records to our results
    state.currentResults = [...state.currentResults, ...allNewRecords];
    state.progressiveLoading.loadedRecords += allNewRecords.length;
    
    // Update the table with all records loaded so far
    // Always render the table for the first batch to ensure users see data immediately
    // For subsequent batches, only render every 5000 records or on the final batch for performance
    if (state.currentResults.length <= 1000 || state.currentResults.length % 5000 < 1000 || lastPageReached || state.progressiveLoading.isComplete) {
      renderTable(state.currentResults);
    }
    
    // Enable CSV download as soon as we have some results
    elements.csvBtn.disabled = state.currentResults.length === 0;
    
    // Check if we're done or need to load more pages
    if (lastPageReached || pagesToLoad[pagesToLoad.length - 1] >= state.progressiveLoading.totalPages) {
      state.progressiveLoading.isComplete = true;
      finishProgressiveLoading();
    } else {
      // Move to the next batch of pages
      state.progressiveLoading.currentPage += parallelPages;
      
      // Load the next batch of pages immediately
      // Use setTimeout with 0ms to allow UI updates between batches
      setTimeout(() => loadNextPage(), 0);
    }
  } catch (error) {
    console.error('Error loading data pages:', error);
    showError('Error loading data: ' + (error.message || 'Unknown error'));
    state.progressiveLoading.active = false;
  }
}

// Function to finalize progressive loading
function finishProgressiveLoading() {
  // Update stats display with final details
  let statsText = `Loaded ${state.progressiveLoading.loadedRecords} records`;
  
  // Count record types
  const typeCounts = {
    Campaign: 0,
    Inbound: 0,
    Outbound: 0
  };
  
  state.currentResults.forEach(record => {
    if (record.record_type && typeCounts[record.record_type] !== undefined) {
      typeCounts[record.record_type]++;
    }
  });
  
  statsText += ` (${typeCounts.Campaign} Campaign, ${typeCounts.Inbound} Inbound, ${typeCounts.Outbound} Outbound)`;
  
  elements.stats.textContent = statsText;
  elements.stats.classList.remove('is-hidden');
  
  // Reset progressive loading state
  state.progressiveLoading.active = false;
}

// Render table with data using infinite scrolling for better performance
function renderTable(data) {
  // Clear existing table content
  elements.resultTable.innerHTML = '';
  
  if (data.length === 0) {
    elements.resultTable.innerHTML = '<tr><td colspan="26" class="has-text-centered">No records found</td></tr>';
    return;
  }
  
  // Create table header
  const thead = createTableHeaders();
  elements.resultTable.appendChild(thead);
  
  // Create table body
  const tbody = document.createElement('tbody');
  tbody.id = 'resultTableBody';
  elements.resultTable.appendChild(tbody);
  
  // Determine how many rows to render initially (first 1000 records)
  const initialBatchSize = Math.min(1000, data.length);
  const initialRows = data.slice(0, initialBatchSize);
  
  // Render initial batch of rows
  appendTableRows(tbody, initialRows, 0);
  
  // FIX 4: Disabled - prefetching downloads full recordings which is slow
  // prefetchRecordings(initialRows);
  
  // Set up infinite scrolling
  if (data.length > initialBatchSize) {
    // Add a small loading indicator at the bottom that's always visible
    const loadingIndicator = document.createElement('div');
    loadingIndicator.id = 'scroll-loading-indicator';
    loadingIndicator.className = 'has-text-centered p-4';
    loadingIndicator.innerHTML = '<div class="loader is-loading"></div>';
    loadingIndicator.style.display = 'none';
    document.querySelector('.table-container').appendChild(loadingIndicator);
    
    // Track loading state
    let isLoading = false;
    let currentlyLoadedCount = initialBatchSize;
    
    // Function to load more data when scrolling near the bottom
    const loadMoreOnScroll = () => {
      if (isLoading || currentlyLoadedCount >= data.length) return;
      
      const tableContainer = document.querySelector('.table-container');
      const scrollPosition = tableContainer.scrollTop + tableContainer.clientHeight;
      const scrollThreshold = tableContainer.scrollHeight - 200; // Load more when within 200px of bottom
      
      if (scrollPosition >= scrollThreshold) {
        isLoading = true;
        loadingIndicator.style.display = 'block';
        
        // Use setTimeout to prevent UI freezing when loading large batches
        setTimeout(() => {
          const nextBatchSize = Math.min(500, data.length - currentlyLoadedCount);
          const nextRows = data.slice(currentlyLoadedCount, currentlyLoadedCount + nextBatchSize);
          
          // Append the next batch of rows
          appendTableRows(tbody, nextRows, currentlyLoadedCount);
          
          // Update counters
          currentlyLoadedCount += nextBatchSize;
          isLoading = false;
          
          // Hide loading indicator if we've loaded all data
          if (currentlyLoadedCount >= data.length) {
            loadingIndicator.style.display = 'none';
          }
        }, 10);
      }
    };
    
    // Attach scroll event listener to the table container
    const tableContainer = document.querySelector('.table-container');
    tableContainer.addEventListener('scroll', loadMoreOnScroll);
  }
}

// Background pre-fetch recordings to warm cache (runs silently)
function prefetchRecordings(rows) {
  // Get first 10 rows that have call_id and called_time
  const rowsWithRecordings = rows
    .filter(r => r.call_id && r.called_time && r.recording)
    .slice(0, 10);
  
  if (rowsWithRecordings.length === 0) return;
  
  console.log(`🔄 Pre-fetching ${rowsWithRecordings.length} recordings in background...`);
  
  // Fetch one at a time to avoid overwhelming the server
  let index = 0;
  const fetchNext = () => {
    if (index >= rowsWithRecordings.length) {
      console.log('✅ Background pre-fetch complete');
      return;
    }
    
    const row = rowsWithRecordings[index];
    fetchRecordingsByCallId(row.call_id, row.called_time)
      .then(() => console.log(`⚡ Pre-cached: ${row.call_id}`))
      .catch(() => {}) // Silently ignore errors
      .finally(() => {
        index++;
        // Delay 500ms between requests to not overwhelm server
        setTimeout(fetchNext, 500);
      });
  };
  
  // Start pre-fetching after 2 second delay (let page load first)
  setTimeout(fetchNext, 2000);
}

// Helper function to append rows to the table body
function appendTableRows(tbody, rows, startIndex = 0) {
  // Create a document fragment to batch DOM operations
  const fragment = document.createDocumentFragment();
  
  rows.forEach((row, index) => {
    const tr = document.createElement('tr');
    
    // Add class based on record type for styling
    const recordTypeClass = row.record_type ? `row-${row.record_type.toLowerCase()}` : '';
    if (recordTypeClass) {
      tr.classList.add(recordTypeClass);
    }
    
    // Calculate the serial number for this row
    const serialNumber = startIndex + index + 1;
    
    // Define columns with special handling for HTML content
    const columns = [
      { value: serialNumber.toString(), isHTML: false }, // S.No. column
      { value: row.record_type || '', isHTML: false },
      { value: row.agent_name || '', isHTML: false },
      { value: row.extension || '', isHTML: false },
      { value: row.queue_campaign_name || '', isHTML: false },
      { value: formatDate(row.called_time, row.called_time_formatted), isHTML: false },
      { value: row.caller_id_number, isHTML: false },
      { value: row.callee_id_number, isHTML: false },
      { value: (row.answered_time_formatted && row.answered_time_formatted !== 'undefined') ? row.answered_time_formatted : (row.answered_time === null ? '' : row.answered_time), isHTML: false },
      { value: (row.hangup_time_formatted && row.hangup_time_formatted !== 'undefined') ? row.hangup_time_formatted : (row.hangup_time === null ? '' : row.hangup_time), isHTML: false },
      { value: (row.wait_duration_formatted && row.wait_duration_formatted !== 'undefined') ? row.wait_duration_formatted : (row.wait_duration === null ? '' : row.wait_duration), isHTML: false },
      { value: (row.talk_duration_formatted && row.talk_duration_formatted !== 'undefined') ? row.talk_duration_formatted : (row.talk_duration === null ? '' : row.talk_duration), isHTML: false },
      { value: (row.hold_duration_formatted && row.hold_duration_formatted !== 'undefined') ? row.hold_duration_formatted : (row.hold_duration === null ? '' : row.hold_duration), isHTML: false },
      { value: row.hold_duration_intervals ? row.hold_duration_intervals.replace(/\n/g, '<br>') : '', isHTML: true },
      { value: row.agent_disposition || '', isHTML: false },
      { value: row.sub_disp_1 || '', isHTML: false },
      { value: row.sub_disp_2 || '', isHTML: false },
      { value: row.follow_up_notes || '', isHTML: false },  // Plain text field
      { value: row.agent_hangup || '', isHTML: false },
      { value: row.status || '', isHTML: false },
      { value: row.campaign_type || '', isHTML: false },
      { value: row.abandoned || '', isHTML: false },
      { value: row.country || '', isHTML: false },
      { value: row.transfer_event === 1 || row.transfer_event === true ? 'Yes' : 'No', isHTML: false },  // Transfer column
      { value: row.transfer_extension || '', isHTML: false },  // Transfer to column
      { value: row.transfer_type || '', isHTML: false },  // Transfer type column
      { value: processHistoryData(row.agent_history, 'agent'), isHTML: true },  // HTML content with eye button
      { value: processHistoryData(row.record_type === 'Campaign' ? (row.lead_history || row.queue_history) : row.queue_history, 'queue'), isHTML: true },   // HTML content with eye button - use lead_history for campaigns
      { value: row.recording ? createRecordingLink(row.recording) : '', isHTML: false, isElement: true },  // Recording button - plays single recording directly
      { value: (row.call_id && row.called_time) ? createAllRecordingsButton(row.call_id, row.called_time) : '', isHTML: false, isElement: true },  // All Recordings button - fetches all via API
      { value: row.call_id || '', isHTML: false },
      { value: row.system_disposition || row.disposition || '', isHTML: false },  // System Disposition column
    ];
    
    // Add each cell to the row
    columns.forEach(column => {
      const td = document.createElement('td');
      
      if (column.isHTML) {
        // For HTML content, set innerHTML directly
        td.innerHTML = column.value;
        
        // Initialize any eye buttons after rendering
        setTimeout(() => {
          initializeEyeButtons(td);
        }, 0);
      } else if (column.isElement && column.value instanceof Node) {
        // For DOM elements like the recording button
        td.appendChild(column.value);
      } else if (typeof column.value === 'string') {
        td.innerHTML = column.value;
      } else if (column.value instanceof Node) {
        td.appendChild(column.value);
      } else {
        // Handle non-Node objects
        td.textContent = column.value ? column.value.toString() : '';
      }
      
      tr.appendChild(td);
    });
    
    fragment.appendChild(tr);
  });
  
  // Append all rows at once for better performance
  tbody.appendChild(fragment);
  
  // Initialize eye buttons in the entire table
  setTimeout(() => {
    initializeEyeButtons(tbody);
  }, 0);
}

// This duplicate function has been removed and consolidated with the one above

// Convert Agent history array into an HTML table for display
function historyToHtml(history) {
  if (!Array.isArray(history) || !history.length) return '';

  // Ensure ascending order by last_attempt (oldest first)
  const sorted = [...history].sort((a, b) => {
    const aTs = a.last_attempt ?? 0;
    const bTs = b.last_attempt ?? 0;
    return aTs - bTs;
  });

  // Define the desired column order & headers
  const COLS = [
    { key: 'last_attempt', label: 'Last Attempt' },
    { key: 'name', label: 'Name' },
    { key: 'ext', label: 'Extension' },
    { key: 'type', label: 'Type' },
    { key: 'event', label: 'Event' },
    { key: 'connected', label: 'Connected' },
    { key: 'queue_name', label: 'Queue Name' }
  ];

  const thead = `<thead><tr>${COLS.map(c => `<th>${c.label}</th>`).join('')}</tr></thead>`;

  const rows = sorted.map(h => {
    const cells = COLS.map(c => {
      let val = '';
  
      if (c.key === 'last_attempt') {
        if (h.last_attempt) {
          const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
          val = formatDate(ms);
          return `<td><span class="history-date">${val}</span></td>`;
        }
        return `<td></td>`;
      }
  
      if (c.key === 'name') {
        val = `${h.first_name || ''} ${h.last_name || ''}`.trim();
        return `<td><span class="history-name">${val}</span></td>`;
      }
  
      if (c.key === 'ext') {
        return `<td><span class="history-ext">${h[c.key] ?? ''}</span></td>`;
      }
  
      if (c.key === 'type') {
        return `<td><span class="history-type">${h[c.key] ?? ''}</span></td>`;
      }
  
      if (c.key === 'event') {
        return `<td><span class="history-event">${h[c.key] ?? ''}</span></td>`;
      }
  
      if (c.key === 'connected') {
        const cls = h.connected ? 'history-yes' : 'history-no';
        val = h.connected ? 'Yes' : 'No';
        return `<td><span class="${cls}">${val}</span></td>`;
      }
  
      if (c.key === 'queue_name') {
        return `<td><span class="history-queue">${h[c.key] ?? ''}</span></td>`;
      }
  
      // Default for any other fields
      val = h[c.key] ?? '';
      return `<td>${val}</td>`;
    }).join('');
  
    return `<tr>${cells}</tr>`;
  }).join('');
  

  const tableHtml = `<div class="modal-card-body"><table class="history-table">${thead}<tbody>${rows}</tbody></table></div>`;
  return `<button class="button is-small is-rounded eye-btn" onclick="showHistoryModal(this)" data-history-type="agent" title="View Agent History">👁️</button>
         <div class="history-data" style="display:none">${tableHtml}</div>`;
}

// Convert Queue history array into an HTML table for display
function queueHistoryToHtml(history) {
  if (!Array.isArray(history) || !history.length) return '';
  const thead = '<thead><tr><th>Date</th><th>Queue Name</th></tr></thead>';
  const rows = history.map(h => {
    let date = '';
    if (h.ts) {
      const ms = h.ts > 10_000_000_000 ? h.ts : h.ts * 1000;
      date = formatDate(ms);
    }
    const q = h.queue_name ?? '';
    // Use direct inline styles with !important to override any other styles
    return `<tr>
      <td><span class="history-date">${date}</span></td>
      <td><span class="history-queue">${q}</span></td>
    </tr>`;
  }).join('');
  const tableHtml = `<div class="modal-card-body"><table class="history-table" style="width: 100%;">${thead}<tbody>${rows}</tbody></table></div>`;
  return `<button class="button is-small is-rounded eye-btn" onclick="showHistoryModal(this)" data-history-type="queue" title="View Queue History">👁️</button>
         <div class="history-data" style="display:none">${tableHtml}</div>`;
}

// Function to safely handle HTML content
function safeHtml(html) {
  if (!html) return '';
  // Check if the content contains HTML tags (specifically looking for eye-btn which is common in our HTML content)
  if (typeof html === 'string' && (html.includes('<button') || html.includes('eye-btn'))) {
    return html; // Return as-is if it contains HTML
  } else {
    // For plain text, escape any HTML characters
    return html;
  }
}

// Convert agent history to clean text format for CSV export
function agentHistoryToText(history) {
  if (!Array.isArray(history) || !history.length) return '';

  // Sort by last_attempt (oldest first)
  const sorted = [...history].sort((a, b) => {
    const aTs = a.last_attempt ?? 0;
    const bTs = b.last_attempt ?? 0;
    return aTs - bTs;
  });

  // Create text representation
  const lines = sorted.map(h => {
    const parts = [];
    
    // Date
    if (h.last_attempt) {
      const ms = h.last_attempt > 10_000_000_000 ? h.last_attempt : h.last_attempt * 1000;
      parts.push(`Date: ${formatDate(ms)}`);
    }
    
    // Name
    const name = `${h.first_name || ''} ${h.last_name || ''}`.trim();
    if (name) parts.push(`Name: ${name}`);
    
    // Extension
    if (h.ext) parts.push(`Ext: ${h.ext}`);
    
    // Type
    if (h.type) parts.push(`Type: ${h.type}`);
    
    // Event
    if (h.event) parts.push(`Event: ${h.event}`);
    
    // Connected
    if (h.connected !== undefined) parts.push(`Connected: ${h.connected ? 'Yes' : 'No'}`);
    
    // Queue Name
    if (h.queue_name) parts.push(`Queue: ${h.queue_name}`);
    
    return parts.join(' | ');
  });

  return lines.join(' || ');
}

// Convert queue history to clean text format for CSV export
function queueHistoryToText(history) {
  if (!Array.isArray(history) || !history.length) return '';

  const lines = history.map(h => {
    const parts = [];
    
    // Date
    if (h.ts) {
      const ms = h.ts > 10_000_000_000 ? h.ts : h.ts * 1000;
      parts.push(`Date: ${formatDate(ms)}`);
    }
    
    // Queue Name
    if (h.queue_name) parts.push(`Queue: ${h.queue_name}`);
    
    return parts.join(' | ');
  });

  return lines.join(' || ');
}

// Convert history data to clean text format for CSV export
function historyToText(historyData, type) {
  if (!historyData) return '';
  
  let parsedHistory = historyData;
  
  // If it's a string, try to parse it
  if (typeof historyData === 'string') {
    // If it contains HTML, try to extract the original JSON data
    if (historyData.includes('eye-btn') && historyData.includes('history-data')) {
      // Try to extract JSON from the HTML structure
      try {
        // Look for JSON data in the HTML - it might be in a data attribute or script tag
        const jsonMatch = historyData.match(/data-history="([^"]+)"/);
        if (jsonMatch) {
          parsedHistory = JSON.parse(decodeURIComponent(jsonMatch[1]));
        } else {
          // If we can't extract the original data, return a simplified message
          return 'View in web interface for detailed history';
        }
      } catch (e) {
        return 'View in web interface for detailed history';
      }
    } else {
      // Try to parse as JSON
      try {
        parsedHistory = JSON.parse(historyData);
      } catch (e) {
        // If not valid JSON, check if it's already formatted text
        if (historyData.includes('Date:') || historyData.includes('Name:') || historyData.includes('Queue:')) {
          return historyData; // Already formatted text
        }
        return historyData; // Return as-is
      }
    }
  }
  
  // If it's an array, convert to text
  if (Array.isArray(parsedHistory)) {
    return type === 'agent' ? agentHistoryToText(parsedHistory) : queueHistoryToText(parsedHistory);
  }
  
  return historyData;
}

// Get history text for CSV export by trying multiple data sources
function getHistoryTextForCSV(row, type) {
  console.log(`DEBUG: getHistoryTextForCSV called for ${type} history, record_type: ${row.record_type}, call_id: ${row.call_id}`);
  
  // For campaigns, try to access original array data first
  if (row.record_type === 'Campaign') {
    console.log('DEBUG: Campaign record detected, checking for original array data');
    
    // Try to access original agent_history or lead_history arrays
    if (type === 'agent' && row.agent_history_array) {
      console.log('DEBUG: Found agent_history_array for campaign');
      return agentHistoryToText(row.agent_history_array);
    }
    
    if (type === 'queue' && row.lead_history_array) {
      console.log('DEBUG: Found lead_history_array for campaign');
      return queueHistoryToText(row.lead_history_array);
    }
    
    // Try alternative field names for campaigns
    if (type === 'agent' && row.agent_history && Array.isArray(row.agent_history)) {
      console.log('DEBUG: Found agent_history array for campaign');
      return agentHistoryToText(row.agent_history);
    }
    
    // For campaigns, queue history is stored in lead_history field
    if (type === 'queue' && row.lead_history && Array.isArray(row.lead_history)) {
      console.log('DEBUG: Found lead_history array for campaign');
      return queueHistoryToText(row.lead_history);
    }
    
    // Also check if queue_history exists for campaigns (fallback)
    if (type === 'queue' && row.queue_history && Array.isArray(row.queue_history)) {
      console.log('DEBUG: Found queue_history array for campaign');
      return queueHistoryToText(row.queue_history);
    }
  }
  
  // First, try to get original data from raw fields if available
  const rawField = type === 'agent' ? 'agent_history_raw' : 'queue_history_raw';
  if (row[rawField]) {
    console.log(`DEBUG: Found raw field ${rawField}`);
    try {
      const parsed = typeof row[rawField] === 'string' ? JSON.parse(row[rawField]) : row[rawField];
      if (Array.isArray(parsed)) {
        const result = type === 'agent' ? agentHistoryToText(parsed) : queueHistoryToText(parsed);
        console.log(`DEBUG: Extracted from raw field:`, result);
        return result;
      }
    } catch (e) {
      console.log(`DEBUG: Failed to parse raw field:`, e);
      // Continue to next method
    }
  }
  
  // Second, try to extract from HTML content
  // For campaigns, queue history might be in lead_history field
  let htmlField = type === 'agent' ? 'agent_history' : 'queue_history';
  let htmlContent = row[htmlField];
  
  // For campaigns, if queue_history is empty, try lead_history
  if (type === 'queue' && row.record_type === 'Campaign' && (!htmlContent || htmlContent === '')) {
    htmlField = 'lead_history';
    htmlContent = row[htmlField];
    console.log('DEBUG: Using lead_history for campaign queue history');
  }
  
  console.log(`DEBUG: HTML field ${htmlField} content length:`, htmlContent ? htmlContent.length : 0);
  console.log(`DEBUG: HTML content preview:`, htmlContent ? htmlContent.substring(0, 100) + '...' : 'empty');
  
  if (!htmlContent) {
    console.log(`DEBUG: No HTML content found for ${htmlField}`);
    return '';
  }
  
  // Special handling for campaign reports where agent_history and queue_history might be the same combined HTML
  if (row.record_type === 'Campaign' && type === 'agent') {
    console.log('DEBUG: Processing campaign agent history');
    // For campaign agent history, try to extract only agent-related tables from combined HTML
    if (typeof htmlContent === 'string' && htmlContent.includes('<table')) {
      const result = extractCampaignAgentHistoryFromHTML(htmlContent);
      console.log('DEBUG: Campaign agent history result:', result);
      return result;
    }
  } else if (row.record_type === 'Campaign' && type === 'queue') {
    console.log('DEBUG: Processing campaign queue history');
    // For campaign queue history, try to extract lead history from combined HTML
    if (typeof htmlContent === 'string' && htmlContent.includes('<table')) {
      const result = extractCampaignLeadHistoryFromHTML(htmlContent);
      console.log('DEBUG: Campaign queue history result:', result);
      return result;
    }
  }
  
  // If it's HTML content, try to extract table data
  if (typeof htmlContent === 'string' && htmlContent.includes('<table')) {
    console.log('DEBUG: Processing generic HTML table');
    const result = extractTextFromHistoryHTML(htmlContent, type);
    console.log('DEBUG: Generic HTML result:', result);
    return result;
  }
  
  // Fall back to the original historyToText function
  console.log('DEBUG: Using fallback historyToText');
  const result = historyToText(htmlContent, type);
  console.log('DEBUG: Fallback result:', result);
  return result;
}

// Extract campaign agent history from combined HTML content
function extractCampaignAgentHistoryFromHTML(htmlContent) {
  try {
    console.log('DEBUG: Extracting campaign agent history from HTML:', htmlContent.substring(0, 200) + '...');
    
    // Create a temporary DOM element to parse the HTML
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = htmlContent;
    
    // Find all tables - the first one is usually agent history
    const tables = tempDiv.querySelectorAll('table');
    console.log('DEBUG: Found', tables.length, 'tables in campaign HTML');
    
    if (!tables.length) return 'No agent history data available';
    
    // Try each table to find agent history
    for (let i = 0; i < tables.length; i++) {
      const table = tables[i];
      console.log(`DEBUG: Processing table ${i + 1}`);
      
      const headerRow = table.querySelector('thead tr');
      if (headerRow) {
        const headers = Array.from(headerRow.querySelectorAll('th')).map(th => th.textContent.trim().toLowerCase());
        console.log(`DEBUG: Table ${i + 1} headers:`, headers);
        
        // Check if this looks like agent history table
        // Campaign tables have: 'first name', 'last name', 'extension/number', 'event'
        if ((headers.includes('first name') || headers.includes('name')) && 
            (headers.includes('extension/number') || headers.includes('extension')) && 
            headers.includes('event')) {
          console.log(`DEBUG: Found agent history table at index ${i + 1}`);
          const rows = table.querySelectorAll('tbody tr');
          if (!rows.length) continue;
          
          const textLines = [];
          rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 5) {
              const parts = [];
              // Campaign table structure: ['last attempt', 'first name', 'last name', 'extension/number', 'event', 'hangup cause']
              if (cells[0].textContent.trim()) parts.push(`Date: ${cells[0].textContent.trim()}`);
              
              // Combine first name and last name
              const firstName = cells[1] ? cells[1].textContent.trim() : '';
              const lastName = cells[2] ? cells[2].textContent.trim() : '';
              const fullName = `${firstName} ${lastName}`.trim();
              if (fullName) parts.push(`Name: ${fullName}`);
              
              if (cells[3] && cells[3].textContent.trim()) parts.push(`Ext: ${cells[3].textContent.trim()}`);
              if (cells[4] && cells[4].textContent.trim()) parts.push(`Event: ${cells[4].textContent.trim()}`);
              if (cells.length > 5 && cells[5] && cells[5].textContent.trim()) parts.push(`Hangup Cause: ${cells[5].textContent.trim()}`);
              
              if (parts.length > 0) {
                textLines.push(parts.join(' | '));
              }
            }
          });
          
          if (textLines.length > 0) {
            console.log('DEBUG: Successfully extracted agent history:', textLines.join(' || '));
            return textLines.join(' || ');
          }
        }
      } else {
        // Try tables without headers - just extract all data from first table
        console.log(`DEBUG: Table ${i + 1} has no headers, trying direct extraction`);
        const rows = table.querySelectorAll('tr');
        if (rows.length > 0) {
          const textLines = [];
          rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 4) { // Minimum 4 columns for meaningful data
              const parts = [];
              for (let j = 0; j < Math.min(cells.length, 7); j++) {
                const cellText = cells[j].textContent.trim();
                if (cellText) {
                  switch (j) {
                    case 0: parts.push(`Date: ${cellText}`); break;
                    case 1: parts.push(`Name: ${cellText}`); break;
                    case 2: parts.push(`Ext: ${cellText}`); break;
                    case 3: parts.push(`Type: ${cellText}`); break;
                    case 4: parts.push(`Event: ${cellText}`); break;
                    case 5: parts.push(`Connected: ${cellText}`); break;
                    case 6: parts.push(`Queue: ${cellText}`); break;
                  }
                }
              }
              
              if (parts.length > 0) {
                textLines.push(parts.join(' | '));
              }
            }
          });
          
          if (textLines.length > 0) {
            console.log('DEBUG: Successfully extracted from headerless table:', textLines.join(' || '));
            return textLines.join(' || ');
          }
        }
      }
    }
    
    // Fallback: use the first table if no specific agent history table found
    console.log('DEBUG: No specific agent history table found, using fallback');
    return extractTextFromHistoryHTML(htmlContent, 'agent');
  } catch (e) {
    console.error('Error extracting campaign agent history from HTML:', e);
    return 'Error processing agent history data';
  }
}

// Extract campaign lead history from combined HTML content
function extractCampaignLeadHistoryFromHTML(htmlContent) {
  try {
    // Create a temporary DOM element to parse the HTML
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = htmlContent;
    
    // Find all tables - look for lead history table
    const tables = tempDiv.querySelectorAll('table');
    if (!tables.length) return 'No lead history data available';
    
    // Look for the table with lead history structure (6 columns: Last Attempt, First Name, Last Name, Extension/Number, Event, Hangup Cause)
    for (const table of tables) {
      const headerRow = table.querySelector('thead tr');
      if (headerRow) {
        const headers = Array.from(headerRow.querySelectorAll('th')).map(th => th.textContent.trim().toLowerCase());
        // Check if this looks like lead history table
        if (headers.includes('hangup cause') || headers.includes('first name') || headers.includes('last name')) {
          const rows = table.querySelectorAll('tbody tr');
          if (!rows.length) continue;
          
          const textLines = [];
          rows.forEach(row => {
            const cells = row.querySelectorAll('td');
            if (cells.length >= 5) {
              const parts = [];
              if (cells[0].textContent.trim()) parts.push(`Date: ${cells[0].textContent.trim()}`);
              if (cells[1].textContent.trim()) parts.push(`First Name: ${cells[1].textContent.trim()}`);
              if (cells[2].textContent.trim()) parts.push(`Last Name: ${cells[2].textContent.trim()}`);
              if (cells[3].textContent.trim()) parts.push(`Ext/Number: ${cells[3].textContent.trim()}`);
              if (cells[4].textContent.trim()) parts.push(`Event: ${cells[4].textContent.trim()}`);
              if (cells.length > 5 && cells[5].textContent.trim()) parts.push(`Hangup Cause: ${cells[5].textContent.trim()}`);
              
              if (parts.length > 0) {
                textLines.push(parts.join(' | '));
              }
            }
          });
          
          if (textLines.length > 0) {
            return textLines.join(' || ');
          }
        }
      }
    }
    
    // Fallback: use the second table if available, or first table as queue history
    if (tables.length > 1) {
      return extractTextFromHistoryHTML(tables[1].outerHTML, 'queue');
    }
    
    return 'No lead history data available';
  } catch (e) {
    console.error('Error extracting campaign lead history from HTML:', e);
    return 'Error processing lead history data';
  }
}

// Extract readable text from HTML table content
function extractTextFromHistoryHTML(htmlContent, type) {
  try {
    // Create a temporary DOM element to parse the HTML
    const tempDiv = document.createElement('div');
    tempDiv.innerHTML = htmlContent;
    
    // Find the table
    const table = tempDiv.querySelector('table');
    if (!table) return 'No history data available';
    
    const rows = table.querySelectorAll('tbody tr');
    if (!rows.length) return 'No history data available';
    
    const textLines = [];
    
    if (type === 'agent') {
      // Agent history format: Date | Name | Ext | Type | Event | Connected | Queue
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 7) {
          const parts = [];
          if (cells[0].textContent.trim()) parts.push(`Date: ${cells[0].textContent.trim()}`);
          if (cells[1].textContent.trim()) parts.push(`Name: ${cells[1].textContent.trim()}`);
          if (cells[2].textContent.trim()) parts.push(`Ext: ${cells[2].textContent.trim()}`);
          if (cells[3].textContent.trim()) parts.push(`Type: ${cells[3].textContent.trim()}`);
          if (cells[4].textContent.trim()) parts.push(`Event: ${cells[4].textContent.trim()}`);
          if (cells[5].textContent.trim()) parts.push(`Connected: ${cells[5].textContent.trim()}`);
          if (cells[6].textContent.trim()) parts.push(`Queue: ${cells[6].textContent.trim()}`);
          
          if (parts.length > 0) {
            textLines.push(parts.join(' | '));
          }
        }
      });
    } else {
      // Queue history format: Date | Queue Name
      rows.forEach(row => {
        const cells = row.querySelectorAll('td');
        if (cells.length >= 2) {
          const parts = [];
          if (cells[0].textContent.trim()) parts.push(`Date: ${cells[0].textContent.trim()}`);
          if (cells[1].textContent.trim()) parts.push(`Queue: ${cells[1].textContent.trim()}`);
          
          if (parts.length > 0) {
            textLines.push(parts.join(' | '));
          }
        }
      });
    }
    
    return textLines.join(' || ');
  } catch (e) {
    console.error('Error extracting text from HTML:', e);
    return 'Error processing history data';
  }
}

// Function to show history modal with proper title based on history type
function showHistoryModal(button) {
  const historyType = button.getAttribute('data-history-type');
  const historyData = button.nextElementSibling.innerHTML;
  
  let title = 'History';
  if (historyType === 'agent') title = 'Agent History';
  if (historyType === 'queue') title = 'Queue History';
  if (historyType === 'lead') title = 'Lead History';
  
  const modal = document.createElement('div');
  modal.className = 'modal is-active history-modal';
  modal.innerHTML = `
    <div class="modal-background"></div>
    <div class="modal-card history-modal-card">
      <header class="modal-card-head">
        <p class="modal-card-title">${title}</p>
        <button class="delete" aria-label="close"></button>
      </header>
      ${historyData}
    </div>`;
  
  const closeModal = () => document.body.removeChild(modal);
  
  // Add to document
  document.body.appendChild(modal);
  
  // Add event listeners for closing
  modal.querySelector('.modal-background').addEventListener('click', closeModal);
  modal.querySelector('.delete').addEventListener('click', closeModal);
  try {
    const closeModalBtn = modal.querySelector('.close-modal');
    if (closeModalBtn) {
      closeModalBtn.addEventListener('click', closeModal);
    }
  } catch (e) {
    // Close modal button might not exist
  }
  
  // Escape key handler
  const escHandler = e => {
    if (e.key === 'Escape') {
      closeModal();
      document.removeEventListener('keydown', escHandler);
    }
  };
  document.addEventListener('keydown', escHandler);
}

// Add showHistoryModal to window object so it can be called from inline event handlers
window.showHistoryModal = showHistoryModal;


// Function to initialize eye buttons within a container (legacy support)
function initializeEyeButtons(container) {
  // This is now handled by the onclick attribute directly
  // But we keep this for backward compatibility
  const eyeButtons = container.querySelectorAll('.eye-btn:not([onclick])');
  eyeButtons.forEach(btn => {
    if (!btn.hasAttribute('data-initialized')) {
      btn.setAttribute('data-initialized', 'true');
      btn.addEventListener('click', function(e) {
        e.preventDefault();
        showHistoryModal(this);
      });
    }
  });
  
  // Initialize play buttons - plays single recording directly (instant)
  const playButtons = container.querySelectorAll('.play-btn');
  playButtons.forEach(btn => {
    if (!btn.hasAttribute('data-initialized')) {
      btn.setAttribute('data-initialized', 'true');
      btn.addEventListener('click', function(e) {
        e.preventDefault();
        if (this.dataset.src) {
          console.log('Playing single recording:', this.dataset.src);
          playRecording(this.dataset.src);
        } else {
          alert('No recording available');
        }
      });
    }
  });

  // Initialize "All Recordings" buttons - fetches all recordings via API
  const allRecordingsButtons = container.querySelectorAll('.all-recordings-btn');
  allRecordingsButtons.forEach(btn => {
    if (!btn.hasAttribute('data-initialized')) {
      btn.setAttribute('data-initialized', 'true');
      btn.addEventListener('click', async function(e) {
        e.preventDefault();
        
        const callId = this.dataset.callId;
        const calledTime = this.dataset.calledTime;
        
        if (!callId || !calledTime) {
          alert('Missing call information');
          return;
        }
        
        console.log('Fetching all recordings for call_id:', callId);
        
        // Create blocking overlay
        const overlay = document.createElement('div');
        overlay.id = 'loading-overlay';
        overlay.style.cssText = `
          position: fixed;
          top: 0;
          left: 0;
          width: 100%;
          height: 100%;
          background: rgba(0, 0, 0, 0.7);
          z-index: 9999;
          display: flex;
          flex-direction: column;
          justify-content: center;
          align-items: center;
          cursor: wait;
        `;
        overlay.innerHTML = `
          <div class="loader is-loading" style="width: 50px; height: 50px; border-width: 4px;"></div>
          <p style="color: white; margin-top: 20px; font-size: 18px;">Loading all recordings...</p>
        `;
        document.body.appendChild(overlay);
        
        try {
          this.disabled = true;
          this.innerHTML = '<span class="icon is-small"><i class="material-icons">hourglass_empty</i></span>';
          
          const response = await fetchRecordingsByCallId(callId, calledTime);
          
          overlay.remove();
          this.disabled = false;
          this.innerHTML = '<span class="icon is-small"><i class="material-icons">library_music</i></span>';
          
          if (response && response.recordings && response.recordings.length > 0) {
            showRecordingsModal(response.recordings);
          } else {
            alert('No recordings found for this call');
          }
        } catch (error) {
          overlay.remove();
          this.disabled = false;
          this.innerHTML = '<span class="icon is-small"><i class="material-icons">library_music</i></span>';
          console.error('Error fetching recordings:', error);
          alert('Error fetching recordings: ' + error.message);
        }
      });
    }
  });
}

// Generate CSV from data with optimized memory usage for large datasets
function generateCSV() {
  if (!state.currentResults || state.currentResults.length === 0) {
    showError('No data to export');
    return;
  }
  
  // Show processing message for large datasets
  if (state.currentResults.length > 10000) {
    elements.stats.textContent = `Preparing CSV export for ${state.currentResults.length.toLocaleString()} records. This may take a moment...`;
  }
  
  // Define headers - match exactly with table headers
  const headers = [
    'S.No.',
    'Record Type',
    'Agent Name',
    'Extension',
    'Queue/Campaign',
    'Called Time',
    'Caller ID Number',
    'Callee ID Number',
    'Answered Time ',
    'Hangup Time',
    'Wait Duration ',
    'Talk Duration ',
    'Hold Duration ',
    'Hold Duration Intervals',
    'Agent Disposition',
    'Sub Disp 1',
    'Sub Disp 2',
    'Follow-up Notes',
    'Agent Hangup',
    'Status',
    'Campaign Type',
    'Abandoned',
    'Country',
    'Transfer',
    'Transfer To Extension',
    'Transfer Type',
    'Agent History',
    'Queue History',
    'Recording',
    'Call ID',
    'System Disposition'
  ];
  
  // Use Blob for better memory efficiency with large datasets
  const csvRows = [];
  csvRows.push(headers.join(','));
  
  // Process in batches to avoid memory issues
  const batchSize = 5000;
  const totalBatches = Math.ceil(state.currentResults.length / batchSize);
  
  // Process first batch immediately
  processCSVBatch(0);
  
  // Function to process CSV data in batches
  function processCSVBatch(batchIndex) {
    // Calculate batch range
    const startIndex = batchIndex * batchSize;
    const endIndex = Math.min(startIndex + batchSize, state.currentResults.length);
    
    // Update progress for large datasets
    if (state.currentResults.length > 10000) {
      const progress = Math.round((batchIndex / totalBatches) * 100);
      elements.stats.textContent = `Preparing CSV: ${progress}% complete (${startIndex.toLocaleString()} of ${state.currentResults.length.toLocaleString()} records processed)`;
    }
    
    // Process this batch
    for (let i = startIndex; i < endIndex; i++) {
      const row = state.currentResults[i];
      const csvRow = [
        // S.No. - row number in the dataset
        (i + 1).toString(),
        // Match the exact order of columns in the table display
        row.record_type || '',
        `"${(row.agent_name || '').replace(/"/g, '""')}"`,
        `"${(row.extension || '').replace(/"/g, '""')}"`,
        `"${(row.queue_campaign_name || '').replace(/"/g, '""')}"`,
        `"${formatDate(row.called_time, row.called_time_formatted)}"`,
        `"${(row.caller_id_number || '').replace(/"/g, '""')}"`,
        `"${(row.callee_id_number || '').replace(/"/g, '""')}"`,
        `"${(row.answered_time_formatted && row.answered_time_formatted !== 'undefined') ? row.answered_time_formatted : formatDate(row.answered_time)}"`,
        `"${(row.hangup_time_formatted && row.hangup_time_formatted !== 'undefined') ? row.hangup_time_formatted : formatDate(row.hangup_time)}"`,
        `"${(row.wait_duration_formatted && row.wait_duration_formatted !== 'undefined') ? row.wait_duration_formatted : formatDuration(row.wait_duration)}"`,
        `"${(row.talk_duration_formatted && row.talk_duration_formatted !== 'undefined') ? row.talk_duration_formatted : formatDuration(row.talk_duration)}"`,
        `"${(row.hold_duration_formatted && row.hold_duration_formatted !== 'undefined') ? row.hold_duration_formatted : formatDuration(row.hold_duration)}"`,
        `"${(row.hold_duration_intervals || '').replace(/"/g, '""')}"`,
        `"${(row.agent_disposition || '').replace(/"/g, '""')}"`,
        `"${(row.sub_disp_1 || '').replace(/"/g, '""')}"`,
        `"${(row.sub_disp_2 || '').replace(/"/g, '""')}"`,
        `"${(row.follow_up_notes || '').replace(/"/g, '""')}"`,
        `"${(row.agent_hangup || '').replace(/"/g, '""')}"`,
        `"${(row.status || '').replace(/"/g, '""')}"`,
        `"${(row.campaign_type || '').replace(/"/g, '""')}"`,
        `"${(row.abandoned || '').replace(/"/g, '""')}"`,
        `"${(row.country || '').replace(/"/g, '""')}"`,
        `"${(row.transfer_event === 1 || row.transfer_event === true ? 'Yes' : 'No') || ''}"`,
        `"${(row.transfer_extension || '').replace(/"/g, '""')}"`,
        `"${(row.transfer_type || '').replace(/"/g, '""')}"`,
        `"${getHistoryTextForCSV(row, 'agent').replace(/"/g, '""')}"`,
        `"${getHistoryTextForCSV(row, 'queue').replace(/"/g, '""')}"`,
        `"${(row.recording || '').replace(/"/g, '""')}"`,
        `"${(row.call_id || '').replace(/"/g, '""')}"`,
        `"${(row.system_disposition || row.disposition || '').replace(/"/g, '""')}"`,
      ].join(',');
      
      csvRows.push(csvRow);
    }
    
    // Check if we need to process more batches
    if (endIndex < state.currentResults.length) {
      // Schedule next batch with setTimeout to avoid blocking the UI
      setTimeout(() => processCSVBatch(batchIndex + 1), 0);
    } else {
      // All batches processed, create and download the file
      finishCSVExport(csvRows);
    }
  }
  
  // Function to finalize CSV export after all batches are processed
  function finishCSVExport(csvRows) {
    // Create blob with all CSV data
    const csvBlob = new Blob([csvRows.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const csvUrl = URL.createObjectURL(csvBlob);
    
    // Create download link
    const link = document.createElement('a');
    link.setAttribute('href', csvUrl);
    
    // Generate filename with current date
    const now = luxon.DateTime.now().setZone(TIMEZONE);
    const filename = `final_report_${now.toFormat('yyyy-MM-dd_HHmmss')}.csv`;
    link.setAttribute('download', filename);
    
    // Trigger download
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    
    // Clean up
    URL.revokeObjectURL(csvUrl);
    
    // Reset status message
    if (state.currentResults.length > 10000) {
      elements.stats.textContent = `Exported ${state.currentResults.length.toLocaleString()} records to CSV`;
    }
  }
}

// Apply filter-active class to inputs with values
function updateFilterActiveClass() {
  // Get all input elements in the form
  const inputs = elements.form.querySelectorAll('input, select');
  
  // Loop through each input
  inputs.forEach(input => {
    // Skip date inputs
    if (input.id === 'start' || input.id === 'end') return;
    
    // Check if the input has a value
    if (input.value && input.value.trim() !== '') {
      // Add the filter-active class
      input.classList.add('filter-active');
    } else {
      // Remove the filter-active class
      input.classList.remove('filter-active');
    }
  });
  
  // Handle select elements separately
  const selects = elements.form.querySelectorAll('select');
  selects.forEach(select => {
    if (select.value && select.value !== '') {
      select.classList.add('filter-active');
    } else {
      select.classList.remove('filter-active');
    }
  });
}

// Handle form submission
function handleSubmit(e) {
  e.preventDefault();
  console.log('Form submitted');
  
  const formData = getFormValues();
  console.log('Form data:', formData);
  
  // Add more detailed debugging
  console.log('Form element:', elements.form);
  console.log('Start input value:', elements.startInput.value);
  console.log('End input value:', elements.endInput.value);
  
  // Check if dates are valid before proceeding
  if (!elements.startInput.value || !elements.endInput.value) {
    showError('Please select both start and end dates');
    return;
  }
  
  // Update filter active class
  updateFilterActiveClass();
  
  fetchData(formData);
}

// No pagination handling needed

// Reset form to defaults
function resetForm() {
  // Reset date inputs to current day
  initializeDateInputs();
  
  // Clear all other inputs
  // document.getElementById('caller_id_number').value = '';
  // document.getElementById('callee_id_number').value = '';
  document.getElementById('contact_number').value = '';
  document.getElementById('agent_name').value = '';
  document.getElementById('queue_campaign_name').value = '';
  document.getElementById('record_type').value = '';
  document.getElementById('agent_disposition').value = '';
  document.getElementById('sub_disp_1').value = '';
  document.getElementById('sub_disp_2').value = '';
  document.getElementById('status').value = '';
  document.getElementById('campaign_type').value = '';
  document.getElementById('country').value = '';
  document.getElementById('transferred').value = '';
  
  // Sort options are now hardcoded in getFormValues
  
  // Clear results
  elements.resultTable.innerHTML = '';
  elements.stats.classList.add('is-hidden');
  elements.csvBtn.disabled = true;
}

// Process history data for display
function processHistoryData(historyData, type) {
  if (!historyData) return '';
  
  // If it's already HTML with our button format, return as is
  if (typeof historyData === 'string' && historyData.includes('eye-btn')) {
    return historyData;
  }
  
  // If it's a string that might be JSON
  if (typeof historyData === 'string') {
    try {
      const parsed = JSON.parse(historyData);
      if (Array.isArray(parsed)) {
        // Detect data structure to choose appropriate function
        if (type === 'agent') {
          return historyToHtml(parsed);
        } else {
          // For queue history, check if it has agent history structure (Campaign calls)
          // or queue history structure (Inbound/Outbound calls)
          if (parsed.length > 0 && parsed[0].hasOwnProperty('last_attempt')) {
            // This is agent history data structure (Campaign calls)
            return historyToHtml(parsed);
          } else {
            // This is queue history data structure (Inbound/Outbound calls)
            return queueHistoryToHtml(parsed);
          }
        }
      }
      return historyData; // Not an array, return as is
    } catch (e) {
      return historyData; // Not valid JSON, return as is
    }
  }
  
  // If it's already an array
  if (Array.isArray(historyData)) {
    if (type === 'agent') {
      return historyToHtml(historyData);
    } else {
      // For queue history, check data structure
      if (historyData.length > 0 && historyData[0].hasOwnProperty('last_attempt')) {
        // This is agent history data structure (Campaign calls)
        return historyToHtml(historyData);
      } else {
        // This is queue history data structure (Inbound/Outbound calls)
        return queueHistoryToHtml(historyData);
      }
    }
  }
  
  return historyData;
}

// Add input change listeners for filter styling
function addFilterChangeListeners() {
  // Get all input and select elements in the form
  const inputs = elements.form.querySelectorAll('input, select');
  
  // Add change event listener to each input
  inputs.forEach(input => {
    // Skip date inputs
    if (input.id === 'start' || input.id === 'end') return;
    
    input.addEventListener('input', function() {
      // Check if the input has a value
      if (this.value && this.value.trim() !== '') {
        // Add the filter-active class
        this.classList.add('filter-active');
      } else {
        // Remove the filter-active class
        this.classList.remove('filter-active');
      }
    });
  });
  
  // Add change event listener to each select
  const selects = elements.form.querySelectorAll('select');
  selects.forEach(select => {
    select.addEventListener('change', function() {
      if (this.value && this.value !== '') {
        this.classList.add('filter-active');
      } else {
        this.classList.remove('filter-active');
      }
    });
  });
}

// Add date change listeners to automatically fetch filter dropdowns
function addDateChangeListeners() {
  let debounceTimer;
  
  const fetchFiltersForDateRange = () => {
    // Clear any existing debounce timer
    if (debounceTimer) {
      clearTimeout(debounceTimer);
    }
    
    // Debounce to avoid excessive API calls while user is typing
    debounceTimer = setTimeout(() => {
      const startDate = elements.startInput.value;
      const endDate = elements.endInput.value;
      
      // Only fetch if both dates are provided
      if (startDate && endDate) {
        // Convert to unix timestamps (seconds)
        const fromTs = Math.floor(new Date(startDate).getTime() / 1000);
        const toTs = Math.floor(new Date(endDate).getTime() / 1000);
        
        // Validate that dates are valid
        if (!isNaN(fromTs) && !isNaN(toTs) && fromTs > 0 && toTs > 0) {
          console.log('Date range changed, fetching filter dropdowns...');
          loadQueueCampaignDropdown(fromTs, toTs);
          loadAgentDispositionDropdown(fromTs, toTs);
        }
      }
    }, 500); // Wait 500ms after user stops typing
  };
  
  // Add event listeners to both date inputs
  elements.startInput.addEventListener('change', fetchFiltersForDateRange);
  elements.endInput.addEventListener('change', fetchFiltersForDateRange);
  
  // Also trigger on input event for more responsive updates
  elements.startInput.addEventListener('input', fetchFiltersForDateRange);
  elements.endInput.addEventListener('input', fetchFiltersForDateRange);
}

// Initialize the page
function init() {
  // Set up date inputs
  initializeDateInputs();
  
  // Add event listeners
  elements.form.addEventListener('submit', handleSubmit);
  elements.csvBtn.addEventListener('click', generateCSV);
  
  // Add filter change listeners
  addFilterChangeListeners();
  
  // Add date change listeners to automatically fetch filter dropdowns
  addDateChangeListeners();
  
  // Fetch filter dropdowns for the default date range on page load
  if (elements.startInput.value && elements.endInput.value) {
    const fromTs = Math.floor(new Date(elements.startInput.value).getTime() / 1000);
    const toTs = Math.floor(new Date(elements.endInput.value).getTime() / 1000);
    
    if (!isNaN(fromTs) && !isNaN(toTs) && fromTs > 0 && toTs > 0) {
      console.log('Loading filter dropdowns for default date range...');
      loadQueueCampaignDropdown(fromTs, toTs);
      loadAgentDispositionDropdown(fromTs, toTs);
    }
  }
  
  // Apply filter-active class to any inputs that already have values
  updateFilterActiveClass();
  
  // Add showHistoryModal to window object
  window.showHistoryModal = showHistoryModal;
  
  // Add Howler script immediately on page load
  if (!window.Howl) {
    const script = document.createElement('script');
    script.src = 'https://cdnjs.cloudflare.com/ajax/libs/howler/2.2.3/howler.min.js';
    script.onload = () => console.log('Howler loaded successfully');
    script.onerror = () => console.error('Failed to load Howler.js');
    document.head.appendChild(script);
  }
  
  // FIX 6: Load WaveSurfer once at page load (saves 300-500ms on first play)
  if (!window.WaveSurfer) {
    const wsScript = document.createElement('script');
    wsScript.src = 'https://unpkg.com/wavesurfer.js@7/dist/wavesurfer.min.js';
    wsScript.onload = () => console.log('WaveSurfer loaded successfully');
    wsScript.onerror = () => console.error('Failed to load WaveSurfer');
    document.head.appendChild(wsScript);
  }
}

// Start the app when DOM is loaded
document.addEventListener('DOMContentLoaded', init);
