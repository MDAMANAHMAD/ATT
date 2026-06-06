/**
 * app.js - TypeCraft Stream Coordinator
 * Coordinates Server-Sent Events stream, IndexedDB updates, search filters, 
 * and auto-discovery of local/cloud API endpoints. No passcode gating.
 */

import { db } from './db.js';

// DOM Elements
const statusBadge = document.getElementById('status-badge');
const statusText = document.getElementById('status-text');
const liveText = document.getElementById('live-text');
const historyContainer = document.getElementById('history-container');
const searchInput = document.getElementById('search-input');
const btnClear = document.getElementById('btn-clear');

const toastNotification = document.getElementById('toast-notification');
const toastIcon = document.getElementById('toast-icon');
const toastMessage = document.getElementById('toast-message');

// State Variables
let eventSource = null;
let allSentences = [];

// Determine API Base URL (Default to local, will auto-discover)
let API_BASE_URL = 'http://localhost:5001';
const CLOUD_API_URL = 'https://att-render-api.onrender.com';

// Initialize Application
document.addEventListener('DOMContentLoaded', async () => {
  try {
    await db.open();
  } catch (e) {
    console.error('Failed to initialize local database:', e);
    showToast('Failed to initialize local database.', 'error');
  }

  // Filter input listener
  searchInput.addEventListener('input', handleSearch);

  // Clear feed listener
  btnClear.addEventListener('click', clearHistory);

  if (window.lucide) {
    window.lucide.createIcons();
  }

  // Initialize stream visualizer directly on load
  initializeVisualizer();
});

/**
 * Auto-discovers endpoints and launches connection
 */
async function initializeVisualizer() {
  // Auto-discover the best API endpoint before fetching history
  await discoverApiEndpoint();

  // Load database cache and start SSE live feed
  loadHistoryFeed();
  connectToSecureStream();
}

/**
 * Auto-discovers whether to use local listener or cloud Render API
 */
async function discoverApiEndpoint() {
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), 1200); // 1.2 seconds timeout
    
    // Quick request to local python listener
    const response = await fetch('http://localhost:5001/activity', {
      signal: controller.signal
    });
    
    clearTimeout(timeoutId);
    
    if (response.status === 200) {
      API_BASE_URL = 'http://localhost:5001';
      console.log("Discovered local tracker active on http://localhost:5001");
      return;
    }
  } catch (err) {
    console.log("Local tracker not running on this machine. Falling back to cloud.");
  }
  
  // Fallback to cloud Render URL
  API_BASE_URL = CLOUD_API_URL;
  console.log("Using Cloud API Endpoint:", API_BASE_URL);
}

/**
 * Fetch historical data from Python server
 */
async function loadHistoryFeed() {
  try {
    const response = await fetch(`${API_BASE_URL}/activity`);
    
    if (!response.ok) {
      throw new Error(`Server returned status ${response.status}`);
    }
    
    const records = await response.json();
    
    // Sync into IndexedDB
    for (const record of records) {
      await db.saveRecord(record);
    }
    
    allSentences = await db.getAllRecords();
    renderHistoryFeed(allSentences);
  } catch (err) {
    console.error('Failed to load history:', err);
    // Offline load local cache
    allSentences = await db.getAllRecords();
    renderHistoryFeed(allSentences);
  }
}

/**
 * Establishes real-time connection to SSE
 */
function connectToSecureStream() {
  if (eventSource) {
    eventSource.close();
  }

  eventSource = new EventSource(`${API_BASE_URL}/stream`);

  eventSource.onopen = () => {
    updateConnectionStatus(true);
  };

  eventSource.onerror = (err) => {
    updateConnectionStatus(false);
    console.warn("Secure stream lost connection. Reconnecting in 3s...", err);
    setTimeout(connectToSecureStream, 3000);
  };

  // Event: Initial Backlog
  eventSource.addEventListener('init', async (e) => {
    try {
      const records = JSON.parse(e.data);
      for (const record of records) {
        await db.saveRecord(record);
      }
      allSentences = await db.getAllRecords();
      renderHistoryFeed(allSentences);
    } catch (err) {
      console.error('Init parse error:', err);
    }
  });

  // Event: Live keyboard update
  eventSource.addEventListener('live', (e) => {
    try {
      const data = JSON.parse(e.data);
      const text = data.text;
      
      if (text && text.trim() !== "") {
        liveText.textContent = text;
        liveText.classList.remove('placeholder-text');
      } else {
        liveText.textContent = "Type anything on your device... (Notepad, browser, code editor, etc.)";
        liveText.classList.add('placeholder-text');
      }
    } catch (err) {
      console.error('Live event parse error:', err);
    }
  });

  // Event: Completed Sentence committed
  eventSource.addEventListener('commit', async (e) => {
    try {
      const record = JSON.parse(e.data);
      
      // Reset live preview area
      liveText.textContent = "Type anything on your device... (Notepad, browser, code editor, etc.)";
      liveText.classList.add('placeholder-text');

      // Save to IndexedDB
      await db.saveRecord(record);
      allSentences.unshift(record);
      renderHistoryFeed(allSentences);

      showToast('Sentence added to stream.', 'success');
    } catch (err) {
      console.error('Commit event parse error:', err);
    }
  });

  // Event: Clear logs triggered
  eventSource.addEventListener('clear', async () => {
    await db.clearAll();
    allSentences = [];
    renderHistoryFeed([]);
    showToast('Typing history wiped.', 'success');
  });
}

function updateConnectionStatus(isConnected) {
  if (isConnected) {
    statusBadge.className = 'status-badge connected';
    statusText.textContent = 'Active Tracker';
  } else {
    statusBadge.className = 'status-badge disconnected';
    statusText.textContent = 'Disconnected';
  }
}

function renderHistoryFeed(list) {
  historyContainer.innerHTML = '';

  const query = searchInput.value.toLowerCase().trim();
  const filtered = query === '' 
    ? list 
    : list.filter(r => r.text.toLowerCase().includes(query));

  if (filtered.length === 0) {
    historyContainer.innerHTML = `
      <div class="empty-state">
        <i data-lucide="${query === '' ? 'inbox' : 'search'}"></i>
        <p>${query === '' ? 'Your typing history is empty. Start typing on your device!' : 'No matching text entries found.'}</p>
      </div>
    `;
    if (window.lucide) window.lucide.createIcons();
    return;
  }

  filtered.forEach(record => {
    const card = document.createElement('div');
    card.className = 'sentence-card';
    
    const dateObj = new Date(record.timestamp);
    const timeFormatted = dateObj.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    const dateFormatted = dateObj.toLocaleDateString([], { month: 'short', day: 'numeric' });

    card.innerHTML = `
      <p class="sentence-text">${escapeHtml(record.text)}</p>
      <div class="sentence-meta">
        <span>${dateFormatted} at ${timeFormatted}</span>
      </div>
    `;

    historyContainer.appendChild(card);
  });

  if (window.lucide) {
    window.lucide.createIcons();
  }
}

function handleSearch() {
  renderHistoryFeed(allSentences);
}

async function clearHistory() {
  if (confirm('Are you sure you want to delete ALL typed history? This will permanently wipe your text logs.')) {
    try {
      const response = await fetch(`${API_BASE_URL}/clear`);
      const data = await response.json();
      
      if (data.status === 'cleared') {
        await db.clearAll();
        allSentences = [];
        renderHistoryFeed([]);
        showToast('Successfully cleared all typing history.', 'success');
      }
    } catch (err) {
      console.error('Failed to wipe typing database:', err);
      showToast('Wipe failed. Background server offline.', 'error');
    }
  }
}

function showToast(message, type = 'success') {
  toastMessage.textContent = message;
  
  if (type === 'success') {
    toastNotification.className = 'toast show';
    toastIcon.setAttribute('data-lucide', 'check-circle');
    toastIcon.style.color = 'var(--color-success)';
  } else {
    toastNotification.className = 'toast show error';
    toastIcon.setAttribute('data-lucide', 'alert-triangle');
    toastIcon.style.color = 'var(--color-error)';
  }

  if (window.lucide) {
    window.lucide.createIcons();
  }

  setTimeout(() => {
    toastNotification.classList.remove('show');
  }, 3000);
}

function escapeHtml(text) {
  const map = {
    '&': '&amp;',
    '<': '&lt;',
    '>': '&gt;',
    '"': '&quot;',
    "'": '&#039;'
  };
  return text.replace(/[&<>"']/g, function(m) { return map[m]; });
}
