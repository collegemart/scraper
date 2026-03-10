/**
 * BharatFare Data Extractor - Popup Script
 */

let extractedData = [];
let allColumns = [];

const btnExtract = document.getElementById('btn-extract');
const statusEl = document.getElementById('status');
const statsEl = document.getElementById('stats');
const actionsEl = document.getElementById('actions');
const previewEl = document.getElementById('preview');
const breakdownEl = document.getElementById('breakdown');
const btnCSV = document.getElementById('btn-csv');
const btnJSON = document.getElementById('btn-json');
const btnCopy = document.getElementById('btn-copy');

// ── Extract button ──────────────────────────────────

btnExtract.addEventListener('click', async () => {
  btnExtract.disabled = true;
  btnExtract.textContent = 'Extracting...';
  statusEl.textContent = '';
  statusEl.className = 'status';

  try {
    const [tab] = await chrome.tabs.query({ active: true, currentWindow: true });
    if (!tab) throw new Error('No active tab');

    const results = await chrome.scripting.executeScript({
      target: { tabId: tab.id },
      files: ['content.js'],
    });

    if (!results || !results[0] || !results[0].result) {
      throw new Error('Extraction returned no data');
    }

    const data = results[0].result;
    processResults(data, tab.url);

  } catch (err) {
    statusEl.textContent = 'Error: ' + err.message;
    statusEl.className = 'status error';
    btnExtract.disabled = false;
    btnExtract.textContent = 'Extract Data From This Page';
  }
});

// ── Process results ─────────────────────────────────

function processResults(data, url) {
  const sources = [];
  extractedData = [];

  const categories = [
    { key: 'json', label: 'JSON Data' },
    { key: 'jsonld', label: 'Schema.org' },
    { key: 'listings', label: 'Listings' },
    { key: 'tables', label: 'Tables' },
    { key: 'contacts', label: 'Contacts' },
  ];

  for (const cat of categories) {
    const arr = data[cat.key] || [];
    if (arr.length > 0) {
      sources.push(`<span class="source-tag">${cat.label}: ${arr.length}</span>`);
      extractedData.push(...arr);
    }
  }

  // Add source URL to all items
  const domain = new URL(url).hostname.replace('www.', '');
  extractedData = extractedData.map(item => ({
    ...item,
    source_url: url,
    source_domain: domain,
  }));

  // Dedup
  const seen = new Set();
  extractedData = extractedData.filter(item => {
    const key = JSON.stringify(Object.values(item).sort()).toLowerCase();
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });

  // Get all columns
  const colSet = new Set();
  const priority = ['name', 'title', 'email', 'phone', 'price', 'fare', 'airline', 'carrier',
    'origin', 'destination', 'departure', 'arrival', 'duration', 'stops',
    'address', 'city', 'location', 'rating', 'category', 'hotel', 'room',
    'description', 'detail_url', 'url', 'source_domain'];
  extractedData.forEach(item => Object.keys(item).forEach(k => colSet.add(k)));
  allColumns = [];
  for (const p of priority) { if (colSet.has(p)) { allColumns.push(p); colSet.delete(p); } }
  for (const k of colSet) allColumns.push(k);

  // Update UI
  const total = extractedData.length;
  if (total === 0) {
    statusEl.textContent = 'No structured data found on this page.';
    statusEl.className = 'status error';
    btnExtract.disabled = false;
    btnExtract.textContent = 'Extract Data From This Page';
    return;
  }

  statusEl.textContent = `Extracted ${total} items successfully!`;
  statusEl.className = 'status success';

  document.getElementById('st-total').textContent = total;
  document.getElementById('st-fields').textContent = allColumns.length;
  document.getElementById('st-source').textContent = domain;
  statsEl.style.display = 'flex';

  breakdownEl.innerHTML = sources.join('');
  breakdownEl.style.display = 'block';

  actionsEl.style.display = 'flex';
  btnCSV.disabled = false;
  btnJSON.disabled = false;
  btnCopy.disabled = false;

  renderPreview();

  btnExtract.disabled = false;
  btnExtract.textContent = 'Re-Extract';
}

// ── Preview table ───────────────────────────────────

function renderPreview() {
  const thead = document.querySelector('#data-table thead');
  const tbody = document.querySelector('#data-table tbody');

  const showCols = allColumns.filter(c => !['source_url', 'source_domain'].includes(c)).slice(0, 8);

  thead.innerHTML = '<tr>' + showCols.map(c =>
    `<th>${c.replace(/_/g, ' ')}</th>`
  ).join('') + '</tr>';

  const rows = extractedData.slice(0, 30);
  tbody.innerHTML = rows.map(row =>
    '<tr>' + showCols.map(c => {
      const v = (row[c] || '').toString().trim();
      const display = v.length > 40 ? v.slice(0, 38) + '..' : v;
      return `<td title="${v.replace(/"/g, '&quot;')}">${display}</td>`;
    }).join('') + '</tr>'
  ).join('');

  previewEl.classList.add('show');
}

// ── Downloads ───────────────────────────────────────

btnCSV.addEventListener('click', () => {
  if (!extractedData.length) return;

  const escape = v => {
    const s = String(v || '').replace(/"/g, '""');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
  };

  const header = allColumns.map(escape).join(',');
  const lines = extractedData.map(row =>
    allColumns.map(c => escape(row[c] || '')).join(',')
  );

  const csv = [header, ...lines].join('\n');
  downloadFile(csv, 'extracted_data.csv', 'text/csv');
});

btnJSON.addEventListener('click', () => {
  if (!extractedData.length) return;
  const json = JSON.stringify(extractedData, null, 2);
  downloadFile(json, 'extracted_data.json', 'application/json');
});

btnCopy.addEventListener('click', async () => {
  if (!extractedData.length) return;

  const escape = v => {
    const s = String(v || '').replace(/"/g, '""');
    return s.includes(',') || s.includes('"') || s.includes('\n') ? `"${s}"` : s;
  };

  const header = allColumns.join('\t');
  const lines = extractedData.map(row =>
    allColumns.map(c => row[c] || '').join('\t')
  );
  const tsv = [header, ...lines].join('\n');

  try {
    await navigator.clipboard.writeText(tsv);
    btnCopy.textContent = 'Copied!';
    setTimeout(() => { btnCopy.textContent = 'Copy to Clipboard'; }, 2000);
  } catch (e) {
    btnCopy.textContent = 'Failed';
    setTimeout(() => { btnCopy.textContent = 'Copy to Clipboard'; }, 2000);
  }
});

function downloadFile(content, filename, type) {
  const blob = new Blob([content], { type });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  a.download = filename;
  a.click();
  URL.revokeObjectURL(url);
}
