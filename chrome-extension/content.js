/**
 * BharatFare Data Extractor - Content Script
 * Runs on the active tab and extracts all structured data from the page.
 */

(() => {
  // ── Regex ──────────────────────────────────────────────
  const EMAIL_RE = /[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}/g;
  const PHONE_INDIAN = /(?:\+91[\s-]?)?[6-9]\d{9}\b/g;
  // International: must start with + country code, then formatted digits
  const PHONE_INTL = /\+\d{1,3}[\s-]\(?\d{1,5}\)?[\s-]?\d[\d\s-]{4,10}\d/g;
  // Formatted landline: must have parens or multiple dashes/spaces separating groups
  const PHONE_FORMATTED = /\(?\d{2,5}\)[\s-]?\d[\d\s-]{5,10}\d|\d{2,5}[\s-]\d{3,5}[\s-]\d{3,5}/g;
  const GST_RE = /\b\d{2}[A-Z]{5}\d{4}[A-Z]\d[Z][A-Z\d]\b/g;
  const PRICE_RE = /[$\u20b9\u00a3\u20ac]\s?\d[\d,]*\.?\d*|\d[\d,]*\.?\d*\s*(?:USD|INR|EUR|GBP)/g;

  const EMAIL_BLACKLIST = new Set([
    'example.com', 'test.com', 'email.com', 'domain.com', 'sentry.io',
    'w3.org', 'schema.org', 'googleapis.com', 'google.com',
    'facebook.com', 'twitter.com', 'instagram.com', 'youtube.com',
  ]);

  const JUNK_KEYS = new Set([
    '$$typeof', '_owner', '_store', 'ref', 'key', 'props', 'children',
    '__typename', 'typename', 'node', 'edges', 'cursor', 'pageinfo',
    'componentname', 'classname', '__n', '__c', '_self', '_source',
  ]);

  const DATA_KEYS = new Set([
    'name', 'title', 'company', 'company_name', 'companyname', 'firm',
    'email', 'mail', 'phone', 'telephone', 'mobile', 'contact',
    'address', 'city', 'state', 'country', 'location', 'pincode', 'zip',
    'website', 'url', 'link', 'gst',
    'price', 'cost', 'amount', 'currency', 'discount', 'offer', 'fare', 'fee',
    'rating', 'score', 'reviews', 'review_count', 'stars',
    'category', 'brand', 'seller', 'vendor', 'supplier',
    'description', 'summary', 'details', 'features',
    // Travel / flights
    'origin', 'destination', 'departure', 'arrival', 'duration',
    'airline', 'carrier', 'flight', 'route', 'stops', 'layover',
    'cabin', 'class', 'baggage', 'direct', 'nonstop', 'transfer',
    'departuretime', 'arrivaltime', 'departuredate', 'arrivaldate',
    'flightnumber', 'airport', 'terminal', 'gate', 'seat',
    'outbound', 'inbound', 'leg', 'segment', 'itinerary',
    // Hotel / accommodation
    'hotel', 'room', 'checkin', 'checkout', 'guests', 'nights',
    'amenities', 'property', 'accommodation', 'roomtype',
    // Time / date
    'date', 'time', 'datetime', 'published', 'created', 'timestamp',
    // People / social
    'username', 'user', 'author', 'owner', 'fullname', 'display_name',
    'followers', 'following', 'posts', 'bio', 'headline', 'experience',
    'profession', 'designation', 'role', 'position', 'department',
    // E-commerce
    'sku', 'asin', 'isbn', 'upc', 'model', 'manufacturer', 'weight',
    'color', 'size', 'material', 'condition', 'availability',
    // Generic
    'id', 'status', 'type', 'label', 'image', 'quantity', 'stock',
    'thumbnail', 'photo', 'logo', 'count', 'total', 'min', 'max',
  ]);

  const NAV_WORDS = new Set([
    'home', 'about', 'contact', 'login', 'signup', 'register', 'menu',
    'back', 'next', 'prev', 'more', 'view all', 'click here',
    'submit', 'download', 'upload', 'share', 'follow', 'subscribe',
    'privacy', 'terms', 'sitemap', 'faq', 'help', 'careers', 'cookie',
    'how it works', 'get started', 'learn more', 'read more',
  ]);

  function cleanEmail(e) {
    if (!e) return null;
    e = e.trim().toLowerCase();
    const domain = e.split('@').pop();
    if (EMAIL_BLACKLIST.has(domain)) return null;
    if (e.length < 6 || e.length > 80) return null;
    return e;
  }

  function cleanPhone(p) {
    if (!p) return null;
    let d = p.replace(/\D/g, '');
    if (d.startsWith('91') && d.length === 12) d = d.slice(2);
    if (d.startsWith('0') && d.length === 11) d = d.slice(1);
    return (d.length >= 7 && d.length <= 12) ? d : null;
  }

  function isDataKey(k) {
    const kl = k.toLowerCase().replace(/-/g, '_');
    if (JUNK_KEYS.has(kl)) return false;
    if (DATA_KEYS.has(kl)) return true;
    for (const dk of DATA_KEYS) { if (kl.includes(dk)) return true; }
    return false;
  }

  function flattenDict(obj, prefix = '', depth = 0) {
    const items = {};
    if (depth > 6) return items;
    for (const [k, v] of Object.entries(obj)) {
      // Skip internal/framework keys
      if (k.startsWith('_') || k.startsWith('$') || k === '@context') continue;
      const key = prefix ? `${prefix}_${k}` : k;
      if (v && typeof v === 'object' && !Array.isArray(v)) {
        Object.assign(items, flattenDict(v, key, depth + 1));
      } else if (Array.isArray(v)) {
        if (v.length === 0) continue;
        // Array of primitives - join them
        if (v.every(x => typeof x !== 'object' || x === null)) {
          items[key] = v.slice(0, 8).join(', ');
        }
        // Array of objects (e.g. flight legs, stops) - flatten each with index
        else if (v.length <= 6 && v.every(x => x && typeof x === 'object' && !Array.isArray(x))) {
          for (let i = 0; i < v.length; i++) {
            const subFlat = flattenDict(v[i], `${key}_${i + 1}`, depth + 1);
            Object.assign(items, subFlat);
          }
          items[`${key}_count`] = String(v.length);
        }
      } else if (v !== null && v !== undefined && typeof v !== 'object') {
        items[key] = String(v);
      }
    }
    return items;
  }

  // ── Strategy 1: JSON data arrays ───────────────────────

  function findArrays(obj, results, depth = 0) {
    if (depth > 12) return;
    if (Array.isArray(obj)) {
      const dicts = obj.filter(x => x && typeof x === 'object' && !Array.isArray(x) && Object.keys(x).length >= 2);
      if (dicts.length >= 2) {
        const keySets = dicts.slice(0, 10).map(d => new Set(Object.keys(d)));
        const common = new Set([...keySets[0]].filter(k => keySets.every(s => s.has(k))));
        const union = new Set(keySets.flatMap(s => [...s]));
        if (union.size && common.size / union.size > 0.3) {
          results.push(dicts);
        }
      }
      for (const item of obj) {
        if (item && typeof item === 'object') findArrays(item, results, depth + 1);
      }
    } else if (obj && typeof obj === 'object') {
      for (const v of Object.values(obj)) {
        if (v && typeof v === 'object') findArrays(v, results, depth + 1);
      }
    }
  }

  function scoreArray(arr) {
    if (arr.length < 2) return 0;
    const sample = arr.slice(0, 10);
    const allKeys = new Set();
    sample.forEach(d => Object.keys(d).forEach(k => allKeys.add(k.toLowerCase().replace(/-/g, '_'))));

    let junkCount = 0;
    for (const k of allKeys) { if (JUNK_KEYS.has(k)) junkCount++; }
    if (allKeys.size && junkCount / allKeys.size > 0.3) return 0;

    let dataKeyCount = 0;
    for (const k of allKeys) { if (isDataKey(k)) dataKeyCount++; }
    if (dataKeyCount === 0) return 0;

    let score = dataKeyCount * 1.5;
    score += Math.min(arr.length, 100) * 0.1;

    // Price/fare fields - very useful
    for (const k of allKeys) {
      if (/price|cost|amount|fare|fee/.test(k)) { score += 5; break; }
    }
    // Contact info
    for (const k of allKeys) {
      if (/email|phone|telephone|mobile/.test(k)) { score += 5; break; }
    }
    // Name/title identifier
    for (const k of allKeys) {
      if (/^(name|title|company)$/.test(k)) { score += 2; break; }
    }
    // Travel-specific bonuses
    for (const k of allKeys) {
      if (/airline|carrier|flight|departure|arrival|origin|destination/.test(k)) { score += 4; break; }
    }
    for (const k of allKeys) {
      if (/duration|stops|layover|cabin|baggage|terminal/.test(k)) { score += 3; break; }
    }
    // Hotel-specific
    for (const k of allKeys) {
      if (/hotel|room|checkin|checkout|amenities|property/.test(k)) { score += 4; break; }
    }

    // Bonus for nested richness - items with sub-objects have deeper data
    let nestedCount = 0;
    let flatVals = 0;
    sample.forEach(d => {
      for (const v of Object.values(d)) {
        if (typeof v !== 'object') flatVals++;
        else if (v && typeof v === 'object') nestedCount++;
      }
    });
    // Nested data means richer extraction when flattened
    if (nestedCount > sample.length) score += Math.min(nestedCount / sample.length, 3) * 2;
    // But must still have some flat values
    if (flatVals < sample.length) score *= 0.5;

    return score;
  }

  function extractFromJSON() {
    const items = [];
    const blobs = [];

    // __NEXT_DATA__
    const nd = document.getElementById('__NEXT_DATA__');
    if (nd) { try { blobs.push(JSON.parse(nd.textContent)); } catch (e) { } }

    // <script type="application/json"> (React server components, etc.)
    document.querySelectorAll('script[type="application/json"]').forEach(el => {
      try { blobs.push(JSON.parse(el.textContent)); } catch (e) { }
    });

    // Inline scripts with JSON assignments
    document.querySelectorAll('script:not([src]):not([type])').forEach(el => {
      const t = (el.textContent || '').slice(0, 200000);
      for (const pat of [
        /window\.__\w+__\s*=\s*(\{[\s\S]+?\})\s*;/g,
        /window\.\w+\s*=\s*(\{[\s\S]+?\})\s*;/g,
        /var\s+\w+\s*=\s*(\[[\s\S]+?\])\s*;/g,
        /const\s+\w+\s*=\s*(\{[\s\S]+?\})\s*;/g,
      ]) {
        let m;
        while ((m = pat.exec(t)) !== null) {
          try { blobs.push(JSON.parse(m[1])); } catch (e) { }
        }
      }
    });

    // Also try to find large JSON objects in data-* attributes on body/main
    for (const el of [document.body, document.querySelector('main'), document.querySelector('#__next')]) {
      if (!el) continue;
      for (const attr of el.attributes || []) {
        if (attr.name.startsWith('data-') && attr.value.length > 100) {
          try { blobs.push(JSON.parse(attr.value)); } catch (e) { }
        }
      }
    }

    if (!blobs.length) return items;

    const allArrays = [];
    blobs.forEach(b => findArrays(b, allArrays));

    const scored = allArrays.map(a => [scoreArray(a), a]).filter(([s]) => s > 2);
    scored.sort((a, b) => b[0] - a[0]);

    // Extract items from top arrays - collect from ALL good ones, not just the first
    const seenKeys = new Set();
    for (const [score, arr] of scored.slice(0, 5)) {
      if (score < 2) break;
      for (const obj of arr) {
        const flat = flattenDict(obj);
        // Filter: keep only entries with real values
        const real = {};
        for (const [k, v] of Object.entries(flat)) {
          if (!v || !String(v).trim() || String(v).trim().length <= 1) continue;
          // Skip keys that are just long hashes/IDs
          if (/^[a-f0-9]{20,}$/i.test(String(v).trim())) continue;
          real[k] = v;
        }
        if (Object.keys(real).length >= 2) {
          // Dedup by serializing key values
          const dk = JSON.stringify(Object.entries(real).sort().slice(0, 5));
          if (!seenKeys.has(dk)) {
            seenKeys.add(dk);
            items.push(real);
          }
        }
      }
    }
    return items;
  }

  // ── Strategy 2: JSON-LD ──────────────────────────────

  function extractJSONLD() {
    const items = [];
    document.querySelectorAll('script[type="application/ld+json"]').forEach(el => {
      let data;
      try { data = JSON.parse(el.textContent); } catch (e) { return; }

      let objects = [];
      if (Array.isArray(data)) objects = data;
      else if (data['@graph']) objects = data['@graph'];
      else if (data['@type'] === 'ItemList') objects = (data.itemListElement || []).map(e => e.item || e);
      else objects = [data];

      for (const obj of objects) {
        if (!obj || typeof obj !== 'object') continue;
        const name = obj.name;
        if (!name || String(name).length < 2) continue;

        const flat = flattenDict(obj);
        flat.name = String(name);

        const useful = ['phone', 'telephone', 'email', 'address', 'price', 'rating', 'description', 'url'];
        const hasUseful = Object.keys(flat).some(k => useful.some(u => k.toLowerCase().includes(u)) && flat[k]);
        if (!hasUseful) continue;

        items.push(flat);
      }
    });
    return items;
  }

  // ── Strategy 3: HTML Listings ────────────────────────

  function extractListings() {
    const items = [];
    const candidates = [];

    const selectors = [
      'div[class*=listing]', 'div[class*=result]', 'div[class*=card]',
      'div[class*=product]', 'div[class*=flight]', 'div[class*=hotel]',
      'div[class*=itinerary]', 'div[class*=offer]', 'div[class*=deal]',
      'div[class*=price]', 'article', 'li[class*=item]', 'li[class*=result]',
      '[data-testid]', '#results > *', '[class*=results] > *',
      '[id*=result] > *', '[id*=listing] > *', '[id*=append] > *',
    ];

    for (const sel of selectors) {
      try {
        const els = document.querySelectorAll(sel);
        if (els.length < 3) continue;
        const score = scoreHTMLBlocks(els);
        if (score > 5) candidates.push({ score, count: els.length, els });
      } catch (e) { }
    }

    // Auto-detect parent with many children
    for (const parent of document.querySelectorAll('ul, ol, div, section, main, [role=list]')) {
      const children = parent.querySelectorAll(':scope > *');
      if (children.length < 3) continue;
      const sample = [...children].slice(0, 8);
      const textCount = sample.filter(c => (c.textContent || '').trim().length > 30).length;
      if (textCount < sample.length * 0.4) continue;
      const score = scoreHTMLBlocks(children);
      if (score > 5) candidates.push({ score, count: children.length, els: children });
    }

    if (!candidates.length) return items;
    candidates.sort((a, b) => b.score - a.score);
    const best = candidates[0];

    for (const block of best.els) {
      const text = (block.textContent || '').trim();
      if (text.length < 15) continue;

      const fields = {};

      // Name
      const name = extractName(block);
      if (name && name.length >= 3 && !NAV_WORDS.has(name.toLowerCase().trim())) {
        fields.name = name;
      }

      // Link
      const a = block.querySelector('a[href]');
      if (a && a.href && !a.href.startsWith('javascript:')) {
        fields.detail_url = a.href;
      }

      // Regex scan
      const html = block.innerHTML || '';
      const scan = text + ' ' + html;

      const emails = (scan.match(EMAIL_RE) || []).filter(cleanEmail);
      const phones = scan.match(PHONE_INDIAN) || scan.match(PHONE_INTL) || scan.match(PHONE_FORMATTED) || [];
      const prices = text.match(PRICE_RE) || [];
      const gsts = scan.match(GST_RE) || [];

      if (emails.length) fields.email = cleanEmail(emails[0]);
      if (phones.length) fields.phone = cleanPhone(phones[0]);
      if (prices.length) fields.price = prices[0].trim();
      if (gsts.length) fields.gst = gsts[0];

      // CSS class selectors
      const classMap = {
        price: '[class*=price],.price,[class*=fare]',
        rating: '[class*=rating],.rating,[class*=score]',
        address: '[class*=address],.address',
        location: '[class*=location],.location,[class*=city]',
        date: '[class*=date],time,[class*=depart],[class*=arrive]',
        duration: '[class*=duration],[class*=travel-time]',
        airline: '[class*=airline],[class*=carrier],[class*=operator]',
        origin: '[class*=origin],[class*=from],[class*=depart]',
        destination: '[class*=destination],[class*=to],[class*=arrive]',
        stops: '[class*=stop],[class*=layover],[class*=transfer]',
        cabin: '[class*=cabin],[class*=class]',
        hotel: '[class*=hotel],[class*=property-name]',
        room: '[class*=room],[class*=accommodation]',
      };
      for (const [key, sel] of Object.entries(classMap)) {
        if (fields[key]) continue;
        const el = block.querySelector(sel);
        if (el && el.textContent.trim()) fields[key] = el.textContent.trim().slice(0, 300);
      }

      // Data attributes
      for (const attr of ['data-name', 'data-company', 'data-title', 'data-price', 'data-id', 'data-rating']) {
        const val = block.getAttribute(attr);
        if (val) fields[attr.replace('data-', '')] = val;
      }

      // Indian directory button title pattern
      for (const btn of block.querySelectorAll('button[title]')) {
        const title = btn.getAttribute('title') || '';
        if (title.includes('#')) {
          const parts = title.split('#');
          if (parts.length >= 3 && parts[2].trim().length > 2) fields.name = parts[2].trim();
        }
      }

      // Quality gate
      const realKeys = Object.keys(fields).filter(k => !['name', 'detail_url'].includes(k) && fields[k]);
      if (!realKeys.length) continue;

      items.push(fields);
    }
    return items;
  }

  function scoreHTMLBlocks(els) {
    let score = 0;
    const sample = [...els].slice(0, 8);
    for (const block of sample) {
      const t = block.textContent || '';
      if (EMAIL_RE.test(t) || PHONE_INDIAN.test(t)) score += 4;
      else if (PHONE_INTL.test(t) || PHONE_FORMATTED.test(t)) score += 3;
      if (PRICE_RE.test(t)) score += 4;
      if (block.querySelector('[class*=price],[class*=rating],[class*=address]')) score += 2;
      if (t.length > 80) score += 1;
      // Reset regex lastIndex
      EMAIL_RE.lastIndex = 0; PHONE_INDIAN.lastIndex = 0;
      PHONE_INTL.lastIndex = 0; PHONE_FORMATTED.lastIndex = 0; PRICE_RE.lastIndex = 0;
    }
    return sample.length ? (score / sample.length) * Math.min(els.length, 50) : 0;
  }

  function extractName(block) {
    for (const attr of ['data-name', 'data-company', 'data-title']) {
      const v = block.getAttribute(attr);
      if (v && v.trim().length > 2) return v.trim();
    }
    for (const btn of block.querySelectorAll('button[title]')) {
      const title = btn.getAttribute('title') || '';
      if (title.includes('#')) {
        const parts = title.split('#');
        if (parts.length >= 3 && parts[2].trim().length > 2) return parts[2].trim();
      }
    }
    for (const sel of ['h1 a', 'h2 a', 'h3 a', 'h4 a', 'h1', 'h2', 'h3', 'h4', '.name', '.title', '.company-name', '[class*=name]', '[itemprop=name]', 'strong', 'b']) {
      const el = block.querySelector(sel);
      if (el) {
        const t = el.textContent.trim();
        if (t.length > 3 && t.length < 150) return t;
      }
    }
    for (const a of block.querySelectorAll('a')) {
      const t = a.textContent.trim();
      if (t.length > 3 && t.length < 150) {
        const cls = (a.className || '').toLowerCase();
        if (!/btn|button|nav|menu|icon/.test(cls)) return t;
      }
    }
    return '';
  }

  // ── Strategy 4: Tables ──────────────────────────────

  function extractTables() {
    const items = [];
    for (const table of document.querySelectorAll('table')) {
      const headerRow = table.querySelector('thead tr, tr:first-child');
      if (!headerRow) continue;
      const headers = [...headerRow.querySelectorAll('th, td')].map(h => h.textContent.trim());
      if (headers.length < 2) continue;

      const rows = table.querySelectorAll('tbody tr');
      if (rows.length < 2) continue;

      for (const row of rows) {
        const cells = row.querySelectorAll('td');
        if (cells.length < 2) continue;
        const fields = {};
        cells.forEach((cell, i) => {
          const header = (headers[i] || `col_${i}`).toLowerCase().replace(/[^a-z0-9_]/g, '_').replace(/^_+|_+$/g, '') || `col_${i}`;
          const val = cell.textContent.trim();
          if (val) fields[header] = val.slice(0, 500);
          const link = cell.querySelector('a[href]');
          if (link && link.href && !link.href.startsWith('javascript:')) {
            fields[header + '_url'] = link.href;
          }
        });
        if (Object.keys(fields).length >= 2) {
          if (!fields.name) fields.name = Object.values(fields)[0];
          items.push(fields);
        }
      }
    }
    return items;
  }

  // ── Strategy 5: Page contacts (last resort) ─────────

  function extractContacts() {
    const items = [];
    const html = document.documentElement.innerHTML;
    const text = document.body.textContent || '';

    const emails = (html.match(EMAIL_RE) || []).filter(cleanEmail);
    const phones = text.match(PHONE_INDIAN) || text.match(PHONE_INTL) || text.match(PHONE_FORMATTED) || [];
    const gsts = html.match(GST_RE) || [];
    EMAIL_RE.lastIndex = 0; PHONE_INDIAN.lastIndex = 0;
    PHONE_INTL.lastIndex = 0; PHONE_FORMATTED.lastIndex = 0; GST_RE.lastIndex = 0;

    if (!emails.length && !phones.length) return items;

    const fields = {};
    const title = document.querySelector('meta[property="og:site_name"]')?.content
      || document.querySelector('meta[property="og:title"]')?.content
      || document.title || '';
    if (title) fields.name = title.split(/[|\-::]/)[0].trim();

    const desc = document.querySelector('meta[name=description]')?.content
      || document.querySelector('meta[property="og:description"]')?.content || '';
    if (desc) fields.description = desc.slice(0, 500);

    if (emails.length) fields.email = cleanEmail(emails[0]);
    if (emails.length > 1) fields.email_2 = cleanEmail(emails[1]);
    if (phones.length) fields.phone = cleanPhone(phones[0]);
    if (phones.length > 1) fields.phone_2 = cleanPhone(phones[1]);
    if (gsts.length) fields.gst = gsts[0];

    const canonical = document.querySelector('link[rel=canonical]')?.href;
    if (canonical) fields.url = canonical;

    items.push(fields);
    return items;
  }

  // ── Main: run all strategies ────────────────────────

  function extractAll() {
    const results = { json: [], jsonld: [], listings: [], tables: [], contacts: [] };

    try { results.json = extractFromJSON(); } catch (e) { console.error('JSON extraction error:', e); }
    try { results.jsonld = extractJSONLD(); } catch (e) { console.error('JSON-LD error:', e); }
    try { results.listings = extractListings(); } catch (e) { console.error('Listings error:', e); }
    try { results.tables = extractTables(); } catch (e) { console.error('Tables error:', e); }

    const total = results.json.length + results.jsonld.length + results.listings.length + results.tables.length;
    if (total === 0) {
      try { results.contacts = extractContacts(); } catch (e) { console.error('Contacts error:', e); }
    }

    return results;
  }

  // Return extraction results
  return extractAll();
})();
