// Front-end: upload images, then poll the API every 1.5s ("real time" feed + cluster view).
const $ = (sel) => document.querySelector(sel);
const FILTERS = JSON.parse(document.body.dataset.filters);
const PALETTE = ['#2563eb', '#ea580c', '#0d9488', '#c026d3', '#65a30d', '#dc2626'];
const workerColor = {};          // worker id -> colour, assigned from the sorted worker list

const esc = (s) => String(s ?? '').replace(/[&<>"']/g, (c) => `&#${c.charCodeAt(0)};`);
const time = (ts) => new Date(ts * 1000).toLocaleTimeString();

function toast(text, kind = '') {
  const el = document.createElement('div');
  el.className = `toast ${kind}`;
  el.textContent = text;
  $('#toasts').append(el);
  setTimeout(() => el.remove(), 5000);
}

// ───────────── upload (API call 1: POST /api/jobs) ─────────────
const fileInput = $('#file');
const drop = $('#drop');
fileInput.addEventListener('change', () => {
  $('#chosen').textContent = [...fileInput.files].map((f) => f.name).join(', ');
});
['dragover', 'dragenter'].forEach((ev) => drop.addEventListener(ev, (e) => { e.preventDefault(); drop.classList.add('over'); }));
['dragleave', 'drop'].forEach((ev) => drop.addEventListener(ev, () => drop.classList.remove('over')));
drop.addEventListener('drop', (e) => {
  e.preventDefault();
  fileInput.files = e.dataTransfer.files;
  fileInput.dispatchEvent(new Event('change'));
});

$('#upload').addEventListener('submit', async (e) => {
  e.preventDefault();
  const files = [...fileInput.files];
  if (!files.length) return toast('Choose at least one image', 'error');
  const filter = document.querySelector('input[name=filter]:checked').value;

  // all files are uploaded in parallel - the master handles them concurrently
  await Promise.all(files.map(async (file) => {
    const form = new FormData();
    form.append('file', file);
    form.append('filter', filter);
    try {
      const res = await fetch('/api/jobs', { method: 'POST', body: form });
      const body = await res.json();
      if (!res.ok) toast(`${file.name}: ${body.error}`, 'error');
      else if (body.cached) toast(`⚡ ${file.name}: served from the Redis cache in ${body.elapsed_ms} ms`);
      else toast(`${file.name}: ${body.total_tiles} tiles published to Kafka`);
    } catch (err) {
      toast(`${file.name}: ${err}`, 'error');
    }
  }));
  fileInput.value = '';
  $('#chosen').textContent = '';
  refreshFeed();
});

// ───────────── feed (API calls 2 & 3: job status + result image) ─────────────
const cards = new Map();   // job id -> {el, ...} so cards are updated, not re-created

function createCard(job) {
  const el = document.createElement('article');
  el.className = 'card';
  el.innerHTML = `
    <div class="media"><img alt=""><div class="grid"></div><span class="flag"></span></div>
    <div class="body">
      <div class="row"><strong class="name"></strong><span class="badge"></span></div>
      <div class="meta"></div>
      <div class="bar"><span></span></div>
      <div class="foot"></div>
    </div>`;
  const card = { el, showing: null, gridBuilt: false };
  el.querySelector('.name').textContent = job.filename;
  // click the image to compare before / after
  el.querySelector('.media').addEventListener('click', () => {
    if (card.done) show(card, card.showing === 'result' ? 'original' : 'result');
  });
  return card;
}

function show(card, which) {
  if (card.showing === which) return;
  card.showing = which;
  if (which === 'result') card.resultShown = true;
  card.el.querySelector('img').src = `/api/jobs/${card.jobId}/${which === 'result' ? 'result' : 'preview'}`;
  card.el.querySelector('.flag').textContent = which === 'result' ? 'after · click for before' : 'original';
}

// Overlay a cols x rows grid on the image; each finished tile gets its worker's colour.
function buildGrid(card, job) {
  const img = card.el.querySelector('img');
  const grid = card.el.querySelector('.grid');
  const place = () => {               // match the grid to the image as drawn (object-fit: contain)
    const box = img.parentElement.getBoundingClientRect();
    const scale = Math.min(box.width / job.width, box.height / job.height);
    const w = job.width * scale, h = job.height * scale;
    Object.assign(grid.style, {
      width: `${w}px`, height: `${h}px`, left: `${(box.width - w) / 2}px`, top: `${(box.height - h) / 2}px`,
      gridTemplateColumns: `repeat(${job.cols}, 1fr)`, gridTemplateRows: `repeat(${job.rows}, 1fr)`,
    });
  };
  img.addEventListener('load', place);
  window.addEventListener('resize', place);
  grid.innerHTML = Array.from({ length: job.total_tiles }, () => '<div></div>').join('');
  card.gridBuilt = true;
}

function updateCard(card, job) {
  card.jobId = job.job_id;
  const el = card.el;
  const cached = job.from_cache === 1;
  const status = cached ? 'cached' : job.status;
  const badge = el.querySelector('.badge');
  badge.textContent = cached ? 'cache hit' : job.status;
  badge.className = `badge s-${status}`;

  card.done = job.status === 'done';
  el.classList.toggle('done', card.done);
  if (card.showing === null) show(card, 'original');
  if (card.done && !card.resultShown) show(card, 'result');   // switch once when the job finishes

  if (!cached && !card.gridBuilt && job.total_tiles) buildGrid(card, job);
  if (card.gridBuilt) {
    [...el.querySelector('.grid').children].forEach((cell, i) => {
      const worker = job.tile_workers[i];
      cell.style.background = worker ? `${workerColor[worker] || '#888'}88` : 'transparent';
      cell.title = worker ? `tile ${i} · ${worker}` : `tile ${i} · waiting`;
    });
  }

  const pct = cached ? 100 : (job.total_tiles ? (100 * job.processed_tiles) / job.total_tiles : 0);
  el.querySelector('.bar span').style.width = `${pct}%`;
  el.querySelector('.meta').textContent = cached
    ? `${FILTERS[job.filter]} · served straight from Redis`
    : `${FILTERS[job.filter]} · ${job.width}×${job.height} · ${job.processed_tiles}/${job.total_tiles} tiles`;
  const split = Object.entries(job.partition_split || {}).map(([p, n]) => `P${p}: ${n}`).join(' · ');
  el.querySelector('.foot').textContent = cached
    ? `⚡ ${job.duration_ms} ms, no Kafka, no workers`
    : `${split ? `image_tasks ${split}` : ''}${job.duration_ms ? ` · done in ${(job.duration_ms / 1000).toFixed(1)} s` : ''}`;
}

async function refreshFeed() {
  const res = await fetch('/api/feed');
  if (!res.ok) return;
  const jobs = await res.json();
  const feed = $('#feed');
  $('#empty').hidden = jobs.length > 0;
  jobs.forEach((job, i) => {
    if (!cards.has(job.job_id)) cards.set(job.job_id, createCard(job));
    const card = cards.get(job.job_id);
    updateCard(card, job);
    if (feed.children[i] !== card.el) feed.insertBefore(card.el, feed.children[i] || null);
  });
}

// ───────────── cluster panel ─────────────
function renderWorkers(workers) {
  workers.slice().sort((a, b) => a.worker_id.localeCompare(b.worker_id)).forEach((w, i) => {
    workerColor[w.worker_id] = PALETTE[i % PALETTE.length];
  });
  $('#workers').innerHTML = workers.length ? `<table>
    <tr><th>worker</th><th>group</th><th>partitions</th><th>TTL</th><th>tiles</th></tr>
    ${workers.map((w) => `<tr>
      <td><span class="dot ${w.status}"></span><span class="swatch" style="background:${workerColor[w.worker_id]}"></span>${esc(w.worker_id)}</td>
      <td class="muted">${esc(w.group)}</td>
      <td>${w.status === 'alive' ? esc(w.partitions.split(',').filter(Boolean).map((p) => `P${p}`).join(', ') || '-') : `<b>${w.status}</b>`}</td>
      <td>${w.status === 'alive' ? `${w.ttl}s` : `${Math.round(w.last_seen_ago)}s ago`}</td>
      <td>${w.tiles_processed}</td></tr>`).join('')}
  </table>` : '<p class="muted">No workers have sent a heartbeat yet.</p>';
}

function renderKafka(kafka) {
  if (!kafka.topics) { $('#partitions').innerHTML = '<p class="muted">Waiting for Kafka…</p>'; return; }
  const rows = Object.entries(kafka.topics).flatMap(([topic, parts]) => parts.map((p) => `<tr>
    <td>${esc(topic)}[P${p.partition}]</td>
    <td>${p.leader >= 0 ? `broker ${p.leader}` : '<b>none</b>'}</td>
    <td>${p.replicas.join(', ')}</td>
    <td>${p.isr.join(', ')}</td></tr>`));
  $('#partitions').innerHTML = `<p class="muted">Brokers online: ${kafka.brokers.join(', ') || 'none'}</p>
    <table><tr><th>partition</th><th>leader</th><th>replicas</th><th>ISR</th></tr>${rows.join('')}</table>`;

  $('#groups').innerHTML = kafka.groups.map((g) => `<div class="group">
      <div class="group-head"><b>${esc(g.group)}</b>
        <span class="muted">coordinator: broker ${g.coordinator ?? '?'} (__consumer_offsets P${g.offsets_partition})</span></div>
      ${g.error ? `<p class="lag">unavailable: ${esc(g.error)}</p>` : `
      <table><tr><th>${esc(g.topic)}</th><th>owner</th><th>committed</th><th>end</th><th>lag</th></tr>
      ${g.partitions.map((p) => `<tr><td>P${p.partition}</td><td>${esc(p.owner || '-')}</td>
        <td>${p.committed >= 0 ? p.committed : '-'}</td><td>${p.end}</td>
        <td class="${p.lag > 0 ? 'lag' : ''}">${p.lag}</td></tr>`).join('')}
      </table>`}</div>`).join('');
}

async function refreshCluster() {
  const res = await fetch('/api/cluster');
  if (!res.ok) return;
  const c = await res.json();
  renderWorkers(c.workers || []);
  renderKafka(c.kafka || {});
  $('#redis').innerHTML = c.redis && c.redis.master
    ? `<table><tr><th>master</th><td><b>${esc(c.redis.master)}</b></td></tr>
       <tr><th>replicas</th><td>${esc(c.redis.replicas.join(', ') || 'none')}</td></tr></table>`
    : '<p class="muted">Waiting for Sentinel…</p>';
  $('#events').innerHTML = (c.events || []).map((e) =>
    `<li class="${e.level}"><span class="t">${time(e.ts)}</span><b>${esc(e.source)}</b> ${esc(e.message)}</li>`).join('');
  const s = c.stats || {};
  $('#stats').innerHTML = [['cache hits', s.cache_hits], ['cache misses', s.cache_misses], ['tiles processed', s.tiles_processed]]
    .map(([label, v]) => `<div class="stat"><b>${v || 0}</b><span>${label}</span></div>`).join('');
}

async function loop() {
  try { await refreshCluster(); await refreshFeed(); } catch (e) { /* master restarting */ }
  setTimeout(loop, 1500);
}
loop();
