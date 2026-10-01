/* 日志搜索 Logs */
Components.init('logs');
const C = Components;

let currentJob = '';

async function render() {
  if (!currentJob) return;
  const params = new URLSearchParams();
  const search = document.getElementById('search').value.trim();
  const stage = document.getElementById('stage').value;
  const level = document.getElementById('level').value;
  const startMs = toMs(document.getElementById('start').value);
  // End is inclusive: push it to the end of the chosen second so a line logged
  // at e.g. 12:00:00.800 still matches an "until 12:00:00" bound.
  const endMs = (() => {
    const v = document.getElementById('end').value;
    if (!v) return null;
    const ms = toMs(v);
    return ms == null ? null : ms + 999;
  })();
  if (search) params.set('search', search);
  if (stage) params.set('stage', stage);
  if (level) params.set('level', level);
  if (startMs != null) params.set('start_ms', startMs);
  if (endMs != null) params.set('end_ms', endMs);
  params.set('limit', '1000');

  let d;
  try { d = await API.get('/api/jobs/' + currentJob + '/logs?' + params.toString()); } catch (e) { return; }
  const records = d.records || [];
  document.getElementById('log-count').textContent =
    `匹配 ${records.length} / ${d.total} 条 · 扫描 ${d.scanned} 条 lines`;

  document.getElementById('logs').innerHTML = records.length
    ? records.map(r => `<div class="log-line lvl-${C.esc(r.level || '')}">
        <span class="ts">${C.fmtDateTime(r.ts_ms)}</span>
        <span class="lvl">${C.esc(r.level)}</span>
        <span class="stage">${C.esc(r.stage)}</span>
        <span class="msg">${C.esc(r.message)}</span>
        ${r.worker_id ? `<span class="stage">${C.esc(r.worker_id)}</span>` : ''}
      </div>`).join('')
    : C.empty('无匹配日志 No matching logs');
}

function toMs(v) {
  if (!v) return null;
  const ms = new Date(v).getTime();
  return isNaN(ms) ? null : ms;
}

C.jobPicker('job-picker', (id) => { currentJob = id; render(); });
document.getElementById('refresh').addEventListener('click', render);
document.getElementById('search').addEventListener('input', debounce(render, 400));
document.getElementById('stage').addEventListener('change', render);
document.getElementById('level').addEventListener('change', render);
document.getElementById('start').addEventListener('change', render);
document.getElementById('end').addEventListener('change', render);

function debounce(fn, ms) {
  let t; return () => { clearTimeout(t); t = setTimeout(fn, ms); };
}

C.poll(render, 3000).start();
