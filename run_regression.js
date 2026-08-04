/**
 * Automated regression: run all legacy scenarios against Haiku / Sonnet / Gemini
 * via the same compare HTTP endpoints the Comparison UI uses.
 *
 * Usage:
 *   node run_regression.js
 *   node run_regression.js --base=http://localhost:3000
 *   node run_regression.js --models=claude-haiku-4-5,gemini-3.1-flash-live-preview
 *   node run_regression.js --runs=3
 *   node run_regression.js --scenarios=fell_down,delayed
 *
 * Exit 0 = all intent checks passed; exit 1 = any failure / hard error.
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const ROOT = __dirname;
const SCENARIOS_PATH = fs.existsSync(path.join(ROOT, 'public', 'scenarios_legacy.json'))
  ? path.join(ROOT, 'public', 'scenarios_legacy.json')
  : path.join(ROOT, 'scenarios_legacy.json');
const AUDIO_DIR = path.join(ROOT, 'public', 'scenarios_legacy');
const REPORT_JSON = path.join(ROOT, 'regression_report.json');
const REPORT_MD = path.join(ROOT, 'regression_report.md');

const DEFAULT_MODELS = [
  'claude-haiku-4-5',
  'claude-sonnet-4-6',
  'gemini-3.1-flash-live-preview',
];

const MODEL_SHORT = {
  'claude-haiku-4-5': 'Haiku',
  'claude-sonnet-4-6': 'Sonnet',
  'gemini-3.1-flash-live-preview': 'Gemini',
};

const LATENCY_WARN_MS = 10000;

function parseArgs(argv) {
  // Prefer local server for regression — do NOT default to PUBLIC_URL (often Render/ngrok).
  const out = {
    base: process.env.REGRESSION_BASE_URL
      || ('http://localhost:' + (process.env.PORT || '3000')),
    models: DEFAULT_MODELS.slice(),
    runs: 1,
    scenarios: null,
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a.startsWith('--base=')) out.base = a.slice(7).replace(/\/$/, '');
    else if (a.startsWith('--models=')) {
      out.models = a.slice(9).split(',').map((s) => s.trim()).filter(Boolean);
    } else if (a.startsWith('--runs=')) {
      out.runs = Math.max(1, Math.min(20, parseInt(a.slice(7), 10) || 1));
    } else if (a.startsWith('--scenarios=')) {
      out.scenarios = a.slice(12).split(',').map((s) => s.trim()).filter(Boolean);
    } else if (a === '--help' || a === '-h') {
      out.help = true;
    }
  }
  return out;
}

function tokenizeTranscript(t) {
  return String(t || '')
    .toLowerCase()
    .replace(/[^\w\s']/g, ' ')
    .split(/\s+/)
    .filter(Boolean);
}

/** Same Levenshtein WER as public/index.html wordErrorRate. */
function wordErrorRate(ref, hyp) {
  const r = tokenizeTranscript(ref);
  const h = tokenizeTranscript(hyp);
  if (!r.length) return h.length ? 1 : 0;
  const d = Array.from({ length: r.length + 1 }, (_, i) => {
    const row = new Array(h.length + 1).fill(0);
    row[0] = i;
    return row;
  });
  for (let j = 0; j <= h.length; j++) d[0][j] = j;
  for (let i = 1; i <= r.length; i++) {
    for (let j = 1; j <= h.length; j++) {
      d[i][j] = Math.min(
        d[i - 1][j - 1] + (r[i - 1] === h[j - 1] ? 0 : 1),
        d[i - 1][j] + 1,
        d[i][j - 1] + 1
      );
    }
  }
  return d[r.length][h.length] / r.length;
}

function normalizeIntent(intent) {
  const t = String(intent || '').trim().toUpperCase().replace(/[\s-]+/g, '_');
  const map = {
    SAFE: 'SAFE_CONFIRMATION',
    SAFE_CONFIRMATION: 'SAFE_CONFIRMATION',
    DELAYED: 'DELAYED_CHECKIN',
    DELAYED_CHECKIN: 'DELAYED_CHECKIN',
    REQUEST_HUMAN: 'REQUEST_HUMAN',
    UNSAFE: 'REQUEST_HUMAN',
    UNRECOGNISED: 'UNRECOGNISED',
    UNRECOGNIZED: 'UNRECOGNISED',
    AMBIGUOUS: 'UNRECOGNISED',
  };
  // lowercase keys from Gemini JSON path
  const lower = String(intent || '').trim().toLowerCase();
  if (lower === 'safe') return 'SAFE_CONFIRMATION';
  if (lower === 'delayed') return 'DELAYED_CHECKIN';
  if (lower === 'request_human' || lower === 'unsafe') return 'REQUEST_HUMAN';
  if (lower === 'ambiguous' || lower === 'unrecognised' || lower === 'unrecognized') {
    return 'UNRECOGNISED';
  }
  return map[t] || t || null;
}

function mimeForFile(filename) {
  const ext = path.extname(filename).toLowerCase();
  if (ext === '.wav') return 'audio/wav';
  if (ext === '.mp3') return 'audio/mpeg';
  if (ext === '.webm') return 'audio/webm';
  if (ext === '.ogg') return 'audio/ogg';
  if (ext === '.m4a') return 'audio/mp4';
  return 'application/octet-stream';
}

function loadScenarios(filterIds) {
  if (!fs.existsSync(SCENARIOS_PATH)) {
    throw new Error('Missing ' + SCENARIOS_PATH);
  }
  const data = JSON.parse(fs.readFileSync(SCENARIOS_PATH, 'utf8'));
  let list = Array.isArray(data.scenarios) ? data.scenarios : [];
  if (filterIds && filterIds.length) {
    const set = new Set(filterIds);
    list = list.filter((s) => set.has(s.id));
  }
  if (!list.length) throw new Error('No scenarios to run');
  return list;
}

function isGeminiModel(modelId) {
  return String(modelId).indexOf('gemini') === 0;
}

async function compareAudioStt(base, audioBuf, contentType) {
  const res = await axios.post(base + '/compare-audio-stt', audioBuf, {
    headers: { 'Content-Type': contentType },
    maxBodyLength: Infinity,
    maxContentLength: Infinity,
    timeout: 120000,
    validateStatus: () => true,
  });
  if (res.status >= 400) {
    const err = new Error((res.data && res.data.error) || ('STT HTTP ' + res.status));
    err.detail = res.data && res.data.error_detail;
    throw err;
  }
  return res.data;
}

async function compareIntent(base, transcript, model, runs, expected, realSttMs) {
  const payload = {
    transcript: transcript,
    models: [model],
    runs: runs,
    expected: expected,
  };
  if (realSttMs != null) payload.real_stt_ms = realSttMs;
  const res = await axios.post(base + '/compare-intent', payload, {
    headers: { 'Content-Type': 'application/json' },
    timeout: 180000,
    validateStatus: () => true,
  });
  if (res.status >= 400) {
    const err = new Error((res.data && res.data.error) || ('intent HTTP ' + res.status));
    err.detail = res.data && res.data.error_detail;
    throw err;
  }
  return (res.data.results && res.data.results[model]) || res.data;
}

async function compareGeminiAudio(base, audioBuf, contentType, modelId) {
  const res = await axios.post(
    base + '/compare-gemini-audio?model=' + encodeURIComponent(modelId),
    audioBuf,
    {
      headers: {
        'Content-Type': contentType,
        'X-Compare-Model': modelId,
      },
      maxBodyLength: Infinity,
      maxContentLength: Infinity,
      timeout: 180000,
      validateStatus: () => true,
    }
  );
  if (res.status >= 400 || (res.data && res.data.ok === false)) {
    const err = new Error((res.data && res.data.error) || ('gemini-audio HTTP ' + res.status));
    err.detail = res.data && res.data.error_detail;
    throw err;
  }
  return res.data;
}

function pickIntentFromResult(row) {
  if (!row) return null;
  if (row.intent) return normalizeIntent(row.intent);
  const okRun = (row.runs || []).find((r) => r && r.ok && r.intent);
  return okRun ? normalizeIntent(okRun.intent) : null;
}

function pickLatencyMs(row, sttMs) {
  if (!row) return null;
  if (row.avg_latency != null && Number.isFinite(Number(row.avg_latency))) {
    return Number(row.avg_latency);
  }
  const okRun = (row.runs || []).find((r) => r && r.ok);
  if (okRun && okRun.latency != null) return Number(okRun.latency);
  if (sttMs != null && okRun && okRun.llm_ms != null) {
    return Number(sttMs) + Number(okRun.llm_ms) + Number(okRun.tts_ms || 0);
  }
  return null;
}

function pad(s, n) {
  s = String(s);
  if (s.length >= n) return s.slice(0, n);
  return s + ' '.repeat(n - s.length);
}

async function runOneScenarioModel(base, scenario, model, runs, audioBuf, contentType, sharedStt) {
  const expected = normalizeIntent(scenario.expectedIntent);
  const notes = [];
  let transcript = null;
  let sttMs = null;
  let intent = null;
  let latencyMs = null;
  let wer = null;
  let ok = false;
  let error = null;

  try {
    if (isGeminiModel(model)) {
      // Gemini path: /compare-gemini-audio (Deepgram STT + Gemini Live text classify)
      // Loop for --runs > 1
      const intents = [];
      const lats = [];
      let lastTx = null;
      for (let i = 0; i < runs; i++) {
        const data = await compareGeminiAudio(base, audioBuf, contentType, model);
        lastTx = (data.transcript || data.input_transcript || '').trim();
        const got = pickIntentFromResult(data);
        intents.push(got);
        const lat = pickLatencyMs(data, null);
        if (lat != null) lats.push(lat);
      }
      transcript = lastTx;
      intent = intents[intents.length - 1];
      // Majority vote if multi-run
      if (runs > 1) {
        const counts = {};
        intents.forEach((x) => {
          if (!x) return;
          counts[x] = (counts[x] || 0) + 1;
        });
        intent = Object.keys(counts).sort((a, b) => counts[b] - counts[a])[0] || intent;
      }
      latencyMs = lats.length
        ? Math.round(lats.reduce((a, b) => a + b, 0) / lats.length)
        : null;
      sttMs = null;
    } else {
      // Claude: shared Deepgram STT once, then /compare-intent
      if (!sharedStt) {
        throw new Error('shared STT missing for Claude model');
      }
      transcript = sharedStt.transcript;
      sttMs = sharedStt.sttMs;
      const data = await compareIntent(
        base,
        transcript,
        model,
        runs,
        expected,
        sttMs
      );
      intent = pickIntentFromResult(data);
      latencyMs = pickLatencyMs(data, sttMs);
      if (data && data.ok === false) {
        throw new Error(data.error || 'compare-intent returned ok:false');
      }
    }

    if (scenario.groundTruth && transcript) {
      wer = wordErrorRate(scenario.groundTruth, transcript);
    }

    ok = intent === expected;
    if (!ok) {
      notes.push(
        (MODEL_SHORT[model] || model) +
          ': got ' +
          (intent || 'null') +
          ', expected ' +
          expected
      );
    }
    if (latencyMs != null && latencyMs > LATENCY_WARN_MS) {
      notes.push('latency ' + latencyMs + 'ms > ' + LATENCY_WARN_MS + 'ms');
    }
  } catch (e) {
    error = e.message || String(e);
    ok = false;
    notes.push((MODEL_SHORT[model] || model) + ' ERROR: ' + error);
  }

  return {
    scenarioId: scenario.id,
    label: scenario.label,
    model: model,
    expectedIntent: expected,
    intent: intent,
    pass: ok,
    transcript: transcript,
    wer: wer,
    sttMs: sttMs,
    latencyMs: latencyMs,
    notes: notes.join('; '),
    error: error,
  };
}

function buildTable(scenarios, models, cells) {
  const shortNames = models.map((m) => MODEL_SHORT[m] || m);
  const colW = Math.max(12, ...shortNames.map((s) => s.length));
  const labelW = Math.min(
    40,
    Math.max(20, ...scenarios.map((s) => String(s.label).length))
  );

  let header =
    pad('Scenario', labelW) +
    ' | ' +
    shortNames.map((s) => pad(s, colW)).join(' | ') +
    ' | Notes';
  let sep =
    '-'.repeat(labelW) +
    '-|-' +
    shortNames.map(() => '-'.repeat(colW)).join('-|-') +
    '-|------';

  const lines = [header, sep];
  for (const sc of scenarios) {
    const marks = models.map((m) => {
      const c = cells[sc.id + '|' + m];
      if (!c) return pad('?', colW);
      if (c.error) return pad('ERR', colW);
      return pad(c.pass ? '✓' : '✗', colW);
    });
    const notes = models
      .map((m) => (cells[sc.id + '|' + m] && cells[sc.id + '|' + m].notes) || '')
      .filter(Boolean)
      .join(' · ');
    lines.push(
      pad(sc.label, labelW) +
        ' | ' +
        marks.join(' | ') +
        ' | ' +
        notes
    );
  }
  return lines.join('\n');
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    console.log(`Usage: node run_regression.js [options]

Options:
  --base=URL          Server base (default: http://localhost:$PORT)
  --models=a,b,c      Models to test (default: Haiku, Sonnet, Gemini)
  --runs=N            Runs per scenario×model (default: 1)
  --scenarios=id,id   Subset of scenario ids from scenarios_legacy.json
  --help              Show this help

Examples:
  node run_regression.js
  node run_regression.js --models=claude-haiku-4-5 --scenarios=fell_down,delayed
  node run_regression.js --runs=3 --base=http://localhost:3000
`);
    process.exit(0);
  }
  const scenarios = loadScenarios(args.scenarios);
  const models = args.models;
  const base = args.base.replace(/\/$/, '');

  console.log('=== REGRESSION TEST ===');
  console.log('Base:     ', base);
  console.log('Models:   ', models.join(', '));
  console.log('Runs:     ', args.runs);
  console.log('Scenarios:', scenarios.length);
  console.log('');

  // Health check
  try {
    await axios.get(base + '/health', { timeout: 15000 });
  } catch (e) {
    console.error('Cannot reach', base + '/health —', e.message);
    console.error('Start the server or pass --base=https://your-host');
    process.exit(1);
  }

  const cells = {};
  const allResults = [];
  const started = new Date();

  for (const scenario of scenarios) {
    const audioPath = path.join(AUDIO_DIR, scenario.audioFile);
    if (!fs.existsSync(audioPath)) {
      console.error('Missing audio:', audioPath);
      for (const model of models) {
        const cell = {
          scenarioId: scenario.id,
          label: scenario.label,
          model: model,
          expectedIntent: scenario.expectedIntent,
          intent: null,
          pass: false,
          notes: 'missing audio file: ' + scenario.audioFile,
          error: 'missing audio',
        };
        cells[scenario.id + '|' + model] = cell;
        allResults.push(cell);
      }
      continue;
    }

    const audioBuf = fs.readFileSync(audioPath);
    const contentType = mimeForFile(scenario.audioFile);
    console.log('→', scenario.label, '(' + scenario.audioFile + ')');

    // Shared Deepgram STT for Claude models
    let sharedStt = null;
    const needsClaude = models.some((m) => !isGeminiModel(m));
    if (needsClaude) {
      try {
        const stt = await compareAudioStt(base, audioBuf, contentType);
        sharedStt = {
          transcript: (stt.transcript || '').trim(),
          sttMs: stt.sttMs,
          confidence: stt.confidence,
        };
        if (!sharedStt.transcript) {
          throw new Error('empty Deepgram transcript');
        }
        console.log(
          '  STT:',
          JSON.stringify(sharedStt.transcript).slice(0, 100),
          '|',
          sharedStt.sttMs + 'ms'
        );
      } catch (e) {
        console.error('  STT failed:', e.message);
        sharedStt = null;
      }
    }

    for (const model of models) {
      process.stdout.write('  ' + (MODEL_SHORT[model] || model) + '… ');
      const cell = await runOneScenarioModel(
        base,
        scenario,
        model,
        args.runs,
        audioBuf,
        contentType,
        sharedStt
      );
      // If Claude STT failed entirely, mark error
      if (!isGeminiModel(model) && !sharedStt && !cell.error) {
        cell.pass = false;
        cell.error = 'STT failed';
        cell.notes = 'STT failed before intent';
      }
      cells[scenario.id + '|' + model] = cell;
      allResults.push(cell);
      const werStr =
        cell.wer != null ? ' WER=' + (cell.wer * 100).toFixed(1) + '%' : '';
      const latStr = cell.latencyMs != null ? ' lat=' + cell.latencyMs + 'ms' : '';
      console.log(
        (cell.pass ? '✓' : '✗') +
          ' ' +
          (cell.intent || '—') +
          werStr +
          latStr +
          (cell.notes ? ' | ' + cell.notes : '')
      );
    }
    console.log('');
  }

  const passed = allResults.filter((r) => r.pass).length;
  const total = allResults.length;
  const failures = allResults.filter((r) => !r.pass);
  const pct = total ? ((passed / total) * 100).toFixed(1) : '0.0';

  const table = buildTable(scenarios, models, cells);
  const dateStr = started.toISOString().replace('T', ' ').slice(0, 16);

  const summaryLines = [
    '=== REGRESSION TEST RESULTS ===',
    'Date: ' + dateStr,
    'Base: ' + base,
    'Runs/scenario/model: ' + args.runs,
    '',
    table,
    '',
    'TOTAL: ' + passed + '/' + total + ' passed (' + pct + '%)',
  ];
  if (failures.length) {
    summaryLines.push(
      'FAILURES: ' +
        failures.length +
        ' (' +
        failures
          .map(
            (f) =>
              f.label +
              ' — ' +
              (MODEL_SHORT[f.model] || f.model) +
              (f.notes ? ': ' + f.notes : '')
          )
          .join('; ') +
        ')'
    );
  } else {
    summaryLines.push('FAILURES: 0');
  }

  const summary = summaryLines.join('\n');
  console.log(summary);

  const report = {
    date: started.toISOString(),
    base: base,
    runs: args.runs,
    models: models,
    scenarios: scenarios.map((s) => ({
      id: s.id,
      label: s.label,
      expectedIntent: s.expectedIntent,
      audioFile: s.audioFile,
    })),
    results: allResults,
    passed: passed,
    total: total,
    passRate: total ? passed / total : 0,
    failures: failures.map((f) => ({
      scenarioId: f.scenarioId,
      label: f.label,
      model: f.model,
      expected: f.expectedIntent,
      got: f.intent,
      notes: f.notes,
      error: f.error,
      wer: f.wer,
      latencyMs: f.latencyMs,
    })),
  };

  // Markdown report with a real table (plus plain summary block)
  const mdShort = models.map((m) => MODEL_SHORT[m] || m);
  const mdHeader = '| Scenario | ' + mdShort.join(' | ') + ' | Notes |';
  const mdSep = '|---|' + mdShort.map(() => '---').join('|') + '|---|';
  const mdRows = scenarios.map((sc) => {
    const marks = models.map((m) => {
      const c = cells[sc.id + '|' + m];
      if (!c) return '?';
      if (c.error) return 'ERR';
      return c.pass ? '✓' : '✗';
    });
    const notes = models
      .map((m) => (cells[sc.id + '|' + m] && cells[sc.id + '|' + m].notes) || '')
      .filter(Boolean)
      .join(' · ')
      .replace(/\|/g, '/');
    return '| ' + sc.label + ' | ' + marks.join(' | ') + ' | ' + notes + ' |';
  });
  const md = [
    '# Regression Test Results',
    '',
    '- **Date:** ' + dateStr,
    '- **Base:** `' + base + '`',
    '- **Runs/scenario/model:** ' + args.runs,
    '- **Total:** ' + passed + '/' + total + ' passed (' + pct + '%)',
    '',
    mdHeader,
    mdSep,
    ...mdRows,
    '',
    '## Failures',
    '',
    failures.length
      ? failures
          .map(
            (f) =>
              '- **' +
              f.label +
              '** — ' +
              (MODEL_SHORT[f.model] || f.model) +
              ': expected `' +
              f.expectedIntent +
              '`, got `' +
              (f.intent || f.error || 'null') +
              '`' +
              (f.wer != null ? ' (WER ' + (f.wer * 100).toFixed(1) + '%)' : '')
          )
          .join('\n')
      : '_None_',
    '',
    '## Console summary',
    '',
    '```',
    summary,
    '```',
    '',
  ].join('\n');

  fs.writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2));
  fs.writeFileSync(REPORT_MD, md);
  console.log('\nWrote', REPORT_JSON);
  console.log('Wrote', REPORT_MD);

  process.exit(failures.length ? 1 : 0);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
