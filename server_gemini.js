const fs = require('fs');
const path = require('path');

require('dotenv').config();

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');
const twilio = require('twilio');
const { insertCallStart, updateCallEnd, updateRecordingUrl, updateCallSessionState, listCalls, getCallBySid } = require('./db');
const { createNoiseEstimator } = require("./noiseEstimator");
const { createSileroVad } = require("./sileroVad");
const { frameStatsMulaw, frameStatsPcm16 } = require("./audioLevel");
const { createDeepFilter, preloadDeepFilter, isDeepFilterReady } = require('./deepFilterWrapper');
const { createRnnoise, preloadRnnoise, isRnnoiseReady } = require('./rnnoiseWrapper');
const { appendTestRow } = require('./testLogger');

// Denoise motoru secimi. server.js'ten farkli olarak burada varsayilan "deepfilter" —
// bu dosyada DEEPFILTER_ENABLED bypass'i hic olmadi, mevcut davranis korunuyor.
const DENOISE_ENGINE = (process.env.DENOISE_ENGINE || 'deepfilter').toLowerCase();
const DENOISE_LABEL = DENOISE_ENGINE === 'rnnoise' ? 'RNNoise' : 'DeepFilterNet3';

function isDenoiserReady() {
  if (DENOISE_ENGINE === 'off') return false;
  return DENOISE_ENGINE === 'rnnoise' ? isRnnoiseReady() : isDeepFilterReady();
}

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ noServer: true });

app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(express.static(path.join(__dirname, 'public')));

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-3.1-flash-live-preview';
const GEMINI_REST_MODEL = process.env.GEMINI_REST_MODEL || 'gemini-3.6-flash';

function pcm16ToWav(pcm, rate) {
  const h = Buffer.alloc(44);
  h.write('RIFF', 0); h.writeUInt32LE(36 + pcm.length, 4); h.write('WAVE', 8);
  h.write('fmt ', 12); h.writeUInt32LE(16, 16); h.writeUInt16LE(1, 20); h.writeUInt16LE(1, 22);
  h.writeUInt32LE(rate, 24); h.writeUInt32LE(rate * 2, 28); h.writeUInt16LE(2, 32); h.writeUInt16LE(16, 34);
  h.write('data', 36); h.writeUInt32LE(pcm.length, 40);
  return Buffer.concat([h, pcm]);
}

// Live API'nin inputTranscription yan kanali gurultude sessizce pes edebiliyor —
// model sesi anlayip dogru cevabi verse bile kayda ne soylendigi gecmiyor.
// O yuzden transcript bos kaldiginda ayni sesi REST uzerinden bir kez daha transkribe ediyoruz.
async function transcribeWithRest(pcm, rate) {
  const url = 'https://generativelanguage.googleapis.com/v1beta/models/'
    + GEMINI_REST_MODEL + ':generateContent?key=' + GEMINI_API_KEY;
  const body = { contents: [{ parts: [
    { text: 'Transcribe this English audio verbatim. Output ONLY the transcript text, nothing else. If nothing is intelligible, output exactly: (unintelligible)' },
    { inline_data: { mime_type: 'audio/wav', data: pcm16ToWav(pcm, rate).toString('base64') } },
  ] }] };
  const res = await axios.post(url, body, { headers: { 'Content-Type': 'application/json' }, timeout: 30000 });
  const parts = (res.data.candidates[0].content.parts) || [];
  return parts.map(function (p) { return p.text || ''; }).join('').trim();
}

const twilioClient = process.env.TWILIO_ACCOUNT_SID && process.env.TWILIO_AUTH_TOKEN
  ? twilio(process.env.TWILIO_ACCOUNT_SID, process.env.TWILIO_AUTH_TOKEN)
  : null;

const amdRedirectedCalls = new Set();

function buildTwilioWebhookUrl(req, pathname) {
  let base;
  if (process.env.NGROK_HOST) {
    base = 'https://' + String(process.env.NGROK_HOST).replace(/^https?:\/\//, '');
  } else {
    base = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
  }
  base = String(base).replace(/\/$/, '');
  const qs = req.originalUrl && req.originalUrl.indexOf('?') !== -1
    ? req.originalUrl.slice(req.originalUrl.indexOf('?'))
    : '';
  return base + pathname + qs;
}

function validateTwilioSignature(req, res, url) {
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  if (!authToken) {
    console.warn('[Twilio] TWILIO_AUTH_TOKEN not set — rejecting request');
    res.status(403).send('Forbidden');
    return false;
  }
  const signature = req.headers['x-twilio-signature'];
  if (!signature) {
    console.warn('[Twilio] Missing signature — rejecting request');
    res.status(403).send('Forbidden');
    return false;
  }
  const params = req.body && typeof req.body === 'object' ? req.body : {};
  const valid = twilio.validateRequest(authToken, signature, url, params);
  if (!valid) {
    console.warn('[Twilio] Invalid signature — rejecting request from', req.ip);
    res.status(403).send('Forbidden');
    return false;
  }
  return true;
}

const SYSTEM_PROMPT = `CRITICAL LANGUAGE RULE: This call is in English only. The worker speaks English. Always transcribe and interpret audio as English, even if the audio quality is poor, words are unclear, or the accent is unfamiliar. Never output text in Korean, Japanese, Chinese, or any language other than English. If you cannot understand the worker's English, respond with the UNCLEAR script in English.

SYSTEM: language=en-US. All input and output in English only.

You are an automated safety monitoring system for Aware360...

Your persona: "Aware360 Monitoring Center" — efficient, neutral, direct.

When the worker speaks:
1. Listen carefully to their response
2. Classify their intent: safe confirmation, needs more time, wants human agent, or unclear
3. Respond appropriately and concisely

Rules:
- SAFE: "Got it — I've reset your safety timer. Stay safe."
- DELAYED: "Understood. I've extended your check-in timer. Please check in when ready."  
- HUMAN AGENT: "Of course. I'm flagging this for a live agent — someone will call you back shortly."
- UNCLEAR: Always respond with exactly this phrase, nothing else: "Sorry, I didn't catch that. Could you repeat?"

CRITICAL SAFETY RULE: If the worker mentions ANY physical injury, pain, fall, or distress — even with positive statements — OR asks for a human/agent/help — immediately escalate to a human agent with the HUMAN AGENT reply.

ESCALATION OVERRIDES NOISE (ABSOLUTE PRIORITY): If the words you DID hear indicate injury, pain, danger, distress, "help"/"SOS", or a request for a human/agent, you MUST escalate immediately with the HUMAN AGENT reply — even if an [ACOUSTIC MONITOR] report says the audio was noisy, had gaps, or had loud bursts. In that case NEVER ask the worker to repeat and NEVER downgrade to "Sorry, it's a bit noisy...". Noise can only DELETE words (turning an unsafe reply into a clean-sounding safe one); it never invents danger or human-request words. Therefore the noise re-ask flow below applies ONLY when the reply you heard would otherwise be "safe" or "needs more time" — never when it is unsafe or a human-agent request.

Keep responses short (under 15 words). Be direct and professional.

ACOUSTIC MONITOR:
Some worker replies are preceded by a message starting with [ACOUSTIC MONITOR]. That message is NOT spoken by the worker — it is an automatic measurement of the audio of their reply. It only appears when the measurement found a real problem, so never dismiss it.

CRITICAL — coherence is not evidence: a reply that sounds complete and sensible is NOT proof that nothing was lost. Deleting one word from "I am not safe" leaves "I am safe", which sounds perfectly coherent and means the opposite. The most dangerous losses are exactly the ones that leave a clean sentence behind. Never reason "what I heard makes sense, so nothing is missing".

When you receive an [ACOUSTIC MONITOR] report:
- If it reports a noise burst LOUDER than the worker's voice, or a gap, assume one or more words were destroyed at that position. If the words you did hear would classify the reply as safe or as needing more time, you must NOT act on that classification — a negation ("not", "never", "can't", "don't") or a number may have been destroyed. Say exactly: "Sorry, it's a bit noisy. Can you repeat that clearly?"
- If you could not make out ANY words at all, do not guess, do not classify, and do NOT escalate. Say exactly: "Sorry, I didn't catch that. Could you repeat?"

Never read the measurements out loud, never mention the monitor, and never mention that words were missing.

IMPORTANT: Only greet the worker ONCE at the very start of the conversation. Do not repeat the greeting.
CRITICAL: After the greeting, do NOT speak, acknowledge, or produce ANY output until the worker speaks. Do not say "standing by", "acknowledged", "waiting", or anything else. Complete silence until worker responds.`;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay?`;

let cachedGreetingChunks = null;
let cachedGreetingMimeType = null;

const GREETING_DIR = path.join(__dirname, "audio");
const GREETING_FILE = path.join(GREETING_DIR, "greeting_24k.pcm");

let GREETING_PCM = null;
let GREETING_ULAW = null;
let GREETING_PCM_MS = 0;

function loadGreetingFromDisk() {
  try {
    const buf = fs.readFileSync(GREETING_FILE);
    if (buf.length < 24000) throw new Error("dosya cok kisa: " + buf.length + " bayt");
    GREETING_PCM = buf;
    GREETING_PCM_MS = Math.round(buf.length / 48);
    GREETING_ULAW = pcmToMulaw(buf);
    console.log("[Greeting] Disk cache yuklendi:", buf.length, "bayt /", GREETING_PCM_MS + "ms");
    return true;
  } catch (e) {
    GREETING_PCM = null; GREETING_ULAW = null; GREETING_PCM_MS = 0;
    console.log("[Greeeting] Disk cache yok - ilk aramada uretilip kaydedilecek (" + e.message + ")");
    return false;
  }
}

function saveGreetingToDisk(b64Chunks) {
  try {
    const buf = Buffer.concat(b64Chunks.map(function (c) { return Buffer.from(c, 'base64'); }));
    if (buf.length < 24000) {
      console.warn('[Greeting] Kayit atlandi — ses cok kisa:', buf.length, 'bayt');
      return;
    }
    if (!fs.existsSync(GREETING_DIR)) fs.mkdirSync(GREETING_DIR, { recursive: true });
    fs.writeFileSync(GREETING_FILE, buf);
    console.log('[Greeting] Kaydedildi ->', GREETING_FILE, '(' + buf.length + ' bayt)');
    loadGreetingFromDisk();
  } catch (e) {
    console.error("[Greeting] kaydedilemedi:", e.message);
  }
}

wss.on('connection', function(ws) {
  console.log('[Gemini WS] Client connected');

  let geminiWs = null;
  let aiTranscriptBuffer = '';
  let sessionState = 'GREETING';
  let clientMimeType = 'audio/webm';
  let actualSampleRate = 16000;
  let greetingSent = false;
  let isCachingGreeting = false;
  let turnHadAudio = false;
  let greetingAnnounced = false; // guards the conversation-history bubble (send once per session)

  function announceGreeting() {
    if (greetingAnnounced) return;
    greetingAnnounced = true;
    send({ type: 'greeting', text: WORKER_INTRO, audio: null });
  }

  function primeGeminiGreetingContext(introText) {
    if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;
    geminiWs.send(JSON.stringify({
      client_content: {
        turns: [
          { role: 'user', parts: [{ text: '[SYSTEM] The greeting has already been played to the worker from a recording. Do not greet again. Stay completely silent until the worker speaks.' }] }
        ],
        turn_complete: false
      }
    }));
  }
  let workerSpeechEndTime = null; let turnStartTime = null;
  let workerSpeechActive = false;
  let workerSilenceTimer = null;
  let lastAudioSentTime = null;
  let setupTime = null;
  let firstAudioTime = null;
  let listeningTransitionTimer = null;
  const TURN_SILENCE_MS = 900;
  const noise = createNoiseEstimator({ frameMs: 20 });
  const vad = createSileroVad({ sampleRate: 16000 });
  let audioClockMs = 0;
  let vadChain = Promise.resolve();
  let activityOpen = false;
  let expectUpload = 0;
  let turnHadTranscript = false;  // bu turn icin inputTranscription geldi mi
  let lastTurnMetrics = null;     // gelmediyse kayda ne yazacagiz

  let dfEnabled = true;        // istemcinin istedigi durum
  let deepFilter = null;       // session boyunca state tasiyan filtre ornegi
  let currentAudioFile = '';   // CSV'de hangi kayit oldugu
  let turnCloseTime = null;    // activityEnd ani — total sure buradan sayilir
  let turnTtfaMs = null;
  let turnDfMs = null;
  let turnWorkerText = '';
  let lastTurnPcm = null;      // REST fallback transkripsiyonu icin bu turn'un sesi

  function ensureDeepFilter() {
    if (!dfEnabled) return null;
    if (!isDenoiserReady()) {
      console.warn('[' + DENOISE_LABEL + '] model hazir degil — bu turn ham ses gidiyor');
      return null;
    }
    if (!deepFilter) {
      deepFilter = DENOISE_ENGINE === 'rnnoise'
        ? createRnnoise({ inputRate: 16000, outputRate: 16000 })
        : createDeepFilter({ inputRate: 16000, outputRate: 16000 });
    }
    return deepFilter;
  }

  function send(obj) {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  function scheduleListeningTransition(reason, delayMs) {
    if (listeningTransitionTimer) {
      clearTimeout(listeningTransitionTimer);
      listeningTransitionTimer = null;
    }
    listeningTransitionTimer = setTimeout(function() {
      sessionState = 'LISTENING';
      console.log('[Gemini] Auto-transition to LISTENING (' + reason + ')');
      listeningTransitionTimer = null;
    }, delayMs || 2000);
  }

  function analyzePcm(buf) {
    const FRAME = 640;
    for (let off = 0; off + 2 <= buf.length; off += FRAME) {
      const sub = buf.subarray(off, Math.min(off + FRAME, buf.length));
      const t = audioClockMs;
      audioClockMs += sub.length / 32;
      const stats = frameStatsPcm16(sub);
      vadChain = vadChain.then(async function () {
        vad.processPcm16(sub);
        const p = await vad.flush();
        noise.pushFrame({ ...stats, speechProb: p }, t);
      }).catch(function (e) { console.error('[VAD] chain error:', e.message); });
    }
    return vadChain;
  }

  function closeTurn() {
    activityOpen = false;
    const spans = vad.takeSpans();
    const m = noise.endTurn({ speechSpans: spans.length ? spans : null });

    const totalSpeechMs = spans.reduce(function (s, sp) { return s + (sp.endMs - sp.startMs); }, 0);
    turnHadTranscript = false;
    lastTurnMetrics = {
      snrDb: m.snrDb, snrReliable: m.snrReliable, noisy: m.noisy, spans: spans.length,
      speechMs: Math.round(totalSpeechMs),
      gaps: m.collisions.filter(function (c) { return c.type === 'gap'; }).length,
    };

    console.log('[Gemini Noise] snr=' + (m.snrDb != null ? m.snrDb.toFixed(1) : 'n/a')
      + ' noisy=' + m.noisy + ' collisions=' + m.collisions.length + ' spans=' + spans.length
      + ' speech=' + Math.round(totalSpeechMs) + 'ms');

      if (m.collisions.length) {
      console.log('[Gemini Collisions] speechP90=' + (m.speechDbfs != null ? m.speechDbfs.toFixed(1) : 'n/a')
        + ' speechMed=' + (m.speechMedDbfs != null ? m.speechMedDbfs.toFixed(1) : 'n/a')
        + ' noiseLevel=' + (m.noiseLevelDbfs != null ? m.noiseLevelDbfs.toFixed(1) : 'n/a') + ' dBFS');
      for (const c of m.collisions) {
        const margin = m.speechMedDbfs != null ? (c.peakDbfs - m.speechMedDbfs) : null;
        console.log('   ' + String(c.type).padEnd(7)
          + ' t=' + Math.round(c.tMs) + 'ms dur=' + Math.round(c.durationMs) + 'ms'
          + ' peak=' + (c.peakDbfs != null ? c.peakDbfs.toFixed(1) : 'n/a') + 'dB'
          + (margin != null ? ' margin=' + (margin >= 0 ? '+' : '') + margin.toFixed(1) + 'dB' : ''));
      }
    }
      
      const gapCount = m.collisions.filter(function (c) { return c.type === "gap";}).length;
      const burstCount = loudOverlaps(m).length;
      const snrLow = m.snrReliable && m.snrDb != null && m.snrDb < 12;
      const degraded = gapCount > 0 || burstCount > 0 || m.noisy || snrLow;      
      send({ type: "noise_metrics", snrDb: m.snrDb, noisy: m.noisy, injected: degraded });

      if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
        if (degraded) {
          const report = buildMonitorReport(m, spans);
          console.log("[Gemini Monitor]", report);
          geminiWs.send(JSON.stringify({
            client_content: { turns: [{ role: "user", parts: [{ text: report }] }],
          turn_complete: false }
          }));
        }
        geminiWs.send(JSON.stringify({ realtimeInput: { activityEnd: {} } }));
      }
      turnStartTime = Date.now();
      firstAudioTime = null;
      turnCloseTime = Date.now();
      turnTtfaMs = null;
      turnWorkerText = '';
  }

  function connectGemini() {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;

    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on('open', function() {
      console.log('[Gemini] Connected at', Date.now());

            const setup = {
  setup: {
    model: `models/${GEMINI_MODEL}`,
    generation_config: {
      response_modalities: ['AUDIO'],
      speech_config: {
        voice_config: {
          prebuilt_voice_config: {
            voice_name: 'Charon'
          }
        }
      }
    },
    realtime_input_config: { automatic_activity_detection: { disabled: true } },
    realtimeInputConfig:   { automaticActivityDetection: { disabled: true } },
    // language_codes removed — caused WebSocket 1007 on Gemini Live setup
    input_audio_transcription: {},
    output_audio_transcription: {},
    // camelCase alias — snake_case form is silently ignored by some preview models,
    // which is why AI turns never reached the transcript buffer
    outputAudioTranscription: {},
    system_instruction: {
      parts: [{ text: SYSTEM_PROMPT }]
    }
  }
};
      geminiWs.send(JSON.stringify(setup));
    });

    geminiWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
console.log('[Gemini RAW]', JSON.stringify(msg).substring(0, 300));

        // Setup complete
        if (msg.setupComplete) {
  console.log('[Gemini] Setup complete at', Date.now());
  setupTime = Date.now();
  send({ type: 'stage', stage: 'stt', status: 'connected' });
  send({ type: 'stage', stage: 'llm', status: 'connected' });
  send({ type: 'stage', stage: 'tts', status: 'connected' });

  if (!greetingSent) {
  greetingSent = true;

  if (GREETING_PCM) {
    // Disk cache var — Gemini'ye greeting urettirme, dosyadan cal
    console.log('[Gemini] Greeting disk cache\'ten caliniyor,', GREETING_PCM_MS + 'ms');
    const CHUNK = 9600; // 200ms @ 24kHz PCM16
    for (let off = 0; off < GREETING_PCM.length; off += CHUNK) {
      send({
        type: 'audio_chunk',
        chunk: GREETING_PCM.subarray(off, off + CHUNK).toString('base64'),
        index: 0,
        mimeType: 'audio/pcm;rate=24000',
      });
      await new Promise(r => setTimeout(r, 10));
    }
    send({ type: 'audio_stream_end' });
    announceGreeting();
    primeGeminiGreetingContext(WORKER_INTRO);
    scheduleListeningTransition('disk greeting', GREETING_PCM_MS + 500);
    return;
  }

  // Cache yok — bir kereligine Gemini'ye urettir, chunk'lari topla, diske yaz
  console.log('[Gemini] Disk cache yok — greeting uretiliyor ve kaydedilecek');
  cachedGreetingChunks = [];
  isCachingGreeting = true;
  geminiWs.send(JSON.stringify({
    client_content: {
      turns: [{
        role: 'user',
        parts: [{ text: `[SYSTEM] Greet the worker now with exactly this message, word for word, and nothing else: "${WORKER_INTRO}"` }]
      }],
      turn_complete: true
    }
  }));
}
}

        // Audio response from Gemini
        if (msg.serverContent) {
  const content = msg.serverContent;

  if (content.modelTurn && content.modelTurn.parts) {
    for (const part of content.modelTurn.parts) {
      if (part.inlineData && part.inlineData.mimeType && part.inlineData.mimeType.startsWith('audio/')) {
  if (turnStartTime && !firstAudioTime) {
    firstAudioTime = Date.now();
    const ttfa = firstAudioTime - turnStartTime;
    console.log('[Gemini] TTFA (end-of-speech → first audio chunk):', ttfa + 'ms');
    send({ type: 'gemini_turn_latency', ms: ttfa });
    turnTtfaMs = ttfa;
    turnStartTime = null;
    lastAudioSentTime = null;
  }
  console.log('[Gemini] Audio chunk received, mimeType:', part.inlineData.mimeType, 'size:', part.inlineData.data.length);
  if (isCachingGreeting) {
    cachedGreetingChunks.push(part.inlineData.data);
    cachedGreetingMimeType = part.inlineData.mimeType;
  }
  turnHadAudio = true;
  send({ type: 'audio_chunk', chunk: part.inlineData.data, index: 0, mimeType: part.inlineData.mimeType });
}
      if (part.text) {
        console.log('[Gemini] Text:', part.text);
        send({ type: 'transcript', text: part.text, role: 'ai' });
      }
    }
  }

  const outTx = content.outputTranscription || content.output_transcription;
  if (outTx && outTx.text) {
    aiTranscriptBuffer += outTx.text;
  }

          if (content.turnComplete) {
            const isGreetingTurn = sessionState === 'GREETING' || isCachingGreeting;
            // inputTranscription ayri bir ASR yan kanali; gurultude pes edebiliyor.
            // Ama ses native olarak modele giriyor, yani model anlamis olabilir.
            // Bos birakip gecmek karari gerekcesiz gosterir — kayda acik bir iz birak.
            if (!isGreetingTurn && !turnHadTranscript && aiTranscriptBuffer && lastTurnMetrics) {
              const mk = '(worker audio not transcribed — snr '
                + (lastTurnMetrics.snrDb != null ? lastTurnMetrics.snrDb.toFixed(1) : 'n/a') + ' dB, '
                + lastTurnMetrics.speechMs + 'ms speech in ' + lastTurnMetrics.spans + ' span(s)'
                + (lastTurnMetrics.gaps ? ', ' + lastTurnMetrics.gaps + ' gap(s)' : '') + ')';
              console.log('[Gemini] NO TRANSCRIPT — model yine de cevapladi ' + mk);
              console.log('[Gemini]   -> AI:', aiTranscriptBuffer.trim());
              send({ type: 'transcript', text: mk, role: 'worker' });
            }
            if (aiTranscriptBuffer) {
              // greeting turn already renders its own bubble via announceGreeting()
              if (!isGreetingTurn) {
                send({ type: 'transcript', text: aiTranscriptBuffer, role: 'ai' });
                const row = {
                  audioFile: currentAudioFile,
                  deepfilter: dfEnabled && isDenoiserReady(),
                  transcript: turnWorkerText.trim(),
                  response: aiTranscriptBuffer.trim(),
                  ttfaMs: turnTtfaMs,
                  totalMs: turnCloseTime ? Date.now() - turnCloseTime : null,
                  dfMs: turnDfMs,
                  snrDb: lastTurnMetrics ? lastTurnMetrics.snrDb : null,
                };
                turnCloseTime = null;

                // Live ASR pes ettiyse sesi REST'e sorup kayda gecir — satiri o zaman yaz
                if (!row.transcript && lastTurnPcm && lastTurnPcm.length) {
                  const pcmForRest = lastTurnPcm;
                  transcribeWithRest(pcmForRest, actualSampleRate).then(function (text) {
                    row.transcriptRest = text;
                    console.log('[REST ASR] fallback transcript:', text);
                    send({ type: 'transcript', text: '(REST) ' + text, role: 'worker' });
                    appendTestRow(row);
                  }).catch(function (e) {
                    console.error('[REST ASR] hata:', e.response ? e.response.status : e.message);
                    appendTestRow(row);
                  });
                } else {
                  appendTestRow(row);
                }
              }
              aiTranscriptBuffer = '';
            }
            // a turn that produced no audio must not trigger playback of the previous buffer
            if (turnHadAudio) send({ type: 'audio_stream_end' });
            turnHadAudio = false;
            if (sessionState === 'GREETING' || isCachingGreeting) {
              announceGreeting();
              if (isCachingGreeting && cachedGreetingChunks && cachedGreetingChunks.length) {
                saveGreetingToDisk(cachedGreetingChunks);
                cachedGreetingChunks = null;
              }
              isCachingGreeting = false;
              console.log('[Gemini] Greeting audio done — auto LISTENING in 2s');
              scheduleListeningTransition('greeting turnComplete');
            }
          }
        }

        // Input transcription (what worker said)
if (msg.serverContent && msg.serverContent.inputTranscription) {
  turnHadTranscript = true;
  console.log('[Gemini] Worker said:', msg.serverContent.inputTranscription.text);
  send({ type: 'transcript', text: msg.serverContent.inputTranscription.text, role: 'worker' });
  turnWorkerText += msg.serverContent.inputTranscription.text;
}

        if (msg.usageMetadata && turnStartTime) {
          const totalTurnMs = Date.now() - turnStartTime;
          console.log('[Gemini] Full turn duration:', totalTurnMs + 'ms (not shown in UI — using TTFA instead)');
        }

        if (msg.usageMetadata) {
          const promptTokens = msg.usageMetadata.promptTokenCount || 0;
          const responseTokens = msg.usageMetadata.responseTokenCount || 0;
          send({ type: 'gemini_token_usage', input: promptTokens, output: responseTokens });
        }
      } catch(e) {
        console.error('[Gemini] Message error:', e.message);
      }
    });

    geminiWs.on('error', function(e) {
      console.error('[Gemini] Error:', e.message);
      send({ type: 'stage', stage: 'stt', status: 'error' });
      send({ type: 'log', message: 'Gemini baglantisi kurulamadi: ' + e.message });
    });

    geminiWs.on('close', function(code, reason) {
      console.log('[Gemini] Closed', code, reason.toString());
    });
  }

  ws.on('message', async function(data) {
    try {
      let isJson = false;
      let msg = null;
      try {
        msg = JSON.parse(data.toString());
        isJson = true;
      } catch (e) {
        isJson = false;
      }

      if (isJson) {
        if (msg.type === 'mime_type') {
          clientMimeType = msg.mimeType;
        }

        if (msg.type === 'gemini_sample_rate') {
          actualSampleRate = msg.rate;
          console.log('[Gemini] Client sample rate:', actualSampleRate);
        }

        if (msg.type === "audio_upload") {
          expectUpload = msg.bytes || 1;
          currentAudioFile = msg.filename || '';
        }

        if (msg.type === 'toggle_denoise') {
          dfEnabled = !!msg.enabled;
          deepFilter = null; // ON/OFF gecisinde filtre state'i sifirlansin
          console.log('[' + DENOISE_LABEL + '] tercih =', dfEnabled ? 'ON' : 'OFF');
          send({ type: 'denoise_engine', engine: DENOISE_ENGINE, label: DENOISE_LABEL, active: dfEnabled && isDenoiserReady() });
        }

        if (msg.type === 'start') {
          console.log('[Gemini] start received at', Date.now());
          send({ type: 'log', message: 'Gemini Live API session started' });
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          connectGemini();
        }

        if (msg.type === 'mock_transcript' && sessionState === 'LISTENING') {
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            const textMsg = {
              client_content: {
                turns: [{
                  role: 'user',
                  parts: [{ text: msg.text }]
                }],
                turn_complete: true
              }
            };
            geminiWs.send(JSON.stringify(textMsg));
          }
        }

        if (msg.type === 'stop') {
          if (listeningTransitionTimer) {
            clearTimeout(listeningTransitionTimer);
            listeningTransitionTimer = null;
          }
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
          sessionState = 'ENDED';
        }

      } else {
        const buf = Buffer.from(data);
        const isFileUpload = expectUpload > 0 || buf.length > 3200;
        if (expectUpload > 0) expectUpload = 0;

        if (isFileUpload) {
          let ready = sessionState === "LISTENING";
          const MAX_WAIT_MS = 15000, STEP = 250;
          for (let waited = 0; !ready && sessionState !== "ENDED" && waited < MAX_WAIT_MS; waited += STEP) {
            if (waited === 0) console.log('[Gemini] File upload buffered — waiting for greeting to finish (state=' + sessionState + ')');
            await new Promise(function (r) { setTimeout(r, STEP); });
            ready = sessionState === "LISTENING";
          }
          if (!ready) {
            console.warn('[Gemini] Dropping file upload — never reached LISTENING (state=' + sessionState + ')');
            return;
          }
          if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;

          vad.resetTurn();

          let sendBuf = buf;
          turnDfMs = null;
          const df = ensureDeepFilter();
          if (df) {
            const t0 = Date.now();
            const DF_CHUNK = 640;
            const parts = [];
            for (let off = 0; off < buf.length; off += DF_CHUNK) {
              const chunk = buf.subarray(off, Math.min(off + DF_CHUNK, buf.length));
              const clean = await df.processPcm16(chunk);
              if (clean && clean.length) parts.push(clean);
            }
            sendBuf = Buffer.concat(parts);
            turnDfMs = Date.now() - t0;
            console.log('[' + DENOISE_LABEL + '] ON | in:', buf.length, '-> out:', sendBuf.length, 'byte |', turnDfMs + 'ms');
          } else {
            console.log('[' + DENOISE_LABEL + '] OFF — ham ses Gemini\'ye gidiyor');
          }

          await analyzePcm(sendBuf);
          lastTurnPcm = sendBuf;

          activityOpen = true;
          geminiWs.send(JSON.stringify({ realtimeInput: { activityStart: {} } }));
          const CHUNK_SIZE = 3200;
          for (let off = 0; off < sendBuf.length; off += CHUNK_SIZE) {
            const slice = sendBuf.subarray(off, Math.min(off + CHUNK_SIZE, sendBuf.length));
            geminiWs.send(JSON.stringify({ realtime_input: { audio: {
              data: slice.toString("base64"), mimeType: "audio/pcm;rate=" + actualSampleRate } } }));
              lastAudioSentTime = Date.now();
              if (off + CHUNK_SIZE < sendBuf.length)  await new Promise(function (r) {
                setTimeout(r, 20); });
          }
          closeTurn();
          console.log('[Gemini] File upload done — activityEnd');
          return;
        }

        if (sessionState !== "LISTENING") return;
        if(!geminiWs || geminiWs.readyState !== WebSocket.OPEN) return;
        
        await analyzePcm(buf);

        if (vad.isSpeaking() && !activityOpen) {
          activityOpen = true;
          geminiWs.send(JSON.stringify({ realtimeInput: { activityStart: {} } }));
          console.log('[Gemini Turn] activityStart @', audioClockMs.toFixed(0) + 'ms');
        }

        if (activityOpen) {
          const CHUNK_SIZE = 3200;
          for (let off = 0; off < buf.length; off += CHUNK_SIZE) {
            const slice = buf.subarray(off, Math.min(off + CHUNK_SIZE, buf.length));
            geminiWs.send(JSON.stringify({ realtime_input: {audio: {
              data: slice.toString("base64"), mimeType: "audio/pcm;rate=" + actualSampleRate } } }));
              lastAudioSentTime = Date.now();
              if (off + CHUNK_SIZE < buf.length) await new Promise(function (r) {
                setTimeout(r, 20); });
          }
        }
        if (activityOpen && !vad.isSpeaking() && vad.idleMs() >= TURN_SILENCE_MS) {
          closeTurn();
          console.log('[Gemini Turn] activityEnd');
        }
      }
    } catch(e) {
      console.error('[WS Error]', e.message);
    }
  });

  ws.on('close', function() {
    console.log('[Gemini WS] Client disconnected');
    if (listeningTransitionTimer) {
      clearTimeout(listeningTransitionTimer);
      listeningTransitionTimer = null;
    }
    if (workerSilenceTimer) clearTimeout(workerSilenceTimer);
    if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
  });

  ws.on('error', function(e) {
    console.error('[WS Error]', e.message);
  });
});

// ─── Twilio WebSocket (Gemini pipeline) ───────────────────────────────────────
const { spawn } = require('child_process');
const { BuildContextImpl } = require('twilio/lib/rest/serverless/v1/service/build');
const { InteractionTransferListInstance } = require('twilio/lib/rest/flexApi/v1/interaction/interactionChannel/interactionTransfer');
const twilioWss = new WebSocket.Server({ noServer: true });

server.on('upgrade', function(request, socket, head) {
  const pathname = new URL(request.url, 'http://localhost').pathname;
  if (pathname === '/twilio-gemini') {
    twilioWss.handleUpgrade(request, socket, head, function(ws) {
      twilioWss.emit('connection', ws, request);
    });
  } else {
    // Browser WebSocket (any other path)
    wss.handleUpgrade(request, socket, head, function(ws) {
      wss.emit('connection', ws, request);
    });
  }
});

// ─── Pure JS mulaw codecs (no ffmpeg — instant, ordered) ─────────────────────
const MULAW_BIAS = 0x84;
const MULAW_MAX = 32635;

function linearToMulawSample(sample) {
  let sign = (sample >> 8) & 0x80;
  if (sign) sample = -sample;
  if (sample > MULAW_MAX) sample = MULAW_MAX;
  sample += MULAW_BIAS;
  let exponent = 7;
  for (let mask = 0x4000; (sample & mask) === 0 && exponent > 0; mask >>= 1) exponent--;
  const mantissa = (sample >> (exponent + 3)) & 0x0F;
  return ~(sign | (exponent << 4) | mantissa) & 0xFF;
}

function mulawToLinearSample(mulaw) {
  mulaw = ~mulaw & 0xFF;
  const sign = mulaw & 0x80;
  const exponent = (mulaw >> 4) & 0x07;
  const mantissa = mulaw & 0x0F;
  let sample = ((mantissa << 3) + MULAW_BIAS) << exponent;
  sample -= MULAW_BIAS;
  return sign ? -sample : sample;
}

// Gemini PCM 24kHz → Twilio mulaw 8kHz (downsample 3:1 + encode)
function pcmToMulaw(pcm24kBuffer) {
  const samples24k = pcm24kBuffer.length >> 1;
  const samples8k = Math.floor(samples24k / 3);
  const out = Buffer.alloc(samples8k);
  for (let i = 0; i < samples8k; i++) {
    // Average 3 samples for basic anti-aliasing
    const s0 = pcm24kBuffer.readInt16LE(i * 6);
    const s1 = (i * 6 + 2 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 2) : s0;
    const s2 = (i * 6 + 4 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 4) : s1;
    const avg = Math.round((s0 + s1 + s2) / 3);
    out[i] = linearToMulawSample(avg);
  }
  return out;
}

loadGreetingFromDisk();

// Twilio mulaw 8kHz → Gemini PCM 16kHz (decode + upsample 1:2)
function mulawToPcm(mulawBuffer) {
  const out = Buffer.alloc(mulawBuffer.length * 4); // each mulaw byte → 2 samples × 2 bytes
  for (let i = 0; i < mulawBuffer.length; i++) {
    const sample = mulawToLinearSample(mulawBuffer[i]);
    const next = (i + 1 < mulawBuffer.length) ? mulawToLinearSample(mulawBuffer[i + 1]) : sample;
    const mid = Math.round((sample + next) / 2); // linear interpolation
    out.writeInt16LE(sample, i * 4);
    out.writeInt16LE(mid, i * 4 + 2);
  }
  return out;
}

const TWILIO_WORKER_INTRO = WORKER_INTRO;

const OVERLAP_ABOVE_FLOOR_DB = 50;

function loudOverlaps(m) {
  if (m.noiseLevelDbfs == null) return [];
  return m.collisions.filter(function (c) {
    return c.type === "overlap" && c.peakDbfs != null && (c.peakDbfs - m.noiseLevelDbfs) >= OVERLAP_ABOVE_FLOOR_DB;
  });
}

function buildMonitorReport(m, spans) {
  const facts = [];
  if (m.snrDb != null && m.snrReliable && m.snrDb < 20) facts.push("signal-to-noise " + m.snrDb.toFixed(0) + " dB");

  if (spans.length) {
    const uttStart = Math.min.apply(null, spans.map(function (s) { return s.startMs; }));
    const uttEnd = Math.max.apply(null, spans.map(function (s) { return s.endMs; }));
    const width = Math.max(1, uttEnd - uttStart);
    const placeOf = function (tMs) {
      const rel = (tMs - uttStart) / width;
      return rel < 0.15 ? "at the very beginning of the reply"
        : rel > 0.85 ? "at the very end of the reply"
        : "in the middle of the reply";
    };

    for (const g of m.collisions.filter(function (c) { return c.type === "gap"; })) {
      facts.push("a " + (g.durationMs / 1000).toFixed(1) + " second noise burst " + placeOf(g.tMs));
    }
    for (const o of loudOverlaps(m)) {
      const cover = Math.round(100 * Math.min(1, o.durationMs / width));
      facts.push("a " + (o.durationMs / 1000).toFixed(1)
        + " second burst of noise LOUDER than the worker's voice, " + placeOf(o.tMs)
        + ", covering about " + cover + "% of the reply — one or more words there were drowned out");
    }
  }
  return "[ACOUSTIC MONITOR] " + facts.join("; ") + ".";
}

twilioWss.on('connection', function(ws) {
  console.log('[Twilio-Gemini WS] Connected');
  let streamSid = null;
  let callLog = {
    callSid: null,
    streamSid: null,
    workerPhone: null,
    startTime: new Date().toISOString(),
    transcript: [],
    finalState: null,
    finalIntent: null,
    recordingUrl: null,
    silencePromptCount: 0,
    ttfa_ms: null,
  };
  let geminiWs = null;
  let sessionState = 'GREETING';
  let pcmBuffer = Buffer.alloc(0);
  let flushInterval = null;
  let silenceInterval = null;
  let twilioAiTranscript = '';
  let workerSilenceTimer = null;
  let silencePromptCount = 0;
  let twilioSetupTime = null;
  let twilioFirstAudioTime = null;
  const noise = createNoiseEstimator({ frameMs: 20 });
  const vad = createSileroVad({ sampleRate: 8000 });
  let audioClockMs = 0;
  let agentPlaybackUntil = 0;
  const TURN_SILENCE_MS = 900;
  const PREROLL_BYTES = 9600;
  let activityOpen = false;

  function isSpuriousUnclearResponse(text) {
    const t = (text || '').toLowerCase();
    return t.includes('sorry') && t.includes('repeat')
      && !t.includes('aware360') && !t.includes('monitoring center');
  }

  function startAudioFlush() {
    if (flushInterval) return;
    flushInterval = setInterval(function() {
      if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN || sessionState !== "LISTENING") return;
      if(!activityOpen) {
        if (pcmBuffer.length > PREROLL_BYTES) pcmBuffer = pcmBuffer.subarray(pcmBuffer.length - PREROLL_BYTES);
        return;
      }
      if (pcmBuffer.length === 0) return;
      const toSend = pcmBuffer;
      pcmBuffer = Buffer.alloc(0);
      geminiWs.send(JSON.stringify({
        realtime_input: { audio: { data: toSend.toString("base64"), mimeType: "audio/pcm;rate=16000" } }
      }));
    }, 100);
  }

  function setCallOutcome(state, intent) {
    callLog.finalState = state;
    if (intent) callLog.finalIntent = intent;
  }

  function startWorkerSilenceTimer() {
    clearWorkerSilenceTimer();
    workerSilenceTimer = setTimeout(() => {
      if (sessionState !== 'LISTENING') return;
      silencePromptCount++;
      if (silencePromptCount === 1) {
        console.log('[Twilio-Gemini] 8s silence — prompting worker');
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: '[SYSTEM] The worker has been silent for 8 seconds. Say exactly: "Are you still there? Please respond if you can hear me."' }] }],
              turn_complete: true
            }
          }));
        }
        startWorkerSilenceTimer();
      } else {
        console.log('[Twilio-Gemini] 16s total silence — likely voicemail/unreachable, ending call');
        sessionState = 'ENDED';
        setCallOutcome('NO_ANSWER', callLog.finalIntent || 'UNRECOGNISED');
        clearWorkerSilenceTimer();
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: '[SYSTEM] No response received. Say exactly: "We could not reach you. A monitoring agent will follow up shortly. Goodbye."' }] }],
              turn_complete: true
            }
          }));
        }
        // Wait for farewell to play before closing
        setTimeout(() => {
          console.log('[Twilio-Gemini] Closing call (no response)');
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
          if (ws.readyState === WebSocket.OPEN) ws.close();
        }, 8000);
      }
    }, 8000);
  }

  function clearWorkerSilenceTimer() {
    if (workerSilenceTimer) { clearTimeout(workerSilenceTimer); workerSilenceTimer = null; }
  }

  const MULAW_SILENCE = Buffer.alloc(160, 0x7F);

  function startSilenceKeepalive() {
    silenceInterval = setInterval(() => {
      if (ws.readyState === WebSocket.OPEN && streamSid && sessionState === 'GREETING') {
        ws.send(JSON.stringify({
          event: 'media',
          streamSid: streamSid,
          media: { payload: MULAW_SILENCE.toString('base64') }
        }));
      } else {
        clearInterval(silenceInterval);
        silenceInterval = null;
      }
    }, 20);
  }

  function sendAudioToTwilio(mulawBuffer) {
    if (ws.readyState !== WebSocket.OPEN || !streamSid) return;
    const durMs = mulawBuffer.length / 8;
    agentPlaybackUntil = Math.max(agentPlaybackUntil, Date.now()) + durMs;
    ws.send(JSON.stringify({
      event: 'media',
      streamSid: streamSid,
      media: { payload: mulawBuffer.toString('base64') }
    }));
  }

  function connectGeminiTwilio() {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on('open', function() {
      console.log('[Twilio-Gemini] Gemini connected');
      geminiWs.send(JSON.stringify({
        setup: {
          model: `models/${GEMINI_MODEL}`,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: { prebuilt_voice_config: { voice_name: 'Charon' } },
            }
          },
          realtime_input_config: { automatic_activity_detection: { disabled: true } },
          realtimeInputConfig: { automaticActivityDetection: { disabled: true } },
          input_audio_transcription: {},
          output_audio_transcription: {},
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
        }
      }));
    });

    geminiWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.error) {
          console.error('[Twilio-Gemini] API ERROR:', JSON.stringify(msg.error));
        }

        if (msg.setupComplete) {
          console.log('[Twilio-Gemini] Setup complete — sending greeting');
          twilioSetupTime = Date.now();
          pcmBuffer = Buffer.alloc(0);
          twilioAiTranscript = '';
          sessionState = 'GREETING';
          if (GREETING_ULAW) {
            console.log('[Twilio-Gemini] Greeting disk cache\'ten caliniyor,', GREETING_PCM_MS + 'ms');
            if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
            twilioFirstAudioTime = Date.now();
            callLog.ttfa_ms = twilioFirstAudioTime - twilioSetupTime;

            for (let off = 0; off < GREETING_ULAW.length; off += 160) {   // 160 bayt = 20ms
              sendAudioToTwilio(GREETING_ULAW.subarray(off, off + 160));
            }
            callLog.transcript.push({ role: 'ai', text: WORKER_INTRO, time: new Date().toISOString() });

            geminiWs.send(JSON.stringify({
              client_content: {
                turns: [
                  { role: 'user',  parts: [{ text: '[SYSTEM] The greeting has already been played to the worker from a recording. Do not greet again. Stay completely silent until the worker speaks.' }] },
                  { role: 'model', parts: [{ text: WORKER_INTRO }] }
                ],
                turn_complete: false
              }
            }));

            setTimeout(function () {
              pcmBuffer = Buffer.alloc(0);
              twilioAiTranscript = '';
              sessionState = 'LISTENING';
              console.log('[Twilio-Gemini] Now listening');
              startAudioFlush();
              startWorkerSilenceTimer();
            }, GREETING_PCM_MS + 500);

            return;
          }
          geminiWs.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: `[SYSTEM] Say this exact greeting word for word and nothing else: "${TWILIO_WORKER_INTRO}"` }] }],
              turn_complete: true
            }
          }));
        }

        if (msg.serverContent) {
          const content = msg.serverContent;

          // Worker spoke — reset silence timer (only after greeting)
          if (content.inputTranscription && content.inputTranscription.text) {
            if (sessionState !== 'LISTENING') {
              console.log('[Twilio-Gemini] Ignoring worker audio during GREETING:', content.inputTranscription.text);
            } else {
            console.log('[Twilio-Gemini] Worker said:', content.inputTranscription.text);
            const workerText = content.inputTranscription.text;
            callLog.transcript.push({ role: 'worker', text: workerText, time: new Date().toISOString() });
            const wt = workerText.toLowerCase();
            if (/\bhuman\b|\bagent\b|connect me|need help|sos\b|help me/.test(wt)) {
              setCallOutcome('ESCALATED', 'REQUEST_HUMAN');
            } else if (/\bi'?m fine\b|\bi am fine\b|\ball good\b|\bsafe\b/.test(wt) && !/\bbut\b|\bnot\b|\bworried\b|\bhurt\b/.test(wt)) {
              callLog.finalIntent = callLog.finalIntent || 'SAFE_CONFIRMATION';
            } else if (/\blate\b|\bmore time\b|\bminutes?\b|\bdelay\b/.test(wt)) {
              callLog.finalIntent = callLog.finalIntent || 'DELAYED_CHECKIN';
            }
            silencePromptCount = 0;
            callLog.silencePromptCount = silencePromptCount;
            startWorkerSilenceTimer();
            }
          }

          // Collect AI transcript for resolution detection
          if (content.outputTranscription && content.outputTranscription.text) {
            twilioAiTranscript += content.outputTranscription.text;
          }

          if (content.modelTurn && content.modelTurn.parts) {
            for (const part of content.modelTurn.parts) {
              if (part.inlineData && part.inlineData.mimeType && part.inlineData.mimeType.startsWith('audio/')) {
                if (sessionState === 'GREETING' && isSpuriousUnclearResponse(twilioAiTranscript)) {
                  continue;
                }
                try {
                  if (!twilioFirstAudioTime) {
                    twilioFirstAudioTime = Date.now();
                    const ttfa = twilioFirstAudioTime - twilioSetupTime;
                    console.log('[Twilio-Gemini] FIRST AUDIO CHUNK | Time-to-first-audio (TTFA):', ttfa + 'ms');
                    callLog.ttfa_ms = ttfa;
                  }
                  if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
                  const audioBuf = Buffer.from(part.inlineData.data, 'base64');
                  const mulawBuf = pcmToMulaw(audioBuf);
                  sendAudioToTwilio(mulawBuf);
                } catch(e) { console.error('[Twilio-Gemini] PCM→mulaw error:', e.message); }
              }
            }
          }
          if (content.turnComplete) {
            // Check for resolution phrases after each AI turn (post-greeting)
            if (sessionState === 'LISTENING' && twilioAiTranscript) {
              const t = twilioAiTranscript.toLowerCase();
              console.log('[Twilio-Gemini] AI said:', twilioAiTranscript.trim());
              callLog.transcript.push({ role: 'ai', text: twilioAiTranscript.trim(), time: new Date().toISOString() });
              twilioAiTranscript = '';
              const resolutionPhrases = [
                'reset your safety timer',
                'extended your check-in timer',
                'flagging this for a live agent',
                'someone will call you back',
                'monitoring agent will follow up',
                'could not reach you'
              ];
              if (resolutionPhrases.some(p => t.includes(p))) {
                console.log('[Twilio-Gemini] Resolution detected — ending call in 4s');
                if (t.includes('could not reach you') || t.includes('monitoring agent will follow up')) {
                  setCallOutcome('NO_ANSWER', callLog.finalIntent || 'UNRECOGNISED');
                } else if (t.includes('flagging this for a live agent') || t.includes('someone will call you back')) {
                  setCallOutcome('ESCALATED', 'REQUEST_HUMAN');
                } else if (t.includes('extended your check-in timer')) {
                  setCallOutcome('RESOLVED', 'DELAYED_CHECKIN');
                } else if (t.includes('reset your safety timer')) {
                  setCallOutcome('RESOLVED', 'SAFE_CONFIRMATION');
                } else if (!callLog.finalState) {
                  setCallOutcome('RESOLVED', callLog.finalIntent || null);
                }
                clearWorkerSilenceTimer();
                setTimeout(() => {
                  console.log('[Twilio-Gemini] Closing call');
                  if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
                  if (ws.readyState === WebSocket.OPEN) ws.close();
                }, 4000);
              }
            }
            if (sessionState === 'GREETING') {
              if (isSpuriousUnclearResponse(twilioAiTranscript)) {
                console.log('[Twilio-Gemini] Ignoring spurious pre-greeting response, re-sending intro');
                twilioAiTranscript = '';
                geminiWs.send(JSON.stringify({
                  client_content: {
                    turns: [{ role: 'user', parts: [{ text: `[SYSTEM] Say this exact greeting word for word and nothing else: "${TWILIO_WORKER_INTRO}"` }] }],
                    turn_complete: true
                  }
                }));
                return;
              }
              console.log('[Twilio-Gemini] Greeting audio done, waiting 2s before listening...');
              setTimeout(() => {
                pcmBuffer = Buffer.alloc(0);
                twilioAiTranscript = '';
                sessionState = 'LISTENING';
                console.log('[Twilio-Gemini] Now listening');
                startAudioFlush();
                startWorkerSilenceTimer();
              }, 2000);
            }
          }
        }
      } catch(e) { console.error('[Twilio-Gemini] Message error:', e.message); }
    });

    geminiWs.on('error', function(e) { console.error('[Twilio-Gemini] Error:', e.message); });
    geminiWs.on('close', function(code, reason) { console.log('[Twilio-Gemini] Gemini closed | code:', code, '| reason:', reason ? reason.toString() : 'none'); });
  }

  ws.on('message', async function(data) {
    try {
      const msg = JSON.parse(data.toString());

      if (msg.event === 'connected') { console.log('[Twilio-Gemini] Stream connected'); }

      if (msg.event === 'start') {
        streamSid = msg.start.streamSid;
        callLog.callSid = msg.start.callSid || streamSid;
        callLog.streamSid = streamSid;
        callLog.workerPhone = (msg.start.customParameters && msg.start.customParameters.caller)
          || msg.start.to
          || null;
        console.log('[Twilio-Gemini] Stream started | SID:', streamSid, '| Call:', callLog.callSid);

        try {
          await insertCallStart({
            callSid: callLog.callSid,
            streamSid: streamSid,
            workerPhone: callLog.workerPhone,
            startTime: callLog.startTime,
            modelUsed: GEMINI_MODEL,
          });
        } catch (e) {
          console.error('[Twilio-Gemini] DB insert error:', e.message);
        }

        connectGeminiTwilio();
        startSilenceKeepalive();
      }

            if (msg.event === 'media') {
        const mulawChunk = Buffer.from(msg.media.payload, 'base64');
        noise.setAgentSpeaking(Date.now() < agentPlaybackUntil + 200);
        vad.processMulaw(mulawChunk);
        noise.pushFrame({ ...frameStatsMulaw(mulawChunk), speechProb: vad.prob() }, audioClockMs);
        audioClockMs += mulawChunk.length / 8;

        if (sessionState === 'LISTENING' && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
          // konusma basladi -> turn ac
          if (vad.isSpeaking() && !activityOpen) {
            activityOpen = true;
            geminiWs.send(JSON.stringify({ realtimeInput: { activityStart: {} } }));
            console.log('[Twilio-Gemini Turn] activityStart @', audioClockMs.toFixed(0) + 'ms');
          }

          // konusma bitti + yeterli sessizlik -> olc, enjekte et, turn'u kapat
          if (activityOpen && !vad.isSpeaking() && vad.idleMs() >= TURN_SILENCE_MS) {
            activityOpen = false;
            const spans = vad.takeSpans();
            const m = noise.endTurn({ speechSpans: spans.length ? spans : null });
            console.log('[Twilio-Gemini Noise] snr=' + (m.snrDb != null ? m.snrDb.toFixed(1) : 'n/a')
              + ' noisy=' + m.noisy + ' collisions=' + m.collisions.length + ' spans=' + spans.length);

            if (!callLog.noiseTurns) callLog.noiseTurns = [];
            const entry = {
              time: new Date().toISOString(), snrDb: m.snrDb, noiseLevelDbfs: m.noiseLevelDbfs,
              speechDbfs: m.speechDbfs, noisy: m.noisy, snrReliable: m.snrReliable,
              collisions: m.collisions.length, injected: false, report: null,
            };
            callLog.noiseTurns.push(entry);

            const gapCount = m.collisions.filter(function (c) { return c.type === "gap";}).length;
            const snrLow = m.snrReliable && m.snrDb != null && m.snrDb < 12;
            const degraded = gapCount > 0 || m.noisy || snrLow;
            
            if (degraded) {
              const report = buildMonitorReport(m, spans);
              console.log('[Twilio-Gemini Monitor]', report);
              entry.injected = true;
              entry.report = report;
              geminiWs.send(JSON.stringify({
                client_content: { turns: [{ role: 'user', parts: [{ text: report }] }], turn_complete: false }
              }));
            }

            geminiWs.send(JSON.stringify({ realtimeInput: { activityEnd: {} } }));
            console.log('[Twilio-Gemini Turn] activityEnd');
          }

          try {
            pcmBuffer = Buffer.concat([pcmBuffer, mulawToPcm(mulawChunk)]);
          } catch (e) {
            console.error('[DEBUG] mulawToPcm error:', e.message);
          }
        }
      }

      if (msg.event === 'stop') {
        console.log('[Twilio-Gemini] Stream stopped');
        if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
      }
    } catch(e) { console.error('[Twilio-Gemini WS Error]', e.message); }
  });

  ws.on('close', async function() {
    console.log('[Twilio-Gemini WS] Disconnected');
    if (callLog.callSid) amdRedirectedCalls.delete(callLog.callSid);
    clearWorkerSilenceTimer();
    if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
    if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
    if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();

    // Save call log to file + database
    try {
      const logsDir = path.join(__dirname, 'call_logs');
      if (!fs.existsSync(logsDir)) fs.mkdirSync(logsDir);
      callLog.endTime = new Date().toISOString();
      callLog.silencePromptCount = silencePromptCount;
      const duration = Math.round((new Date(callLog.endTime) - new Date(callLog.startTime)) / 1000);
      let finalSessionState = callLog.finalState || (silencePromptCount >= 2 ? 'NO_ANSWER' : 'UNKNOWN');
      if (callLog.callSid) {
        const existing = await getCallBySid(callLog.callSid);
        if (existing && existing.session_state === 'VOICEMAIL') {
          finalSessionState = 'VOICEMAIL';
        }
      }

      const filename = path.join(logsDir, `call_${callLog.callSid || Date.now()}.json`);
      fs.writeFileSync(filename, JSON.stringify(callLog, null, 2));
      console.log('[Twilio-Gemini] Call log saved:', filename);

      if (callLog.callSid) {
        await updateCallEnd({
          callSid: callLog.callSid,
          streamSid: callLog.streamSid,
          workerPhone: callLog.workerPhone,
          endTime: callLog.endTime,
          durationSeconds: duration,
          sessionState: finalSessionState,
          finalIntent: callLog.finalIntent,
          recordingUrl: callLog.recordingUrl,
          transcript: callLog.transcript,
          silenceCount: callLog.silencePromptCount,
          ttfaMs: callLog.ttfa_ms,
        });
        console.log('[Twilio-Gemini] Call saved to DB:', callLog.callSid, '|', finalSessionState);
      }
    } catch(e) {
      console.error('[Twilio-Gemini] Log save error:', e.message);
    }
  });
});

// ─── Twilio TwiML endpoint ────────────────────────────────────────────────────
app.post('/outbound-gemini', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/outbound-gemini');
  if (!validateTwilioSignature(req, res, url)) return;

  const baseUrl = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
  const wsUrl = baseUrl.replace('https://', 'wss://').replace(/\/$/, '') + '/twilio-gemini';
  const workerPhone = (req.body && (req.body.To || req.body.to)) || '';
  console.log('[TwiML] Stream URL:', wsUrl, '| To:', workerPhone);
  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Connect>
    <Stream url="${wsUrl}">
      <Parameter name="caller" value="${workerPhone}"/>
    </Stream>
  </Connect>
  <Pause length="30"/>
</Response>`);
});

app.post('/call-status', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/call-status');
  if (!validateTwilioSignature(req, res, url)) return;

  console.log('[Call Status]', req.body.CallStatus, '| SID:', req.body.CallSid);
  res.sendStatus(200);
});

app.post('/recording-status', async function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/recording-status');
  if (!validateTwilioSignature(req, res, url)) return;

  const { RecordingUrl, RecordingDuration, RecordingSid, CallSid, RecordingStatus } = req.body || {};
  console.log('[Recording] Status:', RecordingStatus, '| Duration:', RecordingDuration + 's', '| SID:', RecordingSid);
  if (RecordingStatus === 'completed' && RecordingUrl && CallSid) {
    const recordingUrl = RecordingUrl.endsWith('.mp3') ? RecordingUrl : RecordingUrl + '.mp3';
    try {
      await updateRecordingUrl(CallSid, recordingUrl);
      console.log('[Recording] Saved URL for call:', CallSid);
      console.log('[Recording] URL:', recordingUrl);
    } catch (e) {
      console.error('[Recording] DB update error:', e.message);
    }
  }
  res.sendStatus(200);
});

app.post('/amd-status', async function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/amd-status');
  if (!validateTwilioSignature(req, res, url)) return;

  const { CallSid, AnsweredBy } = req.body || {};
  console.log('[AMD] Call:', CallSid, '| Answered by:', AnsweredBy);

  if (AnsweredBy && AnsweredBy.startsWith('machine') && CallSid) {
    if (amdRedirectedCalls.has(CallSid)) {
      return res.sendStatus(200);
    }
    amdRedirectedCalls.add(CallSid);

    console.log('[AMD] Voicemail detected — will leave message');
    try {
      await updateCallSessionState(CallSid, 'VOICEMAIL');
      console.log('[AMD] DB updated: VOICEMAIL for', CallSid);
    } catch (e) {
      console.error('[AMD] DB error:', e.message);
    }

    if (twilioClient) {
      const baseUrl = process.env.GEMINI_PUBLIC_URL || process.env.PUBLIC_URL || '';
      twilioClient.calls(CallSid).update({
        url: `${baseUrl.replace(/\/$/, '')}/voicemail-twiml`,
        method: 'POST',
      }).then(() => {
        console.log('[AMD] Redirected to voicemail TwiML for call:', CallSid);
      }).catch((err) => {
        console.error('[AMD] Redirect error:', err.message);
        amdRedirectedCalls.delete(CallSid);
      });
    } else {
      console.error('[AMD] Twilio client not configured');
    }
  } else if (AnsweredBy === 'unknown') {
    console.log('[AMD] Unknown — not redirecting, Gemini flow continues');
  } else if (AnsweredBy === 'human') {
    console.log('[AMD] Human answered — Gemini flow continues');
  }

  res.sendStatus(200);
});

app.post('/voicemail-twiml', function(req, res) {
  const url = buildTwilioWebhookUrl(req, '/voicemail-twiml');
  if (!validateTwilioSignature(req, res, url)) return;

  const voicemailMessage = 'Hello, this is an automated safety check from Aware360. ' +
    'We noticed you missed your scheduled check-in. ' +
    'Please call back or check in through the app as soon as possible. ' +
    'If this is an emergency, please contact your supervisor. Thank you.';

  res.set('Content-Type', 'text/xml');
  res.send(`<?xml version="1.0" encoding="UTF-8"?>
<Response>
  <Pause length="2"/>
  <Say voice="Polly.Joanna" language="en-US">${voicemailMessage}</Say>
  <Pause length="1"/>
  <Hangup/>
</Response>`);
});

app.get('/api/calls', async function(req, res) {
  const page = parseInt(req.query.page, 10) || 1;
  const limit = parseInt(req.query.limit, 10) || 20;
  try {
    res.json(await listCalls(page, limit));
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/calls/:callSid', async function(req, res) {
  try {
    const call = await getCallBySid(req.params.callSid);
    if (!call) return res.status(404).json({ error: 'Call not found' });
    call.transcript = call.transcript ? JSON.parse(call.transcript) : [];
    res.json(call);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

app.get('/api/recordings/proxy', async function(req, res) {
  const accountSid = process.env.TWILIO_ACCOUNT_SID;
  const authToken = process.env.TWILIO_AUTH_TOKEN;
  const recordingUrl = req.query.url ? decodeURIComponent(req.query.url) : '';

  if (!accountSid || !authToken) {
    return res.status(500).json({ error: 'Twilio credentials not configured' });
  }
  if (!recordingUrl || !recordingUrl.startsWith('https://api.twilio.com/')) {
    return res.status(400).json({ error: 'Invalid recording URL' });
  }

  console.log('[Recording Proxy] URL:', recordingUrl);

  try {
    const response = await axios.get(recordingUrl, {
      auth: { username: accountSid, password: authToken },
      responseType: 'stream',
    });

    res.setHeader('Content-Type', 'audio/mpeg');
    response.data.pipe(res);
  } catch (err) {
    console.error('[Recording Proxy] Error:', err.response?.status, err.message);
    res.status(404).json({ error: 'Recording not found' });
  }
});

app.get('/health', function(req, res) {
  res.json({
    status: 'ok',
    mode: 'gemini',
    apis: { gemini: !!GEMINI_API_KEY },
    twilio_ws: (process.env.PUBLIC_URL || '').replace('https://', 'wss://') + '/twilio-gemini',
    twilio_twiml: (process.env.PUBLIC_URL || '') + '/outbound-gemini'
  });
});

if (DENOISE_ENGINE === 'rnnoise') {
  preloadRnnoise()
    .then(function () { console.log('[RNNoise] WASM hazir'); })
    .catch(function (e) { console.error('[RNNoise] WASM yuklenemedi:', e.message); });
} else if (DENOISE_ENGINE === 'deepfilter') {
  preloadDeepFilter()
    .then(function () { console.log('[DeepFilter] model hazir'); })
    .catch(function (e) { console.error('[DeepFilter] model yuklenemedi:', e.message); });
} else {
  console.log('[Denoise] Preload atlandi — DENOISE_ENGINE=off');
}

const PORT = process.env.GEMINI_PORT || 3001;
server.listen(PORT, function() {
  console.log(`\n🤖  Gemini Live Demo running at http://localhost:${PORT}`);
  console.log(`Model: ${GEMINI_MODEL}`);
  console.log(`Twilio WS: ${(process.env.PUBLIC_URL || '').replace('https://', 'wss://')}/twilio-gemini`);
  console.log(`Twilio TwiML: ${process.env.PUBLIC_URL || ''}/outbound-gemini\n`);
});