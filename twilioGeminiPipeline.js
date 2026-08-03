/**
 * Twilio Media Streams ↔ Gemini Live pipeline (μ-law 8kHz I/O).
 * Attached to /twilio-gemini via twilioGeminiWss in server.js.
 */
const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
const { insertCallStart, updateCallEnd, updateCallLatency, getCallBySid } = require('./db');
const { createNoiseEstimator } = require('./noiseEstimator');
const { createSileroVad } = require('./sileroVad');
const { frameStatsMulaw } = require('./audioLevel');

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = process.env.GEMINI_TWILIO_MODEL || 'gemini-3.1-flash-live-preview';

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

CRITICAL SAFETY RULE: If the worker mentions ANY physical injury, pain, fall, or distress — even with positive statements — immediately escalate to human agent.

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
const TWILIO_WORKER_INTRO = WORKER_INTRO;

const GREETING_DIR = path.join(__dirname, 'audio');
const GREETING_FILE = path.join(GREETING_DIR, 'greeting_24k.pcm');

let GREETING_PCM = null;
let GREETING_ULAW = null;
let GREETING_PCM_MS = 0;

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

/** Gemini PCM 24kHz → Twilio mulaw 8kHz (downsample 3:1 + encode) */
function pcmToMulaw(pcm24kBuffer) {
  const samples24k = pcm24kBuffer.length >> 1;
  const samples8k = Math.floor(samples24k / 3);
  const out = Buffer.alloc(samples8k);
  for (let i = 0; i < samples8k; i++) {
    const s0 = pcm24kBuffer.readInt16LE(i * 6);
    const s1 = (i * 6 + 2 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 2) : s0;
    const s2 = (i * 6 + 4 < pcm24kBuffer.length - 1) ? pcm24kBuffer.readInt16LE(i * 6 + 4) : s1;
    const avg = Math.round((s0 + s1 + s2) / 3);
    out[i] = linearToMulawSample(avg);
  }
  return out;
}

/** Twilio mulaw 8kHz → Gemini PCM 16kHz (decode + upsample 1:2) */
function mulawToPcm(mulawBuffer) {
  const out = Buffer.alloc(mulawBuffer.length * 4);
  for (let i = 0; i < mulawBuffer.length; i++) {
    const sample = mulawToLinearSample(mulawBuffer[i]);
    const next = (i + 1 < mulawBuffer.length) ? mulawToLinearSample(mulawBuffer[i + 1]) : sample;
    const mid = Math.round((sample + next) / 2);
    out.writeInt16LE(sample, i * 4);
    out.writeInt16LE(mid, i * 4 + 2);
  }
  return out;
}

function loadGreetingFromDisk() {
  try {
    const buf = fs.readFileSync(GREETING_FILE);
    if (buf.length < 24000) throw new Error('file too short: ' + buf.length + ' bytes');
    GREETING_PCM = buf;
    GREETING_PCM_MS = Math.round(buf.length / 48);
    GREETING_ULAW = pcmToMulaw(buf);
    console.log('[Twilio-Gemini Greeting] Disk cache loaded:', buf.length, 'bytes /', GREETING_PCM_MS + 'ms');
    return true;
  } catch (e) {
    GREETING_PCM = null;
    GREETING_ULAW = null;
    GREETING_PCM_MS = 0;
    console.log('[Twilio-Gemini Greeting] No disk cache — first call will synthesize (' + e.message + ')');
    return false;
  }
}

const OVERLAP_ABOVE_FLOOR_DB = 50;

function loudOverlaps(m) {
  if (m.noiseLevelDbfs == null) return [];
  return m.collisions.filter(function(c) {
    return c.type === 'overlap' && c.peakDbfs != null && (c.peakDbfs - m.noiseLevelDbfs) >= OVERLAP_ABOVE_FLOOR_DB;
  });
}

function buildMonitorReport(m, spans) {
  const facts = [];
  if (m.snrDb != null && m.snrReliable && m.snrDb < 20) {
    facts.push('signal-to-noise ' + m.snrDb.toFixed(0) + ' dB');
  }

  if (spans.length) {
    const uttStart = Math.min.apply(null, spans.map(function(s) { return s.startMs; }));
    const uttEnd = Math.max.apply(null, spans.map(function(s) { return s.endMs; }));
    const width = Math.max(1, uttEnd - uttStart);
    const placeOf = function(tMs) {
      const rel = (tMs - uttStart) / width;
      return rel < 0.15 ? 'at the very beginning of the reply'
        : rel > 0.85 ? 'at the very end of the reply'
        : 'in the middle of the reply';
    };

    for (const g of m.collisions.filter(function(c) { return c.type === 'gap'; })) {
      facts.push('a ' + (g.durationMs / 1000).toFixed(1) + ' second noise burst ' + placeOf(g.tMs));
    }
    for (const o of loudOverlaps(m)) {
      const cover = Math.round(100 * Math.min(1, o.durationMs / width));
      facts.push('a ' + (o.durationMs / 1000).toFixed(1)
        + ' second burst of noise LOUDER than the worker\'s voice, ' + placeOf(o.tMs)
        + ', covering about ' + cover + '% of the reply — one or more words there were drowned out');
    }
  }
  return '[ACOUSTIC MONITOR] ' + facts.join('; ') + '.';
}

/**
 * @param {import('ws').Server} twilioGeminiWss
 */
function attachTwilioGeminiWss(twilioGeminiWss) {
  loadGreetingFromDisk();

  twilioGeminiWss.on('connection', function(ws) {
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
    let turnStartTime = null; // activity_end / prompt → first audio (TTFA)
    let callTtfaMs = null;    // first LISTENING-turn TTFA persisted to DB/History
    const noise = createNoiseEstimator({ frameMs: 20 });
    const vad = createSileroVad({ sampleRate: 8000 });
    let audioClockMs = 0;
    let agentPlaybackUntil = 0;
    const TURN_SILENCE_MS = 900;
    const PREROLL_BYTES = 9600;
    let activityOpen = false;
    let twilioMediaCount = 0;

    function isSpuriousUnclearResponse(text) {
      const t = (text || '').toLowerCase();
      return t.includes('sorry') && t.includes('repeat')
        && !t.includes('aware360') && !t.includes('monitoring center');
    }

    function startAudioFlush() {
      if (flushInterval) return;
      flushInterval = setInterval(function() {
        if (!geminiWs || geminiWs.readyState !== WebSocket.OPEN || sessionState !== 'LISTENING') return;
        if (!activityOpen) {
          if (pcmBuffer.length > PREROLL_BYTES) pcmBuffer = pcmBuffer.subarray(pcmBuffer.length - PREROLL_BYTES);
          return;
        }
        if (pcmBuffer.length === 0) return;
        const toSend = pcmBuffer;
        pcmBuffer = Buffer.alloc(0);
        geminiWs.send(JSON.stringify({
          realtime_input: { audio: { data: toSend.toString('base64'), mimeType: 'audio/pcm;rate=16000' } },
        }));
      }, 100);
    }

    function setCallOutcome(state, intent) {
      callLog.finalState = state;
      if (intent) callLog.finalIntent = intent;
    }

    /** Persist Gemini end-of-speech → first-audio latency (History uses ttfa_ms + llm_ms). */
    async function recordListeningTtfa(ttfa) {
      if (ttfa == null || !Number.isFinite(ttfa) || ttfa < 0) return;
      if (callTtfaMs != null) return; // keep first worker-turn TTFA
      callTtfaMs = Math.round(ttfa);
      callLog.ttfa_ms = callTtfaMs;
      console.log('[Twilio-Gemini] Recorded LISTENING TTFA for DB/History:', callTtfaMs + 'ms');
      if (callLog.callSid) {
        try {
          // llm_ms = TTFA for Gemini (no separate STT/TTS stages) so Total Turn / LLM columns work
          await updateCallLatency({
            callSid: callLog.callSid,
            sttMs: null,
            llmMs: callTtfaMs,
            ttsMs: null,
            ttfaMs: callTtfaMs,
          });
        } catch (e) {
          console.error('[Twilio-Gemini] Latency DB update error:', e.message);
        }
      }
    }

    function clearWorkerSilenceTimer() {
      if (workerSilenceTimer) { clearTimeout(workerSilenceTimer); workerSilenceTimer = null; }
    }

    function startWorkerSilenceTimer() {
      clearWorkerSilenceTimer();
      workerSilenceTimer = setTimeout(function() {
        if (sessionState !== 'LISTENING') return;
        silencePromptCount++;
        if (silencePromptCount === 1) {
          console.log('[Twilio-Gemini] 8s silence — prompting worker');
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            turnStartTime = Date.now();
            twilioFirstAudioTime = null; // allow silence-prompt audio to mark a turn if no TTFA yet
            geminiWs.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: '[SYSTEM] The worker has been silent for 8 seconds. Say exactly: "Are you still there? Please respond if you can hear me."' }] }],
                turn_complete: true,
              },
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
                turn_complete: true,
              },
            }));
          }
          setTimeout(function() {
            console.log('[Twilio-Gemini] Closing call (no response)');
            if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
            if (ws.readyState === WebSocket.OPEN) ws.close();
          }, 8000);
        }
      }, 8000);
    }

    const MULAW_SILENCE = Buffer.alloc(160, 0x7F);

    function startSilenceKeepalive() {
      silenceInterval = setInterval(function() {
        if (ws.readyState === WebSocket.OPEN && streamSid && sessionState === 'GREETING') {
          ws.send(JSON.stringify({
            event: 'media',
            streamSid: streamSid,
            media: { payload: MULAW_SILENCE.toString('base64') },
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
        media: { payload: mulawBuffer.toString('base64') },
      }));
    }

    function connectGeminiTwilio() {
      if (!GEMINI_API_KEY || GEMINI_API_KEY === 'your_gemini_key') {
        console.error('[Twilio-Gemini] GEMINI_API_KEY is not set');
        return;
      }
      const geminiUrl = 'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=' + GEMINI_API_KEY;
      geminiWs = new WebSocket(geminiUrl);

      geminiWs.on('open', function() {
        console.log('[Twilio-Gemini] Gemini connected');
        geminiWs.send(JSON.stringify({
          setup: {
            model: 'models/' + GEMINI_MODEL,
            generation_config: {
              response_modalities: ['AUDIO'],
              speech_config: {
                voice_config: { prebuilt_voice_config: { voice_name: 'Charon' } },
              },
            },
            realtime_input_config: { automatic_activity_detection: { disabled: true } },
            input_audio_transcription: {},
            output_audio_transcription: {},
            system_instruction: { parts: [{ text: SYSTEM_PROMPT }] },
          },
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
              console.log('[Twilio-Gemini] Greeting from disk cache,', GREETING_PCM_MS + 'ms');
              if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
              // Greeting playback is NOT call TTFA — History latency is first LISTENING response
              const greetingMs = Date.now() - twilioSetupTime;
              console.log('[Twilio-Gemini] Greeting start latency (not stored as TTFA):', greetingMs + 'ms');

              for (let off = 0; off < GREETING_ULAW.length; off += 160) {
                sendAudioToTwilio(GREETING_ULAW.subarray(off, off + 160));
              }
              callLog.transcript.push({ role: 'ai', text: WORKER_INTRO, time: new Date().toISOString() });

              geminiWs.send(JSON.stringify({
                client_content: {
                  turns: [
                    { role: 'user', parts: [{ text: '[SYSTEM] The greeting has already been played to the worker from a recording. Do not greet again. Stay completely silent until the worker speaks.' }] },
                  ],
                  turn_complete: false,
                },
              }));

              setTimeout(function() {
                pcmBuffer = Buffer.alloc(0);
                twilioAiTranscript = '';
                twilioFirstAudioTime = null;
                turnStartTime = null;
                sessionState = 'LISTENING';
                console.log('[Twilio-Gemini] Now listening');
                startAudioFlush();
                startWorkerSilenceTimer();
              }, GREETING_PCM_MS + 500);

              return;
            }
            geminiWs.send(JSON.stringify({
              client_content: {
                turns: [{ role: 'user', parts: [{ text: '[SYSTEM] Say this exact greeting word for word and nothing else: "' + TWILIO_WORKER_INTRO + '"' }] }],
                turn_complete: true,
              },
            }));
          }

          if (msg.serverContent) {
            const content = msg.serverContent;

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
                    if (sessionState === 'LISTENING' && !twilioFirstAudioTime) {
                      twilioFirstAudioTime = Date.now();
                      const base = turnStartTime || twilioSetupTime;
                      const ttfa = base ? (twilioFirstAudioTime - base) : null;
                      console.log('[Twilio-Gemini] FIRST AUDIO CHUNK | TTFA:', ttfa != null ? (ttfa + 'ms') : 'n/a',
                        '| turnStart=', !!turnStartTime);
                      recordListeningTtfa(ttfa);
                    } else if (sessionState === 'GREETING' && !twilioFirstAudioTime) {
                      twilioFirstAudioTime = Date.now();
                      console.log('[Twilio-Gemini] Greeting audio started (TTFA not stored until LISTENING)');
                    }
                    if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
                    const audioBuf = Buffer.from(part.inlineData.data, 'base64');
                    const mulawBuf = pcmToMulaw(audioBuf);
                    sendAudioToTwilio(mulawBuf);
                  } catch (e) {
                    console.error('[Twilio-Gemini] PCM→mulaw error:', e.message);
                  }
                }
              }
            }

            if (content.turnComplete) {
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
                  'could not reach you',
                ];
                if (resolutionPhrases.some(function(p) { return t.includes(p); })) {
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
                  setTimeout(function() {
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
                      turns: [{ role: 'user', parts: [{ text: '[SYSTEM] Say this exact greeting word for word and nothing else: "' + TWILIO_WORKER_INTRO + '"' }] }],
                      turn_complete: true,
                    },
                  }));
                  return;
                }
                console.log('[Twilio-Gemini] Greeting audio done, waiting 2s before listening...');
                setTimeout(function() {
                  pcmBuffer = Buffer.alloc(0);
                  twilioAiTranscript = '';
                  twilioFirstAudioTime = null;
                  turnStartTime = null;
                  sessionState = 'LISTENING';
                  console.log('[Twilio-Gemini] Now listening');
                  startAudioFlush();
                  startWorkerSilenceTimer();
                }, 2000);
              }
            }
          }
        } catch (e) {
          console.error('[Twilio-Gemini] Message error:', e.message);
        }
      });

      geminiWs.on('error', function(e) { console.error('[Twilio-Gemini] Error:', e.message); });
      geminiWs.on('close', function(code, reason) {
        console.log('[Twilio-Gemini] Gemini closed | code:', code, '| reason:', reason ? reason.toString() : 'none');
      });
    }

    ws.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.event === 'connected') {
          console.log('[Twilio-Gemini] Stream connected');
        }

        if (msg.event === 'start') {
          streamSid = msg.start.streamSid;
          callLog.callSid = msg.start.callSid || streamSid;
          callLog.streamSid = streamSid;
          callLog.workerPhone = (msg.start.customParameters && msg.start.customParameters.worker)
            || (msg.start.customParameters && msg.start.customParameters.caller)
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
          twilioMediaCount++;

          // Throttled Render diagnostics: confirm Silero VAD is producing speech probability
          if (twilioMediaCount % 50 === 0) {
            console.log('[Twilio-Gemini Media]', 'frame#' + twilioMediaCount,
              '| vad.isSpeaking=' + vad.isSpeaking(),
              '| vad.prob=' + (vad.prob ? vad.prob() : 'n/a'));
          }

          if (activityOpen && twilioMediaCount % 25 === 0) {
            console.log('[Twilio-Gemini VAD]', 'isSpeaking=' + vad.isSpeaking(), '| idleMs=' + vad.idleMs(), '| audioClockMs=' + audioClockMs.toFixed(0));
          }

          if (sessionState === 'LISTENING' && geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            if (vad.isSpeaking() && !activityOpen) {
              activityOpen = true;
              clearWorkerSilenceTimer();
              geminiWs.send(JSON.stringify({ realtime_input: { activity_start: {} } }));
              console.log('[Twilio-Gemini Turn] activity_start @', audioClockMs.toFixed(0) + 'ms');
            }

            if (activityOpen && !vad.isSpeaking() && vad.idleMs() >= TURN_SILENCE_MS) {
              activityOpen = false;
              const spans = vad.takeSpans();
              const m = noise.endTurn({ speechSpans: spans.length ? spans : null });
              console.log('[Twilio-Gemini Noise] snr=' + (m.snrDb != null ? m.snrDb.toFixed(1) : 'n/a')
                + ' noisy=' + m.noisy + ' collisions=' + m.collisions.length + ' spans=' + spans.length);

              if (!callLog.noiseTurns) callLog.noiseTurns = [];
              const entry = {
                time: new Date().toISOString(),
                snrDb: m.snrDb,
                noiseLevelDbfs: m.noiseLevelDbfs,
                speechDbfs: m.speechDbfs,
                noisy: m.noisy,
                snrReliable: m.snrReliable,
                collisions: m.collisions.length,
                injected: false,
                report: null,
              };
              callLog.noiseTurns.push(entry);

              const gapCount = m.collisions.filter(function(c) { return c.type === 'gap'; }).length;
              const speechMs = spans.reduce(function(sum, s) { return sum + (s.endMs - s.startMs); }, 0);
              const snrLow = m.snrReliable && m.snrDb != null && m.snrDb < 12 && speechMs >= 1000;
              const degraded = gapCount > 0 || m.noisy || snrLow;

              if (degraded) {
                const report = buildMonitorReport(m, spans);
                console.log('[Twilio-Gemini Monitor]', report);
                entry.injected = true;
                entry.report = report;
                geminiWs.send(JSON.stringify({
                  client_content: { turns: [{ role: 'user', parts: [{ text: report }] }], turn_complete: false },
                }));
              }

              geminiWs.send(JSON.stringify({ realtime_input: { activity_end: {} } }));
              turnStartTime = Date.now();
              twilioFirstAudioTime = null; // next model audio = this turn's TTFA candidate
              startWorkerSilenceTimer();
              console.log('[Twilio-Gemini Turn] activity_end');
            }

            try {
              pcmBuffer = Buffer.concat([pcmBuffer, mulawToPcm(mulawChunk)]);
            } catch (e) {
              console.error('[Twilio-Gemini] mulawToPcm error:', e.message);
            }
          }
        }

        if (msg.event === 'stop') {
          console.log('[Twilio-Gemini] Stream stopped');
          if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
        }
      } catch (e) {
        console.error('[Twilio-Gemini WS Error]', e.message);
      }
    });

    ws.on('close', async function() {
      console.log('[Twilio-Gemini WS] Disconnected');
      clearWorkerSilenceTimer();
      if (silenceInterval) { clearInterval(silenceInterval); silenceInterval = null; }
      if (flushInterval) { clearInterval(flushInterval); flushInterval = null; }
      if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();

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

        const filename = path.join(logsDir, 'call_' + (callLog.callSid || Date.now()) + '.json');
        fs.writeFileSync(filename, JSON.stringify(callLog, null, 2));
        console.log('[Twilio-Gemini] Call log saved:', filename);

        if (callLog.callSid) {
          const ttfa = callTtfaMs != null ? callTtfaMs : callLog.ttfa_ms;
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
            ttfaMs: ttfa,
            // Gemini has no separate STT/TTS — mirror Cascaded Total Turn via llm_ms = TTFA
            sttMs: null,
            llmMs: ttfa,
            ttsMs: null,
          });
          console.log('[Twilio-Gemini] Call saved to DB:', callLog.callSid, '|', finalSessionState,
            '| ttfa_ms:', ttfa != null ? ttfa : 'n/a');
        }
      } catch (e) {
        console.error('[Twilio-Gemini] Log save error:', e.message);
      }
    });
  });
}

module.exports = {
  attachTwilioGeminiWss: attachTwilioGeminiWss,
  pcmToMulaw: pcmToMulaw,
  mulawToPcm: mulawToPcm,
};
