/**
 * One-shot: regenerate Cascaded greeting.mp3 (ElevenLabs) and
 * Gemini greeting_24k.pcm (Gemini Live Charon) for the current WORKER_INTRO text.
 *
 * Usage: node scripts/regenerate_greeting_caches.js
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');
const WebSocket = require('ws');
const { spawn } = require('child_process');

const GREETING =
  'Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay?';

const ROOT = path.join(__dirname, '..');
const MP3_PATH = path.join(ROOT, 'audio_cache', 'greeting.mp3');
const PCM_PATH = path.join(ROOT, 'audio', 'greeting_24k.pcm');

async function generateElevenLabsMp3() {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
  if (!apiKey || apiKey === 'your_elevenlabs_key') {
    throw new Error('ELEVENLABS_API_KEY not set');
  }
  console.log('[ElevenLabs] Generating greeting.mp3…');
  const response = await axios.post(
    'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId,
    {
      text: GREETING,
      model_id: 'eleven_flash_v2_5',
      voice_settings: { stability: 0.5, similarity_boost: 0.75 },
    },
    {
      headers: {
        'xi-api-key': apiKey,
        'Content-Type': 'application/json',
        Accept: 'audio/mpeg',
      },
      responseType: 'arraybuffer',
    }
  );
  const dir = path.dirname(MP3_PATH);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  fs.writeFileSync(MP3_PATH, Buffer.from(response.data));
  console.log('[ElevenLabs] Saved', MP3_PATH, '(' + response.data.byteLength + ' bytes)');
}

function generatePcmFromMp3Fallback() {
  return new Promise(function (resolve, reject) {
    console.log('[ffmpeg] Fallback: converting greeting.mp3 → greeting_24k.pcm (s16le 24kHz)…');
    const dir = path.dirname(PCM_PATH);
    if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
    const ff = spawn(
      'ffmpeg',
      ['-y', '-i', MP3_PATH, '-ac', '1', '-ar', '24000', '-f', 's16le', PCM_PATH],
      { stdio: ['ignore', 'ignore', 'pipe'] }
    );
    let err = '';
    ff.stderr.on('data', function (d) { err += d.toString(); });
    ff.on('close', function (code) {
      if (code !== 0) return reject(new Error('ffmpeg failed: ' + err.slice(-400)));
      const size = fs.statSync(PCM_PATH).size;
      console.log('[ffmpeg] Saved', PCM_PATH, '(' + size + ' bytes, ~' + Math.round(size / 48) + 'ms)');
      resolve();
    });
  });
}

function generateGeminiPcm() {
  return new Promise(function (resolve, reject) {
    const apiKey = process.env.GEMINI_API_KEY;
    if (!apiKey || apiKey === 'your_gemini_key') {
      return reject(new Error('GEMINI_API_KEY not set'));
    }
    const model = process.env.GEMINI_LIVE_MODEL || 'gemini-3.1-flash-live-preview';
    const url =
      'wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=' +
      apiKey;

    console.log('[Gemini Live] Synthesizing greeting_24k.pcm with', model, '…');
    const chunks = [];
    let settled = false;
    let setupDone = false;
    const timeout = setTimeout(function () {
      if (!settled) {
        settled = true;
        try { ws.close(); } catch (e) {}
        reject(new Error('Gemini Live greeting timed out'));
      }
    }, 45000);

    const ws = new WebSocket(url);

    function finishOk() {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (e) {}
      const buf = Buffer.concat(chunks);
      if (buf.length < 24000) {
        return reject(new Error('Gemini greeting too short: ' + buf.length + ' bytes'));
      }
      const dir = path.dirname(PCM_PATH);
      if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
      fs.writeFileSync(PCM_PATH, buf);
      console.log('[Gemini Live] Saved', PCM_PATH, '(' + buf.length + ' bytes, ~' + Math.round(buf.length / 48) + 'ms)');
      resolve();
    }

    function finishErr(err) {
      if (settled) return;
      settled = true;
      clearTimeout(timeout);
      try { ws.close(); } catch (e) {}
      reject(err);
    }

    ws.on('open', function () {
      ws.send(JSON.stringify({
        setup: {
          model: 'models/' + model,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: {
                prebuilt_voice_config: { voice_name: 'Charon' },
              },
            },
          },
          system_instruction: {
            parts: [{
              text: 'You are a TTS engine. Speak the user message exactly, word for word, and nothing else. Do not add greetings of your own.',
            }],
          },
        },
      }));
    });

    ws.on('message', function (data) {
      try {
        const msg = JSON.parse(Buffer.isBuffer(data) ? data.toString('utf8') : data.toString());
        if ((msg.setupComplete || msg.setup_complete) && !setupDone) {
          setupDone = true;
          ws.send(JSON.stringify({
            client_content: {
              turns: [{
                role: 'user',
                parts: [{
                  text: '[SYSTEM] Say this exact greeting word for word and nothing else: "' + GREETING + '"',
                }],
              }],
              turn_complete: true,
            },
          }));
          return;
        }
        if (msg.error) {
          console.error('[Gemini Live] error:', JSON.stringify(msg.error).slice(0, 300));
        }
        const sc = msg.serverContent || msg.server_content;
        if (sc && sc.modelTurn && sc.modelTurn.parts) {
          for (let i = 0; i < sc.modelTurn.parts.length; i++) {
            const part = sc.modelTurn.parts[i];
            const inline = part.inlineData || part.inline_data;
            if (inline && inline.data) {
              chunks.push(Buffer.from(inline.data, 'base64'));
            }
          }
        }
        if (sc && (sc.turnComplete || sc.turn_complete)) {
          finishOk();
        }
      } catch (e) {
        finishErr(e);
      }
    });

    ws.on('error', function (e) { finishErr(e); });
    ws.on('close', function () {
      if (!settled && chunks.length >= 24000) finishOk();
      else if (!settled) finishErr(new Error('Gemini WS closed early, bytes=' + chunks.reduce(function (n, b) { return n + b.length; }, 0)));
    });
  });
}

async function main() {
  await generateElevenLabsMp3();
  try {
    await generateGeminiPcm();
  } catch (e) {
    console.warn('[Gemini Live] Failed:', e.message);
    console.warn('[Gemini Live] Falling back to ffmpeg from greeting.mp3');
    await generatePcmFromMp3Fallback();
  }
  console.log('\nDone. Force-add with:');
  console.log('  git add -f audio_cache/greeting.mp3 audio/greeting_24k.pcm');
}

main().catch(function (e) {
  console.error(e);
  process.exit(1);
});
