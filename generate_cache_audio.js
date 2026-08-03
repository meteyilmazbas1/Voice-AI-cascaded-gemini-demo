/**
 * Generate / refresh all standard audio_cache/*.mp3 phrases via ElevenLabs.
 * Covers INTENT_CACHE_MAP keys + greeting, escalate_max, voicemail.
 *
 * Usage: node generate_cache_audio.js
 *        node generate_cache_audio.js --only greeting,safe_confirmation
 */
require('dotenv').config();
const fs = require('fs');
const path = require('path');
const axios = require('axios');

const VOICE_ID = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
const API_KEY = process.env.ELEVENLABS_API_KEY;

const CACHE_DIR = path.join(__dirname, 'audio_cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR, { recursive: true });

/** Same keys/texts as server.js CACHE_RESPONSES + greeting / escalate / voicemail. */
const PHRASES = {
  greeting:
    'Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay?',
  safe_confirmation: "Got it — I've reset your safety timer. Stay safe.",
  delayed_checkin_15:
    "Understood. I've extended your check-in timer. Please check in when ready.",
  request_human:
    "Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available.",
  unrecognised:
    "I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?",
  escalate_max:
    "I'm unable to process your response. A human agent will follow up shortly.",
  voicemail:
    'Hello, this is the Aware360 Monitoring Center. We tried to reach you regarding a missed check-in. Please check in through your app or call back as soon as possible. If you need immediate assistance, please contact your supervisor. Thank you, stay safe.',
};

function parseOnlyArg() {
  const idx = process.argv.indexOf('--only');
  if (idx === -1 || !process.argv[idx + 1]) return null;
  return process.argv[idx + 1].split(',').map(function (s) { return s.trim(); }).filter(Boolean);
}

async function generateAudio(key, text) {
  console.log('Generating:', key, '…');
  const response = await axios.post(
    'https://api.elevenlabs.io/v1/text-to-speech/' + VOICE_ID,
    {
      text: text,
      model_id: 'eleven_flash_v2_5',
      voice_settings: { stability: 0.5, similarity_boost: 0.75 },
    },
    {
      headers: {
        'xi-api-key': API_KEY,
        'Content-Type': 'application/json',
        Accept: 'audio/mpeg',
      },
      responseType: 'arraybuffer',
    }
  );
  const filePath = path.join(CACHE_DIR, key + '.mp3');
  fs.writeFileSync(filePath, Buffer.from(response.data));
  console.log('  ✓ Saved:', filePath, '(' + response.data.byteLength + ' bytes)');
}

async function main() {
  if (!API_KEY || API_KEY === 'your_elevenlabs_key') {
    throw new Error('ELEVENLABS_API_KEY not set');
  }
  const only = parseOnlyArg();
  const keys = only || Object.keys(PHRASES);
  for (let i = 0; i < keys.length; i++) {
    const key = keys[i];
    const text = PHRASES[key];
    if (!text) {
      console.warn('Unknown key, skip:', key);
      continue;
    }
    await generateAudio(key, text);
    await new Promise(function (r) { setTimeout(r, 300); });
  }
  console.log('\n✅ Done. Force-track with:');
  console.log('  git add -f audio_cache/*.mp3');
}

main().catch(function (e) {
  console.error(e);
  process.exit(1);
});
