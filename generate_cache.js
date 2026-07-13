const fs = require('fs');
const path = require('path');
const axios = require('axios');

// .env oku
const envFile = fs.readFileSync('.env', 'utf8');
envFile.split('\n').forEach(line => {
  const [key, ...val] = line.trim().split('=');
  if (key && val.length) process.env[key] = val.join('=');
});

const VOICE_ID = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
const API_KEY = process.env.ELEVENLABS_API_KEY;

const CACHE_DIR = path.join(__dirname, 'audio_cache');
if (!fs.existsSync(CACHE_DIR)) fs.mkdirSync(CACHE_DIR);

const PHRASES = {
  'greeting': `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`,
  'safe_confirmation': `Got it — I've reset your safety timer. Stay safe.`,
  'delayed_checkin_15': `Understood. I've extended your check-in timer. Please check in when ready.`,
  'request_human': `Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available.`,
  'unrecognised': `I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?`,
  'escalate_max': `I'm unable to process your response. A human agent will follow up shortly.`,
  'voicemail': `Hello, this is the Aware360 Monitoring Center. We tried to reach you regarding a missed check-in. Please check in through your app or call back as soon as possible. If you need immediate assistance, please contact your supervisor. Thank you, stay safe.`,
};

async function generateAudio(key, text) {
  console.log(`Generating: ${key}...`);
  const response = await axios.post(
    `https://api.elevenlabs.io/v1/text-to-speech/${VOICE_ID}`,
    { text, model_id: 'eleven_flash_v2_5', voice_settings: { stability: 0.5, similarity_boost: 0.75 } },
    { headers: { 'xi-api-key': API_KEY, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' }, responseType: 'arraybuffer' }
  );
  const filePath = path.join(CACHE_DIR, `${key}.mp3`);
  fs.writeFileSync(filePath, Buffer.from(response.data));
  console.log(`  ✓ Saved: ${filePath} (${response.data.byteLength} bytes)`);
}

async function main() {
  for (const [key, text] of Object.entries(PHRASES)) {
    await generateAudio(key, text);
    await new Promise(r => setTimeout(r, 300));
  }
  console.log('\n✅ All audio files cached!');
}

main().catch(console.error);
