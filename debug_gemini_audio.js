const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
require('dotenv').config();

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const UPLOAD_DIR = 'C:\\Users\\meyilmaz\\OneDrive - Orion Systems Integrators, LLC\\Documents\\voice_ai\\uploads';

// Test only one file first
const TEST_FILE = UPLOAD_DIR + '/gemini_haiku_comparison1.mp3';

async function convertMp3ToPcm(mp3Path) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(mp3Path)) return reject(new Error(`File not found: ${mp3Path}`));
    const { spawn } = require('child_process');
    const pcmPath = mp3Path.replace('.mp3', '_debug.pcm');
    const ffmpeg = spawn('ffmpeg.exe', [
      '-y', '-i', mp3Path,
      '-ar', '16000', '-ac', '1', '-f', 's16le', pcmPath
    ], { stdio: ['pipe', 'pipe', 'pipe'] });
    ffmpeg.on('close', (code) => {
      if (code === 0) resolve(fs.readFileSync(pcmPath));
      else reject(new Error('ffmpeg failed'));
    });
    ffmpeg.on('error', reject);
  });
}

async function debugGeminiAudio(model, audioData) {
  return new Promise((resolve) => {
    console.log(`\n[${model}] Connecting...`);
    const ws = new WebSocket(`wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`);
    
    let msgCount = 0;
    const timeout = setTimeout(() => {
      console.log(`[${model}] Timeout (30s), closing`);
      ws.close();
      resolve();
    }, 30000);

    ws.on('open', () => {
      console.log(`[${model}] Open, sending setup...`);
      ws.send(JSON.stringify({
        setup: {
          model: `models/${model}`,
          generation_config: { response_modalities: ['TEXT'] },
          input_audio_transcription: {},
          output_audio_transcription: {},
        }
      }));
    });

    ws.on('message', (data) => {
      msgCount++;
      const msg = JSON.parse(data.toString());
      
      console.log(`\n[${model}] MSG #${msgCount}:`);
      console.log(JSON.stringify(msg, null, 2));

      if (msg.setupComplete) {
        console.log(`[${model}] Setup complete, sending audio...`);
        ws.send(JSON.stringify({
          client_content: {
            turns: [{
              role: 'user',
              parts: [{
                inlineData: {
                  mimeType: 'audio/pcm;rate=16000',
                  data: audioData.toString('base64')
                }
              }]
            }],
            turn_complete: true
          }
        }));
      }

      if (msg.serverContent && msg.serverContent.turnComplete) {
        clearTimeout(timeout);
        console.log(`\n[${model}] Done, total messages: ${msgCount}`);
        setTimeout(() => { ws.close(); resolve(); }, 200);
      }
    });

    ws.on('error', (e) => {
      console.log(`[${model}] Error: ${e.message}`);
      clearTimeout(timeout);
      resolve();
    });
  });
}

async function run() {
  console.log('=== GEMINI AUDIO DEBUG ===');
  console.log(`File: ${TEST_FILE}\n`);

  let audioData;
  try {
    console.log('Converting MP3 to PCM...');
    audioData = await convertMp3ToPcm(TEST_FILE);
    console.log(`PCM size: ${audioData.length} bytes (${(audioData.length / 16000 / 2).toFixed(2)}s)`);
  } catch (e) {
    console.log('ERROR:', e.message);
    return;
  }

  // Test only 2.0 first
  await debugGeminiAudio('gemini-2.0-flash-live-001', audioData);
}

run().catch(console.error);
