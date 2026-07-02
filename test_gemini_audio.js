const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
require('dotenv').config();

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-3.1-flash-live-preview';

// Auto-detect upload directory based on OS
const UPLOAD_DIR = 'C:\\Users\\meyilmaz\\OneDrive - Orion Systems Integrators, LLC\\Documents\\voice_ai\\uploads';

const SYSTEM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone worker safety platform handling a MISSED CHECK-IN alert for an industrial/field worker.

Your task: Listen to the worker's audio response and classify their spoken intent into one of three categories:
- SAFE_CONFIRMATION: Worker confirms they are safe
- REQUEST_HUMAN: Worker requests a human agent or expresses distress
- DELAYED_CHECKIN: Worker needs more time before check-in

Respond ONLY with JSON in this exact format:
{"intent":"<SAFE_CONFIRMATION|REQUEST_HUMAN|DELAYED_CHECKIN>","confidence":<0.0-1.0>,"response":"<brief reply>"}

CRITICAL SAFETY RULE: If you detect ANY physical distress (fell, hurt, stuck, scared, dizzy, etc.), ALWAYS return REQUEST_HUMAN regardless of positive statements.

Keep responses brief (under 15 words).`;

const TEST_CASES = [
  {
    file: process.env.UPLOAD_DIR + '/gemini_haiku_comparison(fi... oka... fine).mp3',
    transcript: 'fi... oka... fine',
    category: 'Noisy - Broken'
  },
  {
    file: process.env.UPLOAD_DIR + '/gemini_haiku_comparison(I think I\'m fine but I\'m a bit worried).mp3',
    transcript: "I think I'm fine but I'm a bit worried",
    category: 'Contradictory - Hesitant'
  },
  {
    file: process.env.UPLOAD_DIR + '/gemini_haiku_comparison(I\'m fine I\'m safe. I broke my leg and it hurts too much I\'m not good I\'m okay).mp3',
    transcript: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay",
    category: 'Contradictory - Critical'
  },
  {
    file: process.env.UPLOAD_DIR + '/gemini_haiku_comparison(Everything is fine I just fell down but I\'m okay I promise).mp3',
    transcript: "Everything is fine I just fell down but I'm okay I promise",
    category: 'Distress - Minimizing'
  },
];

function connectGemini(audioData) {
  return new Promise((resolve, reject) => {
    const ws = new WebSocket(`wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`);

    let setupDone = false;
    let turnStartTime = null;
    let outputText = '';
    let promptTokens = 0, responseTokens = 0;
    const timeout = setTimeout(() => { 
      console.log('[Gemini] Timeout after 60s');
      ws.close(); 
      reject(new Error('timeout after 60s')); 
    }, 60000);

    ws.on('open', () => {
      console.log('[Gemini] WebSocket connected');
      ws.send(JSON.stringify({
        setup: {
          model: `models/${GEMINI_MODEL}`,
          generation_config: { response_modalities: ['TEXT'] },
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] }
        }
      }));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.setupComplete && !setupDone) {
          console.log('[Gemini] Setup complete, sending audio...');
          setupDone = true;
          turnStartTime = Date.now();
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

        if (msg.serverContent && msg.serverContent.outputTranscription) {
          outputText += msg.serverContent.outputTranscription.text || '';
        }

        if (msg.serverContent && msg.serverContent.turnComplete) {
          const latency = Date.now() - turnStartTime;
          console.log('[Gemini] Turn complete, latency: ' + latency + 'ms');
          clearTimeout(timeout);
          setTimeout(() => {
            resolve({ latency, output: outputText.trim(), promptTokens, responseTokens });
            ws.close();
          }, 200);
        }

        if (msg.usageMetadata) {
          promptTokens = msg.usageMetadata.promptTokenCount || 0;
          responseTokens = msg.usageMetadata.responseTokenCount || 0;
        }
      } catch (e) {
        console.log('[Gemini] Message parse error:', e.message);
      }
    });

    ws.on('error', (e) => { 
      console.log('[Gemini] WebSocket error:', e.message);
      clearTimeout(timeout); 
      reject(e); 
    });

    ws.on('close', () => {
      console.log('[Gemini] WebSocket closed');
    });
  });
}

async function convertMp3ToPcm(mp3Path) {
  return new Promise((resolve, reject) => {
    // Check if file exists first
    if (!fs.existsSync(mp3Path)) {
      return reject(new Error(`File not found: ${mp3Path}`));
    }

    const { spawn } = require('child_process');
    const pcmPath = mp3Path.replace('.mp3', '.pcm');
    
    // Use full path to ffmpeg on Windows
    const ffmpegCmd = process.platform === 'win32' ? 'ffmpeg.exe' : 'ffmpeg';

    const ffmpeg = spawn(ffmpegCmd, [
      '-i', mp3Path,
      '-ar', '16000',
      '-ac', '1',
      '-f', 's16le',
      pcmPath
    ], { stdio: ['pipe', 'pipe', 'pipe'] });

    let errorOutput = '';
    ffmpeg.stderr.on('data', (data) => { errorOutput += data.toString(); });

    ffmpeg.on('close', (code) => {
      if (code === 0 && fs.existsSync(pcmPath)) {
        resolve(fs.readFileSync(pcmPath));
      } else {
        reject(new Error(`ffmpeg failed: ${errorOutput}`));
      }
    });

    ffmpeg.on('error', (err) => reject(err));
  });
}

async function run() {
  console.log('\n=== GEMINI LIVE AUDIO TEST ===');
  console.log(`Upload directory: ${UPLOAD_DIR}\n`);

  for (const testCase of TEST_CASES) {
    console.log(`Testing: ${testCase.category}`);
    console.log(`Expected: "${testCase.transcript}"\n`);

    try {
      console.log('Converting MP3 to PCM...');
      const audioData = await convertMp3ToPcm(testCase.file);
      console.log(`Audio size: ${(audioData.length / 16000).toFixed(2)}s\n`);

      console.log('Sending to Gemini...');
      const result = await connectGemini(audioData);

      console.log(`Latency: ${result.latency}ms`);
      console.log(`Tokens: ${result.promptTokens} input, ${result.responseTokens} output`);
      console.log(`Response: ${result.output}\n`);

      // Try to parse JSON response
      try {
        const parsed = JSON.parse(result.output);
        console.log(`  Intent: ${parsed.intent}`);
        console.log(`  Confidence: ${(parsed.confidence * 100).toFixed(0)}%`);
        console.log(`  Reply: "${parsed.response}"\n`);
      } catch (e) {
        console.log(`  (Could not parse JSON response)\n`);
      }

      await new Promise(r => setTimeout(r, 1000));
    } catch (e) {
      console.log(`ERROR: ${e.message}\n`);
    }
  }

  console.log('Test complete.');
}

run().catch(console.error);