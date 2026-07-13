const fs = require('fs');
const path = require('path');
const WebSocket = require('ws');
require('dotenv').config();

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

// Hard-code Windows upload directory
const UPLOAD_DIR = 'C:\\Users\\meyilmaz\\OneDrive - Orion Systems Integrators, LLC\\Documents\\voice_ai\\uploads';

const SYSTEM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone worker safety platform handling a MISSED CHECK-IN alert for an industrial/field worker.

When the worker speaks, classify their intent and respond with ONE of these exact phrases:
- If safe: "Got it — I've reset your safety timer. Stay safe."
- If needs more time: "Understood. I've extended your check-in timer. Please check in when ready."
- If distress or wants human: "Of course. I'm flagging this for a live agent — someone will call you back shortly."
- If unclear: "Sorry, I didn't catch that. Could you repeat?"

CRITICAL SAFETY RULE: If the worker mentions ANY physical distress (fell, hurt, stuck, scared, dizzy, broke, injured) — even with positive statements — ALWAYS use the live agent response.

Keep responses short. Be direct and professional.`;

const TEST_CASES = [
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison1.mp3',
    transcript: 'fi... oka... fine',
    category: 'Noisy - Broken',
    expected: 'SAFE_CONFIRMATION'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison2.mp3',
    transcript: "I think I'm fine but I'm a bit worried",
    category: 'Contradictory - Hesitant',
    expected: 'REQUEST_HUMAN'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison3.mp3',
    transcript: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay",
    category: 'Contradictory - Critical',
    expected: 'REQUEST_HUMAN'
  },
  {
    file: UPLOAD_DIR + '/gemini_haiku_comparison4.mp3',
    transcript: "Everything is fine I just fell down but I'm okay I promise",
    category: 'Distress - Minimizing',
    expected: 'REQUEST_HUMAN'
  },
];

const MODELS = [
  'gemini-2.0-flash-live-001',
  'gemini-3.1-flash-live-preview'
];

function connectGemini(model, transcript) {
  return new Promise((resolve, reject) => {
    console.log(`    [${model}] Connecting...`);
    const ws = new WebSocket(`wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`);

    let setupDone = false;
    let turnStartTime = null;
    let outputText = '';
    let promptTokens = 0, responseTokens = 0;
    let messageCount = 0;

    const timeout = setTimeout(() => {
      console.log(`    [${model}] Timeout (60s)`);
      ws.close();
      reject(new Error('timeout'));
    }, 60000);

    ws.on('open', () => {
      console.log(`    [${model}] Open, sending setup...`);
      ws.send(JSON.stringify({
        setup: {
          model: `models/${model}`,
          generation_config: {
            response_modalities: ['AUDIO'],
            speech_config: {
              voice_config: {
                prebuilt_voice_config: { voice_name: 'Charon' }
              }
            }
          },
          output_audio_transcription: {},
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] }
        }
      }));
    });

    ws.on('message', (data) => {
      messageCount++;
      try {
        const msg = JSON.parse(data.toString());

        if (msg.setupComplete && !setupDone) {
          setupDone = true;
          turnStartTime = Date.now();
          console.log(`    [${model}] Setup OK, sending text...`);
          ws.send(JSON.stringify({
            client_content: {
              turns: [{
                role: 'user',
                parts: [{ text: `Worker said: "${transcript}"` }]
              }],
              turn_complete: true
            }
          }));
        }

        if (msg.serverContent) {
          const content = msg.serverContent;
          if (content.outputTranscription && content.outputTranscription.text) {
            outputText += content.outputTranscription.text;
          }
          if (content.modelTurn && content.modelTurn.parts) {
            for (const part of content.modelTurn.parts) {
              if (part.text) outputText += part.text;
            }
          }
          if (content.turnComplete) {
            const latency = Date.now() - turnStartTime;
            console.log(`    [${model}] Done (${latency}ms, ${messageCount} msgs)`);
            clearTimeout(timeout);
            setTimeout(() => { resolve({ latency, output: outputText.trim(), promptTokens, responseTokens }); ws.close(); }, 100);
          }
        }

        if (msg.usageMetadata) {
          promptTokens = msg.usageMetadata.promptTokenCount || 0;
          responseTokens = msg.usageMetadata.responseTokenCount || 0;
        }
      } catch (e) {}
    });

    ws.on('error', (e) => { console.log(`    [${model}] Error: ${e.message}`); clearTimeout(timeout); reject(e); });
    ws.on('close', () => console.log(`    [${model}] Closed`));
  });
}

async function convertMp3ToPcm(mp3Path) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(mp3Path)) {
      return reject(new Error(`File not found: ${mp3Path}`));
    }

    const { spawn } = require('child_process');
    const pcmPath = mp3Path.replace('.mp3', '.pcm');
    
    const ffmpeg = spawn('ffmpeg.exe', [
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

function avg(arr) { return arr.length ? Math.round(arr.reduce((a, b) => a + b, 0) / arr.length) : 0; }

async function run() {
  console.log('\n=== GEMINI 2.0 vs 3.1 MODEL COMPARISON ===');
  console.log('Comparing: gemini-2.0-flash-live-001 vs gemini-3.1-flash-live-preview\n');

  const results = [];

  for (const testCase of TEST_CASES) {
    console.log(`Testing: ${testCase.category}`);
    console.log(`Expected intent: ${testCase.expected}`);
    console.log(`Audio: "${testCase.transcript}"\n`);

    const caseResult = {
      category: testCase.category,
      expected: testCase.expected,
      models: {}
    };

    for (const model of MODELS) {
      const modelName = model.includes('2.0') ? '2.0-flash' : '3.1-flash';
      console.log(`  Testing with ${modelName}...`);

      try {
        // Send transcript as text directly
        try {
          const result = await connectGemini(model, testCase.transcript);
          console.log(`    Latency: ${result.latency}ms | Tokens: ${result.promptTokens + result.responseTokens}`);
          
          let intent = 'UNRECOGNISED', confidence = 0;
          const out = result.output.toLowerCase();
          if (out.includes('safety timer') || out.includes('reset')) {
            intent = 'SAFE_CONFIRMATION'; confidence = 0.95;
          } else if (out.includes('extended') || out.includes('check-in timer')) {
            intent = 'DELAYED_CHECKIN'; confidence = 0.95;
          } else if (out.includes('live agent') || out.includes('call you back')) {
            intent = 'REQUEST_HUMAN'; confidence = 0.95;
          } else if (out.includes('catch that') || out.includes('repeat')) {
            intent = 'UNRECOGNISED'; confidence = 0.90;
          } else if (out.includes('parse_error') || out === '') {
            intent = 'PARSE_ERROR'; confidence = 0;
          }

          const correct = intent === testCase.expected ? '✓' : '✗';

          caseResult.models[modelName] = {
            avgLatency: result.latency,
            avgTokens: result.promptTokens + result.responseTokens,
            intent,
            confidence,
            correct: intent === testCase.expected
          };

          console.log(`    ${modelName}: ${intent} (${(confidence * 100).toFixed(0)}%) ${correct}\n`);
        } catch (e) {
          console.log(`    ERROR: ${e.message}\n`);
        }
      } catch (e) {
        console.log(`  ERROR: ${e.message}\n`);
      }

      await new Promise(r => setTimeout(r, 2000));
    }

    results.push(caseResult);
  }

  // Summary table
  console.log('\n========================================');
  console.log('COMPARISON SUMMARY TABLE');
  console.log('========================================\n');

  const pad = (s, n) => String(s).padEnd(n);
  console.log(
    pad('Category', 25) + '| ' +
    pad('Expected', 18) + '| ' +
    pad('2.0 Intent', 18) + '| ' +
    pad('2.0 Latency', 12) + '| ' +
    pad('3.1 Intent', 18) + '| ' +
    pad('3.1 Latency', 12)
  );
  console.log('-'.repeat(115));

  let model20Correct = 0, model31Correct = 0;
  let model20TotalLatency = 0, model31TotalLatency = 0;

  for (const r of results) {
    const m20 = r.models['2.0-flash'];
    const m31 = r.models['3.1-flash'];

    if (m20 && m20.correct) model20Correct++;
    if (m31 && m31.correct) model31Correct++;
    if (m20) model20TotalLatency += m20.avgLatency;
    if (m31) model31TotalLatency += m31.avgLatency;

    const cat = r.category.substring(0, 23);
    const exp = r.expected;
    const int20 = m20 ? m20.intent + (m20.correct ? ' ✓' : ' ✗') : '-';
    const lat20 = m20 ? m20.avgLatency + 'ms' : '-';
    const int31 = m31 ? m31.intent + (m31.correct ? ' ✓' : ' ✗') : '-';
    const lat31 = m31 ? m31.avgLatency + 'ms' : '-';

    console.log(
      pad(cat, 25) + '| ' +
      pad(exp, 18) + '| ' +
      pad(int20, 18) + '| ' +
      pad(lat20, 12) + '| ' +
      pad(int31, 18) + '| ' +
      pad(lat31, 12)
    );
  }

  const n = TEST_CASES.length;
  console.log('\n========================================');
  console.log('OVERALL SCORES');
  console.log('========================================');
  console.log('');
  console.log('                          2.0-flash-live-001    3.1-flash-live-preview');
  console.log('Accuracy:                 ' + model20Correct + '/' + n + ' (' + Math.round(model20Correct/n*100) + '%)           ' + model31Correct + '/' + n + ' (' + Math.round(model31Correct/n*100) + '%)');
  console.log('Avg Latency:              ' + Math.round(model20TotalLatency/n) + 'ms                   ' + Math.round(model31TotalLatency/n) + 'ms');
  console.log('');

  const faster = model20TotalLatency < model31TotalLatency ? '2.0-flash' : '3.1-flash';
  const moreAccurate = model20Correct > model31Correct ? '2.0-flash' : (model31Correct > model20Correct ? '3.1-flash' : 'TIE');

  console.log('WINNER (Speed):           ' + faster);
  console.log('WINNER (Accuracy):        ' + moreAccurate);
  console.log('');

  // Save JSON
  fs.writeFileSync('gemini_model_comparison.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    models: MODELS,
    testCases: TEST_CASES.length,
    results,
    summary: {
      model20: { accuracy: Math.round(model20Correct/n*100), avgLatency: Math.round(model20TotalLatency/n) },
      model31: { accuracy: Math.round(model31Correct/n*100), avgLatency: Math.round(model31TotalLatency/n) }
    }
  }, null, 2));

  console.log('Results saved to gemini_model_comparison.json');
}

run().catch(console.error);