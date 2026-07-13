const fs = require('fs');
require('dotenv').config();

const WebSocket = require('ws');

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const MODELS = ['gemini-2.0-flash-live-001', 'gemini-3.1-flash-live-preview'];

const SYSTEM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone worker safety platform handling a MISSED CHECK-IN alert for an industrial/field worker.

LANGUAGE RULE: The worker speaks English. Always interpret their speech as English, even if audio quality is poor or words are unclear. Never respond in any language other than English.

Your persona: "Aware360 Monitoring Center" — efficient, neutral, direct.

Rules:
- SAFE: "Got it — I've reset your safety timer. Stay safe."
- DELAYED: "Understood. I've extended your check-in timer. Please check in when ready."
- HUMAN AGENT: "Of course. I'm flagging this for a live agent — someone will call you back shortly."
- UNCLEAR: Always respond with exactly this phrase, nothing else: "Sorry, I didn't catch that. Could you repeat?"

CRITICAL SAFETY RULE: If the worker mentions ANY physical injury, pain, fall, or distress — even with positive statements — immediately escalate to human agent.

Keep responses short (under 15 words). Be direct and professional.`;

const TEST_TRANSCRIPTS = [
  { text: "I'm fine", category: "Net - Safe" },
  { text: "Yes", category: "Net - Safe (short)" },
  { text: "I need a human agent please", category: "Net - Human" },
  { text: "Running late, give me 15 minutes", category: "Net - Delayed" },
  { text: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay", category: "Contradictory - Critical" },
  { text: "I'm fine but I need help", category: "Contradictory - Simple" },
  { text: "Yes I'm okay but something feels wrong", category: "Contradictory - Vague" },
  { text: "All good, actually wait no I'm not okay", category: "Contradictory - Retraction" },
  { text: "I think I'm fine but I'm a bit worried", category: "Contradictory - Hesitant" },
  { text: "I broke my leg I need help", category: "Distress - Injury" },
  { text: "I'm stuck and a bit scared", category: "Distress - Medium" },
  { text: "SOS help me please", category: "Distress - Emergency" },
  { text: "I'm okay just a little dizzy and my head hurts", category: "Distress - Subtle" },
  { text: "Everything is fine I just fell down but I'm okay I promise", category: "Distress - Minimizing" },
  { text: "almost done just a bit more time", category: "Delayed - Vague" },
  { text: "Give me like maybe 10 or 15 minutes I think", category: "Delayed - Vague Time" },
  { text: "I'm running behind but should be done soon", category: "Delayed - Vague" },
  { text: "fi... oka... fine", category: "Noisy - Broken" },
  { text: "I'm fi-- yes I'm good --static-- okay", category: "Noisy - Mixed" },
  { text: "I think I'm okay maybe", category: "Ambiguous - Hesitant" },
  { text: "ye", category: "Too Short" },
];

const RUNS = 3;

function connectAndClassify(model, transcript) {
  return new Promise((resolve, reject) => {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
    const ws = new WebSocket(geminiUrl);

    let setupDone = false;
    let turnStartTime = null;
    let outputBuffer = '';
    let promptTokens = 0, responseTokens = 0;
    const timeout = setTimeout(() => { ws.close(); reject(new Error('timeout')); }, 20000);

    ws.on('open', () => {
      ws.send(JSON.stringify({
        setup: {
          model: `models/${model}`,
          generation_config: { response_modalities: ['AUDIO'] },
          input_audio_transcription: {},
          system_instruction: { parts: [{ text: SYSTEM_PROMPT }] }
        }
      }));
    });

    ws.on('message', (data) => {
      try {
        const msg = JSON.parse(data.toString());

        if (msg.setupComplete && !setupDone) {
          setupDone = true;
          turnStartTime = Date.now();
          ws.send(JSON.stringify({
            client_content: {
              turns: [{ role: 'user', parts: [{ text: `Worker said: "${transcript}"` }] }],
              turn_complete: true
            }
          }));
        }

        if (msg.serverContent && msg.serverContent.outputTranscription && msg.serverContent.outputTranscription.text) {
          outputBuffer += msg.serverContent.outputTranscription.text;
        }

        if (msg.usageMetadata) {
          promptTokens = msg.usageMetadata.promptTokenCount || 0;
          responseTokens = msg.usageMetadata.responseTokenCount || 0;
          const latency = Date.now() - turnStartTime;
          clearTimeout(timeout);
          ws.close();
          resolve({ latency, outputText: outputBuffer.trim(), promptTokens, responseTokens });
        }
      } catch (e) {}
    });

    ws.on('error', (e) => { clearTimeout(timeout); reject(e); });
  });
}

function avg(arr) { return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length); }

async function run() {
  console.log('\n=== GEMINI 2.0 vs 3.1 COMPARISON ===');
  console.log('Models: gemini-2.0-flash-live-001 vs gemini-3.1-flash-live-preview');
  console.log('Runs per transcript: ' + RUNS + '\n');

  const results = { '2.0': [], '3.1': [] };

  for (const model of MODELS) {
    const label = model.includes('2.0') ? '2.0' : '3.1';
    console.log('\n--- Testing model: ' + model + ' ---\n');

    for (const t of TEST_TRANSCRIPTS) {
      process.stdout.write('  "' + t.text + '"\n');
      const runs = [];

      for (let i = 0; i < RUNS; i++) {
        try {
          const r = await connectAndClassify(model, t.text);
          runs.push(r);
          console.log('    Run ' + (i + 1) + ': ' + r.latency + 'ms | "' + r.outputText.substring(0, 50) + '"');
        } catch (e) {
          console.log('    Run ' + (i + 1) + ': ERROR — ' + e.message);
        }
        await new Promise(r => setTimeout(r, 800));
      }

      if (runs.length > 0) {
        const avgLatency = avg(runs.map(r => r.latency));
        const avgTokens = avg(runs.map(r => r.promptTokens + r.responseTokens));
        results[label].push({
          transcript: t.text,
          category: t.category,
          avgLatency,
          minLatency: Math.min(...runs.map(r => r.latency)),
          maxLatency: Math.max(...runs.map(r => r.latency)),
          avgTokens,
          lastResponse: runs[runs.length - 1].outputText
        });
        console.log('  Avg: ' + avgLatency + 'ms\n');
      }

      await new Promise(r => setTimeout(r, 500));
    }
  }

  // Summary
  const pad = (s, n) => String(s).padEnd(n);
  console.log('\n========================================');
  console.log('COMPARISON SUMMARY');
  console.log('========================================\n');
  console.log(pad('Transcript', 35) + '| ' + pad('Category', 22) + '| ' + pad('2.0 Avg', 10) + '| ' + pad('3.1 Avg', 10) + '| Faster');
  console.log('-'.repeat(95));

  const n = Math.min(results['2.0'].length, results['3.1'].length);
  let total20 = 0, total31 = 0;
  for (let i = 0; i < n; i++) {
    const r20 = results['2.0'][i];
    const r31 = results['3.1'][i];
    total20 += r20.avgLatency;
    total31 += r31.avgLatency;
    const faster = r20.avgLatency < r31.avgLatency ? '2.0 ✓' : '3.1 ✓';
    console.log(
      pad(r20.transcript.substring(0, 33), 35) + '| ' +
      pad(r20.category.substring(0, 20), 22) + '| ' +
      pad(r20.avgLatency + 'ms', 10) + '| ' +
      pad(r31.avgLatency + 'ms', 10) + '| ' + faster
    );
  }

  console.log('\n========================================');
  console.log('OVERALL');
  console.log('========================================');
  console.log('Gemini 2.0 avg: ' + Math.round(total20 / n) + 'ms');
  console.log('Gemini 3.1 avg: ' + Math.round(total31 / n) + 'ms');
  console.log('Winner: ' + (total20 < total31 ? 'Gemini 2.0' : 'Gemini 3.1'));

  fs.writeFileSync('gemini_both_models_results.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    runs_per_transcript: RUNS,
    results,
    summary: {
      gemini20: { avgLatency: Math.round(total20 / n) },
      gemini31: { avgLatency: Math.round(total31 / n) }
    }
  }, null, 2));
  console.log('\nSaved to gemini_both_models_results.json');
}

run().catch(console.error);
