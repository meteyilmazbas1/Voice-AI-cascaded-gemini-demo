const fs = require('fs');
require('dotenv').config();

const WebSocket = require('ws');

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-3.1-flash-live-preview';

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
const MIN_VALID_LATENCY = 1000; // anything faster than this is treated as a measurement glitch
const MAX_RETRIES_PER_RUN = 2;

function connectAndClassify(transcript) {
  return new Promise((resolve, reject) => {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;
    const ws = new WebSocket(geminiUrl);

    let setupDone = false;
    let turnStartTime = null;
    let turnCompleteTime = null;
    let finalizeTimer = null;
    let finalized = false;
    let outputBuffer = '';
    let promptTokens = 0, responseTokens = 0;
    const timeout = setTimeout(() => { finalize(); }, 15000);

    function finalize() {
      if (finalized) return;
      finalized = true;
      clearTimeout(timeout);
      resolve({ latency: turnCompleteTime || (Date.now() - turnStartTime), outputText: outputBuffer.trim(), promptTokens, responseTokens });
      ws.close();
    }

    ws.on('open', () => {
      ws.send(JSON.stringify({
        setup: {
          model: `models/${GEMINI_MODEL}`,
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
          if (turnCompleteTime !== null) {
            clearTimeout(finalizeTimer);
            finalizeTimer = setTimeout(finalize, 400);
          }
        }

        if (msg.serverContent && msg.serverContent.turnComplete && turnCompleteTime === null) {
          turnCompleteTime = Date.now() - turnStartTime;
          clearTimeout(finalizeTimer);
          finalizeTimer = setTimeout(finalize, 400);
        }

        if (msg.usageMetadata) {
          promptTokens = msg.usageMetadata.promptTokenCount || 0;
          responseTokens = msg.usageMetadata.responseTokenCount || 0;
        }
      } catch (e) {}
    });

    ws.on('error', (e) => { clearTimeout(timeout); reject(e); });
  });
}

function avg(arr) { return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length); }

async function run() {
  console.log('\n=== GEMINI LIVE LATENCY BENCHMARK ===');
  console.log('Runs per transcript: ' + RUNS + '\n');

  const allResults = [];

  for (const t of TEST_TRANSCRIPTS) {
    process.stdout.write('Testing: "' + t.text + '"\n');
    const runs = [];

    for (let i = 0; i < RUNS; i++) {
      let r = null;
      let attempt = 0;
      while (attempt <= MAX_RETRIES_PER_RUN) {
        try {
          r = await connectAndClassify(t.text);
          if (r.latency >= MIN_VALID_LATENCY) break;
          console.log('  Run ' + (i + 1) + ' attempt ' + (attempt + 1) + ': ' + r.latency + 'ms — too low, retrying');
          r = null;
        } catch (e) {
          console.log('  Run ' + (i + 1) + ' attempt ' + (attempt + 1) + ': ERROR — ' + e.message);
        }
        attempt++;
        await new Promise(res => setTimeout(res, 500));
      }
      if (r) {
        runs.push(r);
        console.log('  Run ' + (i + 1) + ': ' + r.latency + 'ms | tokens: ' + (r.promptTokens + r.responseTokens) + ' | "' + r.outputText.substring(0, 60) + '"');
      } else {
        console.log('  Run ' + (i + 1) + ': skipped (no valid result after retries)');
      }
      await new Promise(r => setTimeout(r, 800));
    }

    if (runs.length > 0) {
      const avgLatency = avg(runs.map(r => r.latency));
      const avgTokens = avg(runs.map(r => r.promptTokens + r.responseTokens));
      allResults.push({
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
  }

  console.log('\n========================================');
  console.log('SUMMARY TABLE');
  console.log('========================================\n');

  const pad = (s, n) => String(s).padEnd(n);
  console.log(pad('Transcript', 35) + '| ' + pad('Category', 22) + '| ' + pad('Avg', 8) + '| ' + pad('Min', 8) + '| ' + pad('Max', 8) + '| ' + 'Tokens');
  console.log('-'.repeat(100));

  let totalLatency = 0, totalTokens = 0;
  for (const r of allResults) {
    totalLatency += r.avgLatency;
    totalTokens += r.avgTokens;
    console.log(
      pad(r.transcript.substring(0, 33), 35) + '| ' +
      pad(r.category.substring(0, 20), 22) + '| ' +
      pad(r.avgLatency + 'ms', 8) + '| ' +
      pad(r.minLatency + 'ms', 8) + '| ' +
      pad(r.maxLatency + 'ms', 8) + '| ' +
      r.avgTokens
    );
  }

  const n = allResults.length;
  console.log('\n========================================');
  console.log('OVERALL');
  console.log('========================================');
  console.log('Avg latency across all transcripts: ' + Math.round(totalLatency / n) + 'ms');
  console.log('Avg tokens: ' + Math.round(totalTokens / n));

  fs.writeFileSync('gemini_comparison_results.json', JSON.stringify({
    timestamp: new Date().toISOString(),
    model: GEMINI_MODEL,
    runs_per_transcript: RUNS,
    results: allResults,
    summary: { avgLatency: Math.round(totalLatency / n), avgTokens: Math.round(totalTokens / n) }
  }, null, 2));
  console.log('\nResults saved to gemini_comparison_results.json');
}

run().catch(console.error);