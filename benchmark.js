const fs = require('fs');
const path = require('path');

const envFile = fs.readFileSync('.env', 'utf8');
envFile.split('\n').forEach(line => {
  const [key, ...val] = line.trim().split('=');
  if (key && val.length) process.env[key] = val.join('=');
});

const Anthropic = require('@anthropic-ai/sdk');
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });
const axios = require('axios');

const SYSTEM_PROMPT = `Aware360 safety system. Worker missed check-in. Classify their response AND detect distress signals.

Return ONLY this JSON: {"intent":"<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>","confidence":<0.0-1.0>,"response":"<reply>","resolved":<bool>,"escalate":<bool>,"delay_minutes":<null|number>,"distress":<null|"low"|"medium"|"high">,"distress_signals":<null|string>}

SAFE_CONFIRMATION: clear safety confirmation -> resolved:true, response:"Got it — I've reset your safety timer. Stay safe."
DELAYED_CHECKIN: needs more time -> resolved:true, delay_minutes:number or 15, response:"Understood. I've extended your check-in timer by [X] minutes. Please check in when ready."
REQUEST_HUMAN: wants human/help/SOS -> escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available."
UNRECOGNISED: unclear/ambiguous -> resolved:false, escalate:false, response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"
Confidence: >0.90 clear | <0.60 hesitation/unclear -> UNRECOGNISED. When in doubt, ask again.`;

const TEST_INPUTS = [
  "I'm fine",
  "All good here",
  "Running late, give me 15 minutes",
  "I need a human agent please",
  "Yes"
];

const MODELS = [
  'claude-haiku-4-5',
  'claude-sonnet-4-6',
  'claude-opus-4-6',
  'claude-opus-4-7',
  'claude-opus-4-8',
];

const RUNS = 5;

async function testStreaming(model, transcript) {
  const start = Date.now();
  const stream = anthropic.messages.stream({
    model,
    max_tokens: 100,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });
  let fullText = '';
  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta && chunk.delta.type === 'text_delta') {
      fullText += chunk.delta.text;
    }
  }
  return Date.now() - start;
}

async function testNonStreaming(model, transcript) {
  const start = Date.now();
  await anthropic.messages.create({
    model,
    max_tokens: 100,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });
  return Date.now() - start;
}

async function testTTS(text) {
  const apiKey = process.env.ELEVENLABS_API_KEY;
  const voiceId = process.env.ELEVENLABS_VOICE_ID || 'EXAVITQu4vr4xnSDxMaL';
  const start = Date.now();
  await axios.post(
    'https://api.elevenlabs.io/v1/text-to-speech/' + voiceId,
    { text, model_id: 'eleven_flash_v2_5', voice_settings: { stability: 0.5, similarity_boost: 0.75 } },
    { headers: { 'xi-api-key': apiKey, 'Content-Type': 'application/json', 'Accept': 'audio/mpeg' }, responseType: 'arraybuffer' }
  );
  return Date.now() - start;
}

function avg(arr) { return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length); }
function min(arr) { return Math.min(...arr); }
function max(arr) { return Math.max(...arr); }

async function runBenchmark() {
  const results = [];
  const transcript = "I'm fine"; // Sabit test girdisi

  console.log('\n=== VOICE AI LATENCY BENCHMARK ===');
  console.log('Test input: "' + transcript + '"');
  console.log('Runs per model:', RUNS);
  console.log('Models:', MODELS.join(', '));
  console.log('\nStarting...\n');

  for (const model of MODELS) {
    console.log('Testing:', model);

    // Streaming test
    const streamingTimes = [];
    for (let i = 0; i < RUNS; i++) {
      process.stdout.write('  Streaming run ' + (i + 1) + '/' + RUNS + '... ');
      try {
        const ms = await testStreaming(model, transcript);
        streamingTimes.push(ms);
        console.log(ms + 'ms');
      } catch (e) {
        console.log('ERROR:', e.message);
        streamingTimes.push(null);
      }
      await new Promise(r => setTimeout(r, 500));
    }

    // Non-streaming test
    const nonStreamingTimes = [];
    for (let i = 0; i < RUNS; i++) {
      process.stdout.write('  Non-streaming run ' + (i + 1) + '/' + RUNS + '... ');
      try {
        const ms = await testNonStreaming(model, transcript);
        nonStreamingTimes.push(ms);
        console.log(ms + 'ms');
      } catch (e) {
        console.log('ERROR:', e.message);
        nonStreamingTimes.push(null);
      }
      await new Promise(r => setTimeout(r, 500));
    }

    const validStreaming = streamingTimes.filter(t => t !== null);
    const validNonStreaming = nonStreamingTimes.filter(t => t !== null);

    results.push({
      model,
      streaming: {
        avg: validStreaming.length ? avg(validStreaming) : 'N/A',
        min: validStreaming.length ? min(validStreaming) : 'N/A',
        max: validStreaming.length ? max(validStreaming) : 'N/A',
        raw: streamingTimes
      },
      nonStreaming: {
        avg: validNonStreaming.length ? avg(validNonStreaming) : 'N/A',
        min: validNonStreaming.length ? min(validNonStreaming) : 'N/A',
        max: validNonStreaming.length ? max(validNonStreaming) : 'N/A',
        raw: nonStreamingTimes
      }
    });
    console.log('');
  }

  // TTS benchmark
  console.log('Testing TTS (ElevenLabs)...');
  const ttsTimes = [];
  for (let i = 0; i < RUNS; i++) {
    process.stdout.write('  TTS run ' + (i + 1) + '/' + RUNS + '... ');
    try {
      const ms = await testTTS("Got it — I've reset your safety timer. Stay safe.");
      ttsTimes.push(ms);
      console.log(ms + 'ms');
    } catch (e) {
      console.log('ERROR:', e.message);
      ttsTimes.push(null);
    }
    await new Promise(r => setTimeout(r, 300));
  }

  const validTTS = ttsTimes.filter(t => t !== null);

  // Print results table
  console.log('\n=== RESULTS ===\n');
  console.log('LLM LATENCY (ms) — Test input: "' + transcript + '"\n');
  console.log('Model            | Streaming Avg | Streaming Min | Streaming Max | NonStream Avg | NonStream Min | NonStream Max');
  console.log('-----------------|---------------|---------------|---------------|---------------|---------------|---------------');
  for (const r of results) {
    const name = r.model.padEnd(17);
    console.log(
      name + '| ' +
      String(r.streaming.avg).padEnd(14) + '| ' +
      String(r.streaming.min).padEnd(14) + '| ' +
      String(r.streaming.max).padEnd(14) + '| ' +
      String(r.nonStreaming.avg).padEnd(14) + '| ' +
      String(r.nonStreaming.min).padEnd(14) + '| ' +
      String(r.nonStreaming.max)
    );
  }

  console.log('\nTTS LATENCY (ElevenLabs eleven_flash_v2_5)');
  console.log('Avg:', validTTS.length ? avg(validTTS) : 'N/A', 'ms');
  console.log('Min:', validTTS.length ? min(validTTS) : 'N/A', 'ms');
  console.log('Max:', validTTS.length ? max(validTTS) : 'N/A', 'ms');

  // Save to JSON
  const output = {
    timestamp: new Date().toISOString(),
    test_input: transcript,
    runs: RUNS,
    llm_results: results,
    tts_results: {
      avg: validTTS.length ? avg(validTTS) : null,
      min: validTTS.length ? min(validTTS) : null,
      max: validTTS.length ? max(validTTS) : null,
      raw: ttsTimes
    }
  };
  fs.writeFileSync('benchmark_results.json', JSON.stringify(output, null, 2));
  console.log('\nResults saved to benchmark_results.json');
}

runBenchmark().catch(console.error);
