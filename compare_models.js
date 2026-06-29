const fs = require('fs');

const envFile = fs.readFileSync('.env', 'utf8');
envFile.split('\n').forEach(line => {
  const [key, ...val] = line.trim().split('=');
  if (key && val.length) process.env[key] = val.join('=');
});

const Anthropic = require('@anthropic-ai/sdk');
const anthropic = new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY });

const SYSTEM_PROMPT = `Aware360 safety system. Worker missed check-in. Classify their response AND detect distress signals.

Return ONLY this JSON: {"intent":"<SAFE_CONFIRMATION|DELAYED_CHECKIN|REQUEST_HUMAN|UNRECOGNISED>","confidence":<0.0-1.0>,"response":"<reply>","resolved":<bool>,"escalate":<bool>,"delay_minutes":<null|number>,"distress":<null|"low"|"medium"|"high">,"distress_signals":<null|string>}

SAFE_CONFIRMATION: clear safety confirmation ("yes", "yep", "fine", "ok", "okay", "good", "safe", "I'm fine", "all good", "no problem") -> resolved:true, confidence:0.95, response:"Got it — I've reset your safety timer. Stay safe."
DELAYED_CHECKIN: needs more time -> resolved:true, delay_minutes:number or 15, response:"Understood. I've extended your check-in timer by [X] minutes. Please check in when ready."
REQUEST_HUMAN: wants human/help/SOS -> escalate:true, response:"Of course. I'm flagging this for a live agent — someone will call you back shortly. Keep your phone available."
UNRECOGNISED: unclear/ambiguous -> resolved:false, escalate:false, response:"I'm sorry, I didn't catch that. Are you safe, need more time, or need a human agent?"
Confidence: >0.90 clear | <0.60 hesitation/unclear -> UNRECOGNISED. When in doubt, ask again.`;

const TEST_TRANSCRIPTS = [
  // Basit net cevaplar — baseline
  { text: "I'm fine", category: "Net - Safe", expected: "SAFE_CONFIRMATION" },
  { text: "Yes", category: "Net - Safe (kısa)", expected: "SAFE_CONFIRMATION" },
  { text: "I need a human agent please", category: "Net - Human", expected: "REQUEST_HUMAN" },
  { text: "Running late, give me 15 minutes", category: "Net - Delayed", expected: "DELAYED_CHECKIN" },

  // Çelişkili — hem olumlu hem olumsuz
  { text: "I'm fine I'm safe. I broke my leg and it hurts too much I'm not good I'm okay", category: "Çelişkili - Kritik", expected: "REQUEST_HUMAN" },
  { text: "I'm fine but I need help", category: "Çelişkili - Basit", expected: "REQUEST_HUMAN" },
  { text: "Yes I'm okay but something feels wrong", category: "Çelişkili - Belirsiz", expected: "REQUEST_HUMAN" },
  { text: "All good, actually wait no I'm not okay", category: "Çelişkili - Geri Alma", expected: "REQUEST_HUMAN" },
  { text: "I think I'm fine but I'm a bit worried", category: "Çelişkili - Tereddütlü", expected: "UNRECOGNISED" },

  // Karmaşık distress
  { text: "I broke my leg I need help", category: "Distress - Yaralanma", expected: "REQUEST_HUMAN" },
  { text: "I'm stuck and a bit scared", category: "Distress - Medium", expected: "REQUEST_HUMAN" },
  { text: "SOS help me please", category: "Distress - Emergency", expected: "REQUEST_HUMAN" },
  { text: "I'm okay just a little dizzy and my head hurts", category: "Distress - Subtle", expected: "REQUEST_HUMAN" },
  { text: "Everything is fine I just fell down but I'm okay I promise", category: "Distress - Minimize Eden", expected: "REQUEST_HUMAN" },

  // Karmaşık delayed
  { text: "almost done just a bit more time", category: "Belirsiz - Delayed", expected: "DELAYED_CHECKIN" },
  { text: "Give me like maybe 10 or 15 minutes I think", category: "Delayed - Belirsiz Süre", expected: "DELAYED_CHECKIN" },
  { text: "I'm running behind but should be done soon", category: "Delayed - Belirsiz", expected: "DELAYED_CHECKIN" },

  // Gürültülü/bozuk
  { text: "fi... oka... fine", category: "Gürültülü - Bozuk", expected: "SAFE_CONFIRMATION" },
  { text: "I'm fi-- yes I'm good --static-- okay", category: "Gürültülü - Karışık", expected: "SAFE_CONFIRMATION" },

  // Karmaşık belirsiz
  { text: "eeee sanırım iyiyim", category: "Belirsiz - Türkçe", expected: "UNRECOGNISED" },
  { text: "I think I'm okay maybe", category: "Belirsiz - Tereddütlü", expected: "UNRECOGNISED" },
  { text: "ye", category: "Çok Kısa", expected: "UNRECOGNISED" },
];

const MODELS = ['claude-haiku-4-5', 'claude-sonnet-4-6'];
const RUNS = 3;

const TOKEN_COSTS = {
  'claude-haiku-4-5':  { input: 0.80, output: 4.00 },
  'claude-sonnet-4-6': { input: 3.00, output: 15.00 },
};

function calcCost(model, inp, out) {
  const r = TOKEN_COSTS[model];
  return (inp / 1_000_000 * r.input) + (out / 1_000_000 * r.output);
}

async function classify(model, transcript) {
  const start = Date.now();
  const stream = anthropic.messages.stream({
    model,
    max_tokens: 200,
    system: SYSTEM_PROMPT,
    messages: [{ role: 'user', content: 'Worker said: "' + transcript + '"' }]
  });

  let fullText = '';
  let inputTokens = 0, outputTokens = 0;

  for await (const chunk of stream) {
    if (chunk.type === 'content_block_delta' && chunk.delta && chunk.delta.type === 'text_delta') {
      fullText += chunk.delta.text;
    }
    if (chunk.type === 'message_start' && chunk.message && chunk.message.usage) {
      inputTokens = chunk.message.usage.input_tokens || 0;
    }
    if (chunk.type === 'message_delta' && chunk.usage) {
      outputTokens = chunk.usage.output_tokens || 0;
    }
  }

  const latency = Date.now() - start;
  let parsed = null;
  try {
    parsed = JSON.parse(fullText.trim());
  } catch (e) {
    const match = fullText.match(/\{[\s\S]*\}/);
    if (match) { try { parsed = JSON.parse(match[0]); } catch(e2) {} }
  }

  return { latency, inputTokens, outputTokens, parsed };
}

function avg(arr) { return Math.round(arr.reduce((a, b) => a + b, 0) / arr.length); }

async function run() {
  console.log('\n=== MODEL KARŞILAŞTIRMA: Haiku 4.5 vs Sonnet 4.6 ===');
  console.log('Her transcript için ' + RUNS + ' tur test\n');

  const allResults = [];

  for (const transcript of TEST_TRANSCRIPTS) {
    const resultRow = { transcript: transcript.text, category: transcript.category, expected: transcript.expected, models: {} };
    process.stdout.write('Testing: "' + transcript.text + '"\n');

    for (const model of MODELS) {
      const modelName = model.includes('haiku') ? 'Haiku' : 'Sonnet';
      const runs = [];

      for (let i = 0; i < RUNS; i++) {
        try {
          const r = await classify(model, transcript.text);
          runs.push(r);
        } catch(e) {
          console.error('  ERROR:', e.message);
        }
        await new Promise(r => setTimeout(r, 300));
      }

      if (runs.length > 0) {
        const avgLatency = avg(runs.map(r => r.latency));
        const avgInput = avg(runs.map(r => r.inputTokens));
        const avgOutput = avg(runs.map(r => r.outputTokens));
        const lastResult = runs[runs.length - 1].parsed;
        const correct = lastResult && lastResult.intent === transcript.expected;

        resultRow.models[modelName] = {
          avgLatency,
          avgInput,
          avgOutput,
          cost: calcCost(model, avgInput, avgOutput),
          intent: lastResult ? lastResult.intent : 'PARSE_ERROR',
          confidence: lastResult ? lastResult.confidence : 0,
          distress: lastResult ? lastResult.distress : null,
          correct
        };

        console.log('  ' + modelName + ': ' + avgLatency + 'ms | ' +
          (avgInput + avgOutput) + ' tokens | ' +
          (lastResult ? lastResult.intent : 'ERROR') +
          ' (' + (lastResult ? Math.round(lastResult.confidence * 100) + '%' : '-') + ')' +
          (correct ? ' ✓' : ' ✗ (beklenen: ' + transcript.expected + ')'));
      }
    }
    allResults.push(resultRow);
    console.log('');
    await new Promise(r => setTimeout(r, 500));
  }

  // Summary table
  console.log('\n========================================');
  console.log('ÖZET TABLO');
  console.log('========================================\n');

  const pad = (s, n) => String(s).padEnd(n);

  console.log(
    pad('Transcript', 35) + '| ' +
    pad('Kategori', 22) + '| ' +
    pad('Beklenen', 18) + '| ' +
    pad('Haiku Intent', 18) + '| ' +
    pad('Haiku ms', 10) + '| ' +
    pad('Haiku Token', 12) + '| ' +
    pad('Sonnet Intent', 18) + '| ' +
    pad('Sonnet ms', 10) + '| ' +
    pad('Sonnet Token', 12)
  );
  console.log('-'.repeat(175));

  let haikuCorrect = 0, sonnetCorrect = 0;
  let haikuTotalLatency = 0, sonnetTotalLatency = 0;
  let haikuTotalTokens = 0, sonnetTotalTokens = 0;
  let haikuTotalCost = 0, sonnetTotalCost = 0;

  for (const row of allResults) {
    const h = row.models['Haiku'];
    const s = row.models['Sonnet'];
    if (h && h.correct) haikuCorrect++;
    if (s && s.correct) sonnetCorrect++;
    if (h) { haikuTotalLatency += h.avgLatency; haikuTotalTokens += h.avgInput + h.avgOutput; haikuTotalCost += h.cost; }
    if (s) { sonnetTotalLatency += s.avgLatency; sonnetTotalTokens += s.avgInput + s.avgOutput; sonnetTotalCost += s.cost; }

    console.log(
      pad(row.transcript.substring(0, 33), 35) + '| ' +
      pad(row.category.substring(0, 20), 22) + '| ' +
      pad(row.expected, 18) + '| ' +
      pad(h ? h.intent + (h.correct ? ' ✓' : ' ✗') : '-', 18) + '| ' +
      pad(h ? h.avgLatency + 'ms' : '-', 10) + '| ' +
      pad(h ? (h.avgInput + h.avgOutput) : '-', 12) + '| ' +
      pad(s ? s.intent + (s.correct ? ' ✓' : ' ✗') : '-', 18) + '| ' +
      pad(s ? s.avgLatency + 'ms' : '-', 10) + '| ' +
      pad(s ? (s.avgInput + s.avgOutput) : '-', 12)
    );
  }

  const n = TEST_TRANSCRIPTS.length;
  console.log('\n========================================');
  console.log('GENEL KARŞILAŞTIRMA');
  console.log('========================================');
  console.log('');
  console.log('                    Haiku 4.5        Sonnet 4.6');
  console.log('Doğruluk:           ' + haikuCorrect + '/' + n + ' (%' + Math.round(haikuCorrect/n*100) + ')          ' + sonnetCorrect + '/' + n + ' (%' + Math.round(sonnetCorrect/n*100) + ')');
  console.log('Ort. Latency:       ' + Math.round(haikuTotalLatency/n) + 'ms              ' + Math.round(sonnetTotalLatency/n) + 'ms');
  console.log('Ort. Token:         ' + Math.round(haikuTotalTokens/n) + '                ' + Math.round(sonnetTotalTokens/n));
  console.log('Toplam Maliyet:     $' + haikuTotalCost.toFixed(6) + '          $' + sonnetTotalCost.toFixed(6));

  // Save JSON
  const output = {
    timestamp: new Date().toISOString(),
    runs_per_transcript: RUNS,
    models: MODELS,
    transcripts: TEST_TRANSCRIPTS.length,
    results: allResults,
    summary: {
      haiku: { correct: haikuCorrect, total: n, accuracy: Math.round(haikuCorrect/n*100), avgLatency: Math.round(haikuTotalLatency/n), avgTokens: Math.round(haikuTotalTokens/n), totalCost: haikuTotalCost },
      sonnet: { correct: sonnetCorrect, total: n, accuracy: Math.round(sonnetCorrect/n*100), avgLatency: Math.round(sonnetTotalLatency/n), avgTokens: Math.round(sonnetTotalTokens/n), totalCost: sonnetTotalCost }
    }
  };
  fs.writeFileSync('comparison_results.json', JSON.stringify(output, null, 2));
  console.log('\nSonuçlar comparison_results.json dosyasına kaydedildi.');
}

run().catch(console.error);