const https = require('https');
require('dotenv').config();

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;

console.log('=== GEMINI API KEY CHECK ===');
console.log('Key prefix:', GEMINI_API_KEY ? GEMINI_API_KEY.substring(0, 8) + '...' : 'NOT FOUND');

// Test 1: Normal REST API
function testREST() {
  return new Promise((resolve) => {
    const body = JSON.stringify({
      contents: [{ parts: [{ text: 'Say OK' }] }]
    });

    const options = {
      hostname: 'generativelanguage.googleapis.com',
      path: `/v1beta/models/gemini-2.0-flash:generateContent?key=${GEMINI_API_KEY}`,
      method: 'POST',
      headers: { 'Content-Type': 'application/json', 'Content-Length': body.length }
    };

    const req = https.request(options, (res) => {
      let data = '';
      res.on('data', (chunk) => data += chunk);
      res.on('end', () => {
        console.log('\n[REST API] Status:', res.statusCode);
        try {
          const parsed = JSON.parse(data);
          if (parsed.candidates) {
            console.log('[REST API] ✓ Works! Response:', parsed.candidates[0].content.parts[0].text);
          } else {
            console.log('[REST API] ✗ Error:', JSON.stringify(parsed).substring(0, 200));
          }
        } catch (e) {
          console.log('[REST API] ✗ Parse error:', data.substring(0, 200));
        }
        resolve();
      });
    });

    req.on('error', (e) => {
      console.log('[REST API] ✗ Connection error:', e.message);
      resolve();
    });

    req.write(body);
    req.end();
  });
}

// Test 2: WebSocket connectivity
function testWS() {
  return new Promise((resolve) => {
    const WebSocket = require('ws');
    const url = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;

    console.log('\n[Live API WS] Connecting...');
    const ws = new WebSocket(url);

    const timeout = setTimeout(() => {
      console.log('[Live API WS] ✗ Timeout — cannot reach WebSocket endpoint');
      ws.close();
      resolve();
    }, 10000);

    ws.on('open', () => {
      console.log('[Live API WS] ✓ WebSocket connected!');
      // Send minimal setup
      ws.send(JSON.stringify({
        setup: { model: 'models/gemini-2.0-flash-live-001' }
      }));
    });

    ws.on('message', (data) => {
      const msg = JSON.parse(data.toString());
      if (msg.setupComplete) {
        console.log('[Live API WS] ✓ setupComplete received — Live API works!');
      } else if (msg.error) {
        console.log('[Live API WS] ✗ Error:', JSON.stringify(msg.error));
      } else {
        console.log('[Live API WS] Message:', JSON.stringify(msg).substring(0, 100));
      }
      clearTimeout(timeout);
      ws.close();
      resolve();
    });

    ws.on('error', (e) => {
      console.log('[Live API WS] ✗ Error:', e.message);
      clearTimeout(timeout);
      resolve();
    });
  });
}

async function run() {
  await testREST();
  await testWS();
  console.log('\nDone.');
}

run().catch(console.error);
