/**
 * Local Twilio Media Stream simulator (no real Twilio account needed).
 * Sends synthetic mulaw silence frames to verify the /twilio WebSocket path.
 */
const WebSocket = require('ws');
const { spawn } = require('child_process');

const PORT = process.env.PORT || 3000;
const ws = new WebSocket(`ws://localhost:${PORT}/twilio`);

let streamSid = 'MZ_TEST_STREAM';
let sentMedia = 0;

ws.on('open', () => {
  console.log('[Sim] Connected to /twilio');
  ws.send(JSON.stringify({ event: 'connected', protocol: 'Call', version: '1.0.0' }));
  ws.send(JSON.stringify({
    event: 'start',
    sequenceNumber: '1',
    start: {
      streamSid,
      callSid: 'CA_TEST_CALL',
      mediaFormat: { encoding: 'audio/x-mulaw', sampleRate: 8000, channels: 1 }
    }
  }));

  // Send 2 seconds of mulaw silence (0xFF) in 20ms chunks
  const interval = setInterval(() => {
    if (sentMedia >= 100 || ws.readyState !== WebSocket.OPEN) {
      clearInterval(interval);
      setTimeout(() => {
        ws.send(JSON.stringify({ event: 'stop', streamSid }));
        console.log('[Sim] Sent stop — closing in 2s');
        setTimeout(() => ws.close(), 2000);
      }, 3000);
      return;
    }
    const chunk = Buffer.alloc(160, 0xff);
    ws.send(JSON.stringify({
      event: 'media',
      streamSid,
      media: { payload: chunk.toString('base64'), track: 'inbound' }
    }));
    sentMedia++;
  }, 20);
});

ws.on('message', (data) => {
  const msg = JSON.parse(data.toString());
  if (msg.event === 'media') {
    const bytes = Buffer.from(msg.media.payload, 'base64').length;
    console.log(`[Sim] Received outbound media chunk: ${bytes} bytes`);
  } else {
    console.log('[Sim] Received:', msg.event || msg.type, JSON.stringify(msg).slice(0, 120));
  }
});

ws.on('close', () => {
  console.log('[Sim] Done');
  process.exit(0);
});

ws.on('error', (err) => {
  console.error('[Sim] Error:', err.message);
  process.exit(1);
});
