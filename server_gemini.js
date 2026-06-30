const fs = require('fs');
const path = require('path');

require('dotenv').config();

const express = require('express');
const WebSocket = require('ws');
const http = require('http');
const axios = require('axios');

const app = express();
const server = http.createServer(app);
const wss = new WebSocket.Server({ server });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

const GEMINI_API_KEY = process.env.GEMINI_API_KEY;
const GEMINI_MODEL = 'gemini-3.1-flash-live-preview';

const SYSTEM_PROMPT = `You are an automated safety monitoring system for Aware360, a lone worker safety platform handling a MISSED CHECK-IN alert for an industrial/field worker.

Your persona: "Aware360 Monitoring Center" — efficient, neutral, direct.

When the worker speaks:
1. Listen carefully to their response
2. Classify their intent: safe confirmation, needs more time, wants human agent, or unclear
3. Respond appropriately and concisely

Rules:
- SAFE: "Got it — I've reset your safety timer. Stay safe."
- DELAYED: "Understood. I've extended your check-in timer. Please check in when ready."  
- HUMAN AGENT: "Of course. I'm flagging this for a live agent — someone will call you back shortly."
- UNCLEAR: Always respond with exactly this phrase, nothing else: "Sorry, I didn't catch that. Could you repeat?"

CRITICAL SAFETY RULE: If the worker mentions ANY physical injury, pain, fall, or distress — even with positive statements — immediately escalate to human agent.

Keep responses short (under 15 words). Be direct and professional.

IMPORTANT: Only greet the worker ONCE at the very start of the conversation. Do not repeat the greeting.`;

const WORKER_INTRO = `Hello, this is the Aware360 Monitoring Center. We noticed you missed your scheduled check-in. Are you okay? Please say "I'm safe", "I need more time", or "connect me to an agent".`;

wss.on('connection', function(ws) {
  console.log('[Gemini WS] Client connected');

  let geminiWs = null;
  let aiTranscriptBuffer = '';
  let sessionState = 'GREETING';
  let clientMimeType = 'audio/webm';
  let actualSampleRate = 16000;
  let greetingSent = false;

  function send(obj) {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(obj));
  }

  function connectGemini() {
    const geminiUrl = `wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent?key=${GEMINI_API_KEY}`;

    geminiWs = new WebSocket(geminiUrl);

    geminiWs.on('open', function() {
      console.log('[Gemini] Connected');

      const setup = {
  setup: {
    model: `models/${GEMINI_MODEL}`,
    generation_config: {
      response_modalities: ['AUDIO'],
      speech_config: {
        voice_config: {
          prebuilt_voice_config: {
            voice_name: 'Charon'
          }
        }
      }
    },
    realtime_input_config: {
      automatic_activity_detection: {
        disabled: false
      }
    },
    input_audio_transcription: {},
    system_instruction: {
      parts: [{ text: SYSTEM_PROMPT }]
    }
  }
};
      geminiWs.send(JSON.stringify(setup));
    });

    geminiWs.on('message', async function(data) {
      try {
        const msg = JSON.parse(data.toString());
console.log('[Gemini RAW]', JSON.stringify(msg).substring(0, 300));

        // Setup complete
        if (msg.setupComplete) {
  console.log('[Gemini] Setup complete');
  send({ type: 'stage', stage: 'stt', status: 'connected' });
  send({ type: 'stage', stage: 'llm', status: 'connected' });
  send({ type: 'stage', stage: 'tts', status: 'connected' });

  if (!greetingSent) {
    greetingSent = true;
    send({ type: 'greeting', text: WORKER_INTRO, audio: null });
    sessionState = 'LISTENING';
  }
}

        // Audio response from Gemini
        if (msg.serverContent) {
  const content = msg.serverContent;

  if (content.modelTurn && content.modelTurn.parts) {
    for (const part of content.modelTurn.parts) {
      if (part.inlineData && part.inlineData.mimeType && part.inlineData.mimeType.startsWith('audio/')) {
        console.log('[Gemini] Audio chunk received, mimeType:', part.inlineData.mimeType, 'size:', part.inlineData.data.length);
        send({ type: 'audio_chunk', chunk: part.inlineData.data, index: 0, mimeType: part.inlineData.mimeType });
      }
      if (part.text) {
        console.log('[Gemini] Text:', part.text);
        send({ type: 'transcript', text: part.text, role: 'ai' });
      }
    }
  }

  if (content.outputTranscription && content.outputTranscription.text) {
  aiTranscriptBuffer += content.outputTranscription.text;
}

          if (content.turnComplete) {
            if (aiTranscriptBuffer) {
    send({ type: 'transcript', text: aiTranscriptBuffer, role: 'ai' });
    aiTranscriptBuffer = '';
  }
            send({ type: 'audio_stream_end' });
            if (sessionState === 'GREETING') {
              send({ type: 'greeting', text: WORKER_INTRO, audio: null });
              sessionState = 'LISTENING';
            }
          }
        }

        // Input transcription (what worker said)
if (msg.serverContent && msg.serverContent.inputTranscription) {
  console.log('[Gemini] Worker said:', msg.serverContent.inputTranscription.text);
  send({ type: 'transcript', text: msg.serverContent.inputTranscription.text, role: 'worker' });
}

      } catch(e) {
        console.error('[Gemini] Message error:', e.message);
      }
    });

    geminiWs.on('error', function(e) {
      console.error('[Gemini] Error:', e.message);
      send({ type: 'stage', stage: 'stt', status: 'error' });
    });

    geminiWs.on('close', function(code, reason) {
      console.log('[Gemini] Closed', code, reason.toString());
    });
  }

  ws.on('message', async function(data) {
    try {
      let isJson = false;
      let msg = null;
      try {
        msg = JSON.parse(data.toString());
        isJson = true;
      } catch (e) {
        isJson = false;
      }

      if (isJson) {
        if (msg.type === 'mime_type') {
          clientMimeType = msg.mimeType;
        }

        if (msg.type === 'gemini_sample_rate') {
          actualSampleRate = msg.rate;
          console.log('[Gemini] Client sample rate:', actualSampleRate);
        }

        if (msg.type === 'start') {
          send({ type: 'log', message: 'Gemini Live API session started' });
          send({ type: 'stage', stage: 'stt', status: 'connecting' });
          connectGemini();
        }

        if (msg.type === 'mock_transcript' && sessionState === 'LISTENING') {
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) {
            const textMsg = {
              client_content: {
                turns: [{
                  role: 'user',
                  parts: [{ text: msg.text }]
                }],
                turn_complete: true
              }
            };
            geminiWs.send(JSON.stringify(textMsg));
          }
        }

        if (msg.type === 'stop') {
          if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
          sessionState = 'ENDED';
        }

      } else {
        // Binary audio — send to Gemini as realtime input
        console.log('[Gemini] Binary audio received, size:', data.length, 'state:', sessionState);
        if (geminiWs && geminiWs.readyState === WebSocket.OPEN && sessionState === 'LISTENING') {
          const audioMsg = {
            realtime_input: {
              audio: {
                data: Buffer.from(data).toString('base64'),
                mimeType: 'audio/pcm;rate=' + actualSampleRate
              }
            }
          };
          geminiWs.send(JSON.stringify(audioMsg));
        }
      }
    } catch(e) {
      console.error('[WS Error]', e.message);
    }
  });

  ws.on('close', function() {
    console.log('[Gemini WS] Client disconnected');
    if (geminiWs && geminiWs.readyState === WebSocket.OPEN) geminiWs.close();
  });

  ws.on('error', function(e) {
    console.error('[WS Error]', e.message);
  });
});

app.get('/health', function(req, res) {
  res.json({
    status: 'ok',
    mode: 'gemini',
    apis: {
      gemini: !!GEMINI_API_KEY
    }
  });
});

const PORT = process.env.GEMINI_PORT || 3001;
server.listen(PORT, function() {
  console.log(`\n🤖  Gemini Live Demo running at http://localhost:${PORT}`);
  console.log(`Model: ${GEMINI_MODEL}\n`);
});