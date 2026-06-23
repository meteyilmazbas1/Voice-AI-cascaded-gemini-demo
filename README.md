# Aware360 — Voice AI Cascaded Pipeline Demo

Deepgram Nova-3 (STT) → Claude Sonnet (LLM) → ElevenLabs (TTS) pipeline'ını kullanan,
Missed Check-In alert'larını işleyen, PRD v1.2 ve Orion önerisine uygun demo uygulama.

## Mimari

```
Worker Ses Girişi
      │
      ▼
[Deepgram Nova-3] — STT (Speech-to-Text)
      │ transcript
      ▼
[Claude Sonnet]   — Intent Classification
      │ intent + response text
      ▼
[ElevenLabs]      — TTS (Text-to-Speech)
      │ audio
      ▼
Worker'a Çalınır
```

## Intent Set (MVP)

| Intent | Örnek | Aksiyon |
|--------|-------|---------|
| SAFE_CONFIRMATION | "I'm fine", "All good", "Yes" | Alert resolved |
| DELAYED_CHECKIN | "Running late, 15 min", "Give me time" | Timer extended |
| REQUEST_HUMAN | "I need a person", "Human please" | Escalate |
| UNRECOGNISED | Anlaşılamayan cevap | Clarify, max 1 kez; sonra escalate |

## Kurulum

### 1. Bağımlılıkları kur

```bash
npm install
```

### 2. API key'lerini ayarla

```bash
cp .env.example .env
```

`.env` dosyasını düzenle:

```env
DEEPGRAM_API_KEY=dg_xxxx...          # https://console.deepgram.com
ANTHROPIC_API_KEY=sk-ant-xxxx...     # https://console.anthropic.com
ELEVENLABS_API_KEY=xxxx...           # https://elevenlabs.io/app/settings/api-keys
ELEVENLABS_VOICE_ID=EXAVITQu4vr4xnSDxMaL   # varsayılan: Sarah sesi
PORT=3000
```

### 3. Başlat

```bash
npm start
# ya da geliştirme modunda:
npm run dev
```

### 4. Tarayıcıda aç

```
http://localhost:3000
```

## Kullanım

### Mock mod (API key gerektirmez)
ElevenLabs ve Deepgram key olmadan da çalışır.
- **Start Call** → AI intro metni ekrana gelir (ses yok)
- **Quick test buttons** → Mock response gönder, Claude Sonnet intent classify eder
- TTS çalışmaz ama yazılı response ve intent görünür

### Tam mod (tüm API key'ler ile)
- **Start Call** → ElevenLabs ile sesli intro
- **Mic** → Tarayıcıdan mikrofon aç, Deepgram'a stream et
- Konuş → STT → Claude → TTS pipeline'ı çalışır
- **Quick test buttons** → Metin olarak mock input

## Dosya Yapısı

```
voice-ai-demo/
├── server.js          — Express + WebSocket sunucusu, pipeline orchestration
├── public/
│   └── index.html     — Frontend UI (tek dosya)
├── .env.example       — API key template
├── .env               — Gerçek key'ler (git'e ekleme!)
├── package.json
└── README.md
```

## WebSocket Mesaj Protokolü

### Client → Server
```json
{ "type": "start" }                          // Oturumu başlat
{ "type": "mock_transcript", "text": "..." } // Test response gönder
{ "type": "stop" }                           // Oturumu kapat
<binary>                                     // PCM audio (16kHz, mono, int16)
```

### Server → Client
```json
{ "type": "stage", "stage": "stt|llm|tts", "status": "connecting|processing|done|error" }
{ "type": "greeting", "text": "...", "audio": "<base64 mp3 | null>" }
{ "type": "interim_transcript", "text": "...", "is_final": false }
{ "type": "transcript", "text": "..." }
{ "type": "intent", "data": { "intent": "...", "confidence": 0.97, ... } }
{ "type": "response", "text": "...", "audio": "<base64 | null>", "resolved": true }
{ "type": "escalate", "reason": "...", "message": "..." }
{ "type": "log", "message": "..." }
```

## PRD Uyumluluğu

| PRD Gereksinimi | Uygulama |
|-----------------|----------|
| FR-1.3 SOS → human always | SOS intent → REQUEST_HUMAN → escalate: true |
| FR-2.1 İlk arama < 30s | WebSocket bağlandıktan 500ms sonra greeting |
| FR-3.1 AI kimliğini tanıt | "Aware360 Monitoring Center" ile başla |
| FR-3.3 Sadece defined intent | System prompt strict JSON schema ile |
| FR-3.4 Max 1 clarification | unrecognisedCount > 1 → escalate |
| FR-3.10 AI olduğunu kabul et | System prompt'ta hardcoded |
| FR-5.4 Post-call summary | Intent card: intent, confidence, response |

## Notlar

- ElevenLabs voice ID değiştirmek için `.env` içindeki `ELEVENLABS_VOICE_ID`'yi değiştir
- Nova-3 model string: `nova-3` — Deepgram'ın en güncel modeli
- Latency panel'i PRD NFR hedeflerine göre renk kodlu (< 1200ms = yeşil)
- Ses giriş format: 16kHz, mono, 16-bit PCM (MediaRecorder'dan)
