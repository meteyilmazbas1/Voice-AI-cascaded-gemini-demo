const ort = require('onnxruntime-node');
const path = require('path');
const { muLawDecodeByte } = require('./audioLevel');

const MODEL_PATH = path.join(__dirname, 'models', 'silero_vad.onnx');

let sessionPromise = null;
const getSession = () => {
    if (!sessionPromise) sessionPromise = ort.InferenceSession.create(MODEL_PATH);
    return sessionPromise;
};

function createSileroVad({
    sampleRate = 8000,
    startProb = 0.5,
    endProb = 0.35,
    minSilenceMs = 220,
    minSpeechMs = 120,
    speechPadMs = 60,
} = {}) {
    const windowSize = sampleRate === 16000 ? 512 : 256;
    const contextSize = sampleRate === 16000 ? 64 : 32;
    let state = new Float32Array(2 * 1 * 128);
    let context = new Float32Array(contextSize);
    let sampleBuf = new Float32Array(0);
    let lastProb = 0;
    let queue = Promise.resolve();
    const srTensor = new ort.Tensor("int64", BigInt64Array.from([BigInt(sampleRate)]), []);
    const frameMs = (windowSize / sampleRate) * 1000;
    let frameCount = 0;
    let clockMs = 0;
    let inSpeech = false;
    let silenceRun = 0;
    let curStart = 0, probSum = 0, probN = 0;
    let spans = [];
    let lastSpeechEndMs = 0;

    const updateSpans = (prob, tMs) => {
        clockMs = tMs + frameMs;
        if (!inSpeech) {
            if (prob >= startProb) {
                inSpeech = true;
                curStart = Math.max(0, tMs - speechPadMs);
                probSum = prob; probN = 1; silenceRun = 0;
            }
            return;
        }
        probSum += prob; probN++;
        if (prob >= endProb) { silenceRun = 0; return; }
        silenceRun += frameMs;
        if (silenceRun < minSilenceMs) return;
        const endMs = tMs - silenceRun + speechPadMs;
        if (endMs - curStart >= minSpeechMs) {
            spans.push({ startMs: curStart, endMs, conf: probSum / probN });
        }
        inSpeech = false; silenceRun = 0; lastSpeechEndMs = endMs;
    };

    const runChunk = async (chunk, frameIndex) => {
        const session = await getSession();
        const input = new Float32Array(contextSize + windowSize);
        input.set(context, 0);
        input.set(chunk, contextSize);
        const out = await session.run({
            input: new ort.Tensor("float32", input, [1, input.length]),
            state: new ort.Tensor("float32", state, [2, 1, 128]),
            sr: srTensor,
        });
        state = out.stateN.data;
        context = input.slice(input.length - contextSize);
        lastProb = out.output.data[0];
        updateSpans(lastProb, frameIndex * frameMs);
    };

    const pushSamples = (f32) => {
        const merged = new Float32Array(sampleBuf.length + f32.length);
        merged.set(sampleBuf); merged.set(f32, sampleBuf.length);
        sampleBuf = merged;
        let off = 0;
        while (sampleBuf.length - off >= windowSize) {
            const chunk = sampleBuf.slice(off, off + windowSize);
            off += windowSize;
            const idx = frameCount++;
            queue = queue.then(() => runChunk(chunk, idx)).catch(() => {});
        }
        sampleBuf = sampleBuf.slice(off);
    };
    
    return {
        processMulaw(buf) {
            const f = new Float32Array(buf.length);
            for (let i = 0; i < buf.length; i++) f[i] = muLawDecodeByte(buf[i]) / 32768;
            pushSamples(f);
        },
        processPcm16(buf) {
            const n = buf.length >> 1;
            const f = new Float32Array(n);
            for (let i = 0; i < n; i++) f[i] = buf.readInt16LE(i * 2) / 32768;
            pushSamples(f);
        },
        prob() { return lastProb; },
        isSpeaking() { return inSpeech; },
        idleMs() { return inSpeech ? 0 : Math.max(0, clockMs - lastSpeechEndMs); },
        takeSpans() { const s = spans; spans = []; return s;},
        resetTurn() { spans = []; inSpeech = false; silenceRun = 0; }, 
        flush() { return queue.then(() => lastProb); },
    };
}

module.exports = { createSileroVad };