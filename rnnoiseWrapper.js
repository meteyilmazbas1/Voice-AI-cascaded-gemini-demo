const { createUpsampler, createDownSampler } = require("./deepFilterWrapper");

const FRAME = 480;

let modulePromise = null;
let moduleReady = false;

function getModule() {
    if (!modulePromise) {
        // Paketin index.js'i ESM + uzantisiz import kullaniyor, CommonJS'ten cozulmuyor.
        // Async build (dist/rnnoise.js) ise Node'da rnnoise.wasm dosyasini bulamiyor
        // ("Failed to parse URL from rnnoise.wasm"). wasm'i base64 gomulu tasiyan
        // sync build Node'da calisan tek varyant.
        const mod = require("@jitsi/rnnoise-wasm/dist/rnnoise-sync.js");
        const factory = mod.default || mod;
        modulePromise = Promise.resolve(factory())
        .then(function (m) { moduleReady = true; return m; });
    }
    return modulePromise;
}

function createRnnoise(opts) {
    const inputRate = (opts && opts.inputRate) || 16000;
    const outputRate = (opts && opts.outputRate) || inputRate;
    const up = createUpsampler(48000 / inputRate);
    const down = createDownSampler(48000 / outputRate);

    let state = null;
    let framePtr = null;
    let pending = new Float32Array(0);
    let queue = Promise.resolve();
    let bypass = false;
    let lastVad = null;

    const toPcm16 = function (f32) {
        const buf = Buffer.alloc(f32.length * 2);
        for (let i = 0; i < f32.length; i++) {
            const s = Math.max(-1, Math.min(1, f32[i]));
            buf.writeInt16LE(Math.round(s < 0 ? s * 0x8000 : s * 0x7fff), i * 2);
        }
        return buf;
    };

    const naive = function (f32) {
        if (inputRate === outputRate) return f32;
        const ratio = outputRate / inputRate;
        const out = new Float32Array(Math.floor(f32.length * ratio));
        for ( let i = 0; i < out.length; i++) out[i] = f32[Math.floor(i / ratio)];
        return out;
    };

    const processFloat = function (f32in) {
        const result = queue.then(async function () {
            if (bypass) return toPcm16(naive(f32in));
            const m = await getModule();
            if (!state) {
                state = m._rnnoise_create(0);
                framePtr = m._malloc(FRAME * 4);
            }
            const up48 = up.process(f32in);
            const merged = new Float32Array(pending.length + up48.length);
            merged.set(pending);
            merged.set(up48, pending.length);
            const nFrames = Math.floor(merged.length / FRAME);
            pending = merged.slice(nFrames * FRAME);
            if (!nFrames) return Buffer.alloc(0);

            const all = new Float32Array(nFrames * FRAME);
            const idx = framePtr >> 2;
            for (let i = 0; i < nFrames; i++) {
                const heap = m.HEAPF32;
                for (let j = 0; j < FRAME; j++) heap[idx + j] = merged[i * FRAME + j] * 32768;
                lastVad = m._rnnoise_process_frame(state,framePtr, framePtr);
                for (let j = 0; j < FRAME; j++) all[i * FRAME + j] = heap[idx + j] / 32768;
            }
            return toPcm16(down.process(all));
        }).catch(function (err) {
            if (!bypass) {
                bypass = true;
                console.error("[RNNoise] Bypass mode activated:", err && err.message);
            }
            return toPcm16(naive(f32in));
        });
        queue = result.then(function () {}, function () {});
        return result;
    };

    return { 
        processPcm16: function (buf) {
            const n = buf.length >> 1;
            const f = new Float32Array(n);
            for (let i = 0; i < n; i++) f[i] = buf.readInt16LE(i * 2) / 32768;
            return processFloat(f);
        },
        vadProb: function () { return lastVad; },
        destroy: function () {
            if (!state) return;
            getModule().then(function (m) {
                m._rnnoise_destroy(state); m._free(framePtr); state = null; framePtr = null;
            });
        },
    };
}

async function preloadRnnoise() {
    await getModule();
    console.log("[RNNoise] WASM module loaded");
    return true;    
}

function isRnnoiseReady() { return moduleReady; }

module.exports = { createRnnoise, preloadRnnoise, isRnnoiseReady };