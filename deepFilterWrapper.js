const ort = require('onnxruntime-node');
const path = require('path');
const fs = require('fs');

const MODEL_PATH = path.join(
  __dirname,
  'models',
  'DeepFilterNet3_torchDF_onnx',
  'denoiser_model.onnx'
);

const FRAME = 512;
const STATE_SHAPES = {
  erb_norm_state: [32],
  band_unit_norm_state: [1, 96, 1],
  analysis_mem: [512],
  synthesis_mem: [512],
  rolling_erb_buf: [1, 1, 3, 32],
  rolling_feat_spec_buf: [1, 2, 3, 96],
  rolling_c0_buf: [1, 64, 5, 96],
  rolling_spec_buf_x: [5, 513, 2],
  rolling_spec_buf_y: [7, 513, 2],
  enc_hidden: [1, 1, 256],
  erb_dec_hidden: [2, 1, 256],
  df_dec_hidden: [2, 1, 256],
};

let sessionPromise = null;
let modelReady = false;

function getSession() {
  if (!sessionPromise) {
    sessionPromise = ort.InferenceSession.create(MODEL_PATH).then(function(session) {
      modelReady = true;
      return session;
    });
  }
  return sessionPromise;
}

function designLowpass(numTaps, cutoff) {
  const h = new Float32Array(numTaps);
  const c = (numTaps - 1) / 2;
  let sum = 0;
  for (let i = 0; i < numTaps; i++) {
    const x = i - c;
    const sinc = x === 0 ? 2 * cutoff : Math.sin(2 * Math.PI * cutoff * x) / (Math.PI * x);
    const w = 0.54 - 0.46 * Math.cos((2 * Math.PI * i) / (numTaps - 1));
    h[i] = sinc * w;
    sum += h[i];
  }
  for (let i = 0; i < numTaps; i++) h[i] /= sum;
  return h;
}

function createUpsampler(factor) {
  if (factor === 1) return { process: function(x) { return x; } };
  const taps = 24 * factor;
  const h = designLowpass(taps, 0.5 / factor).map(function(v) { return v * factor; });
  const histLen = Math.ceil(taps / factor);
  let hist = new Float32Array(histLen);
  return {
    process: function(input) {
      const x = new Float32Array(hist.length + input.length);
      x.set(hist);
      x.set(input, hist.length);
      const out = new Float32Array(input.length * factor);
      for (let m = 0; m < input.length; m++) {
        const base = hist.length + m;
        for (let p = 0; p < factor; p++) {
          let acc = 0;
          for (let k = p, j = 0; k < taps; k += factor, j++) acc += h[k] * x[base - j];
          out[m * factor + p] = acc;
        }
      }
      hist = x.slice(x.length - histLen);
      return out;
    },
  };
}

function createDownSampler(factor) {
  if (factor === 1) return { process: function(x) { return x; } };
  const taps = 24 * factor;
  const h = designLowpass(taps, 0.5 / factor);
  let buf = new Float32Array(0);
  return {
    process: function(input) {
      const x = new Float32Array(buf.length + input.length);
      x.set(buf);
      x.set(input, buf.length);
      const nOut = x.length >= taps ? Math.floor((x.length - taps) / factor) + 1 : 0;
      const out = new Float32Array(nOut);
      for (let n = 0; n < nOut; n++) {
        const end = n * factor + taps - 1;
        let acc = 0;
        for (let k = 0; k < taps; k++) acc += h[k] * x[end - k];
        out[n] = acc;
      }
      buf = x.slice(nOut * factor);
      return out;
    },
  };
}

function createDeepFilter(opts) {
  const inputRate = (opts && opts.inputRate) || 16000;
  const outputRate = (opts && opts.outputRate) || inputRate;
  const up = createUpsampler(48000 / inputRate);
  const down = createDownSampler(48000 / outputRate);
  const states = {};
  for (const [name, shape] of Object.entries(STATE_SHAPES)) {
    states[name] = new ort.Tensor('float32', new Float32Array(shape.reduce(function(a, b) { return a * b; }, 1)), shape);
  }
  let pending = new Float32Array(0);
  let queue = Promise.resolve();
  let bypass = false;

  const toPcm16 = function(f32) {
    const buf = Buffer.alloc(f32.length * 2);
    for (let i = 0; i < f32.length; i++) {
      const s = Math.max(-1, Math.min(1, f32[i]));
      buf.writeInt16LE(Math.round(s < 0 ? s * 0x8000 : s * 0x7fff), i * 2);
    }
    return buf;
  };

  const naive = function(f32) {
    if (inputRate === outputRate) return f32;
    const ratio = outputRate / inputRate;
    const out = new Float32Array(Math.floor(f32.length * ratio));
    for (let i = 0; i < out.length; i++) out[i] = f32[Math.floor(i / ratio)];
    return out;
  };

  const processFloat = function(f32in) {
    const result = queue.then(async function() {
      if (bypass) return toPcm16(naive(f32in));
      const session = await getSession();
      const up48 = up.process(f32in);
      const merged = new Float32Array(pending.length + up48.length);
      merged.set(pending);
      merged.set(up48, pending.length);
      const nFrames = Math.floor(merged.length / FRAME);
      pending = merged.slice(nFrames * FRAME);
      if (!nFrames) return Buffer.alloc(0);
      const all = new Float32Array(nFrames * FRAME);
      for (let i = 0; i < nFrames; i++) {
        const res = await session.run({
          input_frame: new ort.Tensor('float32', merged.slice(i * FRAME, (i + 1) * FRAME), [FRAME]),
          ...states,
        });
        for (const name of Object.keys(STATE_SHAPES)) states[name] = res['new_' + name];
        all.set(res.enhanced_audio_frame.data, i * FRAME);
      }
      return toPcm16(down.process(all));
    }).catch(function(err) {
      if (!bypass) {
        bypass = true;
        console.error('[DeepFilter] Bypass mode activated:', err && err.message);
      }
      return toPcm16(naive(f32in));
    });
    queue = result.then(function() {}, function() {});
    return result;
  };

  return {
    processPcm16: function(buf) {
      const n = buf.length >> 1;
      const f = new Float32Array(n);
      for (let i = 0; i < n; i++) f[i] = buf.readInt16LE(i * 2) / 32768;
      return processFloat(f);
    },
  };
}

async function preloadDeepFilter() {
  if (!fs.existsSync(MODEL_PATH)) {
    throw new Error('Model not found at ' + MODEL_PATH);
  }
  await getSession();
  console.log('[DeepFilter] Model loaded');
  return true;
}

function isDeepFilterReady() {
  return modelReady;
}

module.exports = {
  createDeepFilter,
  preloadDeepFilter,
  isDeepFilterReady,
  MODEL_PATH,
  createUpsampler,
  createDownSampler,
};
