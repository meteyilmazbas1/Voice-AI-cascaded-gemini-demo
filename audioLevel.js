function muLawDecodeByte(byte) {
    const u = ~byte & 0xff;
    let t = ((u & 0x0f) << 3) + 0x84;
    t <<= (u&0x70) >> 4;
    return (u&0x80) ? (0x84 -t) : (t - 0x84);
}

function frameDbfs(buf) {
    if (!buf || buf.length === 0)  return -Infinity;
    let sumSq = 0;
    for(let i = 0; i < buf.length; i++) {
        const s = muLawDecodeByte(buf[i]);
        sumSq += s * s;
    }
    const rms = Math.sqrt(sumSq / buf.length);
    if (rms <= 0) return -Infinity;
    return 20 * Math.log10(rms / 32768);
}

function median(arr) {
    const v = arr.filter(Number.isFinite).sort((a, b) => a - b);
    if (v.length === 0) return null;
    const mid = Math.floor(v.length / 2);
    return v.length % 2 ? v[mid] : (v[mid - 1] + v[mid]) / 2;
}

function percentile(arr, p) {
    const v = arr. filter(Number.isFinite).sort((a, b) => a - b);
    if (v.length === 0) return null;
    const idx = (p / 100) * (v.length - 1);
    const lo = Math.floor(idx);
    const hi = Math.ceil(idx);
    if (lo === hi) return v[lo];
    return v[lo] + (v[hi] - v[lo]) * (idx - lo);
}

function frameDbfsPcm16(buf) {
    if (!buf || buf.length < 2) return -Infinity;
    const n = buf.length >> 1;
    let sumSq = 0;
    for (let i = 0; i < n; i++) {
        const s = buf.readInt16LE(i * 2);
        sumSq += s * s;
    }
    const rms = Math.sqrt(sumSq / n);
    if (rms <= 0) return -Infinity;
    return 20 * Math.log10(rms / 32768);
}

function frameZcrPcm16(buf) {
    if (!buf || buf.length < 4) return 0;
    const n = buf.length >> 1;
    let crossings = 0;
    let prev = buf.readInt16LE(0);
    for ( let i = 1; i < n; i++) {
        const s = buf.readInt16LE(i * 2);
        if ((s >= 0) !== (prev >= 0)) crossings++;
        prev = s;
    }
    return crossings / (n - 1);
}

function frameZcrMulaw(buf) {
    if (!buf || buf.length < 2) return 0;
    let crossings = 0;
    let prev = muLawDecodeByte(buf[0]);
    for (let i = 1; i < buf.length; i++) {
        const s = muLawDecodeByte(buf[i]);
        if ((s >= 0) !== (prev >= 0)) crossings++;
        prev = s;
    }
    return crossings / (buf.length - 1);
}

function frameStatsMulaw(buf) {
    if (!buf || buf.length === 0) return { dbfs: -Infinity, zcr: 0, crestDb: 0};
    let sumSq = 0, peak = 0, crossings = 0;
    let prev = muLawDecodeByte(buf[0]);
    sumSq += prev * prev;
    if (Math.abs(prev) > peak) peak = Math.abs(prev);
    for (let i = 1; i < buf.length; i++) {
        const s = muLawDecodeByte(buf[i]);
        sumSq += s * s;
        const a = Math.abs(s);
        if (a > peak) peak = a;
       if ((s >= 0) !== (prev>= 0)) crossings++;
        prev = s;
    }
    const rms = Math.sqrt(sumSq / buf.length);
    return {
        dbfs: rms > 0 ? 20 * Math.log10(rms / 32768) : -Infinity,
        zcr: buf.length > 1 ? crossings / (buf.length - 1) : 0,
        crestDb: rms > 0 && peak > 0 ? 20 * Math.log10(peak / rms) : 0,
        };
}

function frameStatsPcm16(buf) {
    if (!buf || buf.length < 4) return { dbfs: -Infinity, zcr: 0, crestDb: 0, rms: 0 };
    const n = buf.length >> 1;
    let sumSq = 0, peak = 0, crossings = 0;
    let prev = buf.readInt16LE(0);
    sumSq += prev * prev;
    if (Math.abs(prev) > peak) peak = Math.abs(prev);
    for (let i = 1; i < n; i++) {
        const s = buf.readInt16LE(i * 2);
        sumSq += s * s;
        const a = Math.abs(s);
        if (a > peak) peak = a;
        if ((s >= 0) !== (prev >= 0)) crossings++;
        prev = s;
    }
    const rms = Math.sqrt(sumSq / n);
    return {
        dbfs: rms > 0 ? 20 * Math.log10(rms / 32768) : -Infinity,
        zcr: crossings / (n - 1),
        crestDb: rms > 0 && peak > 0 ? 20 * Math.log10(peak / rms) : 0,
        rms,
        };
}

module.exports = {
    muLawDecodeByte, frameDbfs, median, percentile,
    frameDbfsPcm16, frameZcrPcm16, frameZcrMulaw, frameStatsMulaw,
    frameStatsPcm16,
}