const { percentile } = require("./audioLevel");
function createNoiseEstimator({
    frameMs = 20,
    floorPercentile = 10,
    noiseBufferSize = 300,
    minNoiseFrames = 10,
    spikeMarginDb = 15,
    onsetDb = 12,
    minTransientMs = 20,
    maxTransientMs = 1500,
    collisionMarginMs = 75,
    gapCoverMinMs = 200,
    edgeGapWindowMs = 1200,
    maskConfSkip = 0.9,
    maskMarginDb = 6,
    rateWindowMs = 10000,
    snrNoisyDb = 12,
    snrCleanDb = 18,
    noisyTurnsToEnter = 2,
    quietFloorDbfs = -60,
    dtxFloorDbfs = -68,
    spanPadMs = 60,
    probSpeech = 0.6,
    probNoise = 0.2,
    dtxProbMax = 0.05,
    dtxDropDb = 12,
    gapPeakDropDb = 10,
} = {}) {
    let clockMs = 0;
    let history = [];
    let speechActive = false;
    let agentSpeaking = false;
    let noiseRing = [];
    let floorDbfs = null;
    let bootstrapCount = 0;
    let noiseLevelDbfs = null;
    let spikeProbSum = 0, spikeProbN = 0;

    let spikeState = "idle";
    let spikeStartMs = 0, spikePeak = -Infinity;
    let recentDb = [];
    let events = [];

    let noisy = false;
    let consecNoisy = 0;
    let lastTurnEndMs = 0;

    const median3 = (a) => [...a].sort((x, y) => x- y)[Math.floor(a.length / 2)];

    const pruneEvents = () => {
        const cutoff = clockMs - rateWindowMs;
        while (events.length && events[0].t < cutoff) events.shift();
    };

    const closeSpike = () => {
        const dur = clockMs - spikeStartMs;
        if (dur >= minTransientMs) {
            events.push({ t: clockMs, peak: spikePeak, durationMs: dur, long: dur > maxTransientMs });
        }
        spikeState = "idle";
    };

    const detectTransient = (dbfs) => {
        if (!Number.isFinite(dbfs)) {
            if (spikeState === "spike") closeSpike();
            else spikeState = "idle";
            return;
        }
        const threshold = spikeMarginDb + (floorDbfs ?? quietFloorDbfs);
        const baseline = recentDb.length === 3 ? median3(recentDb) : null;
        const onset = baseline != null && (dbfs - baseline) >= onsetDb;
        if (spikeState === "idle") {
            if (dbfs > threshold && onset && !agentSpeaking) {
                spikeState = "spike";
                spikeStartMs = clockMs;
                spikePeak = dbfs;
            }
        } else if (dbfs > threshold) {
            if (dbfs > spikePeak) spikePeak = dbfs;
        } else {
            closeSpike();
        }
    };
    const computeTurn = (speechSpans, commit) => {
        const frames = history.filter((f) => !f.agent && Number.isFinite(f.dbfs));
        const isSpeech = (f) => (speechSpans
    ? speechSpans.some((s) => f.t >= s.startMs - spanPadMs && f.t <= s.endMs + spanPadMs)
    : f.speech);
        const speechFr = [], noiseFr = [];
        for (const f of frames) {
            if (f.speechProb == null) {
                (isSpeech(f) ? speechFr : noiseFr).push(f);
            } else if (speechSpans) {
                if (isSpeech(f) && f.speechProb >= probSpeech) speechFr.push(f);
                else if (!isSpeech(f) && f.speechProb <= probNoise) noiseFr.push(f);
            } else {
                if (f.speechProb >= probSpeech) speechFr.push(f);
                else if (f.speechProb <= probNoise) noiseFr.push(f);
            }
        }

                let speechFallback = false;
        if (speechSpans && speechFr.length === 0) {
            for (const f of frames) if (isSpeech(f)) speechFr.push(f);
            speechFallback = true;
        }

        const speechDbfs = speechFr.length ? percentile(speechFr.map((f) => f.dbfs), 90) : null;
        const speechMedDbfs = speechFr.length ? percentile(speechFr.map((f) => f.dbfs), 50) : null;

        const dtxCutoff = noiseLevelDbfs != null ? Math.max(dtxFloorDbfs, noiseLevelDbfs - dtxDropDb) : dtxFloorDbfs;
        const realNoiseFr = noiseFr.filter((f) =>
            f.dbfs > dtxFloorDbfs && !(f.speechProb != null && f.speechProb < dtxProbMax && f.dbfs <= dtxCutoff));

        const silentFrames = history.filter((f) => !f.agent
            && (!Number.isFinite(f.dbfs) || f.dbfs <= dtxFloorDbfs)
            && (f.speechProb == null || f.speechProb <= probNoise)).length;
        const quietLine = realNoiseFr.length < minNoiseFrames && silentFrames >= minNoiseFrames;

        const candidateRing = noiseRing.concat(realNoiseFr.map((f) => f.dbfs)).slice(-noiseBufferSize);
        const floorNow = candidateRing.length >= minNoiseFrames
            ? percentile(candidateRing, floorPercentile) : floorDbfs;

        const levelNow = realNoiseFr.length >= minNoiseFrames
            ? percentile(realNoiseFr.map((f) => f.dbfs), 50)
            : quietLine ? quietFloorDbfs
            : noiseLevelDbfs ?? floorNow;

        const snrDb = (levelNow != null && speechDbfs != null) ? speechDbfs - levelNow : null;

        const allTurnEvents = events.filter((e) => e.t >= lastTurnEndMs);
        const collisions = [];
        const gapPushed = new Set();
        if (speechSpans && speechSpans.length && speechMedDbfs != null) {
            const uttStart = Math.min(...speechSpans.map((s) => s.startMs));
            const uttEnd = Math.max(...speechSpans.map((s) => s.endMs));
            for (const e of allTurnEvents) {
                const canOverlap = e.peak >= speechMedDbfs + maskMarginDb;
                const canGap = e.peak >= speechMedDbfs - gapPeakDropDb;
                if (!canOverlap && !canGap) continue;
                const evStart = e.t - e.durationMs, evEnd = e.t;
                const hitSpans = speechSpans.filter((s) =>
                evStart <= s.endMs + collisionMarginMs && evEnd >= s.startMs - collisionMarginMs);
                if (hitSpans.length) {
                    if (!e.long) {
                        for (const s of hitSpans) {
                            if (s.conf !=null && s.conf >= maskConfSkip) continue;
                            collisions.push({tMs: evStart, durationMs: e.durationMs, peakDbfs: e.peak, type: "overlap", word: s.word ?? null, idx: s.idx ?? null });
                        }}
                    for (let i = 0; i < speechSpans.length - 1; i++) {
                        const gapStart = speechSpans[i].endMs, gapEnd = speechSpans[i + 1].startMs;
                        const covStart = Math.max(evStart, gapStart);
                        const covEnd = Math.min(evEnd, gapEnd);
                        if (covEnd - covStart >= gapCoverMinMs && !gapPushed.has(i)) {
                            gapPushed.add(i);
                            collisions.push({ tMs: covStart, durationMs: covEnd - covStart, peakDbfs: e.peak,
                                type: "gap", word: null, prevWord: speechSpans[i].word ?? null, nextWord: speechSpans[i + 1].word ?? null
                            });
                        }
                    }
                } else if (evEnd >= uttStart && evStart <= uttEnd) {
                    let prevWord = null, nextWord = null;
                    for (const s of speechSpans) {
                        if (s.endMs <= evStart) prevWord = s.word ?? prevWord;
                        else if (s.startMs >= evEnd && nextWord == null) nextWord = s.word ?? null;
                    }
                    collisions.push({ tMs: evStart, durationMs: e.durationMs, peakDbfs: e.peak, type: "gap", word: null, prevWord, nextWord });
                }
                const trailCov = Math.min(evEnd, uttEnd + edgeGapWindowMs) - Math.max(evStart, uttEnd);
                if (trailCov >= gapCoverMinMs && !gapPushed.has("trail")) {
                    gapPushed.add("trail");
                    collisions.push({ tMs: Math.max(evStart, uttEnd), durationMs: trailCov, peakDbfs: e.peak,
                        type: "gap", word: null, prevWord: speechSpans[speechSpans.length - 1].word ?? null, nextWord: null 
                    });
                }
                const leadCov = Math.min(evEnd, uttStart) - Math.max(evStart, uttStart - edgeGapWindowMs);
                if (leadCov >= gapCoverMinMs && !gapPushed.has("lead")) {
                    gapPushed.add("lead");
                    collisions.push({ tMs: Math.max(evStart, uttStart - edgeGapWindowMs), durationMs: leadCov, peakDbfs: e.peak,
                        type: "gap", word: null, prevWord: null, nextWord: speechSpans[0].word ?? null });
                } 
            }
        }
        let nextNoisy = noisy, nextConsec = consecNoisy;
        const rawNoisy = snrDb != null && snrDb < snrNoisyDb;
        const rawClean = snrDb != null && snrDb >= snrCleanDb;
        if (rawNoisy) {
            nextConsec = consecNoisy + 1;
            if (nextConsec >= noisyTurnsToEnter) nextNoisy = true;
        } else if (rawClean) {
            nextConsec = 0;
            nextNoisy = false;
        }
        
        const result = {
            floorDbfs: floorNow, noiseLevelDbfs: levelNow, speechDbfs, speechMedDbfs, snrDb,
            snrReliable: realNoiseFr.length >= minNoiseFrames || quietLine, noisy: nextNoisy, rawNoisy,
            collisions, hasCollision: collisions.length > 0, speechFallback,
        };

        if (commit) {
            noiseRing = candidateRing;
            floorDbfs = floorNow;
            noisy = nextNoisy;
            consecNoisy = nextConsec;
            history = [];
            lastTurnEndMs = clockMs;
            noiseLevelDbfs = levelNow;
        }
        return result;
    };
    return {
        pushFrame({ dbfs, zcr = null, crestDb = 0, speechProb = null } = {}, t = null) {
            clockMs = t!= null ? t : clockMs + frameMs;
            history.push({ t: clockMs, dbfs, zcr, crestDb, speechProb, speech: speechActive, agent: agentSpeaking });
            if (history.length > 9000) history.splice(0, history.length - 9000);
            detectTransient(dbfs, zcr, crestDb, speechProb);
            if (Number.isFinite(dbfs)) {
                recentDb.push(dbfs);
                if (recentDb.length > 3) recentDb.shift();
            }
            pruneEvents();
            if (floorDbfs == null) {
                bootstrapCount++;
                if (bootstrapCount % 25 === 0) {
                    const nf = history
                    .filter((f) => !f.agent && !f.speech && Number.isFinite(f.dbfs) && f.dbfs > dtxFloorDbfs
                && (f.speechProb == null || f.speechProb <= probNoise))
                    .map((f) => f.dbfs);
                    if (nf.length >= minNoiseFrames) floorDbfs = percentile(nf, floorPercentile);
                }
            }
        },
        setSpeechActive(v) {speechActive = !!v},
        setAgentSpeaking(v) {agentSpeaking = !!v},
        markRecentSpeech(ms) {
            const cutoff = clockMs - ms;
            for (let i = history.length - 1; i >= 0 && history[i].t >= cutoff; i--) history[i].speech = true;
        },
        endTurn({ speechSpans = null } = {}) {
            if (spikeState === "spike") closeSpike();
            return computeTurn(speechSpans, true); },
        peek({ speechSpans = null} = {}) {return computeTurn(speechSpans, false);},   
    };
}

module.exports = {createNoiseEstimator};