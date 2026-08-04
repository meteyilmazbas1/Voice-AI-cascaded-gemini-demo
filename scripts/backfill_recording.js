require("dotenv").config({ path: require("path").join(__dirname, "..", ".env"), quiet: true });
const axios = require("axios");
const { listCalls, updateRecordingUrl, uploadRecording } = require("../db");

(async () => {
    const { calls } = await listCalls(1, 500);
    const prefix = "https://api.twilio.com/2010-04-01/Accounts/" + process.env.TWILIO_ACCOUNT_SID + "/";
    const todo = (calls || []).filter(c => c.recording_url && c.recording_url.indexOf(prefix) === 0);
        console.log("toplam:", (calls || []).length, "| tasinacak:", todo.length);
        let ok = 0, fail = 0;
        for (const c of todo) {
            try {
                const audio = await axios.get(c.recording_url, {
                    auth: { username: process.env.TWILIO_ACCOUNT_SID, password: process.env.TWILIO_AUTH_TOKEN },
                    responseType: "arraybuffer",
                });
                const uploaded = await uploadRecording(c.call_sid, Buffer.from(audio.data));
                if (!uploaded || !uploaded.url) throw new Error("upload basarisiz");
                await updateRecordingUrl(c.call_sid, uploaded.url, uploaded.storagePath);
                console.log(" ✓", c.call_sid);
                ok++;
            } catch (e) {
                console.warn(" ✗", c.call_sid, e.message);
                fail++;
            }
        }
        console.log("tasinan:", ok, "| hata:", fail);
})();