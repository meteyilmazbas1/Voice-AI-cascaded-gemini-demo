const fs = require('fs');

if (fs.existsSync('.env')) {
  fs.readFileSync('.env', 'utf8').split('\n').forEach(line => {
    const [key, ...val] = line.trim().split('=');
    if (key && val.length) process.env[key] = val.join('=');
  });
}

const twilio = require('twilio');

const {
  TWILIO_ACCOUNT_SID,
  TWILIO_AUTH_TOKEN,
  TWILIO_PHONE_NUMBER,
  TEST_PHONE_NUMBER,
  GEMINI_PUBLIC_URL,
  PUBLIC_URL,
} = process.env;

const geminiUrl = GEMINI_PUBLIC_URL || PUBLIC_URL;
const TWIML_APP_SID = 'APcb8e96e499ec201f3ac9e11832e14079';

const missing = [];
if (!TWILIO_ACCOUNT_SID || TWILIO_ACCOUNT_SID === 'your_twilio_sid') missing.push('TWILIO_ACCOUNT_SID');
if (!TWILIO_AUTH_TOKEN || TWILIO_AUTH_TOKEN === 'your_twilio_auth_token') missing.push('TWILIO_AUTH_TOKEN');
if (!TWILIO_PHONE_NUMBER) missing.push('TWILIO_PHONE_NUMBER');
if (!TEST_PHONE_NUMBER) missing.push('TEST_PHONE_NUMBER');
if (!geminiUrl) missing.push('GEMINI_PUBLIC_URL or PUBLIC_URL');

if (missing.length) {
  console.error('Missing required .env values:', missing.join(', '));
  process.exit(1);
}

const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);

console.log(`\nPlacing Gemini test call to ${TEST_PHONE_NUMBER}`);
console.log(`Using TwiML App SID: ${TWIML_APP_SID}\n`);

client.calls
  .create({
    applicationSid: TWIML_APP_SID,
    to: TEST_PHONE_NUMBER,
    from: TWILIO_PHONE_NUMBER,
    record: true,
    recordingChannels: 'dual',
    recordingStatusCallback: `${geminiUrl.replace(/\/$/, '')}/recording-status`,
    recordingStatusCallbackMethod: 'POST',
  })
  .then((call) => {
    console.log(`Call initiated — SID: ${call.sid}`);
    console.log(`Status: ${call.status}`);
  })
  .catch((err) => {
    console.error('Call failed:', err.message);
    process.exit(1);
  });