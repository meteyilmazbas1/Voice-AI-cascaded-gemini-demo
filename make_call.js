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
  PUBLIC_URL,
} = process.env;

const missing = [];
if (!TWILIO_ACCOUNT_SID || TWILIO_ACCOUNT_SID === 'your_twilio_sid') missing.push('TWILIO_ACCOUNT_SID');
if (!TWILIO_AUTH_TOKEN || TWILIO_AUTH_TOKEN === 'your_twilio_auth_token') missing.push('TWILIO_AUTH_TOKEN');
if (!TWILIO_PHONE_NUMBER) missing.push('TWILIO_PHONE_NUMBER');
if (!TEST_PHONE_NUMBER) missing.push('TEST_PHONE_NUMBER');
if (!PUBLIC_URL) missing.push('PUBLIC_URL');

if (missing.length) {
  console.error('Missing required .env values:', missing.join(', '));
  console.error('\nAdd to .env:');
  console.error('  PUBLIC_URL=https://abc123.ngrok-free.app  (port 3000 tunnel)');
  process.exit(1);
}

const client = twilio(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN);
const webhookUrl = `${PUBLIC_URL.replace(/\/$/, '')}/incoming-call`;

console.log(`\nPlacing Cascaded test call to ${TEST_PHONE_NUMBER}`);
console.log(`Webhook: ${webhookUrl} (server.js port 3000)\n`);

client.calls
  .create({
    url: webhookUrl,
    to: TEST_PHONE_NUMBER,
    from: TWILIO_PHONE_NUMBER,
    method: 'POST',
    record: true,
    recordingChannels: 'dual',
    recordingStatusCallback: `${PUBLIC_URL.replace(/\/$/, '')}/recording-status`,
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