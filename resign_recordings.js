/**
 * Re-sign existing call recordings after making the Supabase `recordings` bucket private.
 *
 * Prerequisites:
 *   1. Dashboard → Storage → recordings → turn OFF "Public bucket"
 *   2. Add nullable text column `recording_storage_path` on `calls`
 *      (see scripts/add_recording_storage_path.sql)
 *
 * Usage:
 *   node resign_recordings.js
 *   node resign_recordings.js --dry-run
 */
require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const SUPABASE_URL = process.env.SUPABASE_URL;
const SUPABASE_SERVICE_KEY = process.env.SUPABASE_SERVICE_KEY;
if (!SUPABASE_URL || !SUPABASE_SERVICE_KEY) {
  console.error('SUPABASE_URL and SUPABASE_SERVICE_KEY must be set in .env');
  process.exit(1);
}

const supabase = createClient(SUPABASE_URL, SUPABASE_SERVICE_KEY, {
  auth: { persistSession: false },
});

const BUCKET = 'recordings';
const SIGNED_URL_EXPIRY_SEC = 60 * 60 * 24 * 365; // 1 year
const dryRun = process.argv.includes('--dry-run');

function extractStoragePath(call) {
  if (call.recording_storage_path) return call.recording_storage_path;
  const url = call.recording_url || '';
  // public: .../object/public/recordings/<path>
  // signed: .../object/sign/recordings/<path>?token=...
  const m = String(url).match(/\/recordings\/([^?]+)/);
  if (m && m[1]) {
    try {
      return decodeURIComponent(m[1]);
    } catch (e) {
      return m[1];
    }
  }
  // Upload convention
  if (call.call_sid) return call.call_sid + '.mp3';
  return null;
}

async function resign() {
  const { data: calls, error } = await supabase
    .from('calls')
    .select('call_sid, recording_url, recording_storage_path')
    .not('recording_url', 'is', null);

  if (error) {
    console.error('Fetch error:', error.message);
    if (/recording_storage_path/.test(error.message)) {
      console.error('Hint: add column with scripts/add_recording_storage_path.sql');
    }
    process.exit(1);
  }

  console.log('Found', calls.length, 'calls with recordings' + (dryRun ? ' (dry-run)' : ''));

  let ok = 0;
  let fail = 0;
  let skip = 0;

  for (const call of calls) {
    try {
      const fileName = extractStoragePath(call);
      if (!fileName) {
        skip++;
        console.warn('Skip (no path):', call.call_sid);
        continue;
      }

      // Skip Twilio-only URLs (not in Supabase yet)
      if (String(call.recording_url || '').indexOf('api.twilio.com') !== -1) {
        skip++;
        console.log('Skip (Twilio URL, not in Storage):', call.call_sid);
        continue;
      }

      const { data: signedData, error: signError } = await supabase.storage
        .from(BUCKET)
        .createSignedUrl(fileName, SIGNED_URL_EXPIRY_SEC);

      if (signError) {
        fail++;
        console.error('Sign failed:', call.call_sid, fileName, signError.message);
        continue;
      }

      if (dryRun) {
        ok++;
        console.log('[dry-run] Would re-sign:', call.call_sid, '→', fileName);
        continue;
      }

      const { error: updateError } = await supabase
        .from('calls')
        .update({
          recording_url: signedData.signedUrl,
          recording_storage_path: fileName,
        })
        .eq('call_sid', call.call_sid);

      if (updateError) {
        fail++;
        console.error('DB update failed:', call.call_sid, updateError.message);
        continue;
      }

      ok++;
      console.log('Re-signed:', call.call_sid);
    } catch (e) {
      fail++;
      console.error('Exception:', call.call_sid, e.message);
    }
  }

  console.log('Done. Success:', ok, 'Failed:', fail, 'Skipped:', skip);
  if (fail) process.exit(1);
}

resign().catch(function (e) {
  console.error(e);
  process.exit(1);
});
