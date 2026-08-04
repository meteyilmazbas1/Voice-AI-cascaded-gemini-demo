-- Add durable storage path for private recordings (signed URL refresh).
-- Run once in Supabase SQL Editor before resign_recordings.js / deploy.

ALTER TABLE calls
  ADD COLUMN IF NOT EXISTS recording_storage_path text;

COMMENT ON COLUMN calls.recording_storage_path IS
  'Object path inside the private recordings bucket (e.g. CAxxxx.mp3)';
