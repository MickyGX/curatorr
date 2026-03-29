import Database from 'better-sqlite3';

const DEFAULT_DB_PATH = process.env.CURATORR_DB_PATH
  ? process.env.CURATORR_DB_PATH
  : process.env.DATA_DIR
    ? `${process.env.DATA_DIR.replace(/\/$/, '')}/curatorr.db`
    : './data/curatorr.db';

const COMMAND = String(process.argv[2] || 'seed').trim().toLowerCase();
const DB_PATH = String(process.argv[3] || DEFAULT_DB_PATH).trim();

const SEED_TRACKS = [
  {
    ratingKey: 'lb-seed-batwam-1',
    artistName: 'Eminem',
    trackTitle: 'The Real Slim Shady',
    albumName: 'The Marshall Mathers LP',
    recordingMbid: '684ca01c-9775-42fe-9cdb-28fa37c27851',
  },
  {
    ratingKey: 'lb-seed-batwam-2',
    artistName: 'Billie Eilish',
    trackTitle: 'bad guy',
    albumName: 'WHEN WE ALL FALL ASLEEP, WHERE DO WE GO?',
    recordingMbid: '694da04d-1ffc-435c-8b4b-59cc23ac8003',
  },
  {
    ratingKey: 'lb-seed-batwam-3',
    artistName: 'Arctic Monkeys',
    trackTitle: 'I Bet You Look Good on the Dancefloor',
    albumName: "Whatever People Say I Am, That's What I'm Not",
    recordingMbid: '',
  },
];

function usage(code = 0) {
  const lines = [
    'Usage:',
    '  node scripts/seed-listenbrainz-batwam.mjs [seed|cleanup] [dbPath]',
    '',
    `Default dbPath: ${DEFAULT_DB_PATH}`,
  ];
  console.log(lines.join('\n'));
  process.exit(code);
}

if (COMMAND === '--help' || COMMAND === '-h') usage(0);
if (!['seed', 'cleanup'].includes(COMMAND)) usage(1);

const db = new Database(DB_PATH);

try {
  if (COMMAND === 'cleanup') {
    const result = db.prepare("DELETE FROM master_tracks WHERE rating_key LIKE 'lb-seed-batwam-%'").run();
    console.log(JSON.stringify({ command: 'cleanup', dbPath: DB_PATH, removed: result.changes }, null, 2));
    process.exit(0);
  }

  const now = Date.now();
  const upsert = db.prepare(`
    INSERT INTO master_tracks (rating_key, artist_name, track_title, album_name, recording_mbid, genres, library_key, rating_count, view_count, updated_at)
    VALUES (?, ?, ?, ?, ?, '[]', 'listenbrainz-seed', 0, 0, ?)
    ON CONFLICT(rating_key) DO UPDATE SET
      artist_name = excluded.artist_name,
      track_title = excluded.track_title,
      album_name = excluded.album_name,
      recording_mbid = excluded.recording_mbid,
      library_key = excluded.library_key,
      updated_at = excluded.updated_at
  `);
  const txn = db.transaction((tracks) => {
    for (const track of tracks) {
      upsert.run(track.ratingKey, track.artistName, track.trackTitle, track.albumName, track.recordingMbid || '', now);
    }
  });
  txn(SEED_TRACKS);
  console.log(JSON.stringify({
    command: 'seed',
    dbPath: DB_PATH,
    inserted: SEED_TRACKS.length,
    tracks: SEED_TRACKS.map(({ artistName, trackTitle, ratingKey }) => ({ artistName, trackTitle, ratingKey })),
  }, null, 2));
} finally {
  db.close();
}
