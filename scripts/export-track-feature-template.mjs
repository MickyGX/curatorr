import fs from 'fs';
import path from 'path';
import { initDb, getMasterTracks } from '../src/db.js';
import { buildTrackFeatureManifest } from '../src/services/track-feature-manifest.js';

function parseArgs(argv) {
  const args = {
    db: path.resolve(process.cwd(), 'data', 'curatorr.db'),
    out: path.resolve(process.cwd(), 'data', 'track-features.template.json'),
    missingOnly: true,
    includeExisting: false,
    limit: 0,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = String(argv[index] || '').trim();
    if (arg === '--db') args.db = path.resolve(process.cwd(), String(argv[index + 1] || args.db));
    else if (arg === '--out') args.out = path.resolve(process.cwd(), String(argv[index + 1] || args.out));
    else if (arg === '--all') args.missingOnly = false;
    else if (arg === '--include-existing') args.includeExisting = true;
    else if (arg === '--limit') {
      const value = Number(argv[index + 1] || 0);
      if (Number.isFinite(value) && value > 0) args.limit = Math.floor(value);
    } else {
      continue;
    }
    if (arg === '--db' || arg === '--out' || arg === '--limit') index += 1;
  }
  return args;
}

function ensureParentDir(filePath) {
  const parent = path.dirname(filePath);
  if (!fs.existsSync(parent)) fs.mkdirSync(parent, { recursive: true });
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const db = initDb(args.db);
  try {
    const manifest = buildTrackFeatureManifest(getMasterTracks(db), {
      missingOnly: args.missingOnly,
      includeExisting: args.includeExisting,
      limit: args.limit,
    });
    ensureParentDir(args.out);
    fs.writeFileSync(args.out, JSON.stringify(manifest, null, 2));
    console.log(`Wrote ${manifest.trackCount} track feature entr${manifest.trackCount === 1 ? 'y' : 'ies'} to ${args.out}`);
    console.log(`Source DB: ${args.db}`);
    console.log(`Mode: ${args.missingOnly ? 'missing-only' : 'all-tracks'}${args.includeExisting ? ', include-existing' : ''}`);
  } finally {
    db.close();
  }
}

main();
