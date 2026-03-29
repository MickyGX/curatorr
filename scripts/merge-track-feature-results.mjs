import fs from 'fs';
import path from 'path';
import {
  mergeAnalyzerResultsIntoManifest,
  parseAnalyzerFeatureInput,
} from '../src/services/track-feature-manifest.js';

function parseArgs(argv) {
  const args = {
    manifest: '',
    input: '',
    format: 'auto',
    out: '',
    overwriteExisting: false,
  };
  for (let index = 0; index < argv.length; index += 1) {
    const arg = String(argv[index] || '').trim();
    if (arg === '--manifest') args.manifest = path.resolve(process.cwd(), String(argv[++index] || ''));
    else if (arg === '--input') args.input = path.resolve(process.cwd(), String(argv[++index] || ''));
    else if (arg === '--format') args.format = String(argv[++index] || 'auto').trim().toLowerCase() || 'auto';
    else if (arg === '--out') args.out = path.resolve(process.cwd(), String(argv[++index] || ''));
    else if (arg === '--overwrite-existing') args.overwriteExisting = true;
  }
  if (!args.manifest) throw new Error('Missing required --manifest argument');
  if (!args.input) throw new Error('Missing required --input argument');
  if (!args.out) args.out = args.manifest;
  return args;
}

function main() {
  const args = parseArgs(process.argv.slice(2));
  const manifestPayload = JSON.parse(fs.readFileSync(args.manifest, 'utf8'));
  const analyzerRaw = fs.readFileSync(args.input, 'utf8');
  const analyzerRows = parseAnalyzerFeatureInput(analyzerRaw, args.format);
  const merged = mergeAnalyzerResultsIntoManifest(manifestPayload, analyzerRows, {
    overwriteExisting: args.overwriteExisting,
  });
  fs.writeFileSync(args.out, JSON.stringify(merged, null, 2));
  const summary = merged.mergeSummary || {};
  console.log(`Merged ${summary.matched || 0} of ${summary.analyzerRows || 0} analyzer rows into ${args.out}`);
  console.log(`Unchanged manifest tracks: ${summary.unchanged || 0}`);
  console.log(`Overwrite existing: ${summary.overwriteExisting ? 'yes' : 'no'}`);
}

main();
