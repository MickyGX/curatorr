import { execFile } from 'node:child_process';
import { mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import assert from 'node:assert/strict';
import test from 'node:test';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const repoRoot = path.resolve(__dirname, '../..');
const analyzerScript = path.join(repoRoot, 'scripts/analyze-track-features.py');

function execFileAsync(file, args, options = {}) {
  return new Promise((resolve, reject) => {
    execFile(file, args, options, (error, stdout, stderr) => {
      if (error) {
        error.stdout = stdout;
        error.stderr = stderr;
        reject(error);
        return;
      }
      resolve({ stdout, stderr });
    });
  });
}

async function writeFakePythonModules(root) {
  const moduleDir = path.join(root, 'python-modules');
  const librosaDir = path.join(moduleDir, 'librosa');
  await mkdir(librosaDir, { recursive: true });
  await writeFile(path.join(moduleDir, 'numpy.py'), '\n', 'utf8');
  await writeFile(path.join(librosaDir, '__init__.py'), `
import json
import os

class FakeAudio:
    size = 1

def load(file_path, **kwargs):
    with open(os.environ['LIBROSA_LOAD_CAPTURE'], 'w', encoding='utf-8') as handle:
        json.dump({'filePath': file_path, 'kwargs': kwargs}, handle)
    raise RuntimeError('captured librosa.load options')
`, 'utf8');
  return moduleDir;
}

async function runAnalyzerWithFakeLibrosa(args = []) {
  const root = await mkdtemp(path.join(tmpdir(), 'curatorr-analyzer-'));
  try {
    const moduleDir = await writeFakePythonModules(root);
    const audioPath = path.join(root, 'long-mix.flac');
    const inputPath = path.join(root, 'manifest.json');
    const outputPath = path.join(root, 'results.json');
    const capturePath = path.join(root, 'load-options.json');
    await writeFile(audioPath, 'fake audio', 'utf8');
    await writeFile(inputPath, JSON.stringify({
      tracks: [{ ratingKey: 'long-mix', filePath: audioPath }],
    }), 'utf8');

    await execFileAsync('python3', [
      analyzerScript,
      '--input',
      inputPath,
      '--output',
      outputPath,
      ...args,
    ], {
      env: {
        ...process.env,
        LIBROSA_LOAD_CAPTURE: capturePath,
        PYTHONPATH: moduleDir,
      },
    });

    const capture = JSON.parse(await readFile(capturePath, 'utf8'));
    const output = JSON.parse(await readFile(outputPath, 'utf8'));
    return { capture, output };
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

test('built-in analyzer caps decoded audio to 180 seconds by default', async () => {
  const { capture, output } = await runAnalyzerWithFakeLibrosa();
  assert.equal(capture.kwargs.sr, 22050);
  assert.equal(capture.kwargs.mono, true);
  assert.equal(capture.kwargs.duration, 180);
  assert.deepEqual(output, { tracks: [] });
});

test('built-in analyzer accepts a custom decoded audio duration cap', async () => {
  const { capture } = await runAnalyzerWithFakeLibrosa(['--max-duration-seconds', '45']);
  assert.equal(capture.kwargs.duration, 45);
});

test('built-in analyzer allows the decoded audio duration cap to be disabled', async () => {
  const { capture } = await runAnalyzerWithFakeLibrosa(['--max-duration-seconds', '0']);
  assert.equal(Object.hasOwn(capture.kwargs, 'duration'), false);
});
