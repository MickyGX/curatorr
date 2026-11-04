import { execFile } from 'node:child_process';
import { chmod, mkdtemp, mkdir, readFile, rm, writeFile } from 'node:fs/promises';
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
  // Just enough numpy for the ffmpeg fallback to turn raw f32le bytes into mono samples.
  await writeFile(path.join(moduleDir, 'numpy.py'), `
import struct

float32 = 'float32'

class FakeArray:
    def __init__(self, values):
        self.values = list(values)
        self.size = len(self.values)

    def __getitem__(self, item):
        return FakeArray(self.values[item])

    def reshape(self, rows, columns):
        return FakeMatrix([self.values[i:i + columns] for i in range(0, self.size, columns)])

class FakeMatrix:
    def __init__(self, rows):
        self.rows = rows

    def mean(self, axis):
        return FakeArray(sum(row) / len(row) for row in self.rows)

def frombuffer(buffer, dtype):
    return FakeArray(struct.unpack('<%df' % (len(buffer) // 4), buffer))
`, 'utf8');
  await writeFile(path.join(librosaDir, '__init__.py'), `
import json
import os

def load(file_path, **kwargs):
    with open(os.environ['LIBROSA_LOAD_CAPTURE'], 'w', encoding='utf-8') as handle:
        json.dump({'filePath': file_path, 'kwargs': kwargs}, handle)
    raise RuntimeError('captured librosa.load options')

class beat:
    @staticmethod
    def beat_track(y, sr):
        with open(os.environ['LIBROSA_BEAT_CAPTURE'], 'w', encoding='utf-8') as handle:
            json.dump({'y': y.values, 'sr': sr}, handle)
        raise RuntimeError('captured decoded audio')
`, 'utf8');
  return moduleDir;
}

// A fake ffmpeg that records its arguments, then either fails or emits two stereo
// float32 frames: (0.5, -0.5) and (1.0, 0.0), which average to mono [0.0, 0.5].
async function writeFakeFfmpeg(root, mode) {
  const binDir = path.join(root, 'bin');
  await mkdir(binDir, { recursive: true });
  const ffmpegPath = path.join(binDir, 'ffmpeg');
  await writeFile(ffmpegPath, `#!/usr/bin/env python3
import json
import os
import struct
import sys

with open(os.environ['FFMPEG_CAPTURE'], 'w', encoding='utf-8') as handle:
    json.dump(sys.argv[1:], handle)
if ${mode === 'succeed' ? 'False' : 'True'}:
    sys.exit(1)
sys.stdout.buffer.write(struct.pack('<4f', 0.5, -0.5, 1.0, 0.0))
`, 'utf8');
  await chmod(ffmpegPath, 0o755);
  return binDir;
}

async function readJsonIfPresent(filePath) {
  try {
    return JSON.parse(await readFile(filePath, 'utf8'));
  } catch (error) {
    if (error.code === 'ENOENT') return null;
    throw error;
  }
}

async function runAnalyzerWithFakeLibrosa(args = [], { ffmpeg = 'fail', fileName = 'long-mix.flac' } = {}) {
  const root = await mkdtemp(path.join(tmpdir(), 'curatorr-analyzer-'));
  try {
    const moduleDir = await writeFakePythonModules(root);
    const binDir = await writeFakeFfmpeg(root, ffmpeg);
    const audioPath = path.join(root, fileName);
    const inputPath = path.join(root, 'manifest.json');
    const outputPath = path.join(root, 'results.json');
    const capturePath = path.join(root, 'load-options.json');
    const ffmpegCapturePath = path.join(root, 'ffmpeg-args.json');
    const beatCapturePath = path.join(root, 'beat-input.json');
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
        LIBROSA_BEAT_CAPTURE: beatCapturePath,
        FFMPEG_CAPTURE: ffmpegCapturePath,
        PATH: `${binDir}${path.delimiter}${process.env.PATH || ''}`,
        PYTHONPATH: moduleDir,
      },
    });

    const capture = JSON.parse(await readFile(capturePath, 'utf8'));
    const output = JSON.parse(await readFile(outputPath, 'utf8'));
    const ffmpegArgs = await readJsonIfPresent(ffmpegCapturePath);
    const beatInput = await readJsonIfPresent(beatCapturePath);
    return { audioPath, capture, output, ffmpegArgs, beatInput };
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

test('built-in analyzer falls back to ffmpeg when librosa cannot open a track', async () => {
  const { audioPath, ffmpegArgs, output } = await runAnalyzerWithFakeLibrosa([], { fileName: 'track.m4a' });
  assert.deepEqual(ffmpegArgs, [
    '-nostdin', '-v', 'error', '-i', audioPath,
    '-t', '180.0',
    '-vn', '-ac', '2', '-ar', '22050', '-f', 'f32le', '-',
  ]);
  assert.deepEqual(output, { tracks: [] });
});

test('built-in analyzer omits the ffmpeg duration cap when it is disabled', async () => {
  const { ffmpegArgs } = await runAnalyzerWithFakeLibrosa(['--max-duration-seconds', '0'], { fileName: 'track.m4a' });
  assert.equal(ffmpegArgs.includes('-t'), false);
});

test('built-in analyzer analyzes the ffmpeg-decoded audio as a mono channel mean', async () => {
  const { beatInput } = await runAnalyzerWithFakeLibrosa([], { ffmpeg: 'succeed', fileName: 'track.m4a' });
  assert.deepEqual(beatInput, { y: [0.0, 0.5], sr: 22050 });
});
