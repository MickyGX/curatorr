#!/usr/bin/env python3

import argparse
import json
import math
import os
import sys


KEY_NAMES = ['C', 'C#', 'D', 'D#', 'E', 'F', 'F#', 'G', 'G#', 'A', 'A#', 'B']
MAJOR_PROFILE = [6.35, 2.23, 3.48, 2.33, 4.38, 4.09, 2.52, 5.19, 2.39, 3.66, 2.29, 2.88]
MINOR_PROFILE = [6.33, 2.68, 3.52, 5.38, 2.6, 3.53, 2.54, 4.75, 3.98, 2.69, 3.34, 3.17]
CAMELOT_BY_KEY = {
    'C major': '8B',
    'G major': '9B',
    'D major': '10B',
    'A major': '11B',
    'E major': '12B',
    'B major': '1B',
    'F# major': '2B',
    'C# major': '3B',
    'G# major': '4B',
    'D# major': '5B',
    'A# major': '6B',
    'F major': '7B',
    'A minor': '8A',
    'E minor': '9A',
    'B minor': '10A',
    'F# minor': '11A',
    'C# minor': '12A',
    'G# minor': '1A',
    'D# minor': '2A',
    'A# minor': '3A',
    'F minor': '4A',
    'C minor': '5A',
    'G minor': '6A',
    'D minor': '7A',
}


def load_manifest(path):
    with open(path, 'r', encoding='utf-8') as handle:
        payload = json.load(handle)
    if isinstance(payload, dict) and isinstance(payload.get('tracks'), list):
        return payload
    if isinstance(payload, list):
        return {'tracks': payload}
    raise ValueError('Manifest must be a JSON array or an object with a tracks array.')


def ensure_dependencies():
    try:
        import librosa  # noqa: F401
        import numpy as np  # noqa: F401
        return
    except Exception as exc:
        raise RuntimeError(
            'Curatorr built-in analysis requires Python packages "librosa" and "numpy". '
            'Install them in the analysis environment or switch Track Analysis to custom mode.'
        ) from exc


def camelot_key(musical_key):
    return CAMELOT_BY_KEY.get(musical_key, '')


def detect_key(chroma):
    import numpy as np

    chroma_sum = np.sum(chroma, axis=1)
    if not np.any(chroma_sum):
        return ''

    best_name = ''
    best_score = -1e9
    for idx, tonic in enumerate(KEY_NAMES):
        major_profile = np.roll(MAJOR_PROFILE, idx)
        minor_profile = np.roll(MINOR_PROFILE, idx)
        major_score = float(np.dot(chroma_sum, major_profile))
        minor_score = float(np.dot(chroma_sum, minor_profile))
        if major_score > best_score:
            best_score = major_score
            best_name = f'{tonic} major'
        if minor_score > best_score:
            best_score = minor_score
            best_name = f'{tonic} minor'
    return best_name


def normalize_energy(rms):
    if rms <= 0:
        return 0.0
    return max(0.0, min(1.0, math.log10(1 + (rms * 100)) / 2.0))


def normalize_danceability(tempo, onset_regularity):
    tempo_score = max(0.0, 1.0 - min(abs(float(tempo) - 122.0), 80.0) / 80.0)
    value = (tempo_score * 0.6) + (max(0.0, min(1.0, onset_regularity)) * 0.4)
    return max(0.0, min(1.0, value))


def coerce_float(value):
    try:
        return float(value)
    except (TypeError, ValueError):
        pass
    try:
        import numpy as np

        if isinstance(value, np.ndarray):
            flattened = value.reshape(-1)
            if flattened.size:
                return float(flattened[0])
            return 0.0
    except Exception:
        pass
    try:
        if isinstance(value, (list, tuple)) and value:
            return float(value[0])
    except (TypeError, ValueError):
        pass
    return 0.0


def analyze_track(track):
    import librosa
    import numpy as np

    file_path = str(track.get('filePath') or '').strip()
    if not file_path or not os.path.isfile(file_path):
        return None
    try:
        y, sr = librosa.load(file_path, sr=22050, mono=True)
        if y.size == 0:
            return None

        tempo, _ = librosa.beat.beat_track(y=y, sr=sr)
        tempo_value = coerce_float(tempo)
        chroma = librosa.feature.chroma_cqt(y=y, sr=sr)
        musical_key = detect_key(chroma)
        camelot = camelot_key(musical_key)
        rms = float(np.mean(librosa.feature.rms(y=y)))
        onset_env = librosa.onset.onset_strength(y=y, sr=sr)
        onset_std = float(np.std(onset_env))
        onset_mean = float(np.mean(onset_env)) or 1.0
        onset_regularity = 1.0 - min(onset_std / onset_mean, 1.0)

        result = {
            'ratingKey': track.get('ratingKey', ''),
            'recordingMbid': track.get('recordingMbid', ''),
            'filePath': file_path,
            'bpm': round(tempo_value, 2) if tempo_value > 0 else None,
            'musicalKey': musical_key,
            'camelotKey': camelot,
            'energy': round(normalize_energy(rms), 4),
            'danceability': round(normalize_danceability(tempo_value, onset_regularity), 4),
            'analysisSource': 'curatorr-builtin',
            'analysisConfidence': 0.65,
        }
        if result['bpm'] is None and not result['musicalKey'] and not result['camelotKey']:
            return None
        return result
    except Exception as exc:
        exc_type = exc.__class__.__name__
        exc_message = str(exc).strip()
        detail = f'{exc_type}: {exc_message}' if exc_message else exc_type
        print(f'[curatorr-analyzer] skipping unreadable track: {file_path} ({detail})', file=sys.stderr, flush=True)
        return None


def main():
    parser = argparse.ArgumentParser(description='Curatorr built-in track feature analyzer.')
    parser.add_argument('--input', required=True, help='Path to the Curatorr feature template JSON file.')
    parser.add_argument('--output', required=True, help='Where to write the analyzer output JSON file.')
    args = parser.parse_args()

    ensure_dependencies()
    manifest = load_manifest(args.input)
    tracks = manifest.get('tracks') or []
    results = []
    for track in tracks:
        analyzed = analyze_track(track)
        if analyzed:
            results.append(analyzed)

    with open(args.output, 'w', encoding='utf-8') as handle:
        json.dump({'tracks': results}, handle, indent=2)
        handle.write('\n')
    return 0


if __name__ == '__main__':
    sys.exit(main())
