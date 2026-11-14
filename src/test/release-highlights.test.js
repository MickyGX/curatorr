import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { parseReleaseHighlights } from '../release-highlights.js';

const SECTIONS = ['Added', 'Changed', 'Fixed', 'Summary', 'Upgrade Notes'];
const releasesDir = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'docs', 'release', 'releases');

describe('parseReleaseHighlights', () => {
  it('returns one highlight per top-level bullet in a highlight section', () => {
    const md = '# v1\n\n## Fixed\n\n- **Thing broke.** Now fixed.\n- Other `fix`.\n\n## Community\n\n- Discord\n';
    assert.deepEqual(parseReleaseHighlights(md, { sections: SECTIONS }), [
      'Fixed: Thing broke. Now fixed.',
      'Fixed: Other fix.',
    ]);
  });

  it('folds nested sub-bullets and continuation text into the parent bullet', () => {
    const md = [
      '## Fixed',
      '',
      '- **Page slow.** Every visit rebuilt these lists:',
      '  - genres and moods',
      '  - the folder browser',
      '',
      '  They are now cached.',
      '- **Second fix.** Done.',
    ].join('\n');
    assert.deepEqual(parseReleaseHighlights(md, { sections: SECTIONS }), [
      'Fixed: Page slow. Every visit rebuilt these lists: genres and moods; the folder browser. They are now cached.',
      'Fixed: Second fix. Done.',
    ]);
  });

  it('respects maxItems', () => {
    const md = '## Added\n\n- a\n- b\n- c\n';
    assert.deepEqual(parseReleaseHighlights(md, { sections: SECTIONS, maxItems: 2 }), ['Added: a', 'Added: b']);
  });

  it('keeps the v0.1.113 nested list inside its parent highlight', () => {
    const md = readFileSync(join(releasesDir, 'v0.1.113.md'), 'utf8');
    const highlights = parseReleaseHighlights(md, { sections: SECTIONS });
    assert.equal(highlights.filter((h) => h.startsWith('Fixed:')).length, 3);
    assert.ok(!highlights.some((h) => /^Fixed: (genres|the folder browser)/.test(h)));
    assert.match(highlights[0], /^Fixed: Playlists page slow on large libraries\..*whole library: genres, moods and artist countries; .*analysed tracks\. Each list meant/);
    assert.match(highlights[1], /^Fixed: Slow Plex held up playlist artwork\./);
  });
});
