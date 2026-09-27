// Public music metadata and artwork only; no account or production data needed.
import fs from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
const dir = path.join(path.dirname(fileURLToPath(import.meta.url)), '../../tmp/demo-catalog');
await fs.mkdir(dir, { recursive: true });
const choices = [
  ['Radiohead', 'In Rainbows', 'alternative'], ['Fleetwood Mac', 'Rumours', 'rock'],
  ['Daft Punk', 'Random Access Memories', 'electronic'], ['Arctic Monkeys', 'AM', 'indie rock'],
  ['Pink Floyd', 'The Dark Side of the Moon', 'progressive rock'], ['David Bowie', 'Heroes', 'art rock'],
  ['Massive Attack', 'Mezzanine', 'trip hop'], ['The Cure', 'Disintegration', 'alternative'],
  ['Gorillaz', 'Demon Days', 'alternative'], ['Oasis', '(What’s the Story) Morning Glory?', 'britpop'],
  ['Portishead', 'Dummy', 'trip hop'], ['The War on Drugs', 'Lost in the Dream', 'indie rock'],
];
const headers = { 'User-Agent': 'CuratorrDocsDemo/1.0 (https://github.com/MickyGX/curatorr)' };
async function get(url) {
  for (let attempt = 0; attempt < 4; attempt++) {
    const r = await fetch(url, { headers, signal: AbortSignal.timeout(45000) });
    if (r.ok) return r;
    if (![429, 502, 503, 504].includes(r.status) || attempt === 3) throw new Error(`${r.status}: ${url}`);
    await new Promise(resolve => setTimeout(resolve, 3000 * (attempt + 1)));
  }
}
async function mb(url) { await new Promise(r => setTimeout(r, 1100)); return (await get(url)).json(); }
const catalog = [];
for (const [artist, album, genre] of choices) {
  const file = path.join(dir, `${catalog.length}.json`);
  try { catalog.push(JSON.parse(await fs.readFile(file, 'utf8'))); continue; } catch {}
  const query = `artist:"${artist}" AND releasegroup:"${album.replace(/[?’]/g, '')}" AND primarytype:album`;
  const search = await mb(`https://musicbrainz.org/ws/2/release-group/?query=${encodeURIComponent(query)}&fmt=json&limit=3`);
  const group = search['release-groups']?.[0];
  if (!group) throw new Error(`Album not found: ${artist} / ${album}`);
  const covers = await (await get(`https://coverartarchive.org/release-group/${group.id}`)).json();
  const cover = covers.images.find(i => i.front) || covers.images[0];
  const releaseId = covers.release.split('/').pop();
  const release = await mb(`https://musicbrainz.org/ws/2/release/${releaseId}?inc=recordings&fmt=json`);
  const imageUrl = cover.thumbnails['500'] || cover.thumbnails.small;
  const image = Buffer.from(await (await get(imageUrl)).arrayBuffer());
  const index = catalog.length;
  await fs.writeFile(path.join(dir, `${index}.jpg`), image);
  const item = { artist, album: release.title, genre, year: Number((group['first-release-date'] || '2000').slice(0, 4)), image: `${index}.jpg`, source: `https://musicbrainz.org/release/${releaseId}`, artworkSource: imageUrl, tracks: release.media[0].tracks.map(t => ({ title: t.title, duration: t.length || t.recording.length || 240000 })) };
  await fs.writeFile(file, JSON.stringify(item, null, 2));
  catalog.push(item);
  console.log(`${artist} — ${item.album}: ${item.tracks.length} tracks, artwork cached`);
}
await fs.writeFile(path.join(dir, 'catalog.json'), JSON.stringify(catalog, null, 2));
console.log(`Catalog saved to ${dir}`);
