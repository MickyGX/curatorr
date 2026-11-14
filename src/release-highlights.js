// Pulls the welcome-modal highlights out of a docs/release/releases/vX.Y.Z.md file.

export function stripMarkdownInline(value) {
  let text = String(value || '').trim();
  if (!text) return '';
  text = text.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1');
  text = text.replace(/`([^`]+)`/g, '$1');
  text = text.replace(/\*\*([^*]+)\*\*/g, '$1');
  text = text.replace(/\*([^*]+)\*/g, '$1');
  text = text.replace(/~~([^~]+)~~/g, '$1');
  return text.replace(/\s+/g, ' ').trim();
}

// One highlight per top-level bullet. Indented sub-bullets and wrapped or
// continuation lines belong to the bullet above them: sub-items are joined
// into the parent sentence ("a: x; y; z.") and continuation text is appended.
export function parseReleaseHighlights(raw, { sections = [], maxItems = 6 } = {}) {
  const sectionSet = new Set(sections.map((section) => String(section).toLowerCase()));
  const lines = String(raw || '').split(/\r?\n/);
  const highlights = [];
  let activeSection = '';
  let current = null;

  const flush = () => {
    if (!current) return;
    let text = stripMarkdownInline(current.lead);
    if (current.subItems.length) {
      const items = current.subItems.map(stripMarkdownInline).filter(Boolean).map((s) => s.replace(/[.;]+$/, ''));
      if (items.length) {
        const joined = items.join('; ');
        text = /:$/.test(text) ? `${text} ${joined}.` : `${text.replace(/[.:]?$/, ':')} ${joined}.`;
      }
    }
    const extra = stripMarkdownInline(current.extra.join(' '));
    if (extra) text = `${text} ${extra}`;
    text = text.trim();
    if (text && highlights.length < maxItems) highlights.push(`${current.section}: ${text}`);
    current = null;
  };

  for (const rawLine of lines) {
    const line = String(rawLine || '');
    const heading = line.match(/^(#{1,6})\s+(.+?)\s*$/);
    if (heading) {
      flush();
      const title = heading[2].trim();
      activeSection = heading[1].length >= 2 && heading[1].length <= 3 && sectionSet.has(title.toLowerCase()) ? title : '';
      continue;
    }
    if (!activeSection) continue;

    const topBullet = line.match(/^[-*]\s+(.+?)\s*$/);
    if (topBullet) {
      flush();
      if (highlights.length >= maxItems) break;
      current = { section: activeSection, lead: topBullet[1], subItems: [], extra: [] };
      continue;
    }
    if (!current) continue;
    if (!line.trim()) continue;

    const subBullet = line.match(/^\s+[-*]\s+(.+?)\s*$/);
    if (subBullet) {
      current.subItems.push(subBullet[1]);
      continue;
    }
    if (/^\s+\S/.test(line)) {
      // Indented text: a wrapped lead line if no sub-list yet, otherwise a continuation paragraph.
      if (current.subItems.length || current.extra.length) current.extra.push(line.trim());
      else current.lead = `${current.lead} ${line.trim()}`;
      continue;
    }
    // Unindented non-bullet text ends the list item.
    flush();
  }
  flush();
  return highlights;
}
