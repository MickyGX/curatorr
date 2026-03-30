#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';

const [, , tagNameArg = '', repoArg = '', outputPathArg = ''] = process.argv;

if (!tagNameArg || !repoArg || !outputPathArg) {
  console.error('Usage: render-release-announcement.mjs <tag> <repo> <output-path>');
  process.exit(1);
}

const rootDir = process.cwd();
const tagName = String(tagNameArg).trim();
const version = tagName.replace(/^v/i, '');
const repo = String(repoArg).trim();
const outputPath = path.resolve(outputPathArg);
const releaseUrl = `https://github.com/${repo}/releases/tag/${tagName}`;
const appImage = 'mickygx/curatorr';
const analyzerImage = 'mickygx/curatorr-analyzer';

const announcementPath = path.join(rootDir, 'docs', 'release', 'releases', `${tagName}-announcement.md`);
const releaseNotesPath = path.join(rootDir, 'docs', 'release', 'releases', `${tagName}.md`);

function fileExists(filePath) {
  try {
    return fs.statSync(filePath).isFile();
  } catch (_) {
    return false;
  }
}

function readIfExists(filePath) {
  if (!fileExists(filePath)) return '';
  return fs.readFileSync(filePath, 'utf8');
}

function replaceTemplateVars(text) {
  return String(text || '')
    .replaceAll('{{VERSION}}', version)
    .replaceAll('{{TAG}}', tagName)
    .replaceAll('{{RELEASE_URL}}', releaseUrl)
    .replaceAll('{{APP_IMAGE}}', appImage)
    .replaceAll('{{ANALYZER_IMAGE}}', analyzerImage);
}

function extractTitle(markdown) {
  const match = String(markdown || '').match(/^#\s+(.+)$/m);
  return match ? match[1].trim() : `Curatorr ${version} released`;
}

function stripLeadingHeading(markdown) {
  return String(markdown || '')
    .replace(/^#\s+.+\n+/, '')
    .trim();
}

function ensureLinksSection(markdown) {
  const text = String(markdown || '').trim();
  if (!text) return text;
  if (/^##\s+Links$/m.test(text)) return text;
  return `${text}\n\n## Links\n\n- GitHub release: ${releaseUrl}\n- Docker Hub app image: \`${appImage}\`\n- Docker Hub analyzer image: \`${analyzerImage}\``;
}

function buildFallbackBody() {
  const releaseNotes = readIfExists(releaseNotesPath);
  const bulletLines = releaseNotes
    .split('\n')
    .filter((line) => /^\s*-\s+/.test(line))
    .slice(0, 6);
  const highlights = bulletLines.length
    ? bulletLines.join('\n')
    : '- Release notes are available on GitHub.\n- Docker images are published for both the main app and analyzer sidecar.';
  return [
    `# Curatorr ${version} Release Announcement`,
    '',
    `Curatorr \`${version}\` is out.`,
    '',
    '## Highlights',
    '',
    highlights,
    '',
    '## Links',
    '',
    `- GitHub release: ${releaseUrl}`,
    `- Docker Hub app image: \`${appImage}\``,
    `- Docker Hub analyzer image: \`${analyzerImage}\``,
  ].join('\n');
}

const sourceMarkdown = readIfExists(announcementPath);
const renderedMarkdown = replaceTemplateVars(sourceMarkdown || buildFallbackBody());
const title = extractTitle(renderedMarkdown);
const body = ensureLinksSection(stripLeadingHeading(renderedMarkdown));

fs.mkdirSync(path.dirname(outputPath), { recursive: true });
fs.writeFileSync(outputPath, `${body.trim()}\n`);
process.stdout.write(title);
