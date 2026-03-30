#!/usr/bin/env node

import fs from 'node:fs';

const requiredEnvNames = [
  'REDDIT_CLIENT_ID',
  'REDDIT_CLIENT_SECRET',
  'REDDIT_USERNAME',
  'REDDIT_PASSWORD',
  'REDDIT_SUBREDDIT',
  'ANNOUNCEMENT_TITLE',
  'ANNOUNCEMENT_BODY_FILE',
];

const missingEnv = requiredEnvNames.filter((name) => !String(process.env[name] || '').trim());
if (missingEnv.length) {
  console.log(`Skipping Reddit release announcement; missing: ${missingEnv.join(', ')}`);
  process.exit(0);
}

const redditClientId = process.env.REDDIT_CLIENT_ID.trim();
const redditClientSecret = process.env.REDDIT_CLIENT_SECRET.trim();
const redditUsername = process.env.REDDIT_USERNAME.trim();
const redditPassword = process.env.REDDIT_PASSWORD.trim();
const subreddit = process.env.REDDIT_SUBREDDIT.trim();
const title = process.env.ANNOUNCEMENT_TITLE.trim();
const bodyFile = process.env.ANNOUNCEMENT_BODY_FILE.trim();
const userAgent = String(process.env.REDDIT_USER_AGENT || 'curatorr-release-bot/1.0').trim();
const body = fs.readFileSync(bodyFile, 'utf8').trim();

if (!body) {
  console.log('Skipping Reddit release announcement; body file was empty.');
  process.exit(0);
}

const tokenParams = new URLSearchParams({
  grant_type: 'password',
  username: redditUsername,
  password: redditPassword,
});

const tokenResponse = await fetch('https://www.reddit.com/api/v1/access_token', {
  method: 'POST',
  headers: {
    Authorization: `Basic ${Buffer.from(`${redditClientId}:${redditClientSecret}`).toString('base64')}`,
    'User-Agent': userAgent,
    'Content-Type': 'application/x-www-form-urlencoded',
  },
  body: tokenParams.toString(),
});

const tokenPayload = await tokenResponse.json().catch(() => ({}));
if (!tokenResponse.ok || !tokenPayload.access_token) {
  console.error('Reddit token request failed:', tokenPayload);
  process.exit(1);
}

const submitParams = new URLSearchParams({
  api_type: 'json',
  kind: 'self',
  sr: subreddit,
  title,
  text: body,
  resubmit: 'true',
  sendreplies: 'false',
});

const submitResponse = await fetch('https://oauth.reddit.com/api/submit', {
  method: 'POST',
  headers: {
    Authorization: `Bearer ${tokenPayload.access_token}`,
    'User-Agent': userAgent,
    'Content-Type': 'application/x-www-form-urlencoded',
  },
  body: submitParams.toString(),
});

const submitPayload = await submitResponse.json().catch(() => ({}));
const errors = submitPayload?.json?.errors || [];
if (!submitResponse.ok || errors.length) {
  console.error('Reddit submission failed:', submitPayload);
  process.exit(1);
}

const permalink = submitPayload?.json?.data?.url || submitPayload?.json?.data?.name || 'submitted';
console.log(`Reddit release announcement posted to r/${subreddit}: ${permalink}`);
