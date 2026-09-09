import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import App from './App';

test('roadway time is the default view and does not wait for overview or OTP requests', () => {
  const html = renderToStaticMarkup(<App />);
  assert.match(html, /href="#row" aria-current="page"/);
  assert.match(html, /Loading roadway observations/);
  assert.doesNotMatch(html, /Loading the transit overview|Loading on-time performance/);
  assert.match(html, /aria-label="Dashboard views"/);
  assert.match(html, /href="#overview"/);
  assert.match(html, /href="#otp"/);
});

test('overview loading leaves navigation available without mounting the streetcar or OTP loaders', () => {
  const html = renderToStaticMarkup(<App initialView="overview" />);
  assert.match(html, /href="#overview" aria-current="page"/);
  assert.match(html, /Loading the transit overview/);
  assert.doesNotMatch(html, /Loading roadway observations|Loading on-time performance/);
  assert.match(html, /href="#row"/);
});

test('OTP loads independently and keeps the other dashboard views accessible', () => {
  const html = renderToStaticMarkup(<App initialView="otp" />);
  assert.match(html, /href="#otp" aria-current="page"/);
  assert.match(html, /Loading on-time performance/);
  assert.doesNotMatch(html, /Loading the transit overview|Loading roadway observations/);
  assert.match(html, /href="#row"/);
});

 test('signal study mounts independently with historical scenarios collapsed', () => {
  const html = renderToStaticMarkup(<App initialView="signals" />);
  assert.match(html, /href="#signals" aria-current="page"/);
  assert.match(html, /Loading signal observations/);
  assert.doesNotMatch(html, /Loading roadway observations|Loading signal-priority estimates|Loading on-time performance/);
});
