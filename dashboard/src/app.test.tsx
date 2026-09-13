import { test } from 'node:test';
import assert from 'node:assert/strict';
import { renderToStaticMarkup } from 'react-dom/server';
import App from './App';

test('the story is the default view and loads the two questions independently', () => {
  const html = renderToStaticMarkup(<App />);
  assert.match(html, /href="#overview" aria-current="page"/);
  assert.match(html, /<h1[^>]*>NOLA transit performance<\/h1>/);
  assert.match(html, /Where does transit/);
  assert.match(html, /Loading roadway observations/);
  assert.match(html, /Loading streetcar signal observations/);
  assert.match(html, /Loading bus signal observations/);
  assert.doesNotMatch(html, /Loading on-time performance|Historical reported|Dedicated ROW Speed/);
  assert.match(html, /aria-label="Dashboard views"/);
  assert.match(html, /href="#row"/);
  assert.match(html, /href="#otp"/);
});

test('the overview hash opens the story without a legacy overview request', () => {
  const html = renderToStaticMarkup(<App initialView="overview" />);
  assert.match(html, /href="#overview" aria-current="page"/);
  assert.match(html, /How much does sharing the road cost streetcars/);
  assert.doesNotMatch(html, /Loading the transit overview|Loading on-time performance/);
  assert.match(html, /href="#signals"/);
});

test('OTP loads independently and keeps the study views accessible', () => {
  const html = renderToStaticMarkup(<App initialView="otp" />);
  assert.match(html, /href="#otp" aria-current="page"/);
  assert.match(html, /Loading on-time performance/);
  assert.doesNotMatch(html, /Loading the transit overview|Loading roadway observations|Loading streetcar signal/);
  assert.match(html, /href="#row"/);
});

test('the roadway detail mounts independently of the story and OTP', () => {
  const html = renderToStaticMarkup(<App initialView="row" />);
  assert.match(html, /href="#row" aria-current="page"/);
  assert.match(html, /Loading roadway observations/);
  assert.doesNotMatch(html, /Loading streetcar signal|Loading bus signal|Loading on-time performance/);
});

test('signal study mounts independently with historical scenarios collapsed', () => {
  const html = renderToStaticMarkup(<App initialView="signals" />);
  assert.match(html, /href="#signals" aria-current="page"/);
  assert.match(html, /Loading signal observations/);
  assert.doesNotMatch(html, /Loading roadway observations|Loading signal-priority estimates|Loading on-time performance/);
});
