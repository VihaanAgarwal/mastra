import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { Agent } from './resources/agent';
import { AgentBuilder } from './resources/agent-builder';
import { Run } from './resources/run';
import type { ClientOptions } from './types';

const RECORD_SEPARATOR = '\x1E';

const clientOptions: ClientOptions = { baseUrl: 'http://localhost:4111', retries: 0 };

/**
 * Splits `text` into two byte chunks so that the UTF-8 bytes of `marker` straddle
 * the boundary, which is what a network stream does to any multi-byte character
 * that happens to land on a chunk edge.
 */
function splitInsideCharacter(text: string, marker: string): [Uint8Array, Uint8Array] {
  const encoder = new TextEncoder();
  const bytes = encoder.encode(text);
  const splitAt = encoder.encode(text.slice(0, text.indexOf(marker))).length + 1;
  return [bytes.slice(0, splitAt), bytes.slice(splitAt)];
}

function streamOf(chunks: Uint8Array[]): ReadableStream<Uint8Array> {
  return new ReadableStream<Uint8Array>({
    start(controller) {
      for (const chunk of chunks) {
        controller.enqueue(chunk);
      }
      controller.close();
    },
  });
}

function respondWith(text: string, marker: string): Response {
  return new Response(streamOf(splitInsideCharacter(text, marker)) as unknown as ReadableStream, { status: 200 });
}

async function readAll<T>(stream: ReadableStream<T>): Promise<T[]> {
  const reader = stream.getReader();
  const values: T[] = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    values.push(value);
  }
  return values;
}

describe('streaming decoders across chunk boundaries', () => {
  let originalFetch: typeof globalThis.fetch;

  beforeEach(() => {
    originalFetch = globalThis.fetch;
  });

  afterEach(() => {
    globalThis.fetch = originalFetch;
    vi.restoreAllMocks();
  });

  it('Run.stream keeps a multi-byte character intact', async () => {
    const record = JSON.stringify({ type: 'text-delta', payload: { text: '日本語' } }) + RECORD_SEPARATOR;
    globalThis.fetch = vi.fn(() => Promise.resolve(respondWith(record, '日'))) as any;

    const run = new Run(clientOptions, 'wf-1', 'run-1');
    const records = await readAll(await run.stream({ inputData: {} }));

    expect(records).toEqual([{ type: 'text-delta', payload: { text: '日本語' } }]);
  });

  it('AgentBuilder.observeStream keeps a multi-byte character intact', async () => {
    const record = JSON.stringify({ type: 'live', payload: { step: '日本語' } }) + RECORD_SEPARATOR;
    globalThis.fetch = vi.fn(() => Promise.resolve(respondWith(record, '日'))) as any;

    const agentBuilder = new AgentBuilder(clientOptions, 'test-action');
    const records = await readAll((await agentBuilder.observeStream({ runId: 'run-1' })) as ReadableStream<any>);

    expect(records).toEqual([{ type: 'live', payload: { step: '日本語' } }]);
  });

  it('Agent.processStreamResponse forwards a multi-byte character intact', async () => {
    const event = `data: ${JSON.stringify({ type: 'text-delta', payload: { text: '日本語' } })}\n\n`;
    globalThis.fetch = vi.fn(() => Promise.resolve(respondWith(event, '日'))) as any;

    const agent = new Agent(clientOptions, 'test-agent');
    let controller!: ReadableStreamDefaultController<Uint8Array>;
    const stream = new ReadableStream<Uint8Array>({
      start(c) {
        controller = c;
      },
    });
    const forwardedChunks = readAll(stream);

    await agent.processStreamResponse({ messages: [] }, controller);

    const decoder = new TextDecoder();
    let forwarded = '';
    for (const chunk of await forwardedChunks) {
      forwarded += decoder.decode(chunk, { stream: true });
    }
    forwarded += decoder.decode();

    expect(forwarded).toContain('日本語');
  });
});
