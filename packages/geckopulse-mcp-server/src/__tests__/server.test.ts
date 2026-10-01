import type { AddressInfo } from 'node:net';

import { Client } from '@modelcontextprotocol/sdk/client/index.js';
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js';
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { type WebSocket, WebSocketServer } from 'ws';

import { createServer } from '../server';
import { SocketManager } from '../socket-manager';

const call = async (client: Client, name: string, args: Record<string, unknown> = {}) => {
  const result = await client.callTool({ name, arguments: args });
  const text = (result.content as { type: string; text: string }[])[0].text;
  return { isError: Boolean(result.isError), text, data: result.isError ? undefined : JSON.parse(text) };
};

// Polls with a real timer between attempts so socket events can be processed
const waitFor = async <T>(fn: () => Promise<T>, done: (value: T) => boolean) => {
  for (let i = 0; i < 50; i++) {
    const value = await fn();
    if (done(value)) {
      return value;
    }
    await new Promise(resolve => setTimeout(resolve, 50));
  }
  throw new Error('waitFor timed out');
};

describe('geckopulse mcp server', () => {
  let wss: WebSocketServer;
  let url: string;
  let serverSockets: WebSocket[];
  let client: Client;
  let close: () => Promise<void>;

  beforeEach(async () => {
    serverSockets = [];
    wss = new WebSocketServer({ port: 0 });
    wss.on('connection', (socket, req) => {
      serverSockets.push(socket);
      socket.send(JSON.stringify({ hello: req.headers['x-token'] ?? null }));
      // Echo back everything the client sends
      socket.on('message', data => socket.send(`echo:${data.toString()}`));
    });
    await new Promise(resolve => wss.once('listening', resolve));
    url = `ws://127.0.0.1:${(wss.address() as AddressInfo).port}`;

    const { server, sockets } = createServer(new SocketManager({ maxMessagesPerConnection: 5 }));
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair();
    await server.connect(serverTransport);
    client = new Client({ name: 'test', version: '1.0.0' });
    await client.connect(clientTransport);
    close = async () => {
      await sockets.closeAll();
      await client.close();
    };
  });

  afterEach(async () => {
    await close();
    await new Promise(resolve => wss.close(resolve));
  });

  it('exposes the socket tools', async () => {
    const { tools } = await client.listTools();
    expect(tools.map(t => t.name).sort()).toEqual([
      'socket_close',
      'socket_connect',
      'socket_list',
      'socket_read_messages',
      'socket_send',
    ]);
  });

  it('connects with headers and reads incoming messages', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url, headers: { 'X-Token': 'abc' } });
    expect(conn.status).toBe('open');

    const { data: read } = await call(client, 'socket_read_messages', {
      connectionId: conn.connectionId,
      waitMs: 2000,
    });
    expect(read.messages).toHaveLength(1);
    expect(read.messages[0]).toMatchObject({ seq: 1, direction: 'incoming', data: '{"hello":"abc"}' });
    expect(read.nextAfterSeq).toBe(1);
  });

  it('sends messages and only returns new ones after the cursor', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url });
    const { connectionId } = conn;
    const first = await call(client, 'socket_read_messages', { connectionId, waitMs: 2000 });

    await call(client, 'socket_send', { connectionId, message: 'ping' });
    // Wait for the echo to arrive
    let messages: any[] = [];
    let afterSeq = first.data.nextAfterSeq;
    while (!messages.some(m => m.direction === 'incoming')) {
      const { data } = await call(client, 'socket_read_messages', { connectionId, afterSeq, waitMs: 2000 });
      messages = messages.concat(data.messages);
      afterSeq = data.nextAfterSeq;
    }
    expect(messages.map(m => [m.direction, m.data])).toEqual([
      ['outgoing', 'ping'],
      ['incoming', 'echo:ping'],
    ]);
  });

  it('waits for a message pushed later by the server', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url });
    const { connectionId } = conn;
    await call(client, 'socket_read_messages', { connectionId, waitMs: 2000 });

    setTimeout(() => serverSockets[0].send('later'), 100);
    const { data } = await call(client, 'socket_read_messages', { connectionId, afterSeq: 1, waitMs: 5000 });
    expect(data.messages.map((m: any) => m.data)).toEqual(['later']);
  });

  it('keeps only the newest messages and reports missed ones', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url });
    const { connectionId } = conn;
    for (let i = 0; i < 10; i++) {
      serverSockets[0].send(`m${i}`);
    }
    const read = await waitFor(
      async () => (await call(client, 'socket_read_messages', { connectionId })).data,
      data => data.connection.lastSeq === 11,
    );

    expect(read.messages.map((m: any) => m.data)).toEqual(['m5', 'm6', 'm7', 'm8', 'm9']);
    expect(read.missedMessages).toBe(true);
    expect(read.connection.droppedMessages).toBe(6);
  });

  it('reports server side close and keeps messages readable', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url });
    const { connectionId } = conn;
    serverSockets[0].close(4000, 'bye');

    const info = await waitFor(
      async () => (await call(client, 'socket_read_messages', { connectionId })).data,
      data => data.connection.status === 'closed',
    );
    expect(info.connection).toMatchObject({ closeCode: 4000, closeReason: 'bye' });
    expect(info.messages).toHaveLength(1);

    const send = await call(client, 'socket_send', { connectionId, message: 'x' });
    expect(send.isError).toBe(true);
  });

  it('lists and closes connections', async () => {
    const { data: conn } = await call(client, 'socket_connect', { url });
    expect((await call(client, 'socket_list')).data).toHaveLength(1);

    const { data: closed } = await call(client, 'socket_close', { connectionId: conn.connectionId, forget: true });
    expect(closed.status).toBe('closed');
    expect((await call(client, 'socket_list')).data).toHaveLength(0);
  });

  it('returns tool errors for bad input', async () => {
    expect((await call(client, 'socket_connect', { url: 'http://example.com' })).text).toMatch(/ws:\/\/ and wss:\/\//);
    expect((await call(client, 'socket_read_messages', { connectionId: 'nope' })).text).toMatch(/Unknown connectionId/);

    const refused = await call(client, 'socket_connect', { url: 'ws://127.0.0.1:1', timeoutMs: 2000 });
    expect(refused.isError).toBe(true);
    expect((await call(client, 'socket_list')).data).toHaveLength(0);
  });
});
