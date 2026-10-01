import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js';
import { z } from 'zod';

import { MAX_WAIT_MS, SocketManager } from './socket-manager';

const json = (value: unknown): CallToolResult => ({
  content: [{ type: 'text', text: JSON.stringify(value, null, 2) }],
});

const toolError = (error: unknown): CallToolResult => ({
  content: [{ type: 'text', text: error instanceof Error ? error.message : String(error) }],
  isError: true,
});

// Wraps a tool handler so thrown errors are returned to the AI as tool errors instead of protocol errors
const safe =
  <Args>(handler: (args: Args) => Promise<CallToolResult>) =>
  async (args: Args) => {
    try {
      return await handler(args);
    } catch (error) {
      return toolError(error);
    }
  };

export const createServer = (sockets = new SocketManager()) => {
  const server = new McpServer({ name: 'geckopulse', version: '0.1.0' });

  server.registerTool(
    'socket_connect',
    {
      title: 'Connect to a WebSocket',
      description:
        'Open a WebSocket connection (ws:// or wss://) and start listening. Incoming messages are buffered in the background; ' +
        'read them with socket_read_messages using the returned connectionId.',
      inputSchema: {
        url: z.string().describe('WebSocket url, e.g. wss://example.com/socket'),
        headers: z
          .record(z.string(), z.string())
          .optional()
          .describe('Extra HTTP headers for the handshake, e.g. Authorization'),
        protocols: z.array(z.string()).optional().describe('WebSocket subprotocols (Sec-WebSocket-Protocol)'),
        timeoutMs: z.number().int().positive().max(60_000).optional().describe('Handshake timeout, default 10000'),
      },
    },
    safe(async args => json(await sockets.connect(args))),
  );

  server.registerTool(
    'socket_read_messages',
    {
      title: 'Read WebSocket messages',
      description:
        'Read messages received (and sent) on a connection. Pass the nextAfterSeq value from the previous call as afterSeq ' +
        'to only get new messages. Set waitMs to wait for new messages to arrive when there are none yet.',
      inputSchema: {
        connectionId: z.string(),
        afterSeq: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe('Only return messages after this seq, default 0 (all buffered)'),
        limit: z.number().int().min(1).max(500).optional().describe('Max messages to return, default 50'),
        waitMs: z
          .number()
          .int()
          .min(0)
          .max(MAX_WAIT_MS)
          .optional()
          .describe(`If there are no new messages, wait up to this many ms for one (max ${MAX_WAIT_MS})`),
      },
    },
    safe(async ({ connectionId, ...options }) => json(await sockets.read(connectionId, options))),
  );

  server.registerTool(
    'socket_send',
    {
      title: 'Send a WebSocket message',
      description: 'Send a text message on an open connection. To send JSON, pass it as a string.',
      inputSchema: {
        connectionId: z.string(),
        message: z.string(),
      },
    },
    safe(async ({ connectionId, message }) => json(await sockets.send(connectionId, message))),
  );

  server.registerTool(
    'socket_list',
    {
      title: 'List WebSocket connections',
      description: 'List all connections with their status and how many messages are buffered.',
      inputSchema: {},
    },
    safe(async () => json(sockets.list())),
  );

  server.registerTool(
    'socket_close',
    {
      title: 'Close a WebSocket connection',
      description:
        'Close a connection. Its buffered messages can still be read afterwards unless forget is true, which also removes it from the list.',
      inputSchema: {
        connectionId: z.string(),
        forget: z.boolean().optional(),
      },
    },
    safe(async ({ connectionId, forget }) => json(await sockets.close(connectionId, { forget }))),
  );

  return { server, sockets };
};
