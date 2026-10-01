import { randomUUID } from 'node:crypto';

import { type RawData, WebSocket } from 'ws';

export type SocketStatus = 'connecting' | 'open' | 'closed';

export interface SocketMessage {
  // Increasing number per connection, used as a cursor when reading
  seq: number;
  timestamp: string;
  direction: 'incoming' | 'outgoing';
  data: string;
  // Binary frames are returned as base64
  binary?: boolean;
  truncated?: boolean;
}

export interface SocketInfo {
  connectionId: string;
  url: string;
  status: SocketStatus;
  openedAt?: string;
  closedAt?: string;
  closeCode?: number;
  closeReason?: string;
  error?: string;
  bufferedMessages: number;
  droppedMessages: number;
  lastSeq: number;
}

export interface ConnectOptions {
  url: string;
  headers?: Record<string, string>;
  protocols?: string[];
  timeoutMs?: number;
}

export interface ReadOptions {
  // Only return messages with seq greater than this value
  afterSeq?: number;
  limit?: number;
  // When there are no new messages, wait up to this long for one to arrive
  waitMs?: number;
}

export interface ReadResult {
  connection: SocketInfo;
  messages: SocketMessage[];
  // Pass this as afterSeq on the next read to only get new messages
  nextAfterSeq: number;
  // True if more messages are available after this page
  hasMore: boolean;
  // True if messages older than afterSeq were dropped from the buffer before they were read
  missedMessages: boolean;
}

interface Connection {
  info: Omit<SocketInfo, 'bufferedMessages' | 'lastSeq'>;
  socket: WebSocket;
  messages: SocketMessage[];
  nextSeq: number;
  waiters: Set<() => void>;
}

export interface SocketManagerOptions {
  maxMessagesPerConnection?: number;
  maxMessageLength?: number;
  maxConnections?: number;
}

export const DEFAULT_MAX_MESSAGES = 500;
export const DEFAULT_MAX_MESSAGE_LENGTH = 100_000;
export const DEFAULT_MAX_CONNECTIONS = 20;
export const MAX_WAIT_MS = 60_000;

export class SocketManager {
  private readonly connections = new Map<string, Connection>();
  private readonly maxMessages: number;
  private readonly maxMessageLength: number;
  private readonly maxConnections: number;

  constructor(options: SocketManagerOptions = {}) {
    this.maxMessages = options.maxMessagesPerConnection ?? DEFAULT_MAX_MESSAGES;
    this.maxMessageLength = options.maxMessageLength ?? DEFAULT_MAX_MESSAGE_LENGTH;
    this.maxConnections = options.maxConnections ?? DEFAULT_MAX_CONNECTIONS;
  }

  async connect({ url, headers, protocols, timeoutMs = 10_000 }: ConnectOptions): Promise<SocketInfo> {
    const parsedUrl = new URL(url);
    if (parsedUrl.protocol !== 'ws:' && parsedUrl.protocol !== 'wss:') {
      throw new Error(`Only ws:// and wss:// urls are supported, got ${parsedUrl.protocol}//`);
    }
    // Closed connections are kept so their messages can still be read, but they don't count towards the limit
    const openCount = [...this.connections.values()].filter(c => c.info.status !== 'closed').length;
    if (openCount >= this.maxConnections) {
      throw new Error(`Too many open connections (max ${this.maxConnections}), close one first`);
    }

    const connectionId = randomUUID();
    const socket = new WebSocket(url, protocols, { headers, handshakeTimeout: timeoutMs });
    const connection: Connection = {
      info: { connectionId, url, status: 'connecting', droppedMessages: 0 },
      socket,
      messages: [],
      nextSeq: 1,
      waiters: new Set(),
    };
    this.connections.set(connectionId, connection);

    socket.on('message', (data, isBinary) => this.pushMessage(connection, 'incoming', data, isBinary));
    socket.on('close', (code, reason) => {
      connection.info.status = 'closed';
      connection.info.closedAt = new Date().toISOString();
      connection.info.closeCode = code;
      connection.info.closeReason = reason.toString() || undefined;
      this.wakeWaiters(connection);
    });

    await new Promise<void>((resolve, reject) => {
      const onOpen = () => {
        cleanup();
        connection.info.status = 'open';
        connection.info.openedAt = new Date().toISOString();
        resolve();
      };
      const onError = (error: Error) => {
        cleanup();
        connection.info.status = 'closed';
        connection.info.error = error.message;
        this.connections.delete(connectionId);
        reject(error);
      };
      const onUnexpectedResponse = (_req: unknown, res: { statusCode?: number; statusMessage?: string }) => {
        socket.terminate();
        onError(new Error(`Server rejected the connection: ${res.statusCode} ${res.statusMessage ?? ''}`.trim()));
      };
      const cleanup = () => {
        socket.off('open', onOpen);
        socket.off('error', onError);
        socket.off('unexpected-response', onUnexpectedResponse);
        // Keep an error listener so later errors don't crash the process
        socket.on('error', error => {
          connection.info.error = error.message;
        });
      };
      socket.once('open', onOpen);
      socket.once('error', onError);
      socket.once('unexpected-response', onUnexpectedResponse);
    });

    return this.getInfo(connection);
  }

  async read(connectionId: string, { afterSeq = 0, limit = 50, waitMs = 0 }: ReadOptions = {}): Promise<ReadResult> {
    const connection = this.getConnection(connectionId);
    const hasNew = () => connection.messages.some(m => m.seq > afterSeq);

    if (!hasNew() && waitMs > 0 && connection.info.status !== 'closed') {
      await new Promise<void>(resolve => {
        const done = () => {
          clearTimeout(timer);
          connection.waiters.delete(done);
          resolve();
        };
        const timer = setTimeout(done, Math.min(waitMs, MAX_WAIT_MS));
        connection.waiters.add(done);
      });
    }

    const pending = connection.messages.filter(m => m.seq > afterSeq);
    const messages = pending.slice(0, limit);
    const oldestBuffered = connection.messages[0]?.seq;
    return {
      connection: this.getInfo(connection),
      messages,
      nextAfterSeq: messages.length ? messages[messages.length - 1].seq : Math.max(afterSeq, 0),
      hasMore: pending.length > messages.length,
      missedMessages: oldestBuffered !== undefined && oldestBuffered > afterSeq + 1,
    };
  }

  async send(connectionId: string, data: string): Promise<SocketMessage> {
    const connection = this.getConnection(connectionId);
    if (connection.socket.readyState !== WebSocket.OPEN) {
      throw new Error(`Connection ${connectionId} is not open (status: ${connection.info.status})`);
    }
    await new Promise<void>((resolve, reject) => {
      connection.socket.send(data, error => (error ? reject(error) : resolve()));
    });
    return this.pushMessage(connection, 'outgoing', Buffer.from(data), false);
  }

  async close(connectionId: string, { forget = false }: { forget?: boolean } = {}): Promise<SocketInfo> {
    const connection = this.getConnection(connectionId);
    if (connection.socket.readyState === WebSocket.OPEN || connection.socket.readyState === WebSocket.CONNECTING) {
      await new Promise<void>(resolve => {
        const timer = setTimeout(() => {
          connection.socket.terminate();
          resolve();
        }, 3000);
        connection.socket.once('close', () => {
          clearTimeout(timer);
          resolve();
        });
        connection.socket.close(1000, 'Closed by client');
      });
    }
    const info = this.getInfo(connection);
    if (forget) {
      this.connections.delete(connectionId);
    }
    return info;
  }

  list(): SocketInfo[] {
    return [...this.connections.values()].map(c => this.getInfo(c));
  }

  async closeAll() {
    await Promise.all([...this.connections.keys()].map(id => this.close(id, { forget: true })));
  }

  private getConnection(connectionId: string) {
    const connection = this.connections.get(connectionId);
    if (!connection) {
      throw new Error(`Unknown connectionId: ${connectionId}. Use socket_list to see available connections.`);
    }
    return connection;
  }

  private getInfo(connection: Connection): SocketInfo {
    return {
      ...connection.info,
      bufferedMessages: connection.messages.length,
      lastSeq: connection.nextSeq - 1,
    };
  }

  private pushMessage(connection: Connection, direction: SocketMessage['direction'], raw: RawData, isBinary: boolean) {
    const buffer = Array.isArray(raw) ? Buffer.concat(raw) : Buffer.from(raw as ArrayBuffer);
    let data = isBinary ? buffer.toString('base64') : buffer.toString('utf8');
    let truncated = false;
    if (data.length > this.maxMessageLength) {
      data = data.slice(0, this.maxMessageLength);
      truncated = true;
    }
    const message: SocketMessage = {
      seq: connection.nextSeq++,
      timestamp: new Date().toISOString(),
      direction,
      data,
      ...(isBinary ? { binary: true } : {}),
      ...(truncated ? { truncated: true } : {}),
    };
    connection.messages.push(message);
    if (connection.messages.length > this.maxMessages) {
      connection.messages.shift();
      connection.info.droppedMessages++;
    }
    this.wakeWaiters(connection);
    return message;
  }

  private wakeWaiters(connection: Connection) {
    for (const waiter of connection.waiters) {
      waiter();
    }
  }
}
