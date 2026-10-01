import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';

import { createServer } from './server';

const { server, sockets } = createServer();

const shutdown = async () => {
  await sockets.closeAll();
  await server.close();
  process.exit(0);
};
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
// The AI client closes stdin when it stops the server
process.stdin.on('close', shutdown);

await server.connect(new StdioServerTransport());
// stdout is used by the MCP protocol, so logs go to stderr
console.error('GeckoPulse MCP server running on stdio');
