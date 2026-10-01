# geckopulse-mcp-server

An MCP server that lets AI assistants (Claude Code, Claude Desktop, …) open WebSocket connections and listen to their messages. It runs as a standalone process over stdio and does not depend on the GeckoPulse desktop app.

## Build

```bash
npm run build -w packages/geckopulse-mcp-server
```

This produces a single self-contained file at `packages/geckopulse-mcp-server/dist/index.js`.

## Add to an AI client

Claude Code:

```bash
claude mcp add geckopulse -- node /absolute/path/to/GeckoPulse/packages/geckopulse-mcp-server/dist/index.js
```

Claude Desktop (`claude_desktop_config.json`):

```json
{
  "mcpServers": {
    "geckopulse": {
      "command": "node",
      "args": ["/absolute/path/to/GeckoPulse/packages/geckopulse-mcp-server/dist/index.js"]
    }
  }
}
```

## Tools

| Tool                   | Description                                                                                      |
| ---------------------- | ------------------------------------------------------------------------------------------------ |
| `socket_connect`       | Open a `ws://` / `wss://` connection (optional headers and subprotocols), returns a connectionId |
| `socket_read_messages` | Read buffered messages after a cursor (`afterSeq`), optionally waiting up to 60s for new ones    |
| `socket_send`          | Send a text message                                                                              |
| `socket_list`          | List connections and their status                                                                |
| `socket_close`         | Close a connection (`forget: true` also drops its buffered messages)                             |

Messages are kept in memory, up to the newest 500 per connection (older ones are dropped and reported via `missedMessages` / `droppedMessages`). Messages longer than 100,000 characters are truncated, binary frames are returned as base64. At most 20 connections can be open at once.
