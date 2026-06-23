# FDL Lineage MCP

This project wraps three tables in `fdldb` as a local stdio MCP server so an LLM can ask lineage questions without writing SQL:

- `fdl_lineage_node`: graph nodes
- `fdl_lineage_edge`: graph edges
- `fdl_connection_lineage`: connection and task lineage metadata

## Why MCP instead of raw SQL

For LLM use, raw SQL is the wrong abstraction. The model should not decide table joins or infer lineage semantics from scratch on every turn.

This server exposes stable domain tools instead:

- `list_graphs`
- `search_nodes`
- `search_target_tables`
- `get_graph`
- `get_target_table_upstream`
- `get_upstream`
- `get_downstream`
- `find_path`
- `refresh_cache`

That gives the model a narrower and more reliable interface:

- Search a table or task by name
- Resolve it to one or more lineage node ids
- Traverse upstream or downstream
- Check whether two nodes are connected
- Pull a whole graph only when needed

## Files

- [fdl_mcp_server.py](/Users/seer/自研项目/图数据库血缘/fdl_mcp_server.py): stdio MCP server
- [lineage_service.py](/Users/seer/自研项目/图数据库血缘/lineage_service.py): graph query service reused by MCP
- [lineage_web_viewer.py](/Users/seer/自研项目/图数据库血缘/lineage_web_viewer.py): local web viewer that can auto-open a browser page
- [data_map_web.py](/Users/seer/自研项目/图数据库血缘/data_map_web.py): data map web server and API
- [templates/map.html](/Users/seer/自研项目/图数据库血缘/templates/map.html): data map homepage template, maintained outside Python string literals
- [db_connect_write_fdldb.py](/Users/seer/自研项目/图数据库血缘/fdl连接/db_connect_write_fdldb.py): MySQL connector

## Architecture

The useful split is:

1. Storage layer: MySQL tables in `fdldb`
2. Service layer: parse rows, normalize metadata, build graph indexes, answer lineage questions
3. Delivery layer:
   - MCP for LLM use
   - REST API for frontend use
   - Web page for visual graph exploration

The same service layer should back both the frontend and the MCP server. Do not implement graph logic twice.

## Tool strategy for LLMs

The model should usually follow this flow:

1. If the user gives a business name or table name, call `search_nodes`
2. If the user asks for a configured destination table such as `PG.public.sales_order`, call `search_target_tables`
3. If the user asks for the upstream of a destination table, call `get_target_table_upstream`
4. If the user asks for full graph context, call `get_graph`
5. If the user asks "what feeds into this graph node", call `get_upstream`
6. If the user asks "what does this affect", call `get_downstream`
7. If the user asks "does A affect B", call `find_path`

This is much better than exposing one generic `query_sql` tool.

## Local run

Start the server locally:

```bash
python3 /Users/seer/自研项目/图数据库血缘/fdl_mcp_server.py
```

It uses stdio transport, so each MCP message is one JSON line on stdin/stdout.

For the data map web UI:

```bash
python3 /Users/seer/自研项目/图数据库血缘/data_map_web.py
```

The homepage template for `/` and `/map` is loaded from `templates/map.html` first, with the Python inline template as fallback.

## Example MCP handshake

Initialize:

```json
{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"local-test","version":"0.1.0"}}}
```

List tools:

```json
{"jsonrpc":"2.0","id":2,"method":"tools/list"}
```

Call a tool:

```json
{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search_nodes","arguments":{"keyword":"purchase_production_order_detail","limit":5}}}
```

Target table upstream example:

```json
{"jsonrpc":"2.0","id":4,"method":"tools/call","params":{"name":"get_target_table_upstream","arguments":{"connection_name":"PG","schema":"public","table":"sales_order"}}}
```

## How to attach this to an MCP client

Most local MCP clients use a config with `command` and `args`. The shape is commonly like this:

```json
{
  "mcpServers": {
    "fdl-lineage": {
      "command": "python3",
      "args": ["/Users/seer/自研项目/图数据库血缘/fdl_mcp_server.py"]
    }
  }
}
```

Then restart the MCP host application so it launches the server as a child process.

The MCP architecture docs describe the same host-client-server pattern and show that:

- the host starts a local stdio server as a subprocess
- the client discovers tools with `tools/list`
- the model uses `tools/call` to execute domain tools

Sources:

- MCP architecture overview: https://modelcontextprotocol.io/docs/learn/architecture
- MCP stdio transport: https://modelcontextprotocol.io/specification/2025-06-18/basic/transports
- Connect local servers: https://modelcontextprotocol.io/docs/tutorials/use-local-mcp-server

## Recommended next step

If you also want a graph page, keep the query logic in `lineage_service.py`, then add:

- a small REST API that returns `{nodes, edges, connections}`
- a frontend that renders the graph

The MCP server and frontend can share the same lineage service instead of diverging.

## Auto-open a lineage page

If you want a local page to pop up and render the graph directly, run:

```bash
python3 /Users/seer/自研项目/图数据库血缘/lineage_web_viewer.py \
  --connection PG \
  --schema public \
  --table sales_order \
  --open
```

The script will:

- preload the lineage snapshot from MySQL
- start a local viewer on `http://127.0.0.1:8765`
- open the default browser automatically

The viewer calls a local JSON endpoint:

```text
/api/target-graph?connection_name=PG&schema=public&table=sales_order&upstream_depth=12
```
