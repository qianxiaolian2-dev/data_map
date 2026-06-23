from __future__ import annotations

import json
import shlex
import sys
import traceback
from pathlib import Path
from typing import Any, Callable, Dict, Optional
from urllib.parse import urlencode

from lineage_service import LineageService


SERVER_INFO = {
    "name": "fdl-lineage-mcp",
    "title": "FDL Lineage MCP",
    "version": "0.1.0",
}
SUPPORTED_PROTOCOL_VERSIONS = {"2024-11-05", "2025-03-26", "2025-06-18"}
ROOT_DIR = Path(__file__).resolve().parent
VIEWER_SCRIPT_PATH = ROOT_DIR / "lineage_web_viewer.py"


class JsonRpcError(Exception):
    def __init__(self, code: int, message: str, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.data = data


class FdlLineageMcpServer:
    def __init__(self) -> None:
        self.service = LineageService()
        self.negotiated_protocol_version = "2025-06-18"
        self._tools = self._build_tools()

    def serve(self) -> None:
        for raw_line in sys.stdin:
            line = raw_line.strip()
            if not line:
                continue
            try:
                message = json.loads(line)
            except json.JSONDecodeError as exc:
                self._send_error(None, -32700, f"Parse error: {exc.msg}")
                continue

            try:
                self._handle_message(message)
            except JsonRpcError as exc:
                message_id = message.get("id") if isinstance(message, dict) else None
                self._send_error(message_id, exc.code, exc.message, exc.data)
            except Exception:
                traceback.print_exc(file=sys.stderr)
                message_id = message.get("id") if isinstance(message, dict) else None
                self._send_error(message_id, -32603, "Internal error")

    def _handle_message(self, message: Dict[str, Any]) -> None:
        if not isinstance(message, dict):
            raise JsonRpcError(-32600, "Invalid Request")

        method = message.get("method")
        message_id = message.get("id")

        if method is None:
            return

        if message_id is None:
            self._handle_notification(method, message.get("params") or {})
            return

        result = self._dispatch_request(method, message.get("params") or {})
        self._send_result(message_id, result)

    def _handle_notification(self, method: str, params: Dict[str, Any]) -> None:
        if method == "notifications/initialized":
            return
        if method == "notifications/cancelled":
            return
        if method == "$/cancelRequest":
            return
        self._log(f"ignored notification: {method} {params}")

    def _dispatch_request(self, method: str, params: Dict[str, Any]) -> Dict[str, Any]:
        try:
            if method == "initialize":
                return self._initialize(params)
            if method == "ping":
                return {}
            if method == "tools/list":
                return {"tools": list(self._tools.values())}
            if method == "tools/call":
                return self._call_tool(params)
            raise JsonRpcError(-32601, f"Method not found: {method}")
        except JsonRpcError:
            raise
        except ValueError as exc:
            raise JsonRpcError(-32602, str(exc)) from exc

    def _initialize(self, params: Dict[str, Any]) -> Dict[str, Any]:
        client_version = params.get("protocolVersion") or self.negotiated_protocol_version
        if client_version in SUPPORTED_PROTOCOL_VERSIONS:
            self.negotiated_protocol_version = client_version
        else:
            self.negotiated_protocol_version = "2025-06-18"

        return {
            "protocolVersion": self.negotiated_protocol_version,
            "capabilities": {
                "tools": {
                    "listChanged": False,
                }
            },
            "serverInfo": SERVER_INFO,
        }

    def _call_tool(self, params: Dict[str, Any]) -> Dict[str, Any]:
        name = params.get("name")
        arguments = params.get("arguments") or {}
        if not isinstance(arguments, dict):
            raise JsonRpcError(-32602, "tool arguments must be an object")
        if name not in self._tools:
            raise JsonRpcError(-32601, f"Unknown tool: {name}")

        handler = self._tool_handlers()[name]
        try:
            payload = handler(arguments)
            summary = self._summarize_tool_result(name, payload)
            return {
                "content": [{"type": "text", "text": summary}],
                "structuredContent": payload,
                "isError": False,
            }
        except ValueError as exc:
            return {
                "content": [{"type": "text", "text": f"{name} failed: {exc}"}],
                "structuredContent": {"error": str(exc), "tool": name},
                "isError": True,
            }

    def _tool_handlers(self) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
        return {
            "list_graphs": lambda args: self.service.list_graphs(limit=self._int_arg(args, "limit", 50, 1, 200)),
            "search_nodes": lambda args: self.service.search_nodes(
                keyword=self._str_arg(args, "keyword"),
                graph_id=self._optional_str_arg(args, "graph_id"),
                resource_type=self._optional_str_arg(args, "resource_type"),
                limit=self._int_arg(args, "limit", 20, 1, 100),
            ),
            "search_target_tables": lambda args: self.service.search_target_tables(
                keyword=self._str_arg(args, "keyword"),
                connection_name=self._optional_str_arg(args, "connection_name"),
                schema=self._optional_str_arg(args, "schema"),
                limit=self._int_arg(args, "limit", 20, 1, 100),
            ),
            "get_table_node_graph": lambda args: self.service.get_table_node_full_lineage_graph(
                table=self._optional_str_arg(args, "table"),
                database_name=self._optional_str_arg(args, "database_name"),
                schema_name=self._optional_str_arg(args, "schema_name"),
                connection_name=self._optional_str_arg(args, "connection_name"),
                node_id=self._optional_str_arg(args, "node_id"),
                direction=self._optional_str_arg(args, "direction") or "both",
                depth=self._int_arg(args, "depth", 3, 1, 20),
            ),
            "get_table_node_activity": lambda args: self.service.get_table_node_activity(
                table=self._optional_str_arg(args, "table"),
                database_name=self._optional_str_arg(args, "database_name"),
                schema_name=self._optional_str_arg(args, "schema_name"),
                connection_name=self._optional_str_arg(args, "connection_name"),
                node_id=self._optional_str_arg(args, "node_id"),
                run_limit=self._int_arg(args, "run_limit", 10, 1, 50),
            ),
            "get_graph_task_activity": lambda args: self.service.get_graph_task_activity(
                node_id=self._str_arg(args, "node_id"),
                run_limit=self._int_arg(args, "run_limit", 10, 1, 50),
            ),
            "get_target_table_activity": lambda args: self.service.get_target_table_activity(
                table=self._optional_str_arg(args, "table"),
                schema=self._optional_str_arg(args, "schema"),
                connection_name=self._optional_str_arg(args, "connection_name"),
                target_table_id=self._optional_str_arg(args, "target_table_id"),
                run_limit=self._int_arg(args, "run_limit", 10, 1, 50),
            ),
            "open_target_table_graph": lambda args: self._open_target_table_graph(args),
            "open_table_node_graph": lambda args: self._open_table_node_graph(args),
            "get_graph": lambda args: self.service.get_graph(
                graph_id=self._str_arg(args, "graph_id"),
                include_connections=self._bool_arg(args, "include_connections", True),
            ),
            "get_target_table_upstream": lambda args: self.service.get_target_table_upstream(
                table=self._optional_str_arg(args, "table"),
                schema=self._optional_str_arg(args, "schema"),
                connection_name=self._optional_str_arg(args, "connection_name"),
                target_table_id=self._optional_str_arg(args, "target_table_id"),
            ),
            "get_upstream": lambda args: self.service.get_upstream(
                node_id=self._str_arg(args, "node_id"),
                depth=self._int_arg(args, "depth", 2, 1, 10),
                include_connections=self._bool_arg(args, "include_connections", True),
            ),
            "get_downstream": lambda args: self.service.get_downstream(
                node_id=self._str_arg(args, "node_id"),
                depth=self._int_arg(args, "depth", 2, 1, 10),
                include_connections=self._bool_arg(args, "include_connections", True),
            ),
            "find_path": lambda args: self.service.find_path(
                source_id=self._str_arg(args, "source_id"),
                target_id=self._str_arg(args, "target_id"),
                max_depth=self._int_arg(args, "max_depth", 8, 1, 20),
            ),
            "refresh_cache": lambda args: self.service.refresh_cache(),
        }

    def _build_tools(self) -> Dict[str, Dict[str, Any]]:
        return {
            "list_graphs": {
                "name": "list_graphs",
                "title": "List Graphs",
                "description": "List available lineage graphs and their approximate sizes. Use this when the user asks what lineage graphs exist or which graph is the largest.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of graphs to return.",
                            "minimum": 1,
                            "maximum": 200,
                            "default": 50,
                        }
                    },
                    "additionalProperties": False,
                },
            },
            "search_nodes": {
                "name": "search_nodes",
                "title": "Search Nodes",
                "description": "Search lineage nodes by table name, task name, node name, resource id, or other metadata. Use this first when the user gives a business name instead of a node id.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Keyword to search for, such as a table name, task name, or resource id.",
                        },
                        "graph_id": {
                            "type": "string",
                            "description": "Optional graph id to narrow the search to a single lineage graph.",
                        },
                        "resource_type": {
                            "type": "string",
                            "description": "Optional node type filter, such as DB_TABLE, DEV_DATA_FLOW, or DEV_DATA_SYNC.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of matched nodes to return.",
                            "minimum": 1,
                            "maximum": 100,
                            "default": 20,
                        },
                    },
                    "required": ["keyword"],
                    "additionalProperties": False,
                },
            },
            "search_target_tables": {
                "name": "search_target_tables",
                "title": "Search Target Tables",
                "description": "Search configured destination tables from pipeline config, including tables that may not yet exist as graph nodes. Use this for target tables such as PG.public.sales_order.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "keyword": {
                            "type": "string",
                            "description": "Keyword to search for, such as sales_order, public.sales_order, or a pipeline name.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Optional target connection filter, such as PG.",
                        },
                        "schema": {
                            "type": "string",
                            "description": "Optional target schema filter, such as public.",
                        },
                        "limit": {
                            "type": "integer",
                            "description": "Maximum number of matched target tables to return.",
                            "minimum": 1,
                            "maximum": 100,
                            "default": 20,
                        },
                    },
                    "required": ["keyword"],
                    "additionalProperties": False,
                },
            },
            "get_table_node_graph": {
                "name": "get_table_node_graph",
                "title": "Get Table Node Graph",
                "description": "Return a combined lineage graph for a regular table node, including graph-table upstream/downstream and any configured target-table branches fed by that table.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Optional exact lineage node id for the root table node.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Optional node connection name, such as dwd_mdbase.",
                        },
                        "database_name": {
                            "type": "string",
                            "description": "Optional database name, such as dwd_mdbase.",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Optional schema name. Leave empty for MySQL-style database nodes.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name, such as sales_order.",
                        },
                        "direction": {
                            "type": "string",
                            "description": "Traversal direction: upstream, downstream, or both.",
                            "enum": ["upstream", "downstream", "both"],
                            "default": "both",
                        },
                        "depth": {
                            "type": "integer",
                            "description": "Maximum graph traversal depth.",
                            "minimum": 1,
                            "maximum": 20,
                            "default": 3,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "get_table_node_activity": {
                "name": "get_table_node_activity",
                "title": "Get Table Node Activity",
                "description": "Return the direct producer tasks and downstream consumer tasks for a regular table node, plus the table-level latest execution summary and latest 10 runs.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Optional exact lineage node id for the root table node.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Optional node connection name, such as dwd_mdbase.",
                        },
                        "database_name": {
                            "type": "string",
                            "description": "Optional database name, such as dwd_mdbase.",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Optional schema name.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name, such as sales_order.",
                        },
                        "run_limit": {
                            "type": "integer",
                            "description": "Maximum number of recent execution records to return per producer task.",
                            "minimum": 1,
                            "maximum": 50,
                            "default": 10,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "get_graph_task_activity": {
                "name": "get_graph_task_activity",
                "title": "Get Graph Task Activity",
                "description": "Return a lineage task node's input tables, output tables, latest 10 execution records, and current scheduling summary.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Exact lineage node id for the task node.",
                        },
                        "run_limit": {
                            "type": "integer",
                            "description": "Maximum number of recent execution records to return.",
                            "minimum": 1,
                            "maximum": 50,
                            "default": 10,
                        },
                    },
                    "required": ["node_id"],
                    "additionalProperties": False,
                },
            },
            "get_target_table_activity": {
                "name": "get_target_table_activity",
                "title": "Get Target Table Activity",
                "description": "Return the direct producer pipelines for a destination table, the current pipeline status/check information, and the upstream source-table latest scheduling summary.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_table_id": {
                            "type": "string",
                            "description": "Optional resolved target-table config id.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Target connection name, such as PG.",
                        },
                        "schema": {
                            "type": "string",
                            "description": "Target schema, such as public.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Target table name, such as sales_order.",
                        },
                        "run_limit": {
                            "type": "integer",
                            "description": "Maximum number of recent upstream execution records to return.",
                            "minimum": 1,
                            "maximum": 50,
                            "default": 10,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "open_target_table_graph": {
                "name": "open_target_table_graph",
                "title": "Open Target Table Graph",
                "description": "Return a local lineage viewer URL and a ready-to-run start command for an interactive target-table graph page.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_table_id": {
                            "type": "string",
                            "description": "Optional resolved target-table config id.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Target connection name, such as PG.",
                        },
                        "schema": {
                            "type": "string",
                            "description": "Target schema, such as public.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Target table name, such as sales_order.",
                        },
                        "viewer_host": {
                            "type": "string",
                            "description": "Host for the local viewer URL.",
                            "default": "127.0.0.1",
                        },
                        "viewer_port": {
                            "type": "integer",
                            "description": "Port for the local viewer URL.",
                            "minimum": 1,
                            "maximum": 65535,
                            "default": 8765,
                        },
                        "upstream_depth": {
                            "type": "integer",
                            "description": "Recursive upstream depth for the page query.",
                            "minimum": 1,
                            "maximum": 20,
                            "default": 12,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "open_table_node_graph": {
                "name": "open_table_node_graph",
                "title": "Open Table Node Graph",
                "description": "Return a local lineage viewer URL and a ready-to-run start command for an interactive regular-table graph page, including configured target-table downstream branches.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Optional exact lineage node id for the root table node.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Optional node connection name, such as dwd_mdbase.",
                        },
                        "database_name": {
                            "type": "string",
                            "description": "Optional database name, such as dwd_mdbase.",
                        },
                        "schema_name": {
                            "type": "string",
                            "description": "Optional schema name.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Table name, such as sales_order.",
                        },
                        "direction": {
                            "type": "string",
                            "description": "Traversal direction: upstream, downstream, or both.",
                            "enum": ["upstream", "downstream", "both"],
                            "default": "both",
                        },
                        "depth": {
                            "type": "integer",
                            "description": "Maximum graph traversal depth.",
                            "minimum": 1,
                            "maximum": 20,
                            "default": 3,
                        },
                        "viewer_host": {
                            "type": "string",
                            "description": "Host for the local viewer URL.",
                            "default": "127.0.0.1",
                        },
                        "viewer_port": {
                            "type": "integer",
                            "description": "Port for the local viewer URL.",
                            "minimum": 1,
                            "maximum": 65535,
                            "default": 8765,
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "get_graph": {
                "name": "get_graph",
                "title": "Get Graph",
                "description": "Return the node-edge structure for a single lineage graph. Use this after you know the graph_id and need the whole local lineage graph.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "graph_id": {
                            "type": "string",
                            "description": "The lineage graph id.",
                        },
                        "include_connections": {
                            "type": "boolean",
                            "description": "Whether to attach connection lineage records linked to nodes in the graph.",
                            "default": True,
                        },
                    },
                    "required": ["graph_id"],
                    "additionalProperties": False,
                },
            },
            "get_target_table_upstream": {
                "name": "get_target_table_upstream",
                "title": "Get Target Table Upstream",
                "description": "Return the configured upstream pipelines, source connections, and source tables for a destination table such as PG.public.sales_order.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "target_table_id": {
                            "type": "string",
                            "description": "Direct target-table config id. Use this when you already resolved a target with search_target_tables.",
                        },
                        "connection_name": {
                            "type": "string",
                            "description": "Target connection name, such as PG.",
                        },
                        "schema": {
                            "type": "string",
                            "description": "Target schema, such as public.",
                        },
                        "table": {
                            "type": "string",
                            "description": "Target table name, such as sales_order.",
                        },
                    },
                    "additionalProperties": False,
                },
            },
            "get_upstream": {
                "name": "get_upstream",
                "title": "Get Upstream",
                "description": "Traverse upstream dependencies from a starting node. Use this to answer what feeds into a table, task, or sync node.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Starting lineage node id.",
                        },
                        "depth": {
                            "type": "integer",
                            "description": "Maximum number of hops to traverse upstream.",
                            "minimum": 1,
                            "maximum": 10,
                            "default": 2,
                        },
                        "include_connections": {
                            "type": "boolean",
                            "description": "Whether to attach connection lineage records linked to returned nodes.",
                            "default": True,
                        },
                    },
                    "required": ["node_id"],
                    "additionalProperties": False,
                },
            },
            "get_downstream": {
                "name": "get_downstream",
                "title": "Get Downstream",
                "description": "Traverse downstream dependencies from a starting node. Use this to answer what a table or task impacts next.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "node_id": {
                            "type": "string",
                            "description": "Starting lineage node id.",
                        },
                        "depth": {
                            "type": "integer",
                            "description": "Maximum number of hops to traverse downstream.",
                            "minimum": 1,
                            "maximum": 10,
                            "default": 2,
                        },
                        "include_connections": {
                            "type": "boolean",
                            "description": "Whether to attach connection lineage records linked to returned nodes.",
                            "default": True,
                        },
                    },
                    "required": ["node_id"],
                    "additionalProperties": False,
                },
            },
            "find_path": {
                "name": "find_path",
                "title": "Find Path",
                "description": "Find a directed dependency path between two nodes in the same graph. Use this to answer whether A influences B and through which steps.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "source_id": {
                            "type": "string",
                            "description": "Source node id.",
                        },
                        "target_id": {
                            "type": "string",
                            "description": "Target node id.",
                        },
                        "max_depth": {
                            "type": "integer",
                            "description": "Maximum hops to search before giving up.",
                            "minimum": 1,
                            "maximum": 20,
                            "default": 8,
                        },
                    },
                    "required": ["source_id", "target_id"],
                    "additionalProperties": False,
                },
            },
            "refresh_cache": {
                "name": "refresh_cache",
                "title": "Refresh Cache",
                "description": "Reload all three lineage tables from MySQL immediately. Use this if the source data changed and you need the newest graph state.",
                "inputSchema": {
                    "type": "object",
                    "properties": {},
                    "additionalProperties": False,
                },
            },
        }

    def _summarize_tool_result(self, name: str, payload: Dict[str, Any]) -> str:
        if name == "list_graphs":
            return f"Loaded {payload['graph_count']} graphs. Returned {len(payload['graphs'])} graph summaries."
        if name == "search_nodes":
            return f"Found {payload['total_matches']} matching nodes for keyword '{payload['keyword']}'. Returned {len(payload['nodes'])} nodes."
        if name == "search_target_tables":
            return f"Found {payload['total_matches']} matching target tables for keyword '{payload['keyword']}'. Returned {len(payload['targets'])} targets."
        if name == "get_table_node_graph":
            return (
                f"Table node graph for {payload['center']['resource_id']} returned "
                f"{len(payload['nodes'])} nodes and {len(payload['edges'])} edges."
            )
        if name == "get_table_node_activity":
            return (
                f"Table {payload['table']['resource_id']} has {payload['producer_count']} producer tasks, "
                f"{payload['consumer_count']} downstream consumer tasks, and "
                f"{len(payload.get('recent_runs', []))} recent runs in the table-level timeline."
            )
        if name == "get_graph_task_activity":
            return (
                f"Task node {payload['task']['resource_id']} has {len(payload.get('input_tables', []))} input tables, "
                f"{len(payload.get('output_tables', []))} output tables, and "
                f"{len(payload.get('recent_runs', []))} recent runs."
            )
        if name == "get_target_table_activity":
            return (
                f"Target activity resolved {payload['match_count']} target configs and "
                f"{len(payload['pipelines'])} producer pipelines, plus "
                f"{len(payload.get('recent_source_runs', []))} recent upstream source-task runs."
            )
        if name == "open_target_table_graph":
            return f"Viewer URL ready: {payload['viewer_url']}"
        if name == "open_table_node_graph":
            return f"Viewer URL ready: {payload['viewer_url']}"
        if name == "get_graph":
            summary = payload["summary"]
            return (
                f"Graph {payload['graph_id']} has {summary['node_count']} nodes, "
                f"{summary['edge_count']} valid edges, and {summary['dangling_edge_count']} dangling edges."
            )
        if name == "get_target_table_upstream":
            target_count = payload["match_count"]
            if target_count == 1:
                target = payload["targets"][0]
                return (
                    f"Target table {target['full_name']} is fed by pipeline {target['pipeline_name'] or target['task_id']} "
                    f"with {len(target['source_tables'])} source tables."
                )
            return f"Matched {target_count} target-table configs."
        if name in {"get_upstream", "get_downstream"}:
            return (
                f"{payload['direction']} traversal from {payload['center']['display_name']} returned "
                f"{len(payload['nodes'])} nodes and {len(payload['edges'])} edges."
            )
        if name == "find_path":
            if payload.get("found"):
                return f"Found a directed path with {payload['hop_count']} hops."
            return f"No path found. Reason: {payload.get('reason', 'unknown')}."
        if name == "refresh_cache":
            return (
                f"Cache refreshed: {payload['graph_count']} graphs, {payload['node_count']} nodes, "
                f"{payload['edge_count']} edges, {payload['connection_count']} connections."
            )
        return json.dumps(payload, ensure_ascii=False)

    def _send_result(self, message_id: Any, result: Dict[str, Any]) -> None:
        self._send({"jsonrpc": "2.0", "id": message_id, "result": result})

    def _send_error(self, message_id: Any, code: int, message: str, data: Any = None) -> None:
        payload = {
            "jsonrpc": "2.0",
            "id": message_id,
            "error": {
                "code": code,
                "message": message,
            },
        }
        if data is not None:
            payload["error"]["data"] = data
        self._send(payload)

    def _send(self, payload: Dict[str, Any]) -> None:
        sys.stdout.write(json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n")
        sys.stdout.flush()

    def _log(self, message: str) -> None:
        print(message, file=sys.stderr, flush=True)

    def _str_arg(self, args: Dict[str, Any], key: str) -> str:
        value = args.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a non-empty string")
        return value.strip()

    def _optional_str_arg(self, args: Dict[str, Any], key: str) -> Optional[str]:
        value = args.get(key)
        if value is None:
            return None
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{key} must be a non-empty string when provided")
        return value.strip()

    def _int_arg(self, args: Dict[str, Any], key: str, default: int, minimum: int, maximum: int) -> int:
        value = args.get(key, default)
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{key} must be an integer")
        if value < minimum or value > maximum:
            raise ValueError(f"{key} must be between {minimum} and {maximum}")
        return value

    def _bool_arg(self, args: Dict[str, Any], key: str, default: bool) -> bool:
        value = args.get(key, default)
        if not isinstance(value, bool):
            raise ValueError(f"{key} must be a boolean")
        return value

    def _open_target_table_graph(self, args: Dict[str, Any]) -> Dict[str, Any]:
        viewer_host = self._optional_str_arg(args, "viewer_host") or "127.0.0.1"
        viewer_port = self._int_arg(args, "viewer_port", 8765, 1, 65535)
        upstream_depth = self._int_arg(args, "upstream_depth", 12, 1, 20)
        target_table_id = self._optional_str_arg(args, "target_table_id")
        connection_name = self._optional_str_arg(args, "connection_name")
        schema = self._optional_str_arg(args, "schema")
        table = self._optional_str_arg(args, "table")

        upstream = self.service.get_target_table_upstream(
            table=table,
            schema=schema,
            connection_name=connection_name,
            target_table_id=target_table_id,
        )
        first_target = upstream["targets"][0]
        resolved_connection = first_target["connection_name"]
        resolved_schema = first_target["fdl_schema"]
        resolved_table = first_target["fdl_table"]
        viewer_url = self._build_target_viewer_url(
            host=viewer_host,
            port=viewer_port,
            connection_name=resolved_connection,
            schema=resolved_schema,
            table=resolved_table,
            upstream_depth=upstream_depth,
        )
        api_url = self._build_target_api_url(
            host=viewer_host,
            port=viewer_port,
            connection_name=resolved_connection,
            schema=resolved_schema,
            table=resolved_table,
            upstream_depth=upstream_depth,
        )
        start_command = " ".join(
            [
                "python3",
                shlex.quote(str(VIEWER_SCRIPT_PATH)),
                "--mode",
                "target_table",
                "--host",
                shlex.quote(viewer_host),
                "--port",
                str(viewer_port),
                "--connection",
                shlex.quote(resolved_connection),
                "--schema",
                shlex.quote(resolved_schema),
                "--table",
                shlex.quote(resolved_table),
                "--upstream-depth",
                str(upstream_depth),
                "--open",
            ]
        )
        return {
            "viewer_url": viewer_url,
            "api_url": api_url,
            "viewer_host": viewer_host,
            "viewer_port": viewer_port,
            "upstream_depth": upstream_depth,
            "connection_name": resolved_connection,
            "schema": resolved_schema,
            "table": resolved_table,
            "target_count": upstream["match_count"],
            "targets": upstream["targets"],
            "start_command": start_command,
        }

    def _open_table_node_graph(self, args: Dict[str, Any]) -> Dict[str, Any]:
        viewer_host = self._optional_str_arg(args, "viewer_host") or "127.0.0.1"
        viewer_port = self._int_arg(args, "viewer_port", 8765, 1, 65535)
        depth = self._int_arg(args, "depth", 3, 1, 20)
        direction = self._optional_str_arg(args, "direction") or "both"
        node_id = self._optional_str_arg(args, "node_id")
        connection_name = self._optional_str_arg(args, "connection_name")
        database_name = self._optional_str_arg(args, "database_name")
        schema_name = self._optional_str_arg(args, "schema_name")
        table = self._optional_str_arg(args, "table")

        graph = self.service.get_table_node_full_lineage_graph(
            table=table,
            database_name=database_name,
            schema_name=schema_name,
            connection_name=connection_name,
            node_id=node_id,
            direction=direction,
            depth=depth,
        )
        center = graph["center"]
        viewer_url = self._build_table_node_viewer_url(
            host=viewer_host,
            port=viewer_port,
            connection_name=center["connection_name"],
            database_name=center["database_name"],
            schema_name=center["schema_name"],
            table=center["table_name"],
            node_id=center["id"],
            direction=direction,
            depth=depth,
        )
        api_url = self._build_table_node_api_url(
            host=viewer_host,
            port=viewer_port,
            connection_name=center["connection_name"],
            database_name=center["database_name"],
            schema_name=center["schema_name"],
            table=center["table_name"],
            node_id=center["id"],
            direction=direction,
            depth=depth,
        )
        start_command = " ".join(
            [
                "python3",
                shlex.quote(str(VIEWER_SCRIPT_PATH)),
                "--mode",
                "table_node",
                "--host",
                shlex.quote(viewer_host),
                "--port",
                str(viewer_port),
                "--connection",
                shlex.quote(center["connection_name"]),
                "--database",
                shlex.quote(center["database_name"]),
                "--schema",
                shlex.quote(center["schema_name"]),
                "--table",
                shlex.quote(center["table_name"]),
                "--node-id",
                shlex.quote(center["id"]),
                "--direction",
                shlex.quote(direction),
                "--depth",
                str(depth),
                "--open",
            ]
        )
        return {
            "viewer_url": viewer_url,
            "api_url": api_url,
            "viewer_host": viewer_host,
            "viewer_port": viewer_port,
            "direction": direction,
            "depth": depth,
            "node_match_count": graph["node_match_count"],
            "related_target_count": graph["related_target_count"],
            "center": center,
            "related_targets": graph["related_targets"],
            "start_command": start_command,
        }

    def _build_target_viewer_url(
        self,
        host: str,
        port: int,
        connection_name: str,
        schema: str,
        table: str,
        upstream_depth: int,
    ) -> str:
        query = urlencode(
            {
                "mode": "target_table",
                "connection_name": connection_name,
                "schema": schema,
                "table": table,
                "upstream_depth": upstream_depth,
            }
        )
        return f"http://{host}:{port}/viewer?{query}"

    def _build_target_api_url(
        self,
        host: str,
        port: int,
        connection_name: str,
        schema: str,
        table: str,
        upstream_depth: int,
    ) -> str:
        query = urlencode(
            {
                "connection_name": connection_name,
                "schema": schema,
                "table": table,
                "upstream_depth": upstream_depth,
            }
        )
        return f"http://{host}:{port}/api/target-graph?{query}"

    def _build_table_node_viewer_url(
        self,
        host: str,
        port: int,
        connection_name: str,
        database_name: str,
        schema_name: str,
        table: str,
        node_id: str,
        direction: str,
        depth: int,
    ) -> str:
        query = urlencode(
            {
                "mode": "table_node",
                "connection_name": connection_name,
                "database_name": database_name,
                "schema_name": schema_name,
                "table": table,
                "node_id": node_id,
                "direction": direction,
                "depth": depth,
            }
        )
        return f"http://{host}:{port}/viewer?{query}"

    def _build_table_node_api_url(
        self,
        host: str,
        port: int,
        connection_name: str,
        database_name: str,
        schema_name: str,
        table: str,
        node_id: str,
        direction: str,
        depth: int,
    ) -> str:
        query = urlencode(
            {
                "connection_name": connection_name,
                "database_name": database_name,
                "schema_name": schema_name,
                "table": table,
                "node_id": node_id,
                "direction": direction,
                "depth": depth,
            }
        )
        return f"http://{host}:{port}/api/table-node-graph?{query}"


def main() -> None:
    server = FdlLineageMcpServer()
    try:
        server.serve()
    except JsonRpcError as exc:
        server._send_error(None, exc.code, exc.message, exc.data)
    except KeyboardInterrupt:
        return


if __name__ == "__main__":
    main()
