from __future__ import annotations

import importlib.util
import json
import time
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parent
DB_CONNECT_PATH = ROOT_DIR / "fdl连接" / "db_connect_write_fdldb.py"


class LineageService:
    def __init__(self, refresh_seconds: int = 15) -> None:
        self.refresh_seconds = refresh_seconds
        self._db_module = self._load_db_module()
        self._snapshot: Optional[Dict[str, Any]] = None

    def list_graphs(self, limit: int = 50) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        graphs = snapshot["graphs"]
        usable_graphs = [item for item in graphs.values() if item["node_count"] > 0]
        edge_only_graph_count = sum(1 for item in graphs.values() if item["node_count"] == 0 and item["edge_count"] > 0)
        items = sorted(usable_graphs, key=lambda item: (-item["node_count"], item["graph_id"]))
        summaries = [
            {
                "graph_id": item["graph_id"],
                "node_count": item["node_count"],
                "edge_count": item["edge_count"],
                "node_type_counts": item["node_type_counts"],
            }
            for item in items[:limit]
        ]
        return {
            "loaded_at": snapshot["loaded_at"],
            "graph_count": len(items),
            "edge_only_graph_count": edge_only_graph_count,
            "graphs": summaries,
        }

    def search_nodes(
        self,
        keyword: str,
        graph_id: Optional[str] = None,
        resource_type: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        needle = keyword.strip().lower()
        if not needle:
            raise ValueError("keyword is required")

        matches: List[Tuple[int, Dict[str, Any]]] = []
        for node in snapshot["nodes"]:
            if graph_id and node["graph_id"] != graph_id:
                continue
            if resource_type and node["resource_type"] != resource_type:
                continue
            score = self._score_node(node, needle)
            if score <= 0:
                continue
            matches.append((score, node))

        matches.sort(key=lambda item: (-item[0], item[1]["display_name"], item[1]["id"]))
        nodes = [self._node_preview(item[1]) for item in matches[:limit]]
        return {
            "loaded_at": snapshot["loaded_at"],
            "keyword": keyword,
            "total_matches": len(matches),
            "nodes": nodes,
        }

    def search_target_tables(
        self,
        keyword: str,
        connection_name: Optional[str] = None,
        schema: Optional[str] = None,
        limit: int = 20,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        needle = keyword.strip().lower()
        if not needle:
            raise ValueError("keyword is required")

        matches: List[Tuple[int, Dict[str, Any]]] = []
        for target in snapshot["target_tables"]:
            if connection_name and target["connection_name"] != connection_name:
                continue
            if schema and target["fdl_schema"] != schema:
                continue
            score = self._score_target_table(target, needle)
            if score <= 0:
                continue
            matches.append((score, target))

        matches.sort(key=lambda item: (-item[0], item[1]["full_name"], item[1]["target_table_id"]))
        targets = [self._target_table_preview(item[1]) for item in matches[:limit]]
        return {
            "loaded_at": snapshot["loaded_at"],
            "keyword": keyword,
            "total_matches": len(matches),
            "targets": targets,
        }

    def get_target_table_upstream(
        self,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        connection_name: Optional[str] = None,
        target_table_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        matched_targets: List[Dict[str, Any]] = []

        if target_table_id:
            target = snapshot["target_tables_by_id"].get(target_table_id)
            if target is None:
                raise ValueError(f"target_table_id not found: {target_table_id}")
            matched_targets = [target]
        else:
            if not table:
                raise ValueError("table is required when target_table_id is not provided")
            lookup_key = (
                (connection_name or "").lower(),
                (schema or "").lower(),
                table.lower(),
            )
            matched_targets = snapshot["target_tables_by_lookup"].get(lookup_key, [])
            if not matched_targets and not connection_name:
                fallback = [
                    target
                    for target in snapshot["target_tables"]
                    if target["fdl_table"].lower() == table.lower()
                    and (schema is None or target["fdl_schema"].lower() == schema.lower())
                ]
                matched_targets = fallback
            if not matched_targets:
                qualified_name = ".".join(filter(None, [connection_name, schema, table]))
                raise ValueError(f"target table not found: {qualified_name or table}")

        return {
            "loaded_at": snapshot["loaded_at"],
            "match_count": len(matched_targets),
            "targets": [self._target_table_payload(snapshot, target) for target in matched_targets],
        }

    def get_target_table_full_upstream_graph(
        self,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        connection_name: Optional[str] = None,
        target_table_id: Optional[str] = None,
        upstream_depth: int = 12,
    ) -> Dict[str, Any]:
        if upstream_depth < 1 or upstream_depth > 20:
            raise ValueError("upstream_depth must be between 1 and 20")

        snapshot = self._ensure_snapshot()
        upstream = self.get_target_table_upstream(
            table=table,
            schema=schema,
            connection_name=connection_name,
            target_table_id=target_table_id,
        )
        targets = upstream["targets"]
        if not targets:
            raise ValueError("no target-table configs resolved")

        root_full_name = targets[0]["full_name"]
        root_id = f"target::{root_full_name}"
        graph_nodes: Dict[str, Dict[str, Any]] = {}
        graph_edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        upstream_cache: Dict[str, Dict[str, Any]] = {}

        self._add_graph_node(
            graph_nodes,
            {
                "id": root_id,
                "label": root_full_name,
                "type": "target_table",
                "meta": {
                    "full_name": root_full_name,
                    "target_table_id": targets[0]["target_table_id"],
                    "target_table_ids": [target["target_table_id"] for target in targets],
                    "target_config_count": len(targets),
                    "connection_name": targets[0]["connection_name"],
                    "schema": targets[0]["fdl_schema"],
                    "table": targets[0]["fdl_table"],
                },
            },
        )

        for target in targets:
            for source_table in target["source_tables"]:
                source_node = self._pick_best_lineage_node(snapshot, source_table)
                if source_node is None:
                    source_graph_node = self._source_table_graph_payload(source_table)
                    self._add_graph_node(graph_nodes, source_graph_node)
                    self._add_graph_edge(graph_edges, source_graph_node["id"], root_id, "lineage")
                    continue

                source_graph_node_id = self._graph_node_id(source_node["id"])
                self._add_graph_node(graph_nodes, self._graph_node_payload(source_node))
                self._add_graph_edge(graph_edges, source_graph_node_id, root_id, "lineage")

                if source_node["id"] not in upstream_cache:
                    upstream_cache[source_node["id"]] = self._build_table_only_lineage_graph(
                        snapshot=snapshot,
                        start_node=source_node,
                        direction="upstream",
                        depth=upstream_depth,
                    )
                upstream_graph = upstream_cache[source_node["id"]]
                for graph_node in upstream_graph["nodes"]:
                    self._add_graph_node(graph_nodes, graph_node)
                for edge in upstream_graph["edges"]:
                    self._add_graph_edge(
                        graph_edges,
                        edge["source"],
                        edge["target"],
                        edge["relation"],
                    )

        return {
            "loaded_at": snapshot["loaded_at"],
            "title": f"{root_full_name} upstream lineage",
            "root_node_id": root_id,
            "target_count": len(targets),
            "nodes": list(graph_nodes.values()),
            "edges": list(graph_edges.values()),
            "targets": targets,
        }

    def get_table_node_full_lineage_graph(
        self,
        table: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        connection_name: Optional[str] = None,
        node_id: Optional[str] = None,
        direction: str = "both",
        depth: int = 3,
    ) -> Dict[str, Any]:
        if direction not in {"upstream", "downstream", "both"}:
            raise ValueError("direction must be one of: upstream, downstream, both")
        if depth < 1 or depth > 20:
            raise ValueError("depth must be between 1 and 20")

        snapshot = self._ensure_snapshot()
        start_node, node_matches = self._resolve_table_node(
            snapshot=snapshot,
            table=table,
            database_name=database_name,
            schema_name=schema_name,
            connection_name=connection_name,
            node_id=node_id,
        )

        table_graph = self._build_table_only_lineage_graph(
            snapshot=snapshot,
            start_node=start_node,
            direction=direction,
            depth=depth,
        )
        graph_nodes = {node["id"]: node for node in table_graph["nodes"]}
        graph_edges = {
            (edge["source"], edge["target"], edge["relation"]): edge
            for edge in table_graph["edges"]
        }
        root_graph_id = self._graph_node_id(start_node["id"])

        related_targets = []
        if direction in {"downstream", "both"}:
            # Collect downstream graph_table node IDs via BFS from root
            downstream_ids: set = set()
            queue: deque = deque([root_graph_id])
            while queue:
                nid = queue.popleft()
                if nid in downstream_ids:
                    continue
                downstream_ids.add(nid)
                for (src, dst, _) in list(graph_edges.keys()):
                    if src == nid and dst not in downstream_ids:
                        queue.append(dst)

            # For every downstream graph_table node, add its target table outputs
            seen_target_ids: set = set()
            for g_node_id in list(downstream_ids):
                g_node = graph_nodes.get(g_node_id)
                if not g_node or g_node.get("type") not in ("graph_table", "graph_task"):
                    continue
                raw_node_id = g_node_id[len("graph::"):] if g_node_id.startswith("graph::") else g_node_id
                raw_node = snapshot["nodes_by_id"].get(raw_node_id)
                if raw_node is None:
                    continue

                for target in self._related_targets_for_node(snapshot, raw_node):
                    if target["target_table_id"] not in seen_target_ids:
                        related_targets.append(target)
                        seen_target_ids.add(target["target_table_id"])

                    target_node_id = f"target::{target['full_name']}"
                    self._add_graph_node(
                        graph_nodes,
                        {
                            "id": target_node_id,
                            "label": target["full_name"],
                            "type": "target_table",
                            "meta": {
                                "full_name": target["full_name"],
                                "target_table_id": target["target_table_id"],
                                "connection_name": target["connection_name"],
                                "schema": target["fdl_schema"],
                                "table": target["fdl_table"],
                                "config_status": target["config_status"],
                                "target_connections": target["target_connections"],
                            },
                        },
                    )
                    # Edge from the actual source graph node (not always root)
                    self._add_graph_edge(graph_edges, g_node_id, target_node_id, "lineage")

                    # Add edges from other source tables of this target that are already in graph
                    for source_table in target["source_tables"]:
                        source_node = self._pick_best_lineage_node(snapshot, source_table)
                        if source_node is None:
                            continue
                        src_graph_id = self._graph_node_id(source_node["id"])
                        if src_graph_id in graph_nodes and src_graph_id != g_node_id:
                            self._add_graph_edge(graph_edges, src_graph_id, target_node_id, "lineage")

        resolved_name = self._node_full_name(start_node)
        return {
            "loaded_at": snapshot["loaded_at"],
            "title": f"{resolved_name} {direction} lineage",
            "root_node_id": root_graph_id,
            "direction": direction,
            "depth": depth,
            "node_match_count": len(node_matches),
            "node_matches": [self._node_payload(node) for node in node_matches],
            "related_target_count": len(related_targets),
            "related_targets": [self._target_table_preview(target) for target in related_targets],
            "center": self._node_payload(start_node),
            "nodes": list(graph_nodes.values()),
            "edges": list(graph_edges.values()),
        }

    def get_table_node_activity(
        self,
        table: Optional[str] = None,
        database_name: Optional[str] = None,
        schema_name: Optional[str] = None,
        connection_name: Optional[str] = None,
        node_id: Optional[str] = None,
        run_limit: int = 10,
    ) -> Dict[str, Any]:
        if run_limit < 1 or run_limit > 50:
            raise ValueError("run_limit must be between 1 and 50")

        snapshot = self._ensure_snapshot()
        node, _ = self._resolve_table_node(
            snapshot=snapshot,
            table=table,
            database_name=database_name,
            schema_name=schema_name,
            connection_name=connection_name,
            node_id=node_id,
        )
        activity = self._collect_table_node_activity(snapshot, node, run_limit)

        return {
            "loaded_at": snapshot["loaded_at"],
            "table": self._node_payload(node),
            "producer_count": len(activity["producers"]),
            "consumer_count": len(activity["consumers"]),
            "producers": activity["producers"],
            "consumers": activity["consumers"],
            "recent_runs": activity["recent_runs"],
            "schedule_summary": activity["schedule_summary"],
        }

    def get_target_table_activity(
        self,
        table: Optional[str] = None,
        schema: Optional[str] = None,
        connection_name: Optional[str] = None,
        target_table_id: Optional[str] = None,
        run_limit: int = 10,
    ) -> Dict[str, Any]:
        if run_limit < 1 or run_limit > 50:
            raise ValueError("run_limit must be between 1 and 50")

        snapshot = self._ensure_snapshot()
        upstream = self.get_target_table_upstream(
            table=table,
            schema=schema,
            connection_name=connection_name,
            target_table_id=target_table_id,
        )
        pipelines = []
        source_run_map: Dict[str, Dict[str, Any]] = {}
        source_activity_cache: Dict[str, Dict[str, Any]] = {}
        source_producer_keys = set()
        resolved_target_nodes: Dict[str, Dict[str, Any]] = {}
        target_activity_cache: Dict[str, Dict[str, Any]] = {}
        for target in upstream["targets"]:
            target_node = self._pick_best_target_lineage_node(snapshot, target)
            if target_node is not None:
                resolved_target_nodes[target_node["id"]] = target_node
                if target_node["id"] not in target_activity_cache:
                    target_activity_cache[target_node["id"]] = self._collect_table_node_activity(
                        snapshot,
                        target_node,
                        run_limit,
                    )
            # 也收集"所有"匹配节点的活动（覆盖跨 graph 的真实下游消费者）
            for extra_node in self._collect_all_target_lineage_nodes(snapshot, target):
                if extra_node["id"] in target_activity_cache:
                    continue
                resolved_target_nodes[extra_node["id"]] = extra_node
                target_activity_cache[extra_node["id"]] = self._collect_table_node_activity(
                    snapshot,
                    extra_node,
                    run_limit,
                )
            pipeline_state = self._query_pipeline_task_state(target["task_id"])
            pipeline_runs = self._query_recent_work_runs(
                target["task_id"],
                run_limit,
                {
                    target["task_id"]: {
                        "task_name": target["pipeline_name"] or target["task_id"],
                        "node_name": "Pipeline",
                        "task_node_resource_id": target["task_id"],
                    }
                },
            )
            source_activities = []
            for source_table in target["source_tables"]:
                source_node = self._pick_best_lineage_node(snapshot, source_table)
                source_activity = None
                if source_node is not None:
                    source_activity = source_activity_cache.get(source_node["id"])
                    if source_activity is None:
                        source_activity = self._collect_table_node_activity(snapshot, source_node, run_limit)
                        source_activity_cache[source_node["id"]] = source_activity
                    for run in source_activity["recent_runs"]:
                        if run.get("record_id"):
                            source_run_map[run["record_id"]] = run
                    for producer in source_activity["producers"]:
                        source_producer_keys.add(
                            (
                                producer.get("node_id"),
                                producer.get("task_resource_id"),
                            )
                        )
                source_activities.append(
                    {
                        **source_table,
                        "resolved_lineage_node": self._node_preview(source_node) if source_node is not None else None,
                        "producer_count": len(source_activity["producers"]) if source_activity else 0,
                        "producers": source_activity["producers"] if source_activity else [],
                        "recent_runs": source_activity["recent_runs"] if source_activity else [],
                        "schedule_summary": source_activity["schedule_summary"] if source_activity else {},
                    }
                )
            pipelines.append(
                {
                    "target_table_id": target["target_table_id"],
                    "pipeline_name": target["pipeline_name"],
                    "task_id": target["task_id"],
                    "config_status": target["config_status"],
                    "to_table_mode": target["to_table_mode"],
                    "full_name": target["full_name"],
                    "source_tables": target["source_tables"],
                    "pipeline_state": pipeline_state,
                    "pipeline_runs": pipeline_runs,
                    "pipeline_schedule_summary": self._summarize_pipeline_schedule(pipeline_state, pipeline_runs),
                    "source_activities": source_activities,
                }
            )

        recent_source_runs = sorted(
            source_run_map.values(),
            key=lambda item: item.get("event_time") or 0,
            reverse=True,
        )[:run_limit]
        merged_target_activity = self._merge_table_node_activities(
            list(target_activity_cache.values()),
            run_limit,
        )
        merged_source_activity = self._merge_table_node_activities(
            list(source_activity_cache.values()),
            run_limit,
        )
        lineage_resolution = "target_table"
        if not merged_target_activity["producers"] and merged_source_activity["producers"]:
            # Source-side producers are exposed separately as reference data only.
            # The target table's main lineage fields must remain target-side only;
            # otherwise a source table's producer task would be misrepresented as
            # the current target table's direct output task.
            lineage_resolution = "source_fallback"

        # 用连接级血缘(fdl_connection_lineage)佐证：源侧产出任务里，哪些真的写到了"目标表所在连接"。
        # 这能在 source_fallback 时把"真正产出目标表的任务"从"只写源表的任务"中区分出来。
        target_conns = {(t.get("connection_name") or "").lower() for t in upstream["targets"] if t.get("connection_name")}
        conn_nodes_by_work = snapshot.get("conn_nodes_by_work_name", {})
        for producer in merged_source_activity["producers"]:
            wn = producer.get("task_name") or ""
            works = conn_nodes_by_work.get(wn, {})
            landing_nodes = set()
            for tc in target_conns:
                landing_nodes |= works.get(tc, set())
            producer["writes_target_connection"] = bool(landing_nodes)
            producer["target_landing_nodes"] = sorted(n for n in landing_nodes if n)

        return {
            "loaded_at": upstream["loaded_at"],
            "match_count": upstream["match_count"],
            "targets": upstream["targets"],
            "pipelines": pipelines,
            "resolved_lineage_nodes": [
                self._node_preview(node)
                for node in sorted(resolved_target_nodes.values(), key=lambda item: (item["display_name"], item["id"]))
            ],
            "lineage_resolution": lineage_resolution,
            "lineage_producer_count": len(merged_target_activity["producers"]),
            "lineage_consumer_count": len(merged_target_activity["consumers"]),
            "lineage_producers": merged_target_activity["producers"],
            "lineage_consumers": merged_target_activity["consumers"],
            "recent_lineage_runs": merged_target_activity["recent_runs"],
            "lineage_schedule_summary": merged_target_activity["schedule_summary"],
            "target_lineage_producer_count": len(merged_target_activity["producers"]),
            "target_lineage_consumer_count": len(merged_target_activity["consumers"]),
            "target_lineage_producers": merged_target_activity["producers"],
            "target_lineage_consumers": merged_target_activity["consumers"],
            "target_recent_lineage_runs": merged_target_activity["recent_runs"],
            "target_lineage_schedule_summary": merged_target_activity["schedule_summary"],
            "source_lineage_producer_count": len(merged_source_activity["producers"]),
            "source_lineage_consumer_count": len(merged_source_activity["consumers"]),
            "source_lineage_producers": merged_source_activity["producers"],
            "source_lineage_consumers": merged_source_activity["consumers"],
            "source_recent_lineage_runs": merged_source_activity["recent_runs"],
            "source_lineage_schedule_summary": merged_source_activity["schedule_summary"],
            "recent_source_runs": recent_source_runs,
            "source_schedule_summary": self._summarize_schedule_runs(recent_source_runs, len(source_producer_keys)),
        }

    def get_table_schedule_info(
        self,
        connection_name: Optional[str] = None,
        schema: Optional[str] = None,
        table: Optional[str] = None,
        table_comment: Optional[str] = None,
        run_limit: int = 10,
    ) -> Dict[str, Any]:
        """表→任务直查链路（不依赖 fdl_lineage_node）：

        覆盖"表无血缘但有任务"的场景（如手工/单点同步任务、被采集遗漏的任务）。
        路径：
          1) connection_lineage：按 connection_id 圈出所有 work_id
          2) work_info：用任务名匹配表名（英文/中文/comment）筛出"看起来跟此表相关"的任务
          3) plan_work + plan_schedule：取调度计划与频率
          4) work_last_record：取最近运行历史
        返回结构与 _collect_table_node_activity 兼容（producers/recent_runs/schedule_summary）。
        """
        if not table:
            raise ValueError("table is required")
        if run_limit < 1 or run_limit > 50:
            raise ValueError("run_limit must be between 1 and 50")

        snapshot = self._ensure_snapshot()
        conn_lower = (connection_name or "").lower()

        # 1) 收集"碰过此 connection"的 work_id 集合
        candidate_work_ids: set = set()
        candidate_work_meta: Dict[str, Dict[str, str]] = {}  # work_id -> {work_name, node_names, operator_types}
        for c in snapshot.get("connections", []) or []:
            cid = (c.get("connection_id") or "").lower()
            if conn_lower and cid != conn_lower:
                continue
            wid = c.get("work_id") or ""
            if not wid:
                continue
            candidate_work_ids.add(wid)
            meta = candidate_work_meta.setdefault(wid, {
                "work_name": c.get("work_name") or "",
                "node_names": set(),
                "operator_types": set(),
            })
            if c.get("node_name"):
                meta["node_names"].add(c["node_name"])
            if c.get("operator_type"):
                meta["operator_types"].add(c["operator_type"])

        # 2) 在 work_info 里按表名/comment 精确或包含匹配，圈出"很可能跟本表相关"的 work
        work_info_rows = self._query(
            "SELECT id, resource_id, name, resource_type FROM fdldb.fdl_work_info"
        )
        needles = [n for n in [
            (table or "").lower(),
            (table_comment or "").strip(),
        ] if n]
        matched_work_ids: List[str] = []
        match_precision: Dict[str, str] = {}   # work_id -> "exact" | "contains"
        # 优先精确匹配（任务名 == 表名 或 任务名 == 表中文注释）
        for needle in needles:
            for w in work_info_rows:
                wid = w.get("resource_id") or ""
                if not wid:
                    continue
                if candidate_work_ids and wid not in candidate_work_ids:
                    continue
                name = (w.get("name") or "").strip()
                if not name:
                    continue
                if name.lower() == needle.lower() and wid not in matched_work_ids:
                    matched_work_ids.append(wid)
                    match_precision[wid] = "exact"

        # 二轮：仅在精确匹配为空时才做包含匹配（避免"商机明细"被"商机"模糊带入）
        if not matched_work_ids:
            for needle in needles:
                if len(needle) < 4:
                    continue   # 太短的关键词容易误匹配
                for w in work_info_rows:
                    wid = w.get("resource_id") or ""
                    name = (w.get("name") or "").strip()
                    if not wid or not name:
                        continue
                    if candidate_work_ids and wid not in candidate_work_ids:
                        continue
                    if wid in matched_work_ids:
                        continue
                    if needle.lower() in name.lower():
                        matched_work_ids.append(wid)
                        match_precision[wid] = "contains"

        work_info_by_id = {w.get("resource_id") or "": w for w in work_info_rows if w.get("resource_id")}

        # 3) 取调度计划 + 4) 运行记录
        schedule_meta = self._query_task_schedule_meta(matched_work_ids) if matched_work_ids else {}
        task_contexts: Dict[str, Dict[str, str]] = {}
        for wid in matched_work_ids:
            wi = work_info_by_id.get(wid, {})
            task_contexts[wid] = {
                "task_name": wi.get("name") or "",
                "node_name": "",
                "task_node_resource_id": wid,
            }
        recent_runs = self._query_recent_work_runs_for_tasks(
            matched_work_ids,
            limit=run_limit,
            task_contexts=task_contexts,
        ) if matched_work_ids else []

        # 装配 producers（每个匹配任务一条）
        producers: List[Dict[str, Any]] = []
        for wid in matched_work_ids:
            wi = work_info_by_id.get(wid, {})
            sm = schedule_meta.get(wid, {})
            cmeta = candidate_work_meta.get(wid, {})
            # 单任务的最近运行
            task_runs = [r for r in recent_runs if r.get("task_id") == wid][:run_limit]
            producers.append({
                "node_id": "",
                "task_resource_id": wid,
                "task_node_resource_id": wid,
                "task_name": wi.get("name") or "",
                "node_name": "、".join(sorted(cmeta.get("node_names", set()))) if cmeta.get("node_names") else "",
                "resource_type": wi.get("resource_type") or "",
                "match_precision": match_precision.get(wid, ""),
                "schedule_plan_id": sm.get("plan_id") or "",
                "schedule_plan_name": sm.get("plan_name") or "",
                "schedule_type": sm.get("schedule_type") or "",
                "schedule_cycle_text": sm.get("schedule_cycle_text") or "",
                "schedule_start_time_text": sm.get("schedule_start_time_text") or "",
                "operator_types": sorted(cmeta.get("operator_types", set())),
                "recent_runs": task_runs,
                "schedule_summary": self._summarize_schedule_runs(task_runs, 1),
            })

        return {
            "loaded_at": snapshot.get("loaded_at"),
            "table": {"connection_name": connection_name or "", "schema": schema or "", "table": table or ""},
            "match_via": "task_name_match" if matched_work_ids else "no_match",
            "candidate_work_count": len(candidate_work_ids),
            "matched_work_count": len(matched_work_ids),
            "producers": producers,
            "consumers": [],
            "recent_runs": recent_runs,
            "schedule_summary": self._summarize_schedule_runs(recent_runs, len(matched_work_ids)),
        }

    def get_graph_task_activity(
        self,
        node_id: str,
        run_limit: int = 10,
    ) -> Dict[str, Any]:
        if run_limit < 1 or run_limit > 50:
            raise ValueError("run_limit must be between 1 and 50")

        snapshot = self._ensure_snapshot()
        node = snapshot["nodes_by_id"].get(node_id)
        if node is None:
            raise ValueError(f"node_id not found: {node_id}")
        if node["resource_type"] == "DB_TABLE":
            raise ValueError(f"node_id is not a task node: {node_id}")

        task_resource_id = self._graph_task_resource_id(node)
        recent_runs = self._query_recent_work_runs(
            task_resource_id,
            run_limit,
            {
                task_resource_id: {
                    "task_name": node.get("work_name") or node.get("display_name"),
                    "node_name": node.get("node_name") or node.get("display_name"),
                    "task_node_resource_id": node.get("resource_id") or "",
                }
            } if task_resource_id else None,
        ) if task_resource_id else []

        inputs = self._collect_neighbor_table_nodes(snapshot, node, direction="upstream")
        outputs = self._collect_neighbor_table_nodes(snapshot, node, direction="downstream")

        return {
            "loaded_at": snapshot["loaded_at"],
            "task": self._node_payload(node),
            "task_resource_id": task_resource_id,
            "input_table_count": len(inputs),
            "output_table_count": len(outputs),
            "input_tables": inputs,
            "output_tables": outputs,
            "recent_runs": recent_runs,
            "schedule_summary": self._summarize_schedule_runs(recent_runs, 1),
        }

    def get_graph(self, graph_id: str, include_connections: bool = True) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        graph = snapshot["graphs"].get(graph_id)
        if graph is None:
            raise ValueError(f"graph_id not found: {graph_id}")

        node_ids = {node["id"] for node in graph["nodes"]}
        valid_edges = []
        dangling_edges = []
        for edge in graph["edges"]:
            if edge["source_id"] in node_ids and edge["target_id"] in node_ids:
                valid_edges.append(edge)
            else:
                dangling_edges.append(edge)

        result = {
            "loaded_at": snapshot["loaded_at"],
            "graph_id": graph_id,
            "summary": {
                "node_count": len(graph["nodes"]),
                "edge_count": len(valid_edges),
                "dangling_edge_count": len(dangling_edges),
                "node_type_counts": graph["node_type_counts"],
            },
            "nodes": [self._node_payload(node) for node in graph["nodes"]],
            "edges": [self._edge_payload(edge) for edge in valid_edges],
            "dangling_edges": [self._edge_payload(edge) for edge in dangling_edges],
        }
        if include_connections:
            result["connections"] = self._connections_for_graph(snapshot, graph["nodes"])
        return result

    def get_upstream(self, node_id: str, depth: int = 2, include_connections: bool = True) -> Dict[str, Any]:
        return self._walk(node_id=node_id, depth=depth, direction="upstream", include_connections=include_connections)

    def get_downstream(self, node_id: str, depth: int = 2, include_connections: bool = True) -> Dict[str, Any]:
        return self._walk(node_id=node_id, depth=depth, direction="downstream", include_connections=include_connections)

    def find_path(self, source_id: str, target_id: str, max_depth: int = 8) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        source = snapshot["nodes_by_id"].get(source_id)
        target = snapshot["nodes_by_id"].get(target_id)
        if source is None:
            raise ValueError(f"source_id not found: {source_id}")
        if target is None:
            raise ValueError(f"target_id not found: {target_id}")
        if source["graph_id"] != target["graph_id"]:
            return {
                "loaded_at": snapshot["loaded_at"],
                "found": False,
                "reason": "source and target belong to different graph_id values",
                "source": self._node_preview(source),
                "target": self._node_preview(target),
            }

        graph_id = source["graph_id"]
        adjacency = snapshot["out_edges_by_graph"][graph_id]
        queue: deque[Tuple[str, int]] = deque([(source_id, 0)])
        visited = {source_id}
        previous: Dict[str, Tuple[str, Dict[str, Any]]] = {}

        while queue:
            current_id, current_depth = queue.popleft()
            if current_id == target_id:
                break
            if current_depth >= max_depth:
                continue
            for edge in adjacency.get(current_id, []):
                next_id = edge["target_id"]
                if next_id in visited:
                    continue
                visited.add(next_id)
                previous[next_id] = (current_id, edge)
                queue.append((next_id, current_depth + 1))

        if target_id not in previous and source_id != target_id:
            return {
                "loaded_at": snapshot["loaded_at"],
                "found": False,
                "reason": "no directed path found within max_depth",
                "source": self._node_preview(source),
                "target": self._node_preview(target),
                "max_depth": max_depth,
            }

        path_node_ids = [target_id]
        path_edges: List[Dict[str, Any]] = []
        current = target_id
        while current != source_id:
            prev_id, edge = previous[current]
            path_edges.append(edge)
            path_node_ids.append(prev_id)
            current = prev_id
        path_node_ids.reverse()
        path_edges.reverse()

        return {
            "loaded_at": snapshot["loaded_at"],
            "found": True,
            "graph_id": graph_id,
            "hop_count": len(path_edges),
            "nodes": [self._node_payload(snapshot["nodes_by_id"][node_id]) for node_id in path_node_ids],
            "edges": [self._edge_payload(edge) for edge in path_edges],
        }

    def refresh_cache(self) -> Dict[str, Any]:
        snapshot = self._load_snapshot()
        return {
            "loaded_at": snapshot["loaded_at"],
            "node_count": len(snapshot["nodes"]),
            "edge_count": len(snapshot["edges"]),
            "connection_count": len(snapshot["connections"]),
            "graph_count": len(snapshot["graphs"]),
        }

    def _walk(
        self,
        node_id: str,
        depth: int,
        direction: str,
        include_connections: bool,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        start = snapshot["nodes_by_id"].get(node_id)
        if start is None:
            raise ValueError(f"node_id not found: {node_id}")

        graph_id = start["graph_id"]
        queue: deque[Tuple[str, int]] = deque([(node_id, 0)])
        visited = {node_id}
        collected_nodes = {node_id}
        collected_edges: Dict[str, Dict[str, Any]] = {}

        if direction == "upstream":
            edge_index = snapshot["in_edges_by_graph"][graph_id]
            next_key = "source_id"
        else:
            edge_index = snapshot["out_edges_by_graph"][graph_id]
            next_key = "target_id"

        while queue:
            current_id, current_depth = queue.popleft()
            if current_depth >= depth:
                continue
            for edge in edge_index.get(current_id, []):
                neighbor_id = edge[next_key]
                collected_edges[edge["id"]] = edge
                collected_nodes.add(neighbor_id)
                if neighbor_id in visited:
                    continue
                visited.add(neighbor_id)
                queue.append((neighbor_id, current_depth + 1))

        nodes = [snapshot["nodes_by_id"][item_id] for item_id in collected_nodes if item_id in snapshot["nodes_by_id"]]
        result = {
            "loaded_at": snapshot["loaded_at"],
            "direction": direction,
            "depth": depth,
            "graph_id": graph_id,
            "center": self._node_payload(start),
            "nodes": [self._node_payload(node) for node in sorted(nodes, key=lambda item: (item["display_name"], item["id"]))],
            "edges": [self._edge_payload(edge) for edge in sorted(collected_edges.values(), key=lambda item: item["id"])],
        }
        if include_connections:
            result["connections"] = self._connections_for_graph(snapshot, nodes)
        return result

    def _connections_for_graph(self, snapshot: Dict[str, Any], nodes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        group_ids = {node["group_id"] for node in nodes if node["group_id"]}
        resource_ids = {node["resource_id"] for node in nodes if node["resource_id"]}
        node_ids = {node["id"] for node in nodes}
        attached: Dict[str, Dict[str, Any]] = {}

        for connection in snapshot["connections"]:
            matched_nodes = set()
            linked_by = []

            if connection["resource_id"] in group_ids:
                for mapped_node_id in snapshot["node_ids_by_group_id"].get(connection["resource_id"], []):
                    if mapped_node_id in node_ids:
                        matched_nodes.add(mapped_node_id)
                if matched_nodes:
                    linked_by.append("work_id")

            if connection["node_id"] and connection["node_id"] in resource_ids:
                for mapped_node_id in snapshot["node_ids_by_resource_id"].get(connection["node_id"], []):
                    if mapped_node_id in node_ids:
                        matched_nodes.add(mapped_node_id)
                if matched_nodes:
                    linked_by.append("node_id")

            if not matched_nodes:
                continue

            attached[connection["id"]] = self._connection_payload(connection, sorted(matched_nodes), linked_by)

        return sorted(attached.values(), key=lambda item: (item["connection_id"], item["id"]))

    def _pick_best_lineage_node(self, snapshot: Dict[str, Any], source_table: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        key = (
            (source_table.get("fdl_database") or "").lower(),
            (source_table.get("fdl_schema") or "").lower(),
            (source_table.get("fdl_table") or "").lower(),
        )
        matches = snapshot["nodes_by_db_schema_table"].get(key, [])
        if not matches:
            return None

        database_prefix = f"{key[0]}." if key[0] else ""

        def rank(node: Dict[str, Any]) -> Tuple[int, str]:
            score = 0
            resource_id = (node.get("resource_id") or "").lower()
            if database_prefix and resource_id.startswith(database_prefix):
                score += 4
            if (node.get("group_id") or "").lower() == key[0]:
                score += 3
            if (node.get("database_name") or "").lower() == key[0]:
                score += 2
            if node.get("resource_type") == "DB_TABLE":
                score += 1
            return (score, resource_id)

        return sorted(matches, key=rank, reverse=True)[0]

    def _collect_all_target_lineage_nodes(
        self,
        snapshot: Dict[str, Any],
        target: Dict[str, Any],
    ) -> List[Dict[str, Any]]:
        """收集所有匹配 target_table 的 lineage_node（DB_TABLE）节点。

        与 _pick_best_target_lineage_node 不同，这里返回全部命中的节点，因为
        下游/消费者会跨多个 graph 分布，单选会漏掉真实下游任务。

        匹配规则（必须严格，避免把不同物理库下的同名表合并）：
        1. table_name 精确匹配；
        2. connection_name 必须一致 —— 同名表分布在不同库时，靠这个区分；
        3. schema 在 lineage_node 里很多 DB_TABLE 没填，所以两边都非空时才比较。
        """
        needle_table = (target.get("fdl_table") or "").lower()
        needle_schema = (target.get("fdl_schema") or "").lower()
        needle_connection = (target.get("connection_name") or "").lower()
        if not needle_table or not needle_connection:
            return []
        result: List[Dict[str, Any]] = []
        for node in snapshot["nodes"]:
            if node["resource_type"] != "DB_TABLE":
                continue
            if (node.get("table_name") or "").lower() != needle_table:
                continue
            # 关键：connection 必须一致，禁止跨 connection 合并
            if (node.get("connection_name") or "").lower() != needle_connection:
                continue
            node_schema = (node.get("schema_name") or "").lower()
            if needle_schema and node_schema and node_schema != needle_schema:
                continue
            result.append(node)
        return result

    def _pick_best_target_lineage_node(
        self,
        snapshot: Dict[str, Any],
        target: Dict[str, Any],
    ) -> Optional[Dict[str, Any]]:
        needle_table = (target.get("fdl_table") or "").lower()
        needle_schema = (target.get("fdl_schema") or "").lower()
        needle_database = (target.get("fdl_database") or "").lower()
        needle_connection = (target.get("connection_name") or "").lower()
        if not needle_table:
            return None

        candidates: List[Tuple[Tuple[int, int, int, str], Dict[str, Any]]] = []
        for node in snapshot["nodes"]:
            if node["resource_type"] != "DB_TABLE":
                continue
            if node["table_name"].lower() != needle_table:
                continue
            if needle_schema and node["schema_name"].lower() != needle_schema:
                continue

            score = 0
            if needle_connection and node["connection_name"].lower() == needle_connection:
                score += 6
            if needle_database and node["database_name"].lower() == needle_database:
                score += 5
            if needle_database and node["connection_name"].lower() == needle_database:
                score += 2
            resource_id = (node.get("resource_id") or "").lower()
            if needle_connection and resource_id.startswith(f"{needle_connection}."):
                score += 1
            if needle_database and resource_id.startswith(f"{needle_database}."):
                score += 1

            sort_key = (
                score,
                1 if node["database_name"].lower() == node["connection_name"].lower() else 0,
                1 if node["schema_name"] else 0,
                node["resource_id"] or "",
            )
            candidates.append((sort_key, node))

        if not candidates:
            return None

        candidates.sort(key=lambda item: item[0], reverse=True)
        return candidates[0][1]

    def _resolve_table_node(
        self,
        snapshot: Dict[str, Any],
        table: Optional[str],
        database_name: Optional[str],
        schema_name: Optional[str],
        connection_name: Optional[str],
        node_id: Optional[str],
    ) -> Tuple[Dict[str, Any], List[Dict[str, Any]]]:
        if node_id:
            node = snapshot["nodes_by_id"].get(node_id)
            if node is None:
                raise ValueError(f"node_id not found: {node_id}")
            if node["resource_type"] != "DB_TABLE":
                raise ValueError(f"node_id is not a DB_TABLE node: {node_id}")
            return node, [node]

        if not table:
            raise ValueError("table is required when node_id is not provided")

        needle_table = table.lower()
        needle_database = (database_name or "").lower()
        needle_schema = (schema_name or "").lower()
        needle_connection = (connection_name or "").lower()

        candidates: List[Tuple[Tuple[int, int, int, str], Dict[str, Any]]] = []
        for node in snapshot["nodes"]:
            if node["resource_type"] != "DB_TABLE":
                continue
            if node["table_name"].lower() != needle_table:
                continue
            if database_name and node["database_name"].lower() != needle_database:
                continue
            if schema_name and node["schema_name"].lower() != needle_schema:
                continue
            if connection_name and node["connection_name"].lower() != needle_connection:
                continue

            score = 0
            if database_name and node["database_name"].lower() == needle_database:
                score += 4
            if schema_name and node["schema_name"].lower() == needle_schema:
                score += 3
            if connection_name and node["connection_name"].lower() == needle_connection:
                score += 5
            if database_name and node["connection_name"].lower() == needle_database:
                score += 2
            if node["resource_id"].lower().startswith(f"{node['connection_name'].lower()}."):
                score += 1

            sort_key = (
                score,
                1 if node["database_name"].lower() == node["connection_name"].lower() else 0,
                1 if node["schema_name"] else 0,
                node["resource_id"] or "",
            )
            candidates.append((sort_key, node))

        if not candidates:
            qualified_name = ".".join(filter(None, [connection_name, database_name, schema_name, table]))
            raise ValueError(f"table node not found: {qualified_name or table}")

        candidates.sort(key=lambda item: item[0], reverse=True)
        matched_nodes = [item[1] for item in candidates]
        return matched_nodes[0], matched_nodes

    def _related_targets_for_node(self, snapshot: Dict[str, Any], node: Dict[str, Any]) -> List[Dict[str, Any]]:
        lookup_key = (
            node["database_name"].lower(),
            node["schema_name"].lower(),
            node["table_name"].lower(),
        )
        related_targets = snapshot["target_tables_by_source_lookup"].get(lookup_key, [])
        deduped = {target["target_table_id"]: target for target in related_targets}
        return sorted(deduped.values(), key=lambda item: (item["full_name"], item["target_table_id"]))

    def _graph_task_resource_id(self, node: Dict[str, Any]) -> str:
        if node["resource_type"] == "DB_TABLE":
            return ""
        return (
            node.get("group_id")
            or node.get("resource_info", {}).get("workId")
            or ""
        )

    def _query_recent_work_runs(
        self,
        task_resource_id: str,
        limit: int,
        task_contexts: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        if not task_resource_id:
            return []
        return self._query_recent_work_runs_for_tasks(
            [task_resource_id],
            limit=limit,
            task_contexts=task_contexts,
        )

    def _query_recent_work_runs_for_tasks(
        self,
        task_resource_ids: List[str],
        limit: int,
        task_contexts: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> List[Dict[str, Any]]:
        deduped_task_ids = [item for item in dict.fromkeys(task_resource_ids) if item]
        if not deduped_task_ids:
            return []

        quoted = ",".join(["%s"] * len(deduped_task_ids))
        rows = self._query(
            f"""
            SELECT id, taskId, taskStatus, startTime, finishTime, triggerTime, triggerMethod, triggerBy, path
            FROM fdldb.fdl_work_last_record
            WHERE taskId IN ({quoted})
            ORDER BY COALESCE(finishTime, startTime, triggerTime, lastModifiedTime) DESC
            LIMIT {limit}
            """,
            tuple(deduped_task_ids),
        )
        trigger_ids = sorted({row.get("triggerBy") for row in rows if row.get("triggerBy")})
        plan_names = self._query_schedule_plan_names(trigger_ids)
        result = []
        for row in rows:
            trigger_by = row.get("triggerBy") or ""
            task_id = row.get("taskId") or ""
            task_context = (task_contexts or {}).get(task_id, {})
            start_time = self._safe_int(row.get("startTime"))
            finish_time = self._safe_int(row.get("finishTime"))
            trigger_time = self._safe_int(row.get("triggerTime"))
            event_time = self._pick_run_event_time_millis(
                finish_time=finish_time,
                start_time=start_time,
                trigger_time=trigger_time,
            )
            duration_ms = finish_time - start_time if finish_time and start_time and finish_time >= start_time else None
            result.append(
                {
                    "record_id": row.get("id"),
                    "task_id": task_id,
                    "task_name": task_context.get("task_name") or "",
                    "node_name": task_context.get("node_name") or "",
                    "task_node_resource_id": task_context.get("task_node_resource_id") or "",
                    "task_status": row.get("taskStatus"),
                    "start_time": start_time,
                    "start_time_text": self._format_epoch_millis(start_time),
                    "finish_time": finish_time,
                    "finish_time_text": self._format_epoch_millis(finish_time),
                    "trigger_time": trigger_time,
                    "trigger_time_text": self._format_epoch_millis(trigger_time),
                    "event_time": event_time,
                    "event_time_text": self._format_epoch_millis(event_time),
                    "duration_ms": duration_ms,
                    "duration_text": self._format_duration(duration_ms),
                    "trigger_method": row.get("triggerMethod") or "",
                    "trigger_by": trigger_by,
                    "trigger_plan_name": plan_names.get(trigger_by, ""),
                    "path": row.get("path") or "",
                    "is_running": self._is_running_task_run(
                        task_status=row.get("taskStatus"),
                        finish_time=finish_time,
                    ),
                }
            )
        return result

    def _query_schedule_plan_names(self, plan_ids: List[str]) -> Dict[str, str]:
        if not plan_ids:
            return {}
        quoted = ",".join(["%s"] * len(plan_ids))
        rows = self._query(
            f"""
            SELECT plan_id, plan_detail
            FROM fdldb.fdl_plan_schedule
            WHERE plan_id IN ({quoted})
            """,
            tuple(plan_ids),
        )
        mapping = {}
        for row in rows:
            detail = self._parse_json(row.get("plan_detail"))
            mapping[row.get("plan_id")] = detail.get("name") or row.get("plan_id") or ""
        return mapping

    def _query_task_schedule_meta(self, task_resource_ids: List[str]) -> Dict[str, Dict[str, str]]:
        deduped_task_ids = [item for item in dict.fromkeys(task_resource_ids) if item]
        if not deduped_task_ids:
            return {}

        quoted = ",".join(["%s"] * len(deduped_task_ids))
        rows = self._query(
            f"""
            SELECT pw.work_id, pw.plan_id, pw.work_type, ps.plan_detail, ps.schedule_type, ps.schedule
            FROM fdldb.fdl_plan_work pw
            LEFT JOIN fdldb.fdl_plan_schedule ps
              ON ps.plan_id = pw.plan_id
            WHERE pw.work_id IN ({quoted})
            """,
            tuple(deduped_task_ids),
        )

        mapping: Dict[str, Dict[str, str]] = {}
        for row in rows:
            work_id = row.get("work_id") or ""
            if not work_id or work_id in mapping:
                continue
            plan_detail = self._parse_json(row.get("plan_detail"))
            schedule = self._parse_json(row.get("schedule"))
            mapping[work_id] = {
                "plan_id": row.get("plan_id") or "",
                "plan_name": plan_detail.get("name") or row.get("plan_id") or "",
                "schedule_type": row.get("schedule_type") or row.get("work_type") or "",
                "schedule_cycle_text": self._format_schedule_cycle(schedule, row.get("schedule_type") or row.get("work_type")),
                "schedule_start_time_text": self._format_schedule_start_time(schedule),
            }
        return mapping

    def _format_schedule_start_time(self, schedule: Dict[str, Any]) -> str:
        start_time = ((schedule.get("startTime") or {}).get("value")) if isinstance(schedule, dict) else None
        if isinstance(start_time, str):
            return start_time
        return ""

    def _format_schedule_cycle(self, schedule: Dict[str, Any], schedule_type: Any) -> str:
        if not isinstance(schedule, dict) or not schedule:
            return self._format_schedule_type_text(schedule_type)

        if schedule.get("scheduleOpen") is False:
            return "未开启"

        frequency = schedule.get("frequency") or {}
        freq_type = frequency.get("type")
        value = frequency.get("value") or {}
        cron = value.get("cron")
        space = value.get("space")
        unit = value.get("unit")
        execute_time = value.get("executeTime")
        execute_day = value.get("executeDay")
        execute_month = value.get("executeMonth")

        if cron:
            return f"Cron: {cron}"
        if freq_type == 1:
            return "一次性"
        if freq_type == 2 and space and unit is not None:
            unit_label = {
                1: "分钟",
                2: "小时",
                3: "天",
                4: "周",
                5: "月",
            }.get(unit, f"单位{unit}")
            return f"每{space}{unit_label}"
        if execute_time:
            parts = [f"执行时间 {execute_time}"]
            if execute_day:
                parts.append(f"执行日 {execute_day}")
            if execute_month:
                parts.append(f"执行月 {execute_month}")
            return " / ".join(parts)

        return self._format_schedule_type_text(schedule_type)

    def _format_schedule_type_text(self, schedule_type: Any) -> str:
        raw = str(schedule_type or "").upper()
        if raw == "TIME":
            return "定时调度"
        if raw == "TIMING":
            return "定时计划"
        if raw == "EVENT":
            return "事件触发"
        return str(schedule_type or "")

    def _safe_int(self, value: Any) -> Optional[int]:
        if value in {None, ""}:
            return None
        try:
            number = int(value)
        except (TypeError, ValueError):
            return None
        return number if number > 0 else None

    def _pick_run_event_time_millis(
        self,
        finish_time: Optional[int],
        start_time: Optional[int],
        trigger_time: Optional[int],
    ) -> Optional[int]:
        return finish_time or start_time or trigger_time

    def _format_duration(self, duration_ms: Optional[int]) -> str:
        if duration_ms is None or duration_ms < 0:
            return ""
        total_seconds = duration_ms // 1000
        minutes, seconds = divmod(total_seconds, 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours}h {minutes}m {seconds}s"
        if minutes > 0:
            return f"{minutes}m {seconds}s"
        return f"{seconds}s"

    def _is_running_task_run(self, task_status: Any, finish_time: Optional[int]) -> bool:
        status = str(task_status or "").upper()
        terminal_statuses = {"SUCCESS", "ERROR", "SKIPPED", "FAILED", "CANCELLED", "STOPPED"}
        if status in terminal_statuses:
            return False
        return finish_time in {None, 0}

    def _query_pipeline_task_state(self, task_id: str) -> Dict[str, Any]:
        rows = self._query(
            """
            SELECT id, create_time, first_start_time, last_start_time, status, update_time, work_node_id, start_message
            FROM fdldb.fdl_pipe_task_record
            WHERE id=%s
            """,
            (task_id,),
        )
        if not rows:
            rows = self._query(
                """
                SELECT id, create_time, first_start_time, last_start_time, status, update_time, work_node_id, start_message
                FROM fdldb.fdl_pipeline_task_record
                WHERE id=%s
                """,
                (task_id,),
            )
        if not rows:
            return {}

        row = rows[0]
        steps = self._parse_json_list(row.get("start_message"))
        return {
            "task_id": row.get("id"),
            "status": row.get("status") or "",
            "create_time": row.get("create_time"),
            "create_time_text": self._format_epoch_millis(row.get("create_time")),
            "first_start_time": row.get("first_start_time"),
            "first_start_time_text": self._format_epoch_millis(row.get("first_start_time")),
            "last_start_time": row.get("last_start_time"),
            "last_start_time_text": self._format_epoch_millis(row.get("last_start_time")),
            "update_time": row.get("update_time"),
            "update_time_text": self._format_epoch_millis(row.get("update_time")),
            "work_node_id": row.get("work_node_id") or "",
            "check_steps": steps,
        }

    def _collect_table_node_activity(
        self,
        snapshot: Dict[str, Any],
        node: Dict[str, Any],
        run_limit: int,
    ) -> Dict[str, Any]:
        producers = self._collect_neighbor_task_nodes(snapshot, node, direction="upstream")
        consumers = self._collect_neighbor_task_nodes(snapshot, node, direction="downstream")

        # Also include pipelines that read this node as a source table (target_table consumers)
        related_targets = self._related_targets_for_node(snapshot, node)
        seen_task_ids = {c["task_resource_id"] for c in consumers if c.get("task_resource_id")}
        for target in related_targets:
            task_id = target.get("task_id") or ""
            if not task_id or task_id in seen_task_ids:
                continue
            seen_task_ids.add(task_id)
            consumers.append({
                "node_id": f"target::{target['full_name']}",
                "resource_type": "PIPELINE",
                "task_name": target.get("pipeline_name") or target["full_name"],
                "node_name": "",
                "pipeline_target": target["full_name"],
                "task_resource_id": task_id,
                "task_node_resource_id": task_id,
                "schedule_plan_id": "",
                "schedule_plan_name": "",
                "schedule_type": "",
                "schedule_cycle_text": "",
                "schedule_start_time_text": "",
            })

        # 通过"流向下游表"佐证真实下游业务任务（数据来源：fdl_connection_lineage）。
        # 一个任务若【产出本表】且【其节点也写到了下游表所在的连接】，说明它是把本表数据
        # 继续搬到下游的多节点任务（如"同步订单主体"先写 dwd，再用"数据同步"节点写 PG）。
        # fdl_lineage_node 常漏采这些落地节点，这里用连接级血缘补回。
        downstream_conns = set()
        downstream_table_names = []
        for target in related_targets:
            conn = (target.get("connection_name") or "").strip().lower()
            if conn:
                downstream_conns.add(conn)
            if target.get("full_name"):
                downstream_table_names.append(target["full_name"])
        downstream_table_names = list(dict.fromkeys(downstream_table_names))
        if downstream_conns and downstream_table_names:
            conn_nodes_by_work = snapshot.get("conn_nodes_by_work_name", {})
            seen_flow = {(c.get("task_name"), c.get("node_name")) for c in consumers}
            for producer in producers:
                wname = producer.get("task_name") or ""
                if not wname:
                    continue
                work_conns = conn_nodes_by_work.get(wname, {})
                landing_nodes = set()
                for conn in downstream_conns:
                    landing_nodes |= work_conns.get(conn, set())
                if not landing_nodes:
                    continue  # 该产出任务没碰下游连接 → 不是真下游（如"整合订单数据"只到 dwd）
                for lnode in sorted(n for n in landing_nodes if n):
                    if (wname, lnode) in seen_flow:
                        continue
                    seen_flow.add((wname, lnode))
                    consumers.append({
                        "node_id": producer.get("node_id") or "",
                        "resource_type": producer.get("resource_type") or "",
                        "task_name": wname,
                        "node_name": lnode,
                        "task_resource_id": producer.get("task_resource_id") or "",
                        "task_node_resource_id": producer.get("task_node_resource_id") or "",
                        "schedule_plan_id": producer.get("schedule_plan_id") or "",
                        "schedule_plan_name": producer.get("schedule_plan_name") or "",
                        "schedule_type": producer.get("schedule_type") or "",
                        "schedule_cycle_text": producer.get("schedule_cycle_text") or "",
                        "schedule_start_time_text": producer.get("schedule_start_time_text") or "",
                        "downstream_tables": downstream_table_names,
                        "is_downstream_flow": True,
                    })

        task_contexts = {
            item["task_resource_id"]: {
                "task_name": item["task_name"],
                "node_name": item["node_name"],
                "task_node_resource_id": item["task_node_resource_id"],
            }
            for item in producers
            if item["task_resource_id"]
        }
        recent_runs = self._query_recent_work_runs_for_tasks(
            list(task_contexts.keys()),
            limit=run_limit,
            task_contexts=task_contexts,
        )

        for producer in producers:
            if producer["task_resource_id"]:
                producer["recent_runs"] = self._query_recent_work_runs(
                    producer["task_resource_id"],
                    run_limit,
                    {
                        producer["task_resource_id"]: {
                            "task_name": producer["task_name"],
                            "node_name": producer["node_name"],
                            "task_node_resource_id": producer["task_node_resource_id"],
                        }
                    },
                )
            else:
                producer["recent_runs"] = []
            producer["schedule_summary"] = self._summarize_schedule_runs(producer["recent_runs"], 1)

        return {
            "producers": producers,
            "consumers": consumers,
            "recent_runs": recent_runs,
            "schedule_summary": self._summarize_schedule_runs(recent_runs, len(producers)),
        }

    def _merge_table_node_activities(
        self,
        activities: List[Dict[str, Any]],
        run_limit: int,
    ) -> Dict[str, Any]:
        valid = [item for item in activities if item]
        if not valid:
            return {
                "producers": [],
                "consumers": [],
                "recent_runs": [],
                "schedule_summary": self._summarize_schedule_runs([], 0),
            }

        producers = self._merge_task_activity_items(
            [item.get("producers", []) for item in valid],
            run_limit,
        )
        consumers = self._merge_task_activity_items(
            [item.get("consumers", []) for item in valid],
            run_limit,
        )
        recent_runs = self._merge_run_records(
            [item.get("recent_runs", []) for item in valid],
            run_limit,
        )
        return {
            "producers": producers,
            "consumers": consumers,
            "recent_runs": recent_runs,
            "schedule_summary": self._summarize_schedule_runs(recent_runs, len(producers)),
        }

    def _merge_task_activity_items(
        self,
        groups: List[List[Dict[str, Any]]],
        run_limit: int,
    ) -> List[Dict[str, Any]]:
        merged: Dict[Tuple[str, str, str, str, str], Dict[str, Any]] = {}
        for group in groups:
            for item in group:
                key = (
                    item.get("node_id") or "",
                    item.get("task_resource_id") or "",
                    item.get("task_name") or "",
                    item.get("node_name") or "",
                    item.get("resource_type") or "",
                )
                current = merged.get(key)
                if current is None:
                    current = dict(item)
                    if "recent_runs" in current:
                        current["recent_runs"] = list(current.get("recent_runs") or [])
                    if "source_table_names" in current:
                        current["source_table_names"] = list(dict.fromkeys(current.get("source_table_names") or []))
                    merged[key] = current
                    continue

                for field, value in item.items():
                    if field == "recent_runs":
                        current["recent_runs"] = self._merge_run_records(
                            [current.get("recent_runs", []), value or []],
                            run_limit,
                        )
                    elif field == "source_table_names":
                        current["source_table_names"] = list(
                            dict.fromkeys((current.get("source_table_names") or []) + (value or []))
                        )
                    elif value and not current.get(field):
                        current[field] = value

        result = []
        for item in merged.values():
            if "recent_runs" in item:
                item["schedule_summary"] = self._summarize_schedule_runs(item["recent_runs"], 1)
            result.append(item)
        result.sort(key=lambda item: (item.get("task_name") or "", item.get("node_name") or "", item.get("node_id") or ""))
        return result

    def _merge_run_records(
        self,
        groups: List[List[Dict[str, Any]]],
        limit: int,
    ) -> List[Dict[str, Any]]:
        merged: Dict[str, Dict[str, Any]] = {}
        for group in groups:
            for item in group:
                key = (
                    item.get("record_id")
                    or "||".join(
                        [
                            item.get("task_id") or "",
                            str(item.get("start_time") or ""),
                            str(item.get("finish_time") or ""),
                            str(item.get("trigger_time") or ""),
                            item.get("task_status") or "",
                        ]
                    )
                )
                if key not in merged:
                    merged[key] = item
        return sorted(
            merged.values(),
            key=lambda item: item.get("event_time") or 0,
            reverse=True,
        )[:limit]

    def _build_table_only_lineage_graph(
        self,
        snapshot: Dict[str, Any],
        start_node: Dict[str, Any],
        direction: str,
        depth: int,
    ) -> Dict[str, Any]:
        graph_nodes: Dict[str, Dict[str, Any]] = {}
        graph_edges: Dict[Tuple[str, str, str], Dict[str, Any]] = {}
        root_id = self._graph_node_id(start_node["id"])
        self._add_graph_node(graph_nodes, self._graph_node_payload(start_node))

        if direction in {"upstream", "both"}:
            self._expand_table_only_direction(
                snapshot=snapshot,
                graph_nodes=graph_nodes,
                graph_edges=graph_edges,
                start_node=start_node,
                direction="upstream",
                depth=depth,
            )
        if direction in {"downstream", "both"}:
            self._expand_table_only_direction(
                snapshot=snapshot,
                graph_nodes=graph_nodes,
                graph_edges=graph_edges,
                start_node=start_node,
                direction="downstream",
                depth=depth,
            )

        return {
            "root_node_id": root_id,
            "nodes": list(graph_nodes.values()),
            "edges": list(graph_edges.values()),
        }

    def _expand_table_only_direction(
        self,
        snapshot: Dict[str, Any],
        graph_nodes: Dict[str, Dict[str, Any]],
        graph_edges: Dict[Tuple[str, str, str], Dict[str, Any]],
        start_node: Dict[str, Any],
        direction: str,
        depth: int,
    ) -> None:
        frontier: deque[Tuple[str, int]] = deque([(start_node["id"], 0)])
        best_depth_by_node: Dict[str, int] = {start_node["id"]: 0}

        while frontier:
            current_table_id, current_depth = frontier.popleft()
            if current_depth >= depth:
                continue
            current_node = snapshot["nodes_by_id"].get(current_table_id)
            if current_node is None:
                continue

            adjacent_tables = self._find_adjacent_table_nodes(
                snapshot=snapshot,
                table_node=current_node,
                direction=direction,
            )
            for adjacent_node in adjacent_tables:
                adjacent_graph_node = self._graph_node_payload(adjacent_node)
                self._add_graph_node(graph_nodes, adjacent_graph_node)
                if direction == "upstream":
                    self._add_graph_edge(
                        graph_edges,
                        self._graph_node_id(adjacent_node["id"]),
                        self._graph_node_id(current_table_id),
                        "lineage",
                    )
                else:
                    self._add_graph_edge(
                        graph_edges,
                        self._graph_node_id(current_table_id),
                        self._graph_node_id(adjacent_node["id"]),
                        "lineage",
                    )

                next_depth = current_depth + 1
                existing_depth = best_depth_by_node.get(adjacent_node["id"])
                if existing_depth is None or next_depth < existing_depth:
                    best_depth_by_node[adjacent_node["id"]] = next_depth
                    frontier.append((adjacent_node["id"], next_depth))

    def _find_adjacent_table_nodes(
        self,
        snapshot: Dict[str, Any],
        table_node: Dict[str, Any],
        direction: str,
    ) -> List[Dict[str, Any]]:
        graph_id = table_node["graph_id"]
        if direction == "upstream":
            edge_index = snapshot["in_edges_by_graph"][graph_id]
            next_key = "source_id"
        else:
            edge_index = snapshot["out_edges_by_graph"][graph_id]
            next_key = "target_id"

        table_matches: Dict[str, Dict[str, Any]] = {}
        visited_non_table_ids = set()
        queue: deque[str] = deque()

        for edge in edge_index.get(table_node["id"], []):
            neighbor_id = edge[next_key]
            neighbor = snapshot["nodes_by_id"].get(neighbor_id)
            if neighbor is None:
                continue
            if neighbor["resource_type"] == "DB_TABLE":
                if neighbor["id"] != table_node["id"]:
                    table_matches[neighbor["id"]] = neighbor
                continue
            if neighbor_id not in visited_non_table_ids:
                visited_non_table_ids.add(neighbor_id)
                queue.append(neighbor_id)

        while queue:
            current_non_table_id = queue.popleft()
            for edge in edge_index.get(current_non_table_id, []):
                neighbor_id = edge[next_key]
                neighbor = snapshot["nodes_by_id"].get(neighbor_id)
                if neighbor is None:
                    continue
                if neighbor["resource_type"] == "DB_TABLE":
                    if neighbor["id"] != table_node["id"]:
                        table_matches[neighbor["id"]] = neighbor
                    continue
                if neighbor_id not in visited_non_table_ids:
                    visited_non_table_ids.add(neighbor_id)
                    queue.append(neighbor_id)

        return sorted(table_matches.values(), key=lambda item: (item["display_name"], item["id"]))

    def _collect_neighbor_task_nodes(
        self,
        snapshot: Dict[str, Any],
        node: Dict[str, Any],
        direction: str,
    ) -> List[Dict[str, Any]]:
        if direction == "upstream":
            edges = snapshot["in_edges_by_graph"][node["graph_id"]].get(node["id"], [])
            neighbor_key = "source_id"
        else:
            edges = snapshot["out_edges_by_graph"][node["graph_id"]].get(node["id"], [])
            neighbor_key = "target_id"

        neighbors = []
        seen_task_keys = set()
        for edge in edges:
            task_node = snapshot["nodes_by_id"].get(edge[neighbor_key])
            if task_node is None or task_node["resource_type"] == "DB_TABLE":
                continue
            task_resource_id = self._graph_task_resource_id(task_node)
            dedupe_key = (task_node["id"], task_resource_id)
            if dedupe_key in seen_task_keys:
                continue
            seen_task_keys.add(dedupe_key)
            neighbors.append(
                {
                    "node_id": task_node["id"],
                    "resource_type": task_node["resource_type"],
                    "task_name": task_node.get("work_name") or task_node.get("display_name"),
                    "node_name": task_node.get("node_name") or task_node.get("display_name"),
                    "task_resource_id": task_resource_id,
                    "task_node_resource_id": task_node.get("resource_id") or "",
                }
            )

        schedule_meta_by_task_id = self._query_task_schedule_meta(
            [item["task_resource_id"] for item in neighbors if item.get("task_resource_id")]
        )
        for item in neighbors:
            schedule_meta = schedule_meta_by_task_id.get(item.get("task_resource_id") or "", {})
            item["schedule_plan_id"] = schedule_meta.get("plan_id") or ""
            item["schedule_plan_name"] = schedule_meta.get("plan_name") or ""
            item["schedule_type"] = schedule_meta.get("schedule_type") or ""
            item["schedule_cycle_text"] = schedule_meta.get("schedule_cycle_text") or ""
            item["schedule_start_time_text"] = schedule_meta.get("schedule_start_time_text") or ""

        neighbors.sort(key=lambda item: (item["task_name"], item["node_name"], item["node_id"]))
        return neighbors

    def _collect_neighbor_table_nodes(
        self,
        snapshot: Dict[str, Any],
        node: Dict[str, Any],
        direction: str,
    ) -> List[Dict[str, Any]]:
        if direction == "upstream":
            edges = snapshot["in_edges_by_graph"][node["graph_id"]].get(node["id"], [])
            neighbor_key = "source_id"
        else:
            edges = snapshot["out_edges_by_graph"][node["graph_id"]].get(node["id"], [])
            neighbor_key = "target_id"

        tables = []
        seen_node_ids = set()
        for edge in edges:
            table_node = snapshot["nodes_by_id"].get(edge[neighbor_key])
            if table_node is None or table_node["resource_type"] != "DB_TABLE":
                continue
            if table_node["id"] in seen_node_ids:
                continue
            seen_node_ids.add(table_node["id"])
            tables.append(self._node_payload(table_node))
        tables.sort(key=lambda item: (item["display_name"], item["id"]))
        return tables

    def _summarize_schedule_runs(self, runs: List[Dict[str, Any]], task_count: int) -> Dict[str, Any]:
        latest_run = runs[0] if runs else None
        status_counts = Counter((item.get("task_status") or "UNKNOWN") for item in runs)
        running_count = sum(1 for item in runs if item.get("is_running"))
        last_success = next((item for item in runs if (item.get("task_status") or "").upper() == "SUCCESS"), None)
        last_error = next((item for item in runs if (item.get("task_status") or "").upper() == "ERROR"), None)

        judgement = "no_task"
        status_label = "未识别到产出任务"
        if running_count > 0:
            judgement = "running"
            status_label = "调度中"
        elif latest_run is not None:
            latest_status = (latest_run.get("task_status") or "").upper()
            if latest_status == "SUCCESS":
                judgement = "recent_success"
                status_label = "最近调度成功"
            elif latest_status == "ERROR":
                judgement = "recent_error"
                status_label = "最近调度失败"
            elif latest_status == "SKIPPED":
                judgement = "recent_skipped"
                status_label = "最近调度跳过"
            else:
                judgement = "recent_other"
                status_label = f"最近状态: {latest_status or '未知'}"
        elif task_count > 0:
            judgement = "no_runs"
            status_label = "未发现调度记录"

        return {
            "task_count": task_count,
            "run_count": len(runs),
            "running_count": running_count,
            "is_running": running_count > 0,
            "judgement": judgement,
            "status_label": status_label,
            "latest_status": latest_run.get("task_status") if latest_run else "",
            "latest_time_text": latest_run.get("event_time_text") if latest_run else "",
            "latest_task_name": latest_run.get("task_name") if latest_run else "",
            "latest_run": latest_run,
            "last_success_time_text": last_success.get("event_time_text") if last_success else "",
            "last_error_time_text": last_error.get("event_time_text") if last_error else "",
            "status_counts": dict(sorted(status_counts.items())),
        }

    def _summarize_pipeline_schedule(
        self,
        pipeline_state: Dict[str, Any],
        pipeline_runs: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        if pipeline_runs:
            return self._summarize_schedule_runs(pipeline_runs, 1)

        raw_status = (pipeline_state.get("status") or "").upper()
        if raw_status in {"RUNNING", "INITIAL", "STARTING"}:
            judgement = "running"
            status_label = "Pipeline 调度中"
        elif raw_status == "IDLE":
            judgement = "idle"
            status_label = "Pipeline 空闲"
        elif raw_status == "PAUSED":
            judgement = "paused"
            status_label = "Pipeline 已暂停"
        elif raw_status == "DRAFT":
            judgement = "draft"
            status_label = "Pipeline 草稿态"
        elif raw_status == "ERROR":
            judgement = "error"
            status_label = "Pipeline 状态异常"
        elif raw_status:
            judgement = "state_only"
            status_label = f"Pipeline 状态: {raw_status}"
        else:
            judgement = "no_state"
            status_label = "未发现 Pipeline 运行记录"

        return {
            "task_count": 1,
            "run_count": 0,
            "running_count": 1 if judgement == "running" else 0,
            "is_running": judgement == "running",
            "judgement": judgement,
            "status_label": status_label,
            "latest_status": raw_status,
            "latest_time_text": pipeline_state.get("last_start_time_text") or "",
            "latest_task_name": pipeline_state.get("task_id") or "",
            "latest_run": None,
            "last_success_time_text": "",
            "last_error_time_text": pipeline_state.get("last_start_time_text") if judgement == "error" else "",
            "status_counts": {raw_status: 1} if raw_status else {},
        }

    def _ensure_snapshot(self) -> Dict[str, Any]:
        if self._snapshot is None:
            return self._load_snapshot()
        age = time.time() - self._snapshot["loaded_at_epoch"]
        if age > self.refresh_seconds:
            return self._load_snapshot()
        return self._snapshot

    def _load_snapshot(self) -> Dict[str, Any]:
        nodes_rows = self._query(
            """
            SELECT id, graph_id, group_id, resource_id, resource_info
            FROM fdldb.fdl_lineage_node
            """
        )
        edges_rows = self._query(
            """
            SELECT id, graph_id, source_id, target_id
            FROM fdldb.fdl_lineage_edge
            """
        )
        connections_rows = self._query(
            """
            SELECT id, connection_id, create_time, publish_state, resource_id, resource_info, resource_type
            FROM fdldb.fdl_connection_lineage
            """
        )
        target_info_rows = self._query(
            """
            SELECT id, connection_name, connection_type, fdl_database, fdl_schema, load_type, delete_strategy
            FROM fdldb.fdl_pipe_target_info_define
            """
        )
        target_table_rows = self._query(
            """
            SELECT id, fdl_database, fdl_schema, fdl_table, task_id, to_table_comment, to_table_mode,
                   full_write_conf, inc_write_conf
            FROM fdldb.fdl_pipe_dest_table_define
            """
        )
        source_table_rows = self._query(
            """
            SELECT id, fdl_database, fdl_schema, fdl_table, target_table_id, task_id, sync_type, start_point_type
            FROM fdldb.fdl_pipe_src_table_define
            """
        )
        table_map_rows = self._query(
            """
            SELECT id, map_type, source_id, table_map_type, target_id, task_id
            FROM fdldb.fdl_pipe_table_map_define
            """
        )
        work_info_rows = self._query(
            """
            SELECT id, name, resource_id, resource_type
            FROM fdldb.fdl_work_info
            """
        )
        work_status_rows = self._query(
            """
            SELECT id, resource_id, resource_type, status, type
            FROM fdldb.fdl_work_status
            """
        )

        nodes = [self._normalize_node(row) for row in nodes_rows]
        edges = [self._normalize_edge(row) for row in edges_rows]
        connections = [self._normalize_connection(row) for row in connections_rows]
        # 连接级血缘索引：任务名 → {连接(小写): set(碰该连接的节点名)}
        # 来源 fdl_connection_lineage，比 fdl_lineage_node 完整（含 PG 等落地连接的节点）
        conn_nodes_by_work_name: Dict[str, Dict[str, set]] = defaultdict(lambda: defaultdict(set))
        for c in connections:
            wn = c.get("work_name")
            cid = (c.get("connection_id") or "").lower()
            if wn and cid:
                conn_nodes_by_work_name[wn][cid].add((c.get("node_name") or "").strip())
        target_infos = [self._normalize_target_info(row) for row in target_info_rows]
        source_tables = [self._normalize_source_table(row) for row in source_table_rows]
        table_maps = [self._normalize_table_map(row) for row in table_map_rows]
        work_infos = [self._normalize_work_info(row) for row in work_info_rows]
        work_statuses = [self._normalize_work_status(row) for row in work_status_rows]

        nodes_by_id = {node["id"]: node for node in nodes}
        node_ids_by_group_id: Dict[str, List[str]] = defaultdict(list)
        node_ids_by_resource_id: Dict[str, List[str]] = defaultdict(list)
        nodes_by_db_schema_table: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for node in nodes:
            if node["group_id"]:
                node_ids_by_group_id[node["group_id"]].append(node["id"])
            if node["resource_id"]:
                node_ids_by_resource_id[node["resource_id"]].append(node["id"])
            key = (
                (node["database_name"] or "").lower(),
                (node["schema_name"] or "").lower(),
                (node["table_name"] or "").lower(),
            )
            if key[2]:
                nodes_by_db_schema_table[key].append(node)

        graphs: Dict[str, Dict[str, Any]] = {}
        for node in nodes:
            graph = graphs.setdefault(
                node["graph_id"],
                {
                    "graph_id": node["graph_id"],
                    "nodes": [],
                    "edges": [],
                    "node_type_counts": {},
                },
            )
            graph["nodes"].append(node)

        node_type_counters: Dict[str, Counter[str]] = defaultdict(Counter)
        for node in nodes:
            node_type_counters[node["graph_id"]][node["resource_type"]] += 1

        out_edges_by_graph: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        in_edges_by_graph: Dict[str, Dict[str, List[Dict[str, Any]]]] = defaultdict(lambda: defaultdict(list))
        for edge in edges:
            graph = graphs.setdefault(
                edge["graph_id"],
                {
                    "graph_id": edge["graph_id"],
                    "nodes": [],
                    "edges": [],
                    "node_type_counts": {},
                },
            )
            graph["edges"].append(edge)
            out_edges_by_graph[edge["graph_id"]][edge["source_id"]].append(edge)
            in_edges_by_graph[edge["graph_id"]][edge["target_id"]].append(edge)

        graph_items = {}
        for graph_id, graph in graphs.items():
            graph_items[graph_id] = {
                "graph_id": graph_id,
                "nodes": sorted(graph["nodes"], key=lambda item: (item["display_name"], item["id"])),
                "edges": sorted(graph["edges"], key=lambda item: item["id"]),
                "node_count": len(graph["nodes"]),
                "edge_count": len(graph["edges"]),
                "node_type_counts": dict(sorted(node_type_counters[graph_id].items())),
            }

        target_info_by_task_id = {item["task_id"]: item for item in target_infos}
        source_tables_by_id = {item["source_table_id"]: item for item in source_tables}
        source_tables_by_target_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in source_tables:
            source_tables_by_target_id[item["target_table_id"]].append(item)

        table_maps_by_target_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in table_maps:
            table_maps_by_target_id[item["target_id"]].append(item)

        work_info_by_resource_id = {item["resource_id"]: item for item in work_infos}
        work_status_by_resource_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for item in work_statuses:
            work_status_by_resource_id[item["resource_id"]].append(item)

        pipeline_connections_by_task_id: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for connection in connections:
            if connection["resource_info"].get("resourceType") == "PIPELINE_TASK":
                pipeline_connections_by_task_id[connection["resource_id"]].append(connection)

        target_tables: List[Dict[str, Any]] = []
        target_tables_by_id: Dict[str, Dict[str, Any]] = {}
        target_tables_by_lookup: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        target_tables_by_source_lookup: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = defaultdict(list)
        for row in target_table_rows:
            target = self._normalize_target_table(
                row=row,
                target_info=target_info_by_task_id.get(row.get("task_id")),
                work_info=work_info_by_resource_id.get(row.get("task_id")),
                work_statuses=work_status_by_resource_id.get(row.get("task_id"), []),
                source_tables_by_target_id=source_tables_by_target_id,
                table_maps_by_target_id=table_maps_by_target_id,
                source_tables_by_id=source_tables_by_id,
                pipeline_connections_by_task_id=pipeline_connections_by_task_id,
                nodes_by_db_schema_table=nodes_by_db_schema_table,
            )
            target_tables.append(target)
            target_tables_by_id[target["target_table_id"]] = target
            lookup_key = (
                target["connection_name"].lower(),
                target["fdl_schema"].lower(),
                target["fdl_table"].lower(),
            )
            target_tables_by_lookup[lookup_key].append(target)
            source_keys = {
                (
                    source_table["fdl_database"].lower(),
                    source_table["fdl_schema"].lower(),
                    source_table["fdl_table"].lower(),
                )
                for source_table in target["source_tables"]
            }
            for source_key in source_keys:
                target_tables_by_source_lookup[source_key].append(target)

        snapshot = {
            "loaded_at": int(time.time()),
            "loaded_at_epoch": time.time(),
            "nodes": nodes,
            "edges": edges,
            "connections": connections,
            "conn_nodes_by_work_name": {k: {kk: vv for kk, vv in v.items()} for k, v in conn_nodes_by_work_name.items()},
            "nodes_by_id": nodes_by_id,
            "node_ids_by_group_id": dict(node_ids_by_group_id),
            "node_ids_by_resource_id": dict(node_ids_by_resource_id),
            "nodes_by_db_schema_table": dict(nodes_by_db_schema_table),
            "graphs": graph_items,
            "out_edges_by_graph": out_edges_by_graph,
            "in_edges_by_graph": in_edges_by_graph,
            "target_tables": sorted(target_tables, key=lambda item: (item["full_name"], item["target_table_id"])),
            "target_tables_by_id": target_tables_by_id,
            "target_tables_by_lookup": dict(target_tables_by_lookup),
            "target_tables_by_source_lookup": dict(target_tables_by_source_lookup),
        }
        self._snapshot = snapshot
        return snapshot

    def _normalize_node(self, row: Dict[str, Any]) -> Dict[str, Any]:
        info = self._parse_json(row.get("resource_info"))
        resource_type = info.get("resourceType") or "UNKNOWN"
        database_name = info.get("databaseName") or ""
        table_name = info.get("tableName") or ""
        work_name = info.get("workName") or ""
        node_name = info.get("nodeName") or ""
        display_name = (
            table_name
            or node_name
            or work_name
            or row.get("resource_id")
            or row.get("id")
        )
        search_blob = " ".join(
            filter(
                None,
                [
                    str(row.get("resource_id") or ""),
                    display_name,
                    work_name,
                    node_name,
                    database_name,
                    table_name,
                    info.get("connectionName") or "",
                    info.get("dsTypeName") or "",
                    json.dumps(info, ensure_ascii=False, sort_keys=True),
                ],
            )
        ).lower()

        return {
            "id": row.get("id"),
            "graph_id": row.get("graph_id"),
            "group_id": row.get("group_id"),
            "resource_id": row.get("resource_id"),
            "resource_info": info,
            "resource_type": resource_type,
            "display_name": display_name,
            "connection_name": info.get("connectionName") or "",
            "database_name": database_name,
            "schema_name": info.get("schemaName") or "",
            "table_name": table_name,
            "work_name": work_name,
            "node_name": node_name,
            "search_blob": search_blob,
        }

    def _normalize_edge(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "graph_id": row.get("graph_id"),
            "source_id": row.get("source_id"),
            "target_id": row.get("target_id"),
        }

    def _normalize_connection(self, row: Dict[str, Any]) -> Dict[str, Any]:
        info = self._parse_json(row.get("resource_info"))
        return {
            "id": row.get("id"),
            "connection_id": row.get("connection_id"),
            "create_time": row.get("create_time"),
            "publish_state": row.get("publish_state"),
            "resource_id": row.get("resource_id"),
            "resource_type": row.get("resource_type"),
            "resource_info": info,
            "node_id": info.get("nodeId"),
            "work_id": info.get("workId"),
            "node_name": info.get("nodeName"),
            "work_name": info.get("workName"),
            "operator_name": info.get("operatorName"),
            "operator_type": info.get("operatorType"),
        }

    def _normalize_target_info(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "task_id": row.get("id"),
            "connection_name": row.get("connection_name") or "",
            "connection_type": row.get("connection_type") or "",
            "fdl_database": row.get("fdl_database") or "",
            "fdl_schema": row.get("fdl_schema") or "",
            "load_type": row.get("load_type"),
            "delete_strategy": row.get("delete_strategy"),
        }

    def _normalize_source_table(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "source_table_id": row.get("id"),
            "fdl_database": row.get("fdl_database") or "",
            "fdl_schema": row.get("fdl_schema") or "",
            "fdl_table": row.get("fdl_table") or "",
            "target_table_id": row.get("target_table_id"),
            "task_id": row.get("task_id"),
            "sync_type": row.get("sync_type"),
            "start_point_type": row.get("start_point_type"),
            "full_name": self._format_table_name(row.get("fdl_database"), row.get("fdl_schema"), row.get("fdl_table")),
        }

    def _normalize_table_map(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "map_type": row.get("map_type"),
            "source_id": row.get("source_id"),
            "table_map_type": row.get("table_map_type"),
            "target_id": row.get("target_id"),
            "task_id": row.get("task_id"),
        }

    def _normalize_work_info(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "name": row.get("name") or "",
            "resource_id": row.get("resource_id"),
            "resource_type": row.get("resource_type"),
        }

    def _normalize_work_status(self, row: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": row.get("id"),
            "resource_id": row.get("resource_id"),
            "resource_type": row.get("resource_type"),
            "status": row.get("status"),
            "type": row.get("type"),
        }

    def _normalize_target_table(
        self,
        row: Dict[str, Any],
        target_info: Optional[Dict[str, Any]],
        work_info: Optional[Dict[str, Any]],
        work_statuses: List[Dict[str, Any]],
        source_tables_by_target_id: Dict[str, List[Dict[str, Any]]],
        table_maps_by_target_id: Dict[str, List[Dict[str, Any]]],
        source_tables_by_id: Dict[str, Dict[str, Any]],
        pipeline_connections_by_task_id: Dict[str, List[Dict[str, Any]]],
        nodes_by_db_schema_table: Dict[Tuple[str, str, str], List[Dict[str, Any]]],
    ) -> Dict[str, Any]:
        task_id = row.get("task_id")
        connection_name = (target_info or {}).get("connection_name") or ""
        connection_type = (target_info or {}).get("connection_type") or ""
        fdl_database = row.get("fdl_database") or (target_info or {}).get("fdl_database") or ""
        fdl_schema = row.get("fdl_schema") or (target_info or {}).get("fdl_schema") or ""
        fdl_table = row.get("fdl_table") or ""
        table_comment = row.get("to_table_comment") or ""
        full_name = ".".join(filter(None, [connection_name, fdl_schema, fdl_table])) or self._format_table_name(
            fdl_database,
            fdl_schema,
            fdl_table,
        )

        mapped_source_tables: List[Dict[str, Any]] = []
        seen_source_ids = set()
        for mapping in table_maps_by_target_id.get(row.get("id"), []):
            source_table = source_tables_by_id.get(mapping["source_id"])
            if source_table is None:
                continue
            seen_source_ids.add(source_table["source_table_id"])
            mapped_source_tables.append(source_table)

        for source_table in source_tables_by_target_id.get(row.get("id"), []):
            if source_table["source_table_id"] in seen_source_ids:
                continue
            mapped_source_tables.append(source_table)

        source_tables_payload = []
        for source_table in mapped_source_tables:
            node_matches = nodes_by_db_schema_table.get(
                (
                    source_table["fdl_database"].lower(),
                    source_table["fdl_schema"].lower(),
                    source_table["fdl_table"].lower(),
                ),
                [],
            )
            source_tables_payload.append(
                {
                    "source_table_id": source_table["source_table_id"],
                    "full_name": source_table["full_name"],
                    "fdl_database": source_table["fdl_database"],
                    "fdl_schema": source_table["fdl_schema"],
                    "fdl_table": source_table["fdl_table"],
                    "task_id": source_table["task_id"],
                    "sync_type": source_table["sync_type"],
                    "start_point_type": source_table["start_point_type"],
                    "lineage_node_matches": [self._node_preview(node) for node in node_matches],
                }
            )

        pipeline_connections = pipeline_connections_by_task_id.get(task_id, [])
        source_connections = sorted(
            {item["connection_id"] for item in pipeline_connections if item["connection_id"] != connection_name}
        )
        target_connections = sorted(
            {item["connection_id"] for item in pipeline_connections if item["connection_id"] == connection_name}
        )

        config_status = next((item["status"] for item in work_statuses if item["type"] == "CONFIG_STATUS"), None)
        return {
            "target_table_id": row.get("id"),
            "task_id": task_id,
            "pipeline_name": (work_info or {}).get("name") or "",
            "pipeline_resource_type": (work_info or {}).get("resource_type"),
            "config_status": config_status,
            "connection_name": connection_name,
            "connection_type": connection_type,
            "fdl_database": fdl_database,
            "fdl_schema": fdl_schema,
            "fdl_table": fdl_table,
            "full_name": full_name,
            "to_table_comment": table_comment,
            "to_table_mode": row.get("to_table_mode"),
            "full_write_conf": self._parse_json(row.get("full_write_conf")),
            "inc_write_conf": self._parse_json(row.get("inc_write_conf")),
            "source_connections": source_connections,
            "target_connections": target_connections,
            "source_tables": source_tables_payload,
            "search_blob": " ".join(
                filter(
                    None,
                    [
                        full_name,
                        connection_name,
                        fdl_schema,
                        fdl_table,
                        table_comment,
                        (work_info or {}).get("name") or "",
                    ],
                )
            ).lower(),
        }

    def _parse_json(self, value: Any) -> Dict[str, Any]:
        if not value:
            return {}
        if isinstance(value, dict):
            return value
        try:
            parsed = json.loads(value)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
        return {"raw": str(value)}

    def _parse_json_list(self, value: Any) -> List[Dict[str, Any]]:
        if not value:
            return []
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [item for item in parsed if isinstance(item, dict)]
        except json.JSONDecodeError:
            pass
        return []

    def _format_epoch_millis(self, value: Any) -> str:
        if value in {None, "", 0, "0"}:
            return ""
        try:
            millis = int(value)
        except (TypeError, ValueError):
            return ""
        if millis <= 0:
            return ""
        return datetime.fromtimestamp(millis / 1000).strftime("%Y-%m-%d %H:%M:%S")

    def _score_node(self, node: Dict[str, Any], needle: str) -> int:
        display_name = node["display_name"].lower()
        resource_id = (node["resource_id"] or "").lower()
        table_name = node["table_name"].lower()
        work_name = node["work_name"].lower()
        node_name = node["node_name"].lower()
        if needle == display_name or needle == table_name or needle == resource_id:
            return 100
        if display_name.startswith(needle) or table_name.startswith(needle):
            return 80
        if needle in display_name or needle in table_name:
            return 60
        if needle in work_name or needle in node_name:
            return 40
        if needle in node["search_blob"]:
            return 20
        return 0

    def _score_target_table(self, target: Dict[str, Any], needle: str) -> int:
        table_name = target["fdl_table"].lower()
        schema = target["fdl_schema"].lower()
        full_name = target["full_name"].lower()
        schema_table = ".".join(filter(None, [schema, table_name]))
        qualified_with_connection = ".".join(filter(None, [target["connection_name"].lower(), schema_table]))
        pipeline_name = target["pipeline_name"].lower()
        if needle == full_name or needle == table_name or needle == schema_table or needle == qualified_with_connection:
            return 100
        if "." in needle and needle not in {full_name, schema_table, qualified_with_connection}:
            return 0
        if full_name.endswith(needle) or table_name.startswith(needle):
            return 80
        if needle in full_name or needle in table_name:
            return 60
        if needle == schema or needle in pipeline_name:
            return 40
        if needle in target["search_blob"]:
            return 20
        return 0

    def _graph_node_id(self, node_id: str) -> str:
        return f"graph::{node_id}"

    def _graph_node_payload(self, node: Dict[str, Any]) -> Dict[str, Any]:
        label = node["display_name"]
        node_type = "graph_table" if node["resource_type"] == "DB_TABLE" else "graph_task"
        return {
            "id": self._graph_node_id(node["id"]),
            "label": label,
            "type": node_type,
            "meta": {
                "graph_id": node["graph_id"],
                "resource_type": node["resource_type"],
                "resource_id": node["resource_id"],
                "group_id": node["group_id"],
                "connection_name": node.get("connection_name", ""),
                "database_name": node.get("database_name", ""),
                "schema_name": node.get("schema_name", ""),
                "table_name": node.get("table_name", ""),
                "work_name": node.get("work_name", ""),
                "node_name": node.get("node_name", ""),
            },
        }

    def _source_table_graph_payload(self, source_table: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": f"sourcecfg::{source_table['source_table_id']}",
            "label": source_table.get("fdl_table") or source_table["full_name"],
            "type": "source_table_config",
            "meta": {
                "full_name": source_table["full_name"],
                "connection_name": source_table.get("fdl_database") or "",
                "database_name": source_table.get("fdl_database") or "",
                "schema_name": source_table.get("fdl_schema") or "",
                "table_name": source_table.get("fdl_table") or "",
                "resource_id": source_table["full_name"],
                "sync_type": source_table.get("sync_type") or "",
                "start_point_type": source_table.get("start_point_type") or "",
            },
        }

    def _add_graph_node(self, graph_nodes: Dict[str, Dict[str, Any]], node: Dict[str, Any]) -> None:
        graph_nodes.setdefault(node["id"], node)

    def _add_graph_edge(
        self,
        graph_edges: Dict[Tuple[str, str, str], Dict[str, Any]],
        source: str,
        target: str,
        relation: str,
    ) -> None:
        key = (source, target, relation)
        graph_edges.setdefault(
            key,
            {
                "source": source,
                "target": target,
                "relation": relation,
            },
        )

    def _node_preview(self, node: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": node["id"],
            "graph_id": node["graph_id"],
            "resource_type": node["resource_type"],
            "display_name": node["display_name"],
            "resource_id": node["resource_id"],
            "group_id": node["group_id"],
            "connection_name": node["connection_name"],
        }

    def _target_table_preview(self, target: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "target_table_id": target["target_table_id"],
            "full_name": target["full_name"],
            "connection_name": target["connection_name"],
            "connection_type": target["connection_type"],
            "fdl_database": target["fdl_database"],
            "fdl_schema": target["fdl_schema"],
            "fdl_table": target["fdl_table"],
            "pipeline_name": target["pipeline_name"],
            "task_id": target["task_id"],
            "config_status": target["config_status"],
            "source_table_count": len(target["source_tables"]),
        }

    def _target_table_payload(self, snapshot: Dict[str, Any], target: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._target_table_preview(target)
        payload.update(
            {
                "to_table_comment": target["to_table_comment"],
                "to_table_mode": target["to_table_mode"],
                "full_write_conf": target["full_write_conf"],
                "inc_write_conf": target["inc_write_conf"],
                "source_connections": target["source_connections"],
                "target_connections": target["target_connections"],
                "source_tables": target["source_tables"],
            }
        )
        return payload

    def _node_payload(self, node: Dict[str, Any]) -> Dict[str, Any]:
        payload = self._node_preview(node)
        payload.update(
            {
                "connection_name": node["connection_name"],
                "database_name": node["database_name"],
                "schema_name": node["schema_name"],
                "table_name": node["table_name"],
                "work_name": node["work_name"],
                "node_name": node["node_name"],
                "resource_info": node["resource_info"],
            }
        )
        return payload

    def _node_full_name(self, node: Dict[str, Any]) -> str:
        return (
            node.get("resource_id")
            or self._format_table_name(node.get("database_name"), node.get("schema_name"), node.get("table_name"))
            or node.get("display_name")
            or node.get("id")
        )

    def _edge_payload(self, edge: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "id": edge["id"],
            "graph_id": edge["graph_id"],
            "source_id": edge["source_id"],
            "target_id": edge["target_id"],
        }

    def _connection_payload(
        self,
        connection: Dict[str, Any],
        linked_node_ids: List[str],
        linked_by: List[str],
    ) -> Dict[str, Any]:
        return {
            "id": connection["id"],
            "connection_id": connection["connection_id"],
            "publish_state": connection["publish_state"],
            "resource_type": connection["resource_type"],
            "resource_id": connection["resource_id"],
            "node_id": connection["node_id"],
            "work_id": connection["work_id"],
            "node_name": connection["node_name"],
            "work_name": connection["work_name"],
            "operator_name": connection["operator_name"],
            "operator_type": connection["operator_type"],
            "linked_node_ids": linked_node_ids,
            "linked_by": linked_by,
            "resource_info": connection["resource_info"],
        }

    def _format_table_name(self, database_name: Any, schema_name: Any, table_name: Any) -> str:
        return ".".join(filter(None, [str(database_name or ""), str(schema_name or ""), str(table_name or "")]))

    def _query(self, sql: str, params: Any = None) -> List[Dict[str, Any]]:
        return self._db_module.query(sql, params)

    def _load_db_module(self) -> Any:
        spec = importlib.util.spec_from_file_location("fdl_db_connect", DB_CONNECT_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load database connector from {DB_CONNECT_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
