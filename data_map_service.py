from __future__ import annotations

import importlib.util
import json
import re
import shutil
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


ROOT_DIR = Path(__file__).resolve().parent
PG_CONNECT_PATH = Path("/Users/seer/code/xgdata/pg_connect.py")
MYSQL_CONNECT_PATH = ROOT_DIR / "fdl连接" / "db_connect_write_fdldb.py"
STORE_PATH = ROOT_DIR / "data_map_store.json"
SQL_WORKSPACE_DIR = ROOT_DIR / "sql_workspace"
SQL_WORKSPACE_META_PATH = SQL_WORKSPACE_DIR / ".workspace.json"
LOCKED_SQL_WORKSPACE_FOLDERS = {"", "个人查询", "临时查询"}
DATASET_DIR = ROOT_DIR / "datasets"
DASHBOARD_DIR = ROOT_DIR / "dashboards"
DATASET_META_PATH = DATASET_DIR / ".datasets.json"
DASHBOARD_META_PATH = DASHBOARD_DIR / ".dashboards.json"
WORKBENCH_FILTER_NULL = "__WB_FILTER_NULL__"
WORKBENCH_FILTER_EMPTY = "__WB_FILTER_EMPTY__"

# ── 多数据源配置 ──
# 新增数据源 = 往这里加一行。schema 在 qualified_name 里充当唯一前缀（MySQL 用库名当 schema）。
DATASOURCES: List[Dict[str, Any]] = [
    {"key": "pg", "label": "360", "type": "postgresql", "schema": "public",
     "source_type": "PostgreSQL", "lineage_connection": "PG"},
    {"key": "dwd_mdbase", "label": "dwd_mdbase", "type": "mysql", "schema": "dwd_mdbase",
     "database": "dwd_mdbase", "source_type": "MySQL", "lineage_connection": "dwd_mdbase"},
    {"key": "ods_kingdee", "label": "ods_kingdee", "type": "mysql", "schema": "ods_kingdee",
     "database": "ods_kingdee", "source_type": "MySQL", "lineage_connection": "ods_kingdee"},
    # 物理库 ods_jiandaoyun，但 fdl 血缘里 connection 名拼写为 ods_jiandanyun（少一个 a）
    {"key": "ods_jiandaoyun", "label": "ods_jiandaoyun", "type": "mysql", "schema": "ods_jiandaoyun",
     "database": "ods_jiandaoyun", "source_type": "MySQL", "lineage_connection": "ods_jiandanyun"},
    {"key": "ods_lark_apaas", "label": "ods_lark_apaas", "type": "mysql", "schema": "ods_lark_apaas",
     "database": "ods_lark_apaas", "source_type": "MySQL", "lineage_connection": "ods_lark_apaas"},
]


class DataMapService:
    def __init__(self, refresh_seconds: int = 300) -> None:
        self.refresh_seconds = refresh_seconds
        self._datasources = DATASOURCES
        self._ds_by_schema = {ds["schema"]: ds for ds in self._datasources}
        self._pg_module = self._load_pg_module()
        self._mysql_module = self._load_mysql_module()
        self._snapshot: Optional[Dict[str, Any]] = None
        self._store_lock = threading.Lock()
        self._store = self._load_store()

    def _datasource_for_schema(self, schema: str) -> Dict[str, Any]:
        return self._ds_by_schema.get(schema) or self._datasources[0]


    def refresh_catalog(self) -> Dict[str, Any]:
        snapshot = self._load_snapshot()
        return {
            "loaded_at": snapshot["loaded_at"],
            "table_count": len(snapshot["tables"]),
            "column_count": snapshot["column_count"],
            "favorite_count": self._favorite_count(),
        }

    def get_dashboard(self, limit: int = 8) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        ranked = self._ranked_tables(snapshot)
        favorites = [item for item in ranked if item["favorite"]][:limit]
        recent = sorted(
            ranked,
            key=lambda item: (
                item.get("last_viewed_at") or "",
                item.get("view_count") or 0,
            ),
            reverse=True,
        )[:limit]
        recommended = ranked[:limit]
        return {
            "loaded_at": snapshot["loaded_at"],
            "summary": {
                "table_count": len(snapshot["tables"]),
                "column_count": snapshot["column_count"],
                "favorite_count": self._favorite_count(),
                "public_schema": "public",
                "source_type": "PostgreSQL",
            },
            "filters": self._filter_options(snapshot),
            "favorites": favorites,
            "recent": recent,
            "recommended": recommended,
        }

    def search_tables(
        self,
        keyword: str = "",
        exact: bool = False,
        source_type: Optional[str] = None,
        business_domain: Optional[str] = None,
        project: Optional[str] = None,
        storage_type: Optional[str] = None,
        owner: Optional[str] = None,
        favorites_only: bool = False,
        limit: int = 50,
    ) -> Dict[str, Any]:
        snapshot = self._ensure_snapshot()
        needle = keyword.strip().lower()
        results: List[Tuple[int, Dict[str, Any]]] = []

        for table in snapshot["tables"]:
            summary = self._table_summary(snapshot, table)
            if source_type and summary["source_type"] != source_type:
                continue
            if business_domain and summary["business_domain"] != business_domain:
                continue
            if project and summary["project"] != project:
                continue
            if storage_type and summary["storage_type"] != storage_type:
                continue
            if owner and summary["owner"] != owner:
                continue
            if favorites_only and not summary["favorite"]:
                continue

            score, match_reasons = self._score_table(table, summary, needle, exact)
            if needle and score <= 0:
                continue

            summary["match_reasons"] = match_reasons
            results.append((score, summary))

        results.sort(
            key=lambda item: (
                -item[0],
                0 if item[1]["favorite"] else 1,
                -(item[1].get("view_count") or 0),
                item[1]["qualified_name"],
            )
        )

        return {
            "loaded_at": snapshot["loaded_at"],
            "keyword": keyword,
            "exact": exact,
            "limit": limit,
            "total_matches": len(results),
            "filters": self._filter_options(snapshot),
            "items": [item[1] for item in results[:limit]],
        }

    def get_table_profile(
        self,
        schema: str,
        table: str,
        sample_limit: int = 20,
    ) -> Dict[str, Any]:
        if sample_limit < 1 or sample_limit > 200:
            raise ValueError("sample_limit must be between 1 and 200")

        snapshot = self._ensure_snapshot()
        qualified_name = self._qualified_name(schema, table)
        table_item = snapshot["tables_by_key"].get(qualified_name)
        if table_item is None:
            raise ValueError(f"table not found: {qualified_name}")

        self._record_usage(qualified_name)
        metadata = self._table_metadata(qualified_name)
        raw_columns = snapshot["columns_by_key"].get(qualified_name, [])
        primary_keys = snapshot["primary_keys_by_key"].get(qualified_name, [])
        foreign_keys_outgoing = snapshot["foreign_keys_outgoing_by_key"].get(qualified_name, [])
        col_dict = self._store.get("column_dict", {}).get(qualified_name, {})
        columns = [
            {**col, **col_dict.get(col["column_name"], {})}
            for col in raw_columns
        ]
        preview = self._build_preview_payload(
            schema=schema,
            table=table,
            sample_limit=sample_limit,
            raw_columns=raw_columns,
        )
        ddl = self._build_table_ddl_payload(
            schema=schema,
            table=table,
            table_item=table_item,
            raw_columns=raw_columns,
            primary_keys=primary_keys,
            foreign_keys_outgoing=foreign_keys_outgoing,
        )

        quality_columns = [
            self._column_quality_payload(column, table_item["estimated_rows"])
            for column in columns
        ]
        quality_summary = self._quality_summary(quality_columns)

        profile = {
            "loaded_at": snapshot["loaded_at"],
            "qualified_name": qualified_name,
            "table": self._table_summary(snapshot, table_item),
            "table_comment": table_item.get("table_comment") or "",
            "manual_metadata": metadata,
            "structure": {
                "column_count": len(columns),
                "columns": columns,
                "primary_keys": primary_keys,
                "foreign_keys_outgoing": foreign_keys_outgoing,
                "foreign_keys_incoming": snapshot["foreign_keys_incoming_by_key"].get(qualified_name, []),
                "partition_info": {
                    "is_partitioned": table_item["is_partitioned"],
                    "partition_key": table_item["partition_key"],
                },
            },
            "ddl": ddl,
            "details": {
                "physical_size_bytes": table_item["total_size_bytes"],
                "physical_size_text": table_item["total_size_pretty"],
                "estimated_rows": table_item["estimated_rows"],
                "db_owner": table_item["db_owner"],
                "responsible_owner": metadata.get("owner") or table_item["db_owner"] or "",
                "created_time_text": metadata.get("created_time_text") or "",
                "last_modified_time_text": metadata.get("last_modified_time_text") or "",
                "ttl_days": metadata.get("ttl_days"),
                "maintenance": {
                    "last_vacuum": table_item["last_vacuum"],
                    "last_autovacuum": table_item["last_autovacuum"],
                    "last_analyze": table_item["last_analyze"],
                    "last_autoanalyze": table_item["last_autoanalyze"],
                },
            },
            "preview": {
                **preview,
            },
            "quality": {
                "summary": quality_summary,
                "columns": quality_columns,
                "note": "质量指标优先使用 pg_stats 和系统估算，适合快速探查，不等同于离线全量质检。",
            },
            "recommendations": self._recommended_related(snapshot, qualified_name, limit=6),
            "lineage_hint": {
                "target_table_url": self._target_lineage_url(table),
            },
        }
        return profile

    def get_table_preview(
        self,
        schema: str,
        table: str,
        sample_limit: int = 20,
        preview_column: str = "",
        preview_keyword: str = "",
    ) -> Dict[str, Any]:
        if sample_limit < 1 or sample_limit > 200:
            raise ValueError("sample_limit must be between 1 and 200")

        snapshot = self._ensure_snapshot()
        qualified_name = self._qualified_name(schema, table)
        table_item = snapshot["tables_by_key"].get(qualified_name)
        if table_item is None:
            raise ValueError(f"table not found: {qualified_name}")

        raw_columns = snapshot["columns_by_key"].get(qualified_name, [])
        return self._build_preview_payload(
            schema=schema,
            table=table,
            sample_limit=sample_limit,
            raw_columns=raw_columns,
            preview_column=preview_column,
            preview_keyword=preview_keyword,
        )

    def run_table_sql(
        self,
        schema: str,
        table: str,
        sql: str,
        row_limit: int = 200,
    ) -> Dict[str, Any]:
        if row_limit < 1 or row_limit > 500:
            raise ValueError("row_limit must be between 1 and 500")

        snapshot = self._ensure_snapshot()
        qualified_name = self._qualified_name(schema, table)
        table_item = snapshot["tables_by_key"].get(qualified_name)
        if table_item is None:
            raise ValueError(f"table not found: {qualified_name}")

        normalized_sql = self._normalize_sql_query(sql)
        limited_sql = self._ensure_sql_limit(normalized_sql, row_limit)
        ds = self._datasource_for_schema(schema)
        rows = self._query(limited_sql, None, ds)
        columns = list(rows[0].keys()) if rows else self._extract_select_aliases(normalized_sql)
        return {
            "sql": limited_sql,
            "row_limit": row_limit,
            "row_count": len(rows),
            "columns": columns,
            "rows": rows,
            "dialect": ds.get("type") or "postgresql",
        }

    def get_sql_workspace(self) -> Dict[str, Any]:
        meta = self._load_sql_workspace_meta()
        return {
            "tree": self._build_sql_workspace_tree(SQL_WORKSPACE_DIR),
            "recent_files": meta.get("recent_files", []),
            "favorites": meta.get("favorites", []),
        }

    def read_sql_file(self, file_path: str) -> Dict[str, Any]:
        path = self._resolve_sql_workspace_path(file_path, expect_suffix=".sql")
        if not path.exists() or not path.is_file():
            raise ValueError("SQL 文件不存在")
        stat = path.stat()
        meta = self._load_sql_workspace_meta()
        self._touch_sql_workspace_recent(file_path)
        return {
            "path": file_path,
            "name": path.name,
            "content": path.read_text(encoding="utf-8"),
            "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            "linked_table": meta.get("linked_tables", {}).get(file_path, ""),
        }

    def save_sql_file(
        self,
        file_path: str,
        content: str,
        linked_table: str = "",
    ) -> Dict[str, Any]:
        path = self._resolve_sql_workspace_path(file_path, expect_suffix=".sql")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(content or ""), encoding="utf-8")
        meta = self._load_sql_workspace_meta()
        if linked_table:
            meta.setdefault("linked_tables", {})[file_path] = linked_table
        else:
            meta.setdefault("linked_tables", {}).pop(file_path, None)
        self._save_sql_workspace_meta(meta)
        self._touch_sql_workspace_recent(file_path)
        return self.read_sql_file(file_path)

    def create_sql_folder(self, folder_path: str) -> Dict[str, Any]:
        path = self._resolve_sql_workspace_path(folder_path)
        path.mkdir(parents=True, exist_ok=True)
        return self.get_sql_workspace()

    def rename_sql_workspace_entry(self, entry_path: str, new_name: str) -> Dict[str, Any]:
        source = self._resolve_sql_workspace_path(entry_path)
        if not source.exists():
            raise ValueError("目标不存在")
        source_relative = self._workspace_relative_path(source)
        self._assert_workspace_entry_mutable(source_relative)
        source_is_file = source.is_file()
        clean_name = str(new_name or "").strip()
        if not clean_name:
            raise ValueError("新名称不能为空")
        target_name = self._sanitize_sql_file_name(clean_name) if source_is_file else re.sub(r"[\\\\/:*?\"<>|]+", "_", clean_name)
        if not target_name:
            raise ValueError("新名称不能为空")
        target = source.with_name(target_name)
        if target.exists():
            raise ValueError("同名文件或文件夹已存在")
        source.rename(target)
        old_relative = source_relative
        new_relative = self._workspace_relative_path(target)
        self._remap_sql_workspace_meta(old_relative, new_relative, source_was_folder=not source_is_file)
        payload = {
            "action": "rename",
            "entry_type": "file" if source_is_file else "folder",
            "old_path": old_relative,
            "new_path": new_relative,
            "workspace": self.get_sql_workspace(),
        }
        if source_is_file:
            payload["file"] = self.read_sql_file(new_relative)
        return payload

    def move_sql_workspace_entry(self, entry_path: str, target_folder_path: str) -> Dict[str, Any]:
        source = self._resolve_sql_workspace_path(entry_path)
        if not source.exists():
            raise ValueError("目标不存在")
        source_relative = self._workspace_relative_path(source)
        self._assert_workspace_entry_mutable(source_relative)
        source_is_file = source.is_file()
        target_folder = self._resolve_sql_workspace_path(target_folder_path)
        if target_folder.exists() and not target_folder.is_dir():
            raise ValueError("目标目录非法")
        target_folder.mkdir(parents=True, exist_ok=True)
        target = target_folder / source.name
        if target.exists():
            raise ValueError("目标位置已存在同名文件或文件夹")
        if source.is_dir():
            source_root = source.resolve()
            target_root = target.resolve()
            if source_root == target_root or source_root in target_root.parents:
                raise ValueError("不能移动到自身或子目录下")
        source.rename(target)
        old_relative = source_relative
        new_relative = self._workspace_relative_path(target)
        self._remap_sql_workspace_meta(old_relative, new_relative, source_was_folder=not source_is_file)
        payload = {
            "action": "move",
            "entry_type": "file" if source_is_file else "folder",
            "old_path": old_relative,
            "new_path": new_relative,
            "workspace": self.get_sql_workspace(),
        }
        if source_is_file:
            payload["file"] = self.read_sql_file(new_relative)
        return payload

    def delete_sql_workspace_entry(self, entry_path: str) -> Dict[str, Any]:
        target = self._resolve_sql_workspace_path(entry_path)
        if not target.exists():
            raise ValueError("目标不存在")
        relative = self._workspace_relative_path(target)
        self._assert_workspace_entry_mutable(relative)
        target_is_dir = target.is_dir()
        entry_type = "folder" if target_is_dir else "file"
        if target_is_dir:
            shutil.rmtree(target)
            self._remove_sql_workspace_meta(relative, target_was_folder=True)
        else:
            target.unlink()
            self._remove_sql_workspace_meta(relative, target_was_folder=False)
        return {
            "action": "delete",
            "entry_type": entry_type,
            "old_path": relative,
            "workspace": self.get_sql_workspace(),
        }

    def create_sql_file(
        self,
        folder_path: str,
        file_name: str,
        initial_sql: str = "",
        linked_table: str = "",
    ) -> Dict[str, Any]:
        safe_name = self._sanitize_sql_file_name(file_name)
        relative = str((Path(folder_path) / safe_name).as_posix()).strip("./")
        path = self._resolve_sql_workspace_path(relative, expect_suffix=".sql")
        if path.exists():
            raise ValueError("SQL 文件已存在")
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(str(initial_sql or ""), encoding="utf-8")
        meta = self._load_sql_workspace_meta()
        if linked_table:
            meta.setdefault("linked_tables", {})[relative] = linked_table
        self._save_sql_workspace_meta(meta)
        self._touch_sql_workspace_recent(relative)
        return self.read_sql_file(relative)

    def run_workspace_sql(
        self,
        schema: str,
        table: str,
        sql: str,
        row_limit: int = 200,
        page: int = 1,
        page_size: int = 200,
        filter_column: str = "",
        filter_keyword: str = "",
        column_filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        effective_page_size = int(page_size or row_limit or 200)
        if effective_page_size < 1 or effective_page_size > 500:
            raise ValueError("page_size must be between 1 and 500")
        current_page = max(1, int(page or 1))

        normalized_sql = self._normalize_sql_query(sql)
        resolved_schema = self._infer_sql_schema(normalized_sql) or schema
        ds = self._datasource_for_schema(resolved_schema)
        base_sql, paging_meta = self._strip_top_level_limit_offset(normalized_sql)
        normalized_column_filters = self._normalize_workspace_column_filters(column_filters)
        legacy_column = str(filter_column or "").strip()
        legacy_keyword = str(filter_keyword or "").strip()
        if legacy_column and legacy_keyword:
            existing = normalized_column_filters.get(legacy_column, [])
            normalized_column_filters[legacy_column] = self._normalize_workspace_column_filters(
                {legacy_column: [*existing, legacy_keyword]}
            ).get(legacy_column, [])
        elif legacy_keyword:
            if legacy_column:
                normalized_column_filters[legacy_column] = [legacy_keyword]
        available_columns: List[str] = []
        if normalized_column_filters:
            available_columns = self._resolve_workspace_columns(base_sql, ds)
        filtered_sql, filter_params, filter_meta = self._build_workspace_filter_query(
            sql=base_sql,
            ds=ds,
            available_columns=available_columns,
            column_filters=normalized_column_filters,
        )
        total_sql = self._build_sql_count_query(filtered_sql)
        total_rows = self._query(total_sql, filter_params, ds)
        total_count = int((total_rows[0].get("total_count") if total_rows else 0) or 0)
        total_pages = max(1, (total_count + effective_page_size - 1) // effective_page_size) if total_count else 1
        current_page = min(current_page, total_pages)
        offset = (current_page - 1) * effective_page_size
        paged_sql = self._build_sql_page_query(filtered_sql, page_size=effective_page_size, offset=offset)
        rows = self._query(paged_sql, filter_params, ds)
        columns = list(rows[0].keys()) if rows else available_columns or self._resolve_workspace_columns(base_sql, ds)
        notices: List[str] = []
        if paging_meta["stripped"]:
            parts = []
            if paging_meta["had_top_level_limit"]:
                parts.append("LIMIT")
            if paging_meta["had_top_level_offset"]:
                parts.append("OFFSET")
            notices.append(f"检测到原 SQL 顶层 {' / '.join(parts)}，工作台分页已自动忽略，页数按完整结果集计算。")
        if filter_meta["applied"]:
            if filter_meta["count"] == 1 and filter_meta["items"]:
                item = filter_meta["items"][0]
                if len(item["labels"]) == 1:
                    notices.append(f"当前结果按 {item['column']} 筛选：{item['labels'][0]}")
                else:
                    notices.append(f"当前结果按 {item['column']} 筛选：已勾选 {len(item['labels'])} 个值")
            elif filter_meta["count"] > 1:
                preview = "；".join(
                    f"{item['column']}({len(item['labels'])}项)"
                    for item in filter_meta["items"][:3]
                )
                suffix = " 等" if filter_meta["count"] > 3 else ""
                notices.append(f"当前结果按 {filter_meta['count']} 个字段筛选：{preview}{suffix}")
        return {
            "sql": normalized_sql,
            "base_sql": base_sql,
            "paged_sql": paged_sql,
            "page": current_page,
            "page_size": effective_page_size,
            "total_count": total_count,
            "total_pages": total_pages,
            "offset": offset,
            "row_count": len(rows),
            "resolved_schema": resolved_schema,
            "resolved_source_type": ds.get("type") or "postgresql",
            "columns": columns,
            "rows": rows,
            "has_prev": current_page > 1,
            "has_next": current_page < total_pages,
            "dialect": ds.get("type") or "postgresql",
            "filter": filter_meta,
            "notices": notices,
            "pagination": paging_meta,
        }

    def get_workspace_filter_options(
        self,
        schema: str,
        table: str,
        sql: str,
        column: str,
        column_filters: Optional[Dict[str, Any]] = None,
        keyword: str = "",
        limit: int = 200,
    ) -> Dict[str, Any]:
        target_column = str(column or "").strip()
        if not target_column:
            raise ValueError("column 不能为空")
        option_limit = max(20, min(int(limit or 200), 500))
        option_keyword = str(keyword or "").strip()

        normalized_sql = self._normalize_sql_query(sql)
        resolved_schema = self._infer_sql_schema(normalized_sql) or schema
        ds = self._datasource_for_schema(resolved_schema)
        base_sql, _ = self._strip_top_level_limit_offset(normalized_sql)
        available_columns = self._resolve_workspace_columns(base_sql, ds)
        if available_columns and target_column not in available_columns:
            raise ValueError(f"筛选字段不存在：{target_column}")

        normalized_filters = self._normalize_workspace_column_filters(column_filters)
        active_values = normalized_filters.get(target_column, [])
        sibling_filters = {key: values for key, values in normalized_filters.items() if key != target_column}
        filtered_sql, filter_params, _ = self._build_workspace_filter_query(
            sql=base_sql,
            ds=ds,
            available_columns=available_columns,
            column_filters=sibling_filters,
        )
        options = self._query_workspace_filter_options(
            sql=filtered_sql,
            params=filter_params,
            ds=ds,
            column=target_column,
            keyword=option_keyword,
            limit=option_limit,
        )
        return {
            "column": target_column,
            "options": options,
            "active_values": active_values,
            "limit": option_limit,
            "keyword": option_keyword,
            "resolved_schema": resolved_schema,
            "dialect": ds.get("type") or "postgresql",
        }

    def update_table_metadata(
        self,
        schema: str,
        table: str,
        patch: Dict[str, Any],
    ) -> Dict[str, Any]:
        qualified_name = self._qualified_name(schema, table)
        snapshot = self._ensure_snapshot()
        if qualified_name not in snapshot["tables_by_key"]:
            raise ValueError(f"table not found: {qualified_name}")

        normalized = {
            "alias": self._clean_text(patch.get("alias")),
            "business_terms": self._normalize_list(patch.get("business_terms")),
            "owner": self._clean_text(patch.get("owner")),
            "business_domain": self._clean_text(patch.get("business_domain")),
            "project": self._clean_text(patch.get("project")),
            "source_type": self._clean_text(patch.get("source_type")) or "PostgreSQL",
            "storage_type": self._clean_text(patch.get("storage_type")),
            "ttl_days": self._normalize_int(patch.get("ttl_days")),
            "description": self._clean_text(patch.get("description")),
            "created_time_text": self._clean_text(patch.get("created_time_text")),
            "last_modified_time_text": self._clean_text(patch.get("last_modified_time_text")),
        }

        with self._store_lock:
            table_meta = self._store.setdefault("table_meta", {})
            current = table_meta.setdefault(qualified_name, {})
            for key, value in normalized.items():
                if value in ("", [], None):
                    current.pop(key, None)
                else:
                    current[key] = value
            if not current:
                table_meta.pop(qualified_name, None)
            self._save_store()

        return self.get_table_profile(schema=schema, table=table, sample_limit=20)

    def toggle_favorite(self, schema: str, table: str) -> Dict[str, Any]:
        qualified_name = self._qualified_name(schema, table)
        snapshot = self._ensure_snapshot()
        if qualified_name not in snapshot["tables_by_key"]:
            raise ValueError(f"table not found: {qualified_name}")

        with self._store_lock:
            favorites = self._store.setdefault("favorites", {})
            next_state = not bool(favorites.get(qualified_name))
            if next_state:
                favorites[qualified_name] = True
            else:
                favorites.pop(qualified_name, None)
            self._save_store()

        return {
            "qualified_name": qualified_name,
            "favorite": next_state,
            "favorite_count": self._favorite_count(),
        }

    def update_column_dict(
        self,
        schema: str,
        table: str,
        column: str,
        patch: Dict[str, Any],
    ) -> Dict[str, Any]:
        qualified_name = self._qualified_name(schema, table)
        snapshot = self._ensure_snapshot()
        if qualified_name not in snapshot["tables_by_key"]:
            raise ValueError(f"table not found: {qualified_name}")

        business_def = self._clean_text(patch.get("business_def"))
        enum_values = self._clean_text(patch.get("enum_values"))

        with self._store_lock:
            col_dict = self._store.setdefault("column_dict", {})
            table_cols = col_dict.setdefault(qualified_name, {})
            entry = table_cols.setdefault(column, {})

            if business_def:
                entry["business_def"] = business_def
            else:
                entry.pop("business_def", None)

            if enum_values:
                entry["enum_values"] = enum_values
            else:
                entry.pop("enum_values", None)

            if not entry:
                table_cols.pop(column, None)
            if not table_cols:
                col_dict.pop(qualified_name, None)

            self._save_store()

        return {
            "qualified_name": qualified_name,
            "column": column,
            "entry": entry,
        }

    def _load_pg_module(self) -> Any:
        if not PG_CONNECT_PATH.exists():
            raise FileNotFoundError(f"pg_connect not found: {PG_CONNECT_PATH}")
        spec = importlib.util.spec_from_file_location("pg_connect_runtime", PG_CONNECT_PATH)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load pg_connect from {PG_CONNECT_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def _load_mysql_module(self) -> Any:
        if not MYSQL_CONNECT_PATH.exists():
            raise FileNotFoundError(f"mysql connect not found: {MYSQL_CONNECT_PATH}")
        spec = importlib.util.spec_from_file_location("mysql_connect_runtime", str(MYSQL_CONNECT_PATH))
        if spec is None or spec.loader is None:
            raise RuntimeError(f"failed to load mysql connect from {MYSQL_CONNECT_PATH}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


    def _load_store(self) -> Dict[str, Any]:
        if STORE_PATH.exists():
            try:
                return json.loads(STORE_PATH.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                pass
        store = {
            "favorites": {},
            "usage": {},
            "table_meta": {},
            "column_dict": {},
        }
        STORE_PATH.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding="utf-8")
        return store

    def _ensure_sql_workspace(self) -> None:
        SQL_WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
        for folder_name in ("个人查询", "临时查询"):
            (SQL_WORKSPACE_DIR / folder_name).mkdir(exist_ok=True)
        if not SQL_WORKSPACE_META_PATH.exists():
            payload = {
                "recent_files": [],
                "favorites": [],
                "linked_tables": {},
            }
            SQL_WORKSPACE_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _ensure_dataset_store(self) -> None:
        DATASET_DIR.mkdir(parents=True, exist_ok=True)
        if not DATASET_META_PATH.exists():
            payload = {
                "recent_ids": [],
            }
            DATASET_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _ensure_dashboard_store(self) -> None:
        DASHBOARD_DIR.mkdir(parents=True, exist_ok=True)
        if not DASHBOARD_META_PATH.exists():
            payload = {
                "recent_ids": [],
            }
            DASHBOARD_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _load_sql_workspace_meta(self) -> Dict[str, Any]:
        self._ensure_sql_workspace()
        try:
            return json.loads(SQL_WORKSPACE_META_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"recent_files": [], "favorites": [], "linked_tables": {}}
            SQL_WORKSPACE_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return payload

    def _load_dataset_meta(self) -> Dict[str, Any]:
        self._ensure_dataset_store()
        try:
            return json.loads(DATASET_META_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"recent_ids": []}
            DATASET_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return payload

    def _load_dashboard_meta(self) -> Dict[str, Any]:
        self._ensure_dashboard_store()
        try:
            return json.loads(DASHBOARD_META_PATH.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {"recent_ids": []}
            DASHBOARD_META_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            return payload

    def _save_sql_workspace_meta(self, meta: Dict[str, Any]) -> None:
        self._ensure_sql_workspace()
        SQL_WORKSPACE_META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def _save_dataset_meta(self, meta: Dict[str, Any]) -> None:
        self._ensure_dataset_store()
        DATASET_META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def _save_dashboard_meta(self, meta: Dict[str, Any]) -> None:
        self._ensure_dashboard_store()
        DASHBOARD_META_PATH.write_text(json.dumps(meta, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def _resolve_sql_workspace_path(self, relative_path: str, expect_suffix: str = "") -> Path:
        self._ensure_sql_workspace()
        cleaned = str(relative_path or "").strip().strip("/")
        if expect_suffix and not cleaned:
            raise ValueError("文件路径不能为空")
        target = (SQL_WORKSPACE_DIR / cleaned).resolve()
        if expect_suffix and target.suffix.lower() != expect_suffix.lower():
            target = target.with_suffix(expect_suffix)
        workspace_root = SQL_WORKSPACE_DIR.resolve()
        if workspace_root not in [target, *target.parents]:
            raise ValueError("非法路径")
        return target

    def _build_sql_workspace_tree(self, base: Path, current: Optional[Path] = None) -> Dict[str, Any]:
        self._ensure_sql_workspace()
        node = current or base
        relative = "" if node == base else str(node.relative_to(base)).replace("\\", "/")
        if node.is_file():
            stat = node.stat()
            return {
                "type": "file",
                "name": node.name,
                "path": relative,
                "updated_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC"),
            }
        children: List[Dict[str, Any]] = []
        for child in sorted(node.iterdir(), key=lambda item: (item.is_file(), item.name.lower())):
            if child.name.startswith("."):
                continue
            child_node = self._build_sql_workspace_tree(base, child)
            if child_node.get("type") == "folder" and not child_node.get("children") and child_node.get("path") not in LOCKED_SQL_WORKSPACE_FOLDERS:
                continue
            children.append(child_node)
        return {
            "type": "folder",
            "name": node.name if node != base else "SQL 工作台",
            "path": relative,
            "children": children,
        }

    def _touch_sql_workspace_recent(self, file_path: str) -> None:
        meta = self._load_sql_workspace_meta()
        current = [item for item in meta.get("recent_files", []) if item != file_path]
        current.insert(0, file_path)
        meta["recent_files"] = current[:30]
        self._save_sql_workspace_meta(meta)

    def _workspace_relative_path(self, path: Path) -> str:
        return str(path.resolve().relative_to(SQL_WORKSPACE_DIR.resolve())).replace("\\", "/")

    def _assert_workspace_entry_mutable(self, relative_path: str) -> None:
        normalized = str(relative_path or "").strip().strip("/")
        if normalized in LOCKED_SQL_WORKSPACE_FOLDERS:
            raise ValueError("系统目录不支持重命名、移动或删除")

    def _remap_sql_workspace_meta(self, old_path: str, new_path: str, source_was_folder: bool) -> None:
        meta = self._load_sql_workspace_meta()
        if source_was_folder:
            prefix = old_path.rstrip("/") + "/"
            linked_tables = {}
            for key, value in meta.get("linked_tables", {}).items():
                if key == old_path or key.startswith(prefix):
                    suffix = key[len(old_path):].lstrip("/")
                    next_key = "/".join(part for part in [new_path, suffix] if part)
                    linked_tables[next_key] = value
                else:
                    linked_tables[key] = value
            meta["linked_tables"] = linked_tables
            meta["recent_files"] = [
                "/".join(part for part in [new_path, item[len(old_path):].lstrip("/")] if part) if (item == old_path or item.startswith(prefix)) else item
                for item in meta.get("recent_files", [])
            ]
        else:
            linked = meta.get("linked_tables", {})
            if old_path in linked:
                linked[new_path] = linked.pop(old_path)
            meta["recent_files"] = [new_path if item == old_path else item for item in meta.get("recent_files", [])]
        self._save_sql_workspace_meta(meta)

    def _remove_sql_workspace_meta(self, target_path: str, target_was_folder: bool) -> None:
        meta = self._load_sql_workspace_meta()
        if target_was_folder:
            prefix = target_path.rstrip("/") + "/"
            meta["linked_tables"] = {
                key: value
                for key, value in meta.get("linked_tables", {}).items()
                if key != target_path and not key.startswith(prefix)
            }
            meta["recent_files"] = [
                item for item in meta.get("recent_files", [])
                if item != target_path and not item.startswith(prefix)
            ]
        else:
            meta.setdefault("linked_tables", {}).pop(target_path, None)
            meta["recent_files"] = [item for item in meta.get("recent_files", []) if item != target_path]
        self._save_sql_workspace_meta(meta)

    def _sanitize_sql_file_name(self, file_name: str) -> str:
        value = re.sub(r"[\\\\/:*?\"<>|]+", "_", str(file_name or "").strip())
        value = value or "未命名.sql"
        if not value.lower().endswith(".sql"):
            value += ".sql"
        return value

    def _sanitize_asset_id(self, value: str, fallback: str) -> str:
        raw = re.sub(r"[^a-zA-Z0-9_\\-]+", "_", str(value or "").strip().lower()).strip("_")
        return raw or fallback

    def _dataset_file_path(self, dataset_id: str) -> Path:
        self._ensure_dataset_store()
        safe_id = self._sanitize_asset_id(dataset_id, "dataset")
        return DATASET_DIR / f"{safe_id}.json"

    def _dashboard_file_path(self, dashboard_id: str) -> Path:
        self._ensure_dashboard_store()
        safe_id = self._sanitize_asset_id(dashboard_id, "dashboard")
        return DASHBOARD_DIR / f"{safe_id}.json"

    def list_datasets(self) -> Dict[str, Any]:
        self._ensure_dataset_store()
        items: List[Dict[str, Any]] = []
        for path in sorted(DATASET_DIR.glob("*.json"), key=lambda item: item.name.lower()):
            if path.name.startswith("."):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            items.append({
                "id": payload.get("id") or path.stem,
                "name": payload.get("name") or path.stem,
                "description": payload.get("description") or "",
                "source_type": payload.get("source_type") or "",
                "source_ref": payload.get("source_ref") or "",
                "datasource_key": payload.get("datasource_key") or "",
                "status": payload.get("status") or "draft",
                "updated_at": payload.get("updated_at") or "",
                "created_by": payload.get("created_by") or "",
            })
        return {"items": items}

    def read_dataset(self, dataset_id: str) -> Dict[str, Any]:
        path = self._dataset_file_path(dataset_id)
        if not path.exists():
            raise ValueError(f"dataset not found: {dataset_id}")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"dataset corrupted: {dataset_id}") from exc

    def create_dataset(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        dataset_id = self._sanitize_asset_id(payload.get("id") or payload.get("name") or "dataset", "dataset")
        path = self._dataset_file_path(dataset_id)
        if path.exists():
            raise ValueError(f"dataset already exists: {dataset_id}")
        now = self._iso_now()
        dataset = {
            "id": dataset_id,
            "name": self._clean_text(payload.get("name")) or dataset_id,
            "description": self._clean_text(payload.get("description")),
            "source_type": self._clean_text(payload.get("source_type")) or "table",
            "source_ref": self._clean_text(payload.get("source_ref")),
            "datasource_key": self._clean_text(payload.get("datasource_key")),
            "linked_table": self._clean_text(payload.get("linked_table")),
            "grain": self._clean_text(payload.get("grain")),
            "time_field": self._clean_text(payload.get("time_field")),
            "dimensions": payload.get("dimensions") if isinstance(payload.get("dimensions"), list) else [],
            "metrics": payload.get("metrics") if isinstance(payload.get("metrics"), list) else [],
            "default_filters": payload.get("default_filters") if isinstance(payload.get("default_filters"), list) else [],
            "status": self._clean_text(payload.get("status")) or "draft",
            "created_by": self._clean_text(payload.get("created_by")) or "system",
            "created_at": now,
            "updated_at": now,
        }
        path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
        meta = self._load_dataset_meta()
        recent = [item for item in meta.get("recent_ids", []) if item != dataset_id]
        recent.insert(0, dataset_id)
        meta["recent_ids"] = recent[:30]
        self._save_dataset_meta(meta)
        return dataset

    def update_dataset(self, dataset_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._dataset_file_path(dataset_id)
        if not path.exists():
            raise ValueError(f"dataset not found: {dataset_id}")
        dataset = self.read_dataset(dataset_id)
        linked_table = self._clean_text(payload.get("linked_table")) or dataset.get("linked_table") or ""
        source_ref = self._clean_text(payload.get("source_ref")) or linked_table or dataset.get("source_ref") or ""
        datasource_key = self._clean_text(payload.get("datasource_key"))
        if not datasource_key and "." in linked_table:
            datasource_key = linked_table.split(".", 1)[0]
        datasource_key = datasource_key or dataset.get("datasource_key") or ""
        dimensions = self._normalize_dataset_dimensions(payload.get("dimensions"))
        metrics = self._normalize_dataset_metrics(payload.get("metrics"))
        default_filters = payload.get("default_filters") if isinstance(payload.get("default_filters"), list) else dataset.get("default_filters", [])
        dataset.update({
            "name": self._clean_text(payload.get("name")) or dataset.get("name") or dataset_id,
            "description": self._clean_text(payload.get("description")),
            "source_type": self._clean_text(payload.get("source_type")) or dataset.get("source_type") or "table",
            "source_ref": source_ref,
            "datasource_key": datasource_key,
            "linked_table": linked_table,
            "grain": self._clean_text(payload.get("grain")) or dataset.get("grain") or "",
            "time_field": self._clean_text(payload.get("time_field")),
            "dimensions": dimensions,
            "metrics": metrics,
            "default_filters": default_filters,
            "status": self._clean_text(payload.get("status")) or dataset.get("status") or "draft",
            "updated_at": self._iso_now(),
        })
        path.write_text(json.dumps(dataset, ensure_ascii=False, indent=2), encoding="utf-8")
        return dataset

    def delete_dataset(self, dataset_id: str) -> Dict[str, Any]:
        path = self._dataset_file_path(dataset_id)
        if not path.exists():
            raise ValueError(f"dataset not found: {dataset_id}")
        path.unlink()
        meta = self._load_dataset_meta()
        meta["recent_ids"] = [item for item in meta.get("recent_ids", []) if item != dataset_id]
        self._save_dataset_meta(meta)
        return {
            "ok": True,
            "deleted_id": dataset_id,
        }

    def list_dashboards(self) -> Dict[str, Any]:
        self._ensure_dashboard_store()
        items: List[Dict[str, Any]] = []
        for path in sorted(DASHBOARD_DIR.glob("*.json"), key=lambda item: item.name.lower()):
            if path.name.startswith("."):
                continue
            try:
                payload = json.loads(path.read_text(encoding="utf-8"))
            except json.JSONDecodeError:
                continue
            items.append({
                "id": payload.get("id") or path.stem,
                "name": payload.get("name") or path.stem,
                "description": payload.get("description") or "",
                "dataset_id": payload.get("dataset_id") or "",
                "status": payload.get("status") or "draft",
                "updated_at": payload.get("updated_at") or "",
                "created_by": payload.get("created_by") or "",
            })
        return {"items": items}

    def read_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        path = self._dashboard_file_path(dashboard_id)
        if not path.exists():
            raise ValueError(f"dashboard not found: {dashboard_id}")
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"dashboard corrupted: {dashboard_id}") from exc

    def create_dashboard(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        dashboard_id = self._sanitize_asset_id(payload.get("id") or payload.get("name") or "dashboard", "dashboard")
        path = self._dashboard_file_path(dashboard_id)
        if path.exists():
            raise ValueError(f"dashboard already exists: {dashboard_id}")
        now = self._iso_now()
        dataset_id = self._clean_text(payload.get("dataset_id"))
        dataset = self.read_dataset(dataset_id) if dataset_id else None
        widgets = self._normalize_dashboard_widgets(payload.get("widgets"))
        if not widgets:
            widgets = self._default_dashboard_widgets(dataset)
        dashboard = {
            "id": dashboard_id,
            "name": self._clean_text(payload.get("name")) or dashboard_id,
            "description": self._clean_text(payload.get("description")),
            "dataset_id": dataset_id,
            "status": self._clean_text(payload.get("status")) or "draft",
            "layout_mode": self._clean_text(payload.get("layout_mode")) or "overview",
            "theme": payload.get("theme") if isinstance(payload.get("theme"), dict) else {"name": "slate_blue", "density": "normal", "radius": 12},
            "layout": payload.get("layout") if isinstance(payload.get("layout"), dict) else {"columns": 12, "row_height": 72, "gap": 12},
            "global_filters": payload.get("global_filters") if isinstance(payload.get("global_filters"), list) else [],
            "widgets": widgets,
            "created_by": self._clean_text(payload.get("created_by")) or "system",
            "created_at": now,
            "updated_at": now,
        }
        path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
        meta = self._load_dashboard_meta()
        recent = [item for item in meta.get("recent_ids", []) if item != dashboard_id]
        recent.insert(0, dashboard_id)
        meta["recent_ids"] = recent[:30]
        self._save_dashboard_meta(meta)
        return dashboard

    def update_dashboard(self, dashboard_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        path = self._dashboard_file_path(dashboard_id)
        if not path.exists():
            raise ValueError(f"dashboard not found: {dashboard_id}")
        dashboard = self.read_dashboard(dashboard_id)
        dataset_id = self._clean_text(payload.get("dataset_id")) or dashboard.get("dataset_id") or ""
        widgets = self._normalize_dashboard_widgets(payload.get("widgets"))
        dashboard.update({
            "name": self._clean_text(payload.get("name")) or dashboard.get("name") or dashboard_id,
            "description": self._clean_text(payload.get("description")),
            "dataset_id": dataset_id,
            "status": self._clean_text(payload.get("status")) or dashboard.get("status") or "draft",
            "layout_mode": self._clean_text(payload.get("layout_mode")) or dashboard.get("layout_mode") or "overview",
            "global_filters": payload.get("global_filters") if isinstance(payload.get("global_filters"), list) else dashboard.get("global_filters", []),
            "widgets": widgets or dashboard.get("widgets", []),
            "updated_at": self._iso_now(),
        })
        path.write_text(json.dumps(dashboard, ensure_ascii=False, indent=2), encoding="utf-8")
        return dashboard

    def preview_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        dashboard = self.read_dashboard(dashboard_id)
        dataset_id = self._clean_text(dashboard.get("dataset_id"))
        if not dataset_id:
            raise ValueError("dashboard dataset_id is empty")
        dataset = self.read_dataset(dataset_id)
        context = self._get_dataset_query_context(dataset)
        widgets = []
        for widget in dashboard.get("widgets", []):
            try:
                preview = self._preview_dashboard_widget(context, widget)
            except Exception as exc:
                preview = {
                    "id": widget.get("id") or "",
                    "type": widget.get("type") or "",
                    "title": widget.get("title") or "",
                    "binding": widget.get("binding") if isinstance(widget.get("binding"), dict) else {},
                    "position": widget.get("position") if isinstance(widget.get("position"), dict) else {},
                    "error": str(exc),
                    "data": None,
                }
            widgets.append(preview)
        return {
            "dashboard_id": dashboard_id,
            "dataset_id": dataset_id,
            "dataset": dataset,
            "widgets": widgets,
            "generated_at": self._iso_now(),
        }

    def _save_store(self) -> None:
        payload = json.dumps(self._store, ensure_ascii=False, indent=2, sort_keys=True)
        STORE_PATH.write_text(payload, encoding="utf-8")

    def _ensure_snapshot(self) -> Dict[str, Any]:
        snapshot = self._snapshot
        if snapshot is None:
            return self._load_snapshot()
        if time.time() - snapshot["loaded_at_epoch"] > self.refresh_seconds:
            return self._load_snapshot()
        return snapshot

    def _empty_snapshot(self, load_errors: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
        now = time.time()
        return {
            "loaded_at": self._iso_now(),
            "loaded_at_epoch": now,
            "table_count": 0,
            "column_count": 0,
            "tables": [],
            "tables_by_key": {},
            "columns_by_key": {},
            "primary_keys_by_key": {},
            "foreign_keys_outgoing_by_key": {},
            "foreign_keys_incoming_by_key": {},
            "load_errors": load_errors or [],
        }

    def _load_snapshot(self) -> Dict[str, Any]:
        previous_snapshot = self._snapshot
        columns_by_key: Dict[str, List[Dict[str, Any]]] = {}
        primary_keys_by_key: Dict[str, List[Dict[str, Any]]] = {}
        foreign_keys_outgoing_by_key: Dict[str, List[Dict[str, Any]]] = {}
        foreign_keys_incoming_by_key: Dict[str, List[Dict[str, Any]]] = {}
        tables: List[Dict[str, Any]] = []
        tables_by_key: Dict[str, Dict[str, Any]] = {}
        column_count = 0
        load_errors: List[Dict[str, Any]] = []

        for ds in self._datasources:
            try:
                if ds["type"] == "mysql":
                    db = ds["database"]
                    tables_rows = self._query(self._mysql_tables_sql(), (db,), ds)
                    columns_rows = self._query(self._mysql_columns_sql(), (db,), ds)
                    pk_rows = self._query(self._mysql_primary_keys_sql(), (db,), ds)
                    fk_rows = self._query(self._mysql_foreign_keys_sql(), (db,), ds)
                else:
                    tables_rows = self._query(self._tables_sql(), None, ds)
                    columns_rows = self._query(self._columns_sql(), None, ds)
                    pk_rows = self._query(self._primary_keys_sql(), None, ds)
                    fk_rows = self._query(self._foreign_keys_sql(), None, ds)
            except Exception as exc:
                load_errors.append({
                    "datasource": ds.get("label") or ds.get("key") or ds.get("schema") or "unknown",
                    "error": str(exc),
                })
                continue

            column_count += len(columns_rows)

            for row in columns_rows:
                key = self._qualified_name(row["table_schema"], row["table_name"])
                columns_by_key.setdefault(key, []).append(
                    {
                        "column_name": row["column_name"],
                        "data_type": row["data_type"],
                        "udt_name": row.get("udt_name"),
                        "is_nullable": row["is_nullable"] == "YES",
                        "ordinal_position": row["ordinal_position"],
                        "character_maximum_length": row.get("character_maximum_length"),
                        "numeric_precision": row.get("numeric_precision"),
                        "numeric_scale": row.get("numeric_scale"),
                        "column_default": row.get("column_default"),
                        "column_comment": row.get("column_comment") or "",
                        "null_frac": self._safe_float(row.get("null_frac")),
                        "n_distinct": self._safe_float(row.get("n_distinct")),
                        "most_common_vals": row.get("most_common_vals") or "",
                        "histogram_bounds": row.get("histogram_bounds") or "",
                        "avg_width": row.get("avg_width"),
                        "correlation": self._safe_float(row.get("correlation")),
                    }
                )

            for row in pk_rows:
                key = self._qualified_name(row["table_schema"], row["table_name"])
                primary_keys_by_key.setdefault(key, []).append(
                    {
                        "constraint_name": row["constraint_name"],
                        "column_name": row["column_name"],
                        "ordinal_position": row["ordinal_position"],
                    }
                )

            for row in fk_rows:
                source_key = self._qualified_name(row["table_schema"], row["table_name"])
                target_key = self._qualified_name(row["foreign_table_schema"], row["foreign_table_name"])
                item = {
                    "constraint_name": row["constraint_name"],
                    "column_name": row["column_name"],
                    "foreign_table_schema": row["foreign_table_schema"],
                    "foreign_table_name": row["foreign_table_name"],
                    "foreign_column_name": row["foreign_column_name"],
                    "foreign_qualified_name": target_key,
                }
                foreign_keys_outgoing_by_key.setdefault(source_key, []).append(item)
                foreign_keys_incoming_by_key.setdefault(target_key, []).append(
                    {
                        "constraint_name": row["constraint_name"],
                        "source_table_schema": row["table_schema"],
                        "source_table_name": row["table_name"],
                        "source_column_name": row["column_name"],
                        "source_qualified_name": source_key,
                        "column_name": row["foreign_column_name"],
                    }
                )

            for row in tables_rows:
                key = self._qualified_name(row["table_schema"], row["table_name"])
                total_bytes = int(row.get("total_size_bytes") or 0)
                item = {
                    "schema_name": row["table_schema"],
                    "table_name": row["table_name"],
                    "qualified_name": key,
                    "table_comment": row.get("table_comment") or "",
                    # PG 未执行 ANALYZE 时 reltuples 为 -1；MySQL information_schema TABLE_ROWS 也偶尔为 NULL/-1
                    # 统一规整：< 0 视为"未知"（0），交给前端展示降级文案
                    "estimated_rows": max(int(row.get("estimated_rows") or 0), 0),
                    "total_size_bytes": total_bytes,
                    "total_size_pretty": row.get("total_size_pretty") or self._pretty_size(total_bytes),
                    "db_owner": row.get("db_owner") or "",
                    "is_partitioned": bool(row.get("is_partitioned")),
                    "partition_key": row.get("partition_key") or "",
                    "last_vacuum": row.get("last_vacuum"),
                    "last_autovacuum": row.get("last_autovacuum"),
                    "last_analyze": row.get("last_analyze"),
                    "last_autoanalyze": row.get("last_autoanalyze"),
                    "n_live_tup": int(row.get("n_live_tup") or 0),
                    "n_dead_tup": int(row.get("n_dead_tup") or 0),
                    "source_type": ds["source_type"],
                    "storage_type": row.get("storage_type") or "BASE TABLE",
                    "database": ds["label"],
                    "lineage_connection": ds.get("lineage_connection") or "",
                }
                tables.append(item)
                tables_by_key[key] = item

        now = time.time()
        snapshot = {
            "loaded_at": self._iso_now(),
            "loaded_at_epoch": now,
            "table_count": len(tables),
            "column_count": column_count,
            "tables": tables,
            "tables_by_key": tables_by_key,
            "columns_by_key": columns_by_key,
            "primary_keys_by_key": primary_keys_by_key,
            "foreign_keys_outgoing_by_key": foreign_keys_outgoing_by_key,
            "foreign_keys_incoming_by_key": foreign_keys_incoming_by_key,
            "load_errors": load_errors,
        }
        if not tables and previous_snapshot is not None:
            previous_snapshot["load_errors"] = load_errors
            return previous_snapshot
        if not tables:
            return self._empty_snapshot(load_errors)
        self._snapshot = snapshot
        return snapshot

    @staticmethod
    def _pretty_size(num_bytes: int) -> str:
        size = float(num_bytes or 0)
        for unit in ("bytes", "kB", "MB", "GB", "TB"):
            if size < 1024 or unit == "TB":
                if unit == "bytes":
                    return f"{int(size)} bytes"
                return f"{size:.0f} {unit}"
            size /= 1024
        return f"{int(num_bytes)} bytes"


    def _ranked_tables(self, snapshot: Dict[str, Any]) -> List[Dict[str, Any]]:
        ranked = [self._table_summary(snapshot, item) for item in snapshot["tables"]]
        ranked.sort(
            key=lambda item: (
                0 if item["favorite"] else 1,
                -(item.get("view_count") or 0),
                -(item.get("estimated_rows") or 0),
                item["qualified_name"],
            )
        )
        return ranked

    def _table_summary(self, snapshot: Dict[str, Any], table: Dict[str, Any]) -> Dict[str, Any]:
        metadata = self._table_metadata(table["qualified_name"])
        usage = self._usage(table["qualified_name"])
        primary_keys = snapshot["primary_keys_by_key"].get(table["qualified_name"], [])
        foreign_keys_outgoing = snapshot["foreign_keys_outgoing_by_key"].get(table["qualified_name"], [])
        foreign_keys_incoming = snapshot["foreign_keys_incoming_by_key"].get(table["qualified_name"], [])
        columns = snapshot["columns_by_key"].get(table["qualified_name"], [])
        return {
            "schema_name": table["schema_name"],
            "table_name": table["table_name"],
            "qualified_name": table["qualified_name"],
            "table_comment": table["table_comment"],
            "alias": metadata.get("alias") or "",
            "business_terms": metadata.get("business_terms") or [],
            "description": metadata.get("description") or "",
            "owner": metadata.get("owner") or table["db_owner"],
            "db_owner": table["db_owner"],
            "business_domain": metadata.get("business_domain") or "",
            "project": metadata.get("project") or "",
            "source_type": metadata.get("source_type") or table["source_type"],
            "storage_type": metadata.get("storage_type") or table["storage_type"],
            "ttl_days": metadata.get("ttl_days"),
            "created_time_text": metadata.get("created_time_text") or "",
            "last_modified_time_text": metadata.get("last_modified_time_text") or "",
            "favorite": bool(self._store.get("favorites", {}).get(table["qualified_name"])),
            "view_count": usage.get("view_count", 0),
            "last_viewed_at": usage.get("last_viewed_at") or "",
            "estimated_rows": table["estimated_rows"],
            "total_size_bytes": table["total_size_bytes"],
            "total_size_pretty": table["total_size_pretty"],
            "column_count": len(columns),
            "primary_key_count": len(primary_keys),
            "foreign_key_outgoing_count": len(foreign_keys_outgoing),
            "foreign_key_incoming_count": len(foreign_keys_incoming),
            "is_partitioned": table["is_partitioned"],
            "partition_key": table["partition_key"],
            "database": table.get("database") or "",
            "lineage_connection": table.get("lineage_connection") or "",
        }

    def _table_metadata(self, qualified_name: str) -> Dict[str, Any]:
        return dict(self._store.get("table_meta", {}).get(qualified_name, {}))

    def _usage(self, qualified_name: str) -> Dict[str, Any]:
        return dict(self._store.get("usage", {}).get(qualified_name, {}))

    def _record_usage(self, qualified_name: str) -> None:
        with self._store_lock:
            usage = self._store.setdefault("usage", {}).setdefault(qualified_name, {})
            usage["view_count"] = int(usage.get("view_count") or 0) + 1
            usage["last_viewed_at"] = self._iso_now()
            self._save_store()

    def _score_table(
        self,
        table: Dict[str, Any],
        summary: Dict[str, Any],
        keyword: str,
        exact: bool,
    ) -> Tuple[int, List[str]]:
        if not keyword:
            return 1, []

        reasons: List[str] = []
        lower_table = summary["table_name"].lower()
        lower_qualified = summary["qualified_name"].lower()
        alias = (summary.get("alias") or "").lower()
        comment = (summary.get("table_comment") or "").lower()
        owner = (summary.get("owner") or "").lower()
        terms = [term.lower() for term in summary.get("business_terms") or []]
        columns = self._snapshot["columns_by_key"].get(summary["qualified_name"], []) if self._snapshot else []
        column_names = [column["column_name"].lower() for column in columns]
        column_comments = [(column.get("column_comment") or "").lower() for column in columns]

        exact_candidates = {
            lower_table,
            lower_qualified,
            alias,
            owner,
            *terms,
        }
        if exact:
            if keyword in exact_candidates:
                return 120, ["exact"]
            if keyword in column_names:
                return 110, ["column"]
            return 0, []

        score = 0
        if keyword == lower_table or keyword == lower_qualified:
            score = max(score, 120)
            reasons.append("table")
        elif lower_table.startswith(keyword) or lower_qualified.endswith(keyword):
            score = max(score, 100)
            reasons.append("table")
        elif keyword in lower_table or keyword in lower_qualified:
            score = max(score, 88)
            reasons.append("table")

        if alias:
            if keyword == alias:
                score = max(score, 98)
                reasons.append("alias")
            elif keyword in alias:
                score = max(score, 82)
                reasons.append("alias")

        if any(keyword == term for term in terms):
            score = max(score, 90)
            reasons.append("term")
        elif any(keyword in term for term in terms):
            score = max(score, 74)
            reasons.append("term")

        if keyword == owner or keyword in owner:
            score = max(score, 72)
            reasons.append("owner")

        if comment:
            if keyword == comment or keyword in comment:
                score = max(score, 76)
                reasons.append("comment")

        if any(keyword == name for name in column_names):
            score = max(score, 86)
            reasons.append("column")
        elif any(keyword in name for name in column_names):
            score = max(score, 68)
            reasons.append("column")

        if any(keyword and keyword in text for text in column_comments if text):
            score = max(score, 64)
            reasons.append("column_comment")

        if keyword == (summary.get("business_domain") or "").lower():
            score = max(score, 62)
            reasons.append("domain")
        if keyword == (summary.get("project") or "").lower():
            score = max(score, 60)
            reasons.append("project")

        return score, sorted(set(reasons))

    def _filter_options(self, snapshot: Dict[str, Any]) -> Dict[str, List[str]]:
        summaries = [self._table_summary(snapshot, item) for item in snapshot["tables"]]
        return {
            "source_types": self._sorted_unique(item["source_type"] for item in summaries),
            "databases": self._sorted_unique(item.get("database") or "" for item in summaries),
            "business_domains": self._sorted_unique(item["business_domain"] for item in summaries),
            "projects": self._sorted_unique(item["project"] for item in summaries),
            "storage_types": self._sorted_unique(item["storage_type"] for item in summaries),
            "owners": self._sorted_unique(item["owner"] for item in summaries),
        }

    def _build_preview_payload(
        self,
        schema: str,
        table: str,
        sample_limit: int,
        raw_columns: List[Dict[str, Any]],
        preview_column: str = "",
        preview_keyword: str = "",
    ) -> Dict[str, Any]:
        column_names = [str(column.get("column_name") or "") for column in raw_columns if column.get("column_name")]
        selected_column = self._clean_text(preview_column)
        keyword = self._clean_text(preview_keyword)
        if selected_column and selected_column not in column_names:
            raise ValueError(f"preview column not found: {selected_column}")

        sample_rows, preview_error = self._preview_rows(
            schema=schema,
            table=table,
            sample_limit=sample_limit,
            column_names=column_names,
            preview_column=selected_column,
            preview_keyword=keyword,
        )
        return {
            "sample_limit": sample_limit,
            "rows": sample_rows,
            "error": preview_error,
            "filter": {
                "column": selected_column,
                "keyword": keyword,
                "applied": bool(keyword),
            },
        }

    def _build_table_ddl_payload(
        self,
        schema: str,
        table: str,
        table_item: Dict[str, Any],
        raw_columns: List[Dict[str, Any]],
        primary_keys: List[Dict[str, Any]],
        foreign_keys_outgoing: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        ds = self._datasource_for_schema(schema)
        ddl_text, source, notice = self._fetch_native_table_ddl(schema, table, ds)
        if not ddl_text:
            ddl_text = self._generate_table_ddl(
                schema=schema,
                table=table,
                ds=ds,
                table_item=table_item,
                raw_columns=raw_columns,
                primary_keys=primary_keys,
                foreign_keys_outgoing=foreign_keys_outgoing,
            )
            source = "generated"
            notice = notice or (
                "当前展示按元数据生成的 DDL，适合快速查看表结构。"
                if ds.get("type") != "mysql"
                else "未取到数据库原始 DDL，当前展示按元数据生成的版本。"
            )
        return {
            "text": ddl_text,
            "dialect": ds.get("type") or "postgresql",
            "source": source,
            "notice": notice,
        }

    def _fetch_native_table_ddl(self, schema: str, table: str, ds: Dict[str, Any]) -> Tuple[str, str, str]:
        if ds.get("type") != "mysql":
            return "", "", ""
        try:
            sql = f"SHOW CREATE TABLE {self._quote_ident_for(schema, ds)}.{self._quote_ident_for(table, ds)}"
            rows = self._query(sql, None, ds)
            if not rows:
                return "", "", ""
            row = rows[0]
            ddl_text = ""
            for key, value in row.items():
                if str(key).lower().startswith("create") and value:
                    ddl_text = str(value)
                    break
            return ddl_text, ("native" if ddl_text else ""), ""
        except Exception as exc:
            return "", "", f"原始 DDL 获取失败：{exc}"

    def _generate_table_ddl(
        self,
        schema: str,
        table: str,
        ds: Dict[str, Any],
        table_item: Dict[str, Any],
        raw_columns: List[Dict[str, Any]],
        primary_keys: List[Dict[str, Any]],
        foreign_keys_outgoing: List[Dict[str, Any]],
    ) -> str:
        q = lambda value: self._quote_ident_for(value, ds)
        column_lines: List[str] = []
        for column in raw_columns:
            line = f"  {q(column['column_name'])} {self._ddl_type_text(column, ds)}"
            default = column.get("column_default")
            if default not in (None, ""):
                line += f" DEFAULT {default}"
            line += " NULL" if column.get("is_nullable") else " NOT NULL"
            if ds.get("type") == "mysql" and column.get("column_comment"):
                line += f" COMMENT '{self._sql_escape_literal(column.get('column_comment') or '')}'"
            column_lines.append(line)

        constraint_lines: List[str] = []
        pk_columns = [item.get("column_name") for item in primary_keys if item.get("column_name")]
        if pk_columns:
            pk_sql = ", ".join(q(column_name) for column_name in pk_columns)
            if ds.get("type") == "mysql":
                constraint_lines.append(f"  PRIMARY KEY ({pk_sql})")
            else:
                pk_name = primary_keys[0].get("constraint_name") or f"{table}_pkey"
                constraint_lines.append(f"  CONSTRAINT {q(pk_name)} PRIMARY KEY ({pk_sql})")

        for fk in foreign_keys_outgoing:
            column_name = fk.get("column_name")
            foreign_schema = fk.get("foreign_table_schema")
            foreign_table = fk.get("foreign_table_name")
            foreign_column = fk.get("foreign_column_name")
            constraint_name = fk.get("constraint_name") or ""
            if not all([column_name, foreign_schema, foreign_table, foreign_column]):
                continue
            fk_line = (
                f"  CONSTRAINT {q(constraint_name or f'{table}_{column_name}_fkey')} "
                f"FOREIGN KEY ({q(column_name)}) REFERENCES {q(foreign_schema)}.{q(foreign_table)} ({q(foreign_column)})"
            )
            constraint_lines.append(fk_line)

        body = ",\n".join(column_lines + constraint_lines)
        ddl = f"CREATE TABLE {q(schema)}.{q(table)} (\n{body}\n)"

        table_comment = str(table_item.get("table_comment") or "")
        if ds.get("type") == "mysql":
            if table_comment:
                ddl += f"\nCOMMENT='{self._sql_escape_literal(table_comment)}'"
            ddl += ";"
            return ddl

        ddl += ";"
        if table_comment:
            ddl += f"\n\nCOMMENT ON TABLE {q(schema)}.{q(table)} IS '{self._sql_escape_literal(table_comment)}';"
        comment_lines = []
        for column in raw_columns:
            column_comment = str(column.get("column_comment") or "")
            if not column_comment:
                continue
            comment_lines.append(
                f"COMMENT ON COLUMN {q(schema)}.{q(table)}.{q(column['column_name'])} IS '{self._sql_escape_literal(column_comment)}';"
            )
        if comment_lines:
            ddl += "\n\n" + "\n".join(comment_lines)
        return ddl

    def _preview_rows(
        self,
        schema: str,
        table: str,
        sample_limit: int,
        column_names: List[str],
        preview_column: str = "",
        preview_keyword: str = "",
    ) -> Tuple[List[Dict[str, Any]], str]:
        ds = self._datasource_for_schema(schema)
        is_mysql = ds.get("type") == "mysql"
        like_op = "LIKE" if is_mysql else "ILIKE"
        cast_type = "CHAR" if is_mysql else "text"
        q = lambda v: self._quote_ident_for(v, ds)
        try:
            sql = f"SELECT * FROM {q(schema)}.{q(table)}"
            params: Optional[Tuple[Any, ...]] = None
            if preview_keyword:
                needle = f"%{preview_keyword}%"
                if preview_column:
                    sql += f" WHERE CAST({q(preview_column)} AS {cast_type}) {like_op} %s"
                    params = (needle,)
                elif column_names:
                    clauses = [f"CAST({q(column_name)} AS {cast_type}) {like_op} %s" for column_name in column_names]
                    sql += " WHERE " + " OR ".join(clauses)
                    params = tuple(needle for _ in clauses)
            sql += f" LIMIT {int(sample_limit)}"
            rows = self._query(sql, params, ds)
            return rows, ""
        except Exception as exc:
            return [], str(exc)

    def _column_quality_payload(self, column: Dict[str, Any], estimated_rows: int) -> Dict[str, Any]:
        null_frac = column.get("null_frac")
        n_distinct = column.get("n_distinct")
        distinct_estimate = None
        if n_distinct is not None:
            if n_distinct >= 0:
                distinct_estimate = int(n_distinct)
            elif estimated_rows > 0:
                distinct_estimate = int(abs(n_distinct) * estimated_rows)
        uniqueness_ratio = None
        if distinct_estimate is not None and estimated_rows > 0:
            uniqueness_ratio = round(min(distinct_estimate / estimated_rows, 1.0), 4)
        return {
            **column,
            "null_rate": round(null_frac, 4) if null_frac is not None else None,
            "missing_rate_text": self._percent_text(null_frac),
            "distinct_estimate": distinct_estimate,
            "uniqueness_ratio": uniqueness_ratio,
            "uniqueness_text": self._percent_text(uniqueness_ratio),
            "distribution_hint": column.get("most_common_vals") or column.get("histogram_bounds") or "",
        }

    def _quality_summary(self, columns: List[Dict[str, Any]]) -> Dict[str, Any]:
        nullable_columns = [item for item in columns if item.get("null_rate") is not None]
        high_missing = [item for item in nullable_columns if (item.get("null_rate") or 0) >= 0.5]
        high_unique = [item for item in columns if (item.get("uniqueness_ratio") or 0) >= 0.95]
        return {
            "column_count": len(columns),
            "high_missing_count": len(high_missing),
            "high_unique_count": len(high_unique),
            "top_missing_columns": [
                {
                    "column_name": item["column_name"],
                    "missing_rate_text": item["missing_rate_text"],
                }
                for item in sorted(high_missing, key=lambda item: item.get("null_rate") or 0, reverse=True)[:5]
            ],
            "top_unique_columns": [
                {
                    "column_name": item["column_name"],
                    "uniqueness_text": item["uniqueness_text"],
                }
                for item in sorted(high_unique, key=lambda item: item.get("uniqueness_ratio") or 0, reverse=True)[:5]
            ],
        }

    def _normalize_sql_query(self, sql: str) -> str:
        normalized = str(sql or "").strip()
        if not normalized:
            raise ValueError("SQL 不能为空")
        normalized = normalized.rstrip(";").rstrip()
        if not normalized:
            raise ValueError("SQL 不能为空")
        if ";" in normalized:
            raise ValueError("仅支持单条 SELECT 查询，不允许多语句")
        lowered = normalized.lower()
        if not (lowered.startswith("select") or lowered.startswith("with")):
            raise ValueError("当前仅支持 SELECT / WITH 查询")
        compact = re.sub(r"\s+", " ", lowered)
        if re.search(r"\b(insert|update|delete|drop|alter|create|truncate|replace|merge|grant|revoke|call|execute|exec|commit|rollback)\b", compact):
            raise ValueError("当前仅支持只读查询")
        return normalized

    def _ensure_sql_limit(self, sql: str, row_limit: int) -> str:
        compact = re.sub(r"\s+", " ", sql.strip().lower())
        if re.search(r"\blimit\s+\d+(\s*,\s*\d+)?\b", compact):
            return sql
        return f"{sql}\nLIMIT {int(row_limit)}"

    def _build_sql_count_query(self, sql: str) -> str:
        return f"SELECT COUNT(*) AS total_count FROM (\n{sql}\n) AS sql_workbench_count"

    def _build_sql_page_query(self, sql: str, page_size: int, offset: int) -> str:
        return (
            "SELECT * FROM (\n"
            f"{sql}\n"
            ") AS sql_workbench_page\n"
            f"LIMIT {int(page_size)} OFFSET {int(offset)}"
        )

    def _resolve_workspace_columns(self, sql: str, ds: Dict[str, Any]) -> List[str]:
        aliases = [column for column in self._extract_select_aliases(sql) if column and column != "*"]
        try:
            probe_sql = self._build_sql_page_query(sql, page_size=1, offset=0)
            rows = self._query(probe_sql, None, ds)
            if rows:
                return list(rows[0].keys())
        except Exception:
            pass
        return aliases

    def _normalize_workspace_column_filters(self, column_filters: Optional[Dict[str, Any]]) -> Dict[str, List[str]]:
        normalized: Dict[str, List[str]] = {}
        if not isinstance(column_filters, dict):
            return normalized
        for raw_column, raw_values in column_filters.items():
            column_name = str(raw_column or "").strip()
            if not column_name:
                continue
            values = raw_values if isinstance(raw_values, (list, tuple, set)) else [raw_values]
            deduped: List[str] = []
            seen = set()
            for value in values:
                token = str(value if value is not None else "").strip()
                if not token or token in seen:
                    continue
                seen.add(token)
                deduped.append(token)
            if deduped:
                normalized[column_name] = deduped
        return normalized

    def _workspace_filter_label(self, token: str) -> str:
        if token == WORKBENCH_FILTER_NULL:
            return "(空值)"
        if token == WORKBENCH_FILTER_EMPTY:
            return "(空字符串)"
        return str(token or "")

    def _build_workspace_filter_query(
        self,
        sql: str,
        ds: Dict[str, Any],
        available_columns: List[str],
        column_filters: Optional[Dict[str, Any]] = None,
    ) -> Tuple[str, Optional[Tuple[Any, ...]], Dict[str, Any]]:
        normalized_filters = self._normalize_workspace_column_filters(column_filters)
        meta = {
            "applied": False,
            "count": len(normalized_filters),
            "items": [
                {
                    "column": column_name,
                    "values": values,
                    "labels": [self._workspace_filter_label(value) for value in values],
                }
                for column_name, values in normalized_filters.items()
            ],
            "values": normalized_filters,
        }
        if not normalized_filters:
            return sql, None, meta

        normalized_columns = [str(item) for item in available_columns if str(item or "").strip() and str(item).strip() != "*"]
        for column_name in normalized_filters:
            if normalized_columns and column_name not in normalized_columns:
                raise ValueError(f"筛选字段不存在：{column_name}")
        if normalized_filters and not normalized_columns:
            raise ValueError("当前查询还未识别到结果字段，请先执行出字段后再筛选")

        cast_type = "CHAR" if ds.get("type") == "mysql" else "text"
        q = lambda value: self._quote_ident_for(value, ds)
        target_alias = "sql_workbench_filtered"
        clauses: List[str] = []
        params: List[str] = []

        for column_name, values in normalized_filters.items():
            column_expr = f"{target_alias}.{q(column_name)}"
            same_column_clauses: List[str] = []
            exact_values = [value for value in values if value not in {WORKBENCH_FILTER_NULL, WORKBENCH_FILTER_EMPTY}]
            if exact_values:
                placeholders = ", ".join(["%s"] * len(exact_values))
                same_column_clauses.append(f"CAST({column_expr} AS {cast_type}) IN ({placeholders})")
                params.extend(exact_values)
            if WORKBENCH_FILTER_EMPTY in values:
                same_column_clauses.append(f"CAST({column_expr} AS {cast_type}) = ''")
            if WORKBENCH_FILTER_NULL in values:
                same_column_clauses.append(f"{column_expr} IS NULL")
            if same_column_clauses:
                clauses.append("(" + " OR ".join(same_column_clauses) + ")")

        filtered_sql = (
            "SELECT * FROM (\n"
            f"{sql}\n"
            f") AS {target_alias}\n"
            "WHERE " + " AND ".join(clauses)
        )
        meta["applied"] = True
        return filtered_sql, tuple(params), meta

    def _query_workspace_filter_options(
        self,
        sql: str,
        params: Optional[Tuple[Any, ...]],
        ds: Dict[str, Any],
        column: str,
        keyword: str,
        limit: int,
    ) -> List[Dict[str, Any]]:
        q = lambda value: self._quote_ident_for(value, ds)
        source_alias = "sql_workbench_filter_source"
        column_expr = f"{source_alias}.{q(column)}"
        cast_type = "CHAR" if ds.get("type") == "mysql" else "text"
        where_clauses: List[str] = []
        query_params: List[Any] = list(params or [])
        if keyword:
            keyword_lower = keyword.lower()
            special_clauses: List[str] = []
            if ds.get("type") == "mysql":
                text_match = f"CAST({column_expr} AS {cast_type}) LIKE %s"
            else:
                text_match = f"CAST({column_expr} AS {cast_type}) ILIKE %s"
            if keyword_lower == "空" or "空值" in keyword_lower or "null" in keyword_lower:
                special_clauses.append(f"{column_expr} IS NULL")
            if keyword_lower == "空" or "空字符串" in keyword_lower or "empty" in keyword_lower or "blank" in keyword_lower:
                special_clauses.append(f"CAST({column_expr} AS {cast_type}) = ''")
            where_clauses.append("(" + " OR ".join([text_match, *special_clauses]) + ")")
            query_params.append(f"%{keyword}%")
        option_sql = (
            "SELECT "
            f"{column_expr} AS filter_value_raw, "
            f"CAST({column_expr} AS {cast_type}) AS filter_value_text, "
            "COUNT(*) AS option_count "
            "FROM (\n"
            f"{sql}\n"
            f") AS {source_alias}\n"
        )
        if where_clauses:
            option_sql += "WHERE " + " AND ".join(where_clauses) + "\n"
        option_sql += (
            f"GROUP BY {column_expr}, CAST({column_expr} AS {cast_type})\n"
            f"ORDER BY CASE WHEN {column_expr} IS NULL THEN 1 ELSE 0 END, COUNT(*) DESC, CAST({column_expr} AS {cast_type})\n"
            f"LIMIT {int(limit)}"
        )
        rows = self._query(option_sql, tuple(query_params) if query_params else None, ds)
        options: List[Dict[str, Any]] = []
        for row in rows:
            raw_value = row.get("filter_value_raw")
            text_value = row.get("filter_value_text")
            count = int(row.get("option_count") or 0)
            if raw_value is None:
                token = WORKBENCH_FILTER_NULL
                label = "(空值)"
            elif str(text_value or "") == "":
                token = WORKBENCH_FILTER_EMPTY
                label = "(空字符串)"
            else:
                token = str(text_value)
                label = str(text_value)
            options.append(
                {
                    "value": token,
                    "label": label,
                    "count": count,
                }
            )
        return options

    def _scan_top_level_keywords(self, sql: str, keywords: set[str]) -> List[Tuple[str, int]]:
        hits: List[Tuple[str, int]] = []
        depth = 0
        idx = 0
        length = len(sql)

        while idx < length:
            ch = sql[idx]

            if ch in ("'", '"', "`"):
                quote = ch
                idx += 1
                while idx < length:
                    current = sql[idx]
                    if current == "\\" and quote in ("'", '"') and idx + 1 < length:
                        idx += 2
                        continue
                    if current == quote:
                        if idx + 1 < length and sql[idx + 1] == quote:
                            idx += 2
                            continue
                        idx += 1
                        break
                    idx += 1
                continue

            if ch == "#" and depth == 0:
                idx += 1
                while idx < length and sql[idx] not in "\r\n":
                    idx += 1
                continue

            if ch == "-" and idx + 1 < length and sql[idx + 1] == "-":
                idx += 2
                while idx < length and sql[idx] not in "\r\n":
                    idx += 1
                continue

            if ch == "/" and idx + 1 < length and sql[idx + 1] == "*":
                idx += 2
                while idx + 1 < length and not (sql[idx] == "*" and sql[idx + 1] == "/"):
                    idx += 1
                idx = min(idx + 2, length)
                continue

            if ch == "(":
                depth += 1
                idx += 1
                continue

            if ch == ")":
                depth = max(0, depth - 1)
                idx += 1
                continue

            if depth == 0 and (ch.isalpha() or ch == "_"):
                start = idx
                idx += 1
                while idx < length and (sql[idx].isalnum() or sql[idx] == "_"):
                    idx += 1
                token = sql[start:idx].lower()
                if token in keywords:
                    hits.append((token, start))
                continue

            idx += 1

        return hits

    def _is_limit_offset_suffix(self, suffix: str) -> bool:
        value = str(suffix or "").strip()
        if not value:
            return False
        operand = r"(?:all|[^\s,]+)"
        patterns = [
            rf"^limit\s+{operand}(?:\s*,\s*{operand}|\s+offset\s+{operand}(?:\s+rows?)?)?\s*$",
            rf"^offset\s+{operand}(?:\s+rows?)?(?:\s+limit\s+{operand})?\s*$",
        ]
        return any(re.match(pattern, value, flags=re.I | re.S) for pattern in patterns)

    def _strip_top_level_limit_offset(self, sql: str) -> Tuple[str, Dict[str, Any]]:
        meta = {
            "had_top_level_limit": False,
            "had_top_level_offset": False,
            "stripped": False,
        }
        hits = self._scan_top_level_keywords(sql, {"limit", "offset"})
        if not hits:
            return sql, meta

        for token, position in hits:
            suffix = sql[position:].strip()
            if not self._is_limit_offset_suffix(suffix):
                continue
            meta["had_top_level_limit"] = any(hit_token == "limit" for hit_token, hit_pos in hits if hit_pos >= position)
            meta["had_top_level_offset"] = any(hit_token == "offset" for hit_token, hit_pos in hits if hit_pos >= position)
            meta["stripped"] = True
            return sql[:position].rstrip(), meta

        return sql, meta

    def _infer_sql_schema(self, sql: str) -> str:
        normalized = str(sql or "")
        patterns = [
            r"\bfrom\s+[`\"]?([a-zA-Z_][\w]*)[`\"]?\s*\.\s*[`\"]?([a-zA-Z_][\w]*)[`\"]?",
            r"\bjoin\s+[`\"]?([a-zA-Z_][\w]*)[`\"]?\s*\.\s*[`\"]?([a-zA-Z_][\w]*)[`\"]?",
        ]
        for pattern in patterns:
            match = re.search(pattern, normalized, flags=re.I)
            if not match:
                continue
            schema = match.group(1)
            if schema in self._ds_by_schema:
                return schema
        return ""

    def _extract_select_aliases(self, sql: str) -> List[str]:
        select_sql = sql.strip()
        lowered = select_sql.lower()
        if not lowered.startswith("select"):
            return []
        body = select_sql[6:]
        from_idx = body.lower().find(" from ")
        if from_idx >= 0:
            body = body[:from_idx]
        items = [item.strip() for item in body.split(",") if item.strip()]
        aliases: List[str] = []
        for item in items:
            match = re.search(r"\bas\b\s+([`\"\w]+)\s*$", item, flags=re.I)
            if match:
                aliases.append(match.group(1).strip("`\" "))
                continue
            aliases.append(item.split(".")[-1].strip("`\" "))
        return aliases

    def _recommended_related(self, snapshot: Dict[str, Any], qualified_name: str, limit: int) -> List[Dict[str, Any]]:
        target = snapshot["tables_by_key"][qualified_name]
        target_terms = set(
            filter(
                None,
                [
                    target["table_name"].lower(),
                    *(term.lower() for term in self._table_metadata(qualified_name).get("business_terms", [])),
                ],
            )
        )
        candidates: List[Tuple[int, Dict[str, Any]]] = []
        for item in snapshot["tables"]:
            if item["qualified_name"] == qualified_name:
                continue
            summary = self._table_summary(snapshot, item)
            score = 0
            if summary["business_domain"] and summary["business_domain"] == self._table_metadata(qualified_name).get("business_domain"):
                score += 3
            if summary["project"] and summary["project"] == self._table_metadata(qualified_name).get("project"):
                score += 2
            if any(term in summary["table_name"].lower() or term in (summary["table_comment"] or "").lower() for term in target_terms):
                score += 4
            score += min(summary.get("view_count") or 0, 5)
            if summary["favorite"]:
                score += 2
            if score <= 0:
                continue
            candidates.append((score, summary))
        candidates.sort(key=lambda item: (-item[0], item[1]["qualified_name"]))
        return [item[1] for item in candidates[:limit]]

    def _favorite_count(self) -> int:
        return sum(1 for value in self._store.get("favorites", {}).values() if value)

    def _query(self, sql: str, params: Optional[Tuple[Any, ...]] = None, ds: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        if ds and ds.get("type") == "mysql":
            return self._mysql_module.query(sql, params)
        return self._pg_module.query(sql, params)

    def _tables_sql(self) -> str:
        return """
            SELECT
                t.table_schema,
                t.table_name,
                c.reltuples::bigint AS estimated_rows,
                pg_total_relation_size(c.oid) AS total_size_bytes,
                pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size_pretty,
                r.rolname AS db_owner,
                pg_catalog.obj_description(c.oid, 'pg_class') AS table_comment,
                CASE WHEN pt.partrelid IS NOT NULL THEN TRUE ELSE FALSE END AS is_partitioned,
                COALESCE(pg_get_partkeydef(c.oid), '') AS partition_key,
                stat.last_vacuum,
                stat.last_autovacuum,
                stat.last_analyze,
                stat.last_autoanalyze,
                stat.n_live_tup,
                stat.n_dead_tup
            FROM information_schema.tables t
            JOIN pg_class c
              ON c.relname = t.table_name
            JOIN pg_namespace n
              ON n.oid = c.relnamespace
             AND n.nspname = t.table_schema
            LEFT JOIN pg_roles r
              ON r.oid = c.relowner
            LEFT JOIN pg_partitioned_table pt
              ON pt.partrelid = c.oid
            LEFT JOIN pg_stat_all_tables stat
              ON stat.relid = c.oid
            WHERE t.table_schema = 'public'
              AND t.table_type = 'BASE TABLE'
            ORDER BY t.table_name
        """

    def _columns_sql(self) -> str:
        return """
            SELECT
                c.table_schema,
                c.table_name,
                c.column_name,
                c.data_type,
                c.udt_name,
                c.is_nullable,
                c.ordinal_position,
                c.character_maximum_length,
                c.numeric_precision,
                c.numeric_scale,
                c.column_default,
                pgd.description AS column_comment,
                stats.null_frac,
                stats.n_distinct,
                stats.most_common_vals::text AS most_common_vals,
                stats.histogram_bounds::text AS histogram_bounds,
                stats.avg_width,
                stats.correlation
            FROM information_schema.columns c
            JOIN pg_class cls
              ON cls.relname = c.table_name
            JOIN pg_namespace ns
              ON ns.oid = cls.relnamespace
             AND ns.nspname = c.table_schema
            LEFT JOIN pg_description pgd
              ON pgd.objoid = cls.oid
             AND pgd.objsubid = c.ordinal_position
            LEFT JOIN pg_stats stats
              ON stats.schemaname = c.table_schema
             AND stats.tablename = c.table_name
             AND stats.attname = c.column_name
            WHERE c.table_schema = 'public'
            ORDER BY c.table_name, c.ordinal_position
        """

    def _primary_keys_sql(self) -> str:
        return """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                tc.constraint_name,
                kcu.ordinal_position
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
             AND tc.table_name = kcu.table_name
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY tc.table_name, kcu.ordinal_position
        """

    def _foreign_keys_sql(self) -> str:
        return """
            SELECT
                tc.table_schema,
                tc.table_name,
                kcu.column_name,
                ccu.table_schema AS foreign_table_schema,
                ccu.table_name AS foreign_table_name,
                ccu.column_name AS foreign_column_name,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
              ON ccu.constraint_name = tc.constraint_name
             AND ccu.table_schema = tc.table_schema
            WHERE tc.table_schema = 'public'
              AND tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name, kcu.column_name
        """

    def _target_lineage_url(self, table: str) -> str:
        return f"http://127.0.0.1:8778/viewer?mode=target_table&connection_name=PG&schema=public&table={table}&upstream_depth=12"

    # ── MySQL 方言 SQL（列名 alias 对齐 PG 版，复用同一套行处理逻辑）──
    def _mysql_tables_sql(self) -> str:
        return """
            SELECT
                TABLE_SCHEMA            AS table_schema,
                TABLE_NAME              AS table_name,
                TABLE_ROWS              AS estimated_rows,
                (DATA_LENGTH + INDEX_LENGTH) AS total_size_bytes,
                TABLE_COMMENT           AS table_comment,
                CASE WHEN CREATE_OPTIONS LIKE '%%partitioned%%' THEN 1 ELSE 0 END AS is_partitioned,
                ''                      AS partition_key,
                TABLE_TYPE              AS storage_type
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
              AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """

    def _mysql_columns_sql(self) -> str:
        return """
            SELECT
                TABLE_SCHEMA       AS table_schema,
                TABLE_NAME         AS table_name,
                COLUMN_NAME        AS column_name,
                DATA_TYPE          AS data_type,
                COLUMN_TYPE        AS udt_name,
                IS_NULLABLE        AS is_nullable,
                ORDINAL_POSITION   AS ordinal_position,
                CHARACTER_MAXIMUM_LENGTH AS character_maximum_length,
                NUMERIC_PRECISION  AS numeric_precision,
                NUMERIC_SCALE      AS numeric_scale,
                COLUMN_DEFAULT     AS column_default,
                COLUMN_COMMENT     AS column_comment
            FROM information_schema.COLUMNS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """

    def _mysql_primary_keys_sql(self) -> str:
        return """
            SELECT
                TABLE_SCHEMA    AS table_schema,
                TABLE_NAME      AS table_name,
                CONSTRAINT_NAME AS constraint_name,
                COLUMN_NAME     AS column_name,
                ORDINAL_POSITION AS ordinal_position
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """

    def _mysql_foreign_keys_sql(self) -> str:
        return """
            SELECT
                TABLE_SCHEMA            AS table_schema,
                TABLE_NAME             AS table_name,
                COLUMN_NAME            AS column_name,
                REFERENCED_TABLE_SCHEMA AS foreign_table_schema,
                REFERENCED_TABLE_NAME  AS foreign_table_name,
                REFERENCED_COLUMN_NAME AS foreign_column_name,
                CONSTRAINT_NAME        AS constraint_name
            FROM information_schema.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY TABLE_NAME, COLUMN_NAME
        """

    def _qualified_name(self, schema: str, table: str) -> str:
        return f"{schema}.{table}"

    def _quote_ident(self, value: str) -> str:
        return '"' + value.replace('"', '""') + '"'

    def _quote_ident_for(self, value: str, ds: Optional[Dict[str, Any]]) -> str:
        if ds and ds.get("type") == "mysql":
            return "`" + value.replace("`", "``") + "`"
        return '"' + value.replace('"', '""') + '"'

    def _sql_escape_literal(self, value: str) -> str:
        return str(value).replace("\\", "\\\\").replace("'", "''")

    def _ddl_type_text(self, column: Dict[str, Any], ds: Dict[str, Any]) -> str:
        udt_name = str(column.get("udt_name") or "").strip()
        data_type = str(column.get("data_type") or "").strip()
        if ds.get("type") == "mysql":
            return udt_name or data_type or "text"
        return udt_name or data_type or "text"

    def _iso_now(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    def _normalize_list(self, value: Any) -> List[str]:
        if value is None:
            return []
        if isinstance(value, str):
            parts = [item.strip() for item in value.replace("，", ",").split(",")]
            return [item for item in parts if item]
        if isinstance(value, list):
            return [str(item).strip() for item in value if str(item).strip()]
        return []

    def _normalize_dataset_dimensions(self, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        items: List[Dict[str, Any]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            field = self._clean_text(raw.get("field"))
            if not field:
                continue
            items.append({
                "field": field,
                "label": self._clean_text(raw.get("label")) or field,
                "group": self._clean_text(raw.get("group")) or "",
            })
        return items

    def _normalize_dataset_metrics(self, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        items: List[Dict[str, Any]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            field = self._clean_text(raw.get("field"))
            agg = self._clean_text(raw.get("agg")).lower() or "sum"
            label = self._clean_text(raw.get("label"))
            if agg != "count" and not field:
                continue
            items.append({
                "field": field,
                "agg": agg,
                "label": label or (f"{agg.upper()}_{field}" if field else "COUNT"),
                "format": self._clean_text(raw.get("format")) or "",
            })
        return items

    def _normalize_dashboard_widgets(self, value: Any) -> List[Dict[str, Any]]:
        if not isinstance(value, list):
            return []
        items: List[Dict[str, Any]] = []
        for raw in value:
            if not isinstance(raw, dict):
                continue
            items.append({
                "id": self._clean_text(raw.get("id")) or f"widget_{len(items) + 1}",
                "type": self._clean_text(raw.get("type")) or "kpi",
                "title": self._clean_text(raw.get("title")) or f"组件 {len(items) + 1}",
                "position": raw.get("position") if isinstance(raw.get("position"), dict) else {},
                "binding": raw.get("binding") if isinstance(raw.get("binding"), dict) else {},
            })
        return items

    def _default_dashboard_widgets(self, dataset: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
        metrics = dataset.get("metrics", []) if isinstance(dataset, dict) else []
        dimensions = dataset.get("dimensions", []) if isinstance(dataset, dict) else []
        time_field = self._clean_text((dataset or {}).get("time_field"))
        primary_metric = metrics[0] if metrics else {"label": "记录数", "agg": "count", "field": ""}
        secondary_metric = metrics[1] if len(metrics) > 1 else None
        primary_dimension = dimensions[0] if dimensions else {"field": "", "label": ""}
        primary_dimension_field = self._clean_text(primary_dimension.get("field"))

        widgets: List[Dict[str, Any]] = [
            {
                "id": "kpi_main",
                "type": "kpi",
                "title": primary_metric.get("label") or "核心指标",
                "position": {"x": 0, "y": 0, "w": 3, "h": 2},
                "binding": {
                    "metric_key": self._metric_binding_key(primary_metric),
                    "metric": primary_metric,
                },
            },
        ]
        if secondary_metric:
            widgets.append({
                "id": "kpi_secondary",
                "type": "kpi",
                "title": secondary_metric.get("label") or "次级指标",
                "position": {"x": 3, "y": 0, "w": 3, "h": 2},
                "binding": {
                    "metric_key": self._metric_binding_key(secondary_metric),
                    "metric": secondary_metric,
                },
            })

        series_binding = {
            "metric_key": self._metric_binding_key(primary_metric),
            "metric": primary_metric,
        }
        if time_field:
            series_binding["time_field"] = time_field
        elif primary_dimension_field:
            series_binding["dimension_field"] = primary_dimension_field
        widgets.append({
            "id": "trend_main",
            "type": "line",
            "title": f"{primary_metric.get('label') or '核心指标'}{'趋势' if time_field else '分布'}",
            "position": {"x": 0, "y": 2, "w": 8, "h": 4},
            "binding": series_binding,
        })
        widgets.append({
            "id": "table_main",
            "type": "table",
            "title": "明细表",
            "position": {"x": 0, "y": 6, "w": 12, "h": 5},
            "binding": {
                "columns": [item.get("field") for item in dimensions[:3] if item.get("field")] or ([time_field] if time_field else ([primary_dimension_field] if primary_dimension_field else [])),
                "metric_key": self._metric_binding_key(primary_metric),
                "metric": primary_metric,
            },
        })
        return widgets

    def _metric_binding_key(self, metric: Dict[str, Any]) -> str:
        agg = self._clean_text(metric.get("agg")).lower() or "sum"
        field = self._clean_text(metric.get("field"))
        return f"{agg}:{field}" if field else agg

    def _get_dataset_query_context(self, dataset: Dict[str, Any]) -> Dict[str, Any]:
        linked_table = self._clean_text(dataset.get("linked_table")) or self._clean_text(dataset.get("source_ref"))
        if not linked_table or "." not in linked_table:
            raise ValueError("dataset linked_table is empty")
        schema, table = linked_table.split(".", 1)
        ds = self._datasource_for_schema(schema)
        return {
            "dataset": dataset,
            "linked_table": linked_table,
            "schema": schema,
            "table": table,
            "datasource": ds,
        }

    def _preview_dashboard_widget(self, context: Dict[str, Any], widget: Dict[str, Any]) -> Dict[str, Any]:
        widget_type = self._clean_text(widget.get("type")) or "kpi"
        binding = widget.get("binding") if isinstance(widget.get("binding"), dict) else {}
        if widget_type == "kpi":
            data = self._preview_dashboard_kpi(context, binding)
        elif widget_type in {"line", "bar", "rank_bar"}:
            data = self._preview_dashboard_series(context, widget_type, binding)
        elif widget_type == "table":
            data = self._preview_dashboard_table(context, binding)
        else:
            data = {"kind": "empty", "rows": []}
        return {
            "id": widget.get("id") or "",
            "type": widget_type,
            "title": widget.get("title") or "",
            "position": widget.get("position") if isinstance(widget.get("position"), dict) else {},
            "binding": binding,
            "data": data,
        }

    def _resolve_metric_definition(self, dataset: Dict[str, Any], binding: Dict[str, Any]) -> Dict[str, Any]:
        metrics = dataset.get("metrics") if isinstance(dataset.get("metrics"), list) else []
        metric_key = self._clean_text(binding.get("metric_key"))
        if metric_key:
            for metric in metrics:
                if self._metric_binding_key(metric) == metric_key:
                    return metric
        raw_metric = binding.get("metric")
        if isinstance(raw_metric, dict):
            agg = self._clean_text(raw_metric.get("agg")).lower() or "sum"
            field = self._clean_text(raw_metric.get("field"))
            if agg == "count" or field:
                return {
                    "field": field,
                    "agg": agg,
                    "label": self._clean_text(raw_metric.get("label")) or (f"{agg.upper()}_{field}" if field else "COUNT"),
                }
        if metrics:
            return metrics[0]
        return {"field": "", "agg": "count", "label": "记录数"}

    def _metric_sql_expr(self, metric: Dict[str, Any], ds: Dict[str, Any]) -> str:
        agg = self._clean_text(metric.get("agg")).lower() or "sum"
        field = self._clean_text(metric.get("field"))
        if agg == "count":
            if field:
                return f"COUNT({self._quote_ident_for(field, ds)})"
            return "COUNT(*)"
        if not field:
            raise ValueError("metric field is empty")
        quoted = self._quote_ident_for(field, ds)
        if agg == "count_distinct":
            return f"COUNT(DISTINCT {quoted})"
        if agg in {"sum", "avg", "max", "min"}:
            return f"{agg.upper()}({quoted})"
        raise ValueError(f"unsupported metric agg: {agg}")

    def _normalize_positive_int(self, value: Any, default: int, minimum: int = 1, maximum: int = 200) -> int:
        number = self._normalize_int(value)
        if number is None:
            return default
        return max(minimum, min(number, maximum))

    def _series_bucket_expr(
        self,
        ds: Dict[str, Any],
        bucket_field: str,
        widget_type: str,
        grain: str,
    ) -> Tuple[str, str]:
        quoted = self._quote_ident_for(bucket_field, ds)
        normalized_grain = self._clean_text(grain).lower()
        ds_type = self._clean_text(ds.get("type") or ds.get("dialect")).lower()
        if widget_type != "line":
            return quoted, normalized_grain or "raw"
        if normalized_grain in {"day", "week", "month"}:
            if ds_type == "postgresql":
                return f"DATE_TRUNC('{normalized_grain}', {quoted})", normalized_grain
            if ds_type == "mysql":
                if normalized_grain == "day":
                    return f"DATE({quoted})", normalized_grain
                if normalized_grain == "week":
                    return f"STR_TO_DATE(DATE_FORMAT({quoted}, '%x-%v-1'), '%x-%v-%w')", normalized_grain
                if normalized_grain == "month":
                    return f"DATE_FORMAT({quoted}, '%Y-%m-01')", normalized_grain
        return quoted, "raw"

    def _preview_dashboard_kpi(self, context: Dict[str, Any], binding: Dict[str, Any]) -> Dict[str, Any]:
        dataset = context["dataset"]
        ds = context["datasource"]
        schema = context["schema"]
        table = context["table"]
        metric = self._resolve_metric_definition(dataset, binding)
        metric_expr = self._metric_sql_expr(metric, ds)
        sql = (
            f"SELECT {metric_expr} AS metric_value "
            f"FROM {self._quote_ident_for(schema, ds)}.{self._quote_ident_for(table, ds)}"
        )
        rows = self._query(sql, None, ds)
        row = rows[0] if rows else {}
        return {
            "kind": "kpi",
            "value": row.get("metric_value"),
            "metric": metric,
            "sql": sql,
        }

    def _preview_dashboard_series(self, context: Dict[str, Any], widget_type: str, binding: Dict[str, Any]) -> Dict[str, Any]:
        dataset = context["dataset"]
        ds = context["datasource"]
        schema = context["schema"]
        table = context["table"]
        metric = self._resolve_metric_definition(dataset, binding)
        metric_expr = self._metric_sql_expr(metric, ds)
        dataset_time_field = self._clean_text(dataset.get("time_field"))
        dataset_dimensions = dataset.get("dimensions") if isinstance(dataset.get("dimensions"), list) else []
        default_dimension = ""
        for item in dataset_dimensions:
            if isinstance(item, dict):
                default_dimension = self._clean_text(item.get("field"))
                if default_dimension:
                    break
        time_field = self._clean_text(binding.get("time_field")) or dataset_time_field
        dimension_field = self._clean_text(binding.get("dimension_field")) or default_dimension
        if widget_type == "line":
            bucket_field = time_field or dimension_field
        else:
            bucket_field = dimension_field or time_field
        if not bucket_field:
            raise ValueError("series binding missing time_field or dimension_field")
        grain = self._clean_text(binding.get("time_grain")).lower() or ("day" if widget_type == "line" and time_field else "raw")
        bucket_expr, resolved_grain = self._series_bucket_expr(ds, bucket_field, widget_type, grain)
        sort_order = self._clean_text(binding.get("sort_order")).lower()
        if sort_order not in {"asc", "desc", "metric_desc", "metric_asc"}:
            sort_order = "asc" if widget_type == "line" else "metric_desc"
        limit = self._normalize_positive_int(binding.get("limit"), 24 if widget_type == "line" else 12, minimum=1, maximum=200)
        if sort_order == "metric_desc":
            order_sql = "metric_value DESC, bucket ASC"
        elif sort_order == "metric_asc":
            order_sql = "metric_value ASC, bucket ASC"
        else:
            order_sql = f"bucket {sort_order.upper()}"
        sql = (
            f"SELECT {bucket_expr} AS bucket, {metric_expr} AS metric_value "
            f"FROM {self._quote_ident_for(schema, ds)}.{self._quote_ident_for(table, ds)} "
            f"GROUP BY {bucket_expr} "
            f"ORDER BY {order_sql} "
            f"LIMIT {limit}"
        )
        rows = self._query(sql, None, ds)
        return {
            "kind": "series",
            "rows": rows,
            "metric": metric,
            "bucket_field": bucket_field,
            "time_grain": resolved_grain,
            "sort_order": sort_order,
            "limit": limit,
            "series_style": "line" if widget_type == "line" else "bar",
            "sql": sql,
        }

    def _preview_dashboard_table(self, context: Dict[str, Any], binding: Dict[str, Any]) -> Dict[str, Any]:
        ds = context["datasource"]
        schema = context["schema"]
        table = context["table"]
        columns = binding.get("columns") if isinstance(binding.get("columns"), list) else []
        clean_columns = [self._clean_text(item) for item in columns if self._clean_text(item)]
        if not clean_columns:
            dataset = context["dataset"]
            dims = dataset.get("dimensions") if isinstance(dataset.get("dimensions"), list) else []
            clean_columns = [self._clean_text(item.get("field")) for item in dims[:5] if self._clean_text(item.get("field"))]
        select_columns = clean_columns[:8]
        select_sql = ", ".join(self._quote_ident_for(column, ds) for column in select_columns) if select_columns else "*"
        sql = (
            f"SELECT {select_sql} "
            f"FROM {self._quote_ident_for(schema, ds)}.{self._quote_ident_for(table, ds)} "
            "LIMIT 20"
        )
        rows = self._query(sql, None, ds)
        return {
            "kind": "table",
            "columns": select_columns or (list(rows[0].keys()) if rows else []),
            "rows": rows,
            "sql": sql,
        }

    def _clean_text(self, value: Any) -> str:
        return str(value or "").strip()

    def _normalize_int(self, value: Any) -> Optional[int]:
        if value in (None, ""):
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            return None

    def _safe_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except (TypeError, ValueError):
            return None

    def _percent_text(self, value: Optional[float]) -> str:
        if value is None:
            return ""
        return f"{round(value * 100, 2)}%"

    def _sorted_unique(self, values: Any) -> List[str]:
        seen = sorted({str(value).strip() for value in values if str(value).strip()})
        return seen
