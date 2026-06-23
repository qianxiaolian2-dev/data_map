from __future__ import annotations

import argparse
import json
import os
import re
import threading
import urllib.error
import urllib.request
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Dict, List, Optional, Set, Tuple
from urllib.parse import parse_qs, urlencode, urlparse

from data_map_service import DataMapService
from lineage_service import LineageService

APP_DIR = os.path.dirname(__file__)
TEMPLATES_DIR = os.path.join(APP_DIR, "templates")


def _load_dotenv_file() -> None:
    dotenv_path = os.environ.get("DATA_MAP_DOTENV_PATH") or ".env"
    candidate = dotenv_path if os.path.isabs(dotenv_path) else os.path.join(APP_DIR, dotenv_path)
    if not os.path.exists(candidate):
        return
    try:
        with open(candidate, "r", encoding="utf-8") as fh:
            for raw_line in fh:
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, value = line.split("=", 1)
                key = key.strip()
                if not key or key in os.environ:
                    continue
                value = value.strip().strip("'").strip('"')
                os.environ[key] = value
    except OSError:
        return


_load_dotenv_file()


def _load_repo_html_template(filename: str, fallback: str) -> str:
    candidate = os.path.join(TEMPLATES_DIR, filename)
    try:
        with open(candidate, "r", encoding="utf-8") as fh:
            return fh.read()
    except OSError:
        return fallback

OPENAI_BASE_URL = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
OPENAI_DEFAULT_MODEL = os.environ.get("OPENAI_MODEL", "gpt-5.5")
OPENAI_TIMEOUT_SECONDS = max(5, min(int(os.environ.get("OPENAI_TIMEOUT_SECONDS") or "60"), 300))
AI_SQL_REPLY_PROMPT = """你是企业数据团队里的 SQL 助手，负责在中文对话中帮助用户进行数据探查、SQL 编写、结果解读和下一步分析建议。

请严格遵守：
1. 只基于提供的表结构、DDL、预览数据、当前 SQL、执行结果上下文回答。
2. 如果上下文不足，不要编造字段；要明确指出缺什么，并给一个最合理的下一步 SQL。
3. SQL 必须尽量可直接执行，优先单条查询；默认不要写 destructive SQL。
4. 回答要简洁、务实，适合直接放进工作台。
5. 返回必须是 JSON 对象，不要加 markdown 代码块，不要加额外解释。

JSON 格式固定为：
{
  "reply": "给用户看的中文说明，可包含简短分析建议",
  "sql": "可执行 SQL；如果这次不需要生成 SQL，就返回空字符串",
  "title": "这次 SQL 的简短标题，可为空",
  "follow_ups": ["后续建议1", "后续建议2"]
}
"""

AI_TABLE_SEARCH_STOPWORDS = {
    "帮我", "给我", "一下", "一个", "这张", "当前", "现在", "这里", "这个", "哪些", "什么", "怎么",
    "以及", "还有", "然后", "并且", "并", "同时", "或者", "如果", "是否", "可以", "需要", "想看",
    "想要", "看看", "查询", "查", "统计", "分析", "汇总", "生成", "解释", "优化", "质量", "数据",
    "字段", "表", "sql", "select", "with", "基于", "根据", "按照", "按", "告诉", "应该", "补",
    "筛选", "条件", "优先", "适合", "当前表", "当前sql", "编辑器", "什么样", "怎么做",
}

AI_TABLE_REASON_LABELS = {
    "table": "表名命中",
    "alias": "别名命中",
    "term": "业务词命中",
    "owner": "负责人命中",
    "comment": "表注释命中",
    "column": "字段名命中",
    "column_comment": "字段注释命中",
    "domain": "业务域命中",
    "project": "项目命中",
}

AI_MULTI_TABLE_PROMPT_HINTS = (
    "关系", "关联", "join", "映射", "对照", "对比", "区别", "转化", "来源", "来自", "链路", "影响",
)

AI_SQL_DIRECT_TOKENS = (
    "sql", "查询", "查一下", "查出", "统计", "汇总", "生成", "写一条", "写个", "试跑", "跑一下",
)

AI_CONFIRM_TOKENS = (
    "继续", "确认", "可以", "就这", "没问题", "开始", "生成", "继续生成", "确定", "好",
)

AI_REFINE_TOKENS = (
    "用", "改用", "不要", "不是", "换成", "应该是", "客户表", "合同表", "订单表", "线索表", "商机表", "关联",
)

AI_DOMAIN_TERMS = (
    "客户", "合同", "订单", "线索", "商机", "成单", "负责人", "部门", "金额", "回款", "产品", "项目",
)

AI_JOIN_SAMPLE_LIMIT = 300
AI_JOIN_MAX_CANDIDATES_PER_TABLE = 6
AI_JOIN_MAX_PAIRS_PER_TABLE_PAIR = 16
AI_JOIN_MAX_RELATIONS = 6
AI_JOIN_SKIP_EXACT_NAMES = {
    "name", "title", "remark", "description", "content", "status", "type", "category",
    "amount", "price", "fee", "cost", "gmv", "revenue", "quantity", "qty",
    "created_at", "updated_at", "deleted_at", "create_time", "update_time", "delete_time",
    "created_by", "updated_by", "creator", "modifier", "date", "dt", "day", "month",
}
AI_JOIN_SUBJECT_TAGS = {
    "customer", "contract", "order", "opportunity", "clue", "user", "department", "product", "project",
}
AI_JOIN_SYSTEM_FIELD_PATTERNS = (
    "created_by", "updated_by", "owner_org", "used_org", "organizations_space", "space_id",
    "tenant", "app_id", "creator", "modifier", "deleted_by", "sync", "version",
)


def _as_dict_list(value: Any) -> List[Dict[str, Any]]:
    return [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _filter_pipeline_tasks(value: Any) -> List[Dict[str, Any]]:
    return [item for item in _as_dict_list(value) if item.get("resource_type") != "PIPELINE"]


def _normalize_consumers(value: Any) -> List[Dict[str, Any]]:
    rows = _as_dict_list(value)
    direct_task_consumers = [item for item in rows if item.get("resource_type") != "PIPELINE"]
    return direct_task_consumers or rows


def _clear_node_names(value: Any) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in _as_dict_list(value):
        row = dict(item)
        row["node_name"] = ""
        rows.append(row)
    return rows


def _extract_source_table_names(raw: Dict[str, Any]) -> List[str]:
    names: List[str] = []
    for pipeline in _as_dict_list(raw.get("pipelines")):
        for source_table in _as_dict_list(pipeline.get("source_tables")):
            full_name = source_table.get("full_name")
            if full_name and full_name not in names:
                names.append(full_name)
    return names


def _normalize_target_activity_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    direct_target_producers = _filter_pipeline_tasks(raw.get("target_lineage_producers"))
    direct_target_consumers = _normalize_consumers(raw.get("target_lineage_consumers"))
    direct_target_summary = raw.get("target_lineage_schedule_summary") if isinstance(raw.get("target_lineage_schedule_summary"), dict) else {}
    direct_target_runs = _as_dict_list(raw.get("target_recent_lineage_runs"))
    source_reference_producers = _filter_pipeline_tasks(raw.get("source_lineage_producers"))
    source_reference_runs = _as_dict_list(raw.get("source_recent_lineage_runs") or raw.get("recent_source_runs"))
    source_summary = raw.get("source_schedule_summary") if isinstance(raw.get("source_schedule_summary"), dict) else {}
    if not source_summary:
        source_summary = raw.get("source_lineage_schedule_summary") if isinstance(raw.get("source_lineage_schedule_summary"), dict) else {}
    is_source_fallback = raw.get("lineage_resolution") == "source_fallback"
    lineage_verified = False

    if is_source_fallback:
        # 优先：连接级血缘(fdl_connection_lineage)已佐证"真的写到了目标连接"的任务，
        # 这才是目标表的真实产出任务（能把"只写源表"的任务排除掉）。
        verified = [dict(p) for p in source_reference_producers if p.get("writes_target_connection")]
        if verified:
            producers = []
            for p in verified:
                landing = p.get("target_landing_nodes") or []
                # 用真正写到目标连接的节点名（如"数据同步"），而非写源表的节点名
                p["node_name"] = "、".join(landing)
                producers.append(p)
            runs = []
            for p in verified:
                runs.extend(p.get("recent_runs") or [])
            recent_runs = runs or source_reference_runs
            schedule_summary = (verified[0].get("schedule_summary") if isinstance(verified[0].get("schedule_summary"), dict) else {}) or source_summary
            overview_note = ""
            lineage_verified = True
            is_source_fallback = False   # 已佐证，不再当作"回退猜测"
            source_reference_producers = []
            source_reference_runs = []
        else:
            fallback_producers = _clear_node_names(source_reference_producers)
            latest_task_name = str(source_summary.get("latest_task_name") or "")
            matched_producer = next((item for item in fallback_producers if item.get("task_name") == latest_task_name), None) if latest_task_name else None
            selected_producer = matched_producer or (fallback_producers[0] if fallback_producers else None)
            producers = [selected_producer] if selected_producer else []
            recent_runs = _clear_node_names((selected_producer or {}).get("recent_runs") or source_reference_runs)
            schedule_summary = (selected_producer or {}).get("schedule_summary") if isinstance((selected_producer or {}).get("schedule_summary"), dict) else {}
            if not schedule_summary:
                schedule_summary = source_summary
            overview_note = "以下为根据源表推测的产出任务，未经连接级血缘佐证"
            source_reference_producers = []
            source_reference_runs = []
    else:
        producers = direct_target_producers
        recent_runs = direct_target_runs
        schedule_summary = direct_target_summary
        overview_note = ""

    consumers = direct_target_consumers
    return {
        "recent_runs": recent_runs,
        "producers": producers,
        "consumers": consumers,
        "schedule_summary": schedule_summary,
        "producer_count": len(producers),
        "consumer_count": len(consumers),
        "lineage_verified": lineage_verified,
        "lineage_resolution": raw.get("lineage_resolution") or "",
        "is_source_fallback": is_source_fallback,
        "overview_note": overview_note,
        "source_table_names": _extract_source_table_names(raw),
        "source_reference_producers": source_reference_producers,
        "source_reference_runs": source_reference_runs if not is_source_fallback else [],
    }


def _normalize_generic_activity_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    producers = _filter_pipeline_tasks(raw.get("producers"))
    consumers = _normalize_consumers(raw.get("consumers"))
    recent_runs = _as_dict_list(raw.get("recent_runs"))
    schedule_summary = raw.get("schedule_summary") if isinstance(raw.get("schedule_summary"), dict) else {}
    return {
        "recent_runs": recent_runs,
        "producers": producers,
        "consumers": consumers,
        "schedule_summary": schedule_summary,
        "producer_count": len(producers),
        "consumer_count": len(consumers),
        "lineage_resolution": raw.get("lineage_resolution") or "",
        "is_source_fallback": False,
        "overview_note": "",
        "source_table_names": [],
        "source_reference_producers": [],
        "source_reference_runs": [],
    }


def _normalize_activity_payload(raw: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(raw, dict):
        return _normalize_generic_activity_payload({})
    if any(key in raw for key in ("target_lineage_producers", "source_lineage_producers", "targets", "pipelines", "lineage_resolution")):
        return _normalize_target_activity_payload(raw)
    return _normalize_generic_activity_payload(raw)


def _clip_text(value: Any, max_chars: int = 4000) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "\n...（以下内容已截断）"


def _normalize_ai_history(value: Any) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    for item in value if isinstance(value, list) else []:
        if not isinstance(item, dict):
            continue
        role = str(item.get("role") or "").strip().lower()
        if role not in {"user", "assistant"}:
            continue
        content = str(item.get("content") or "").strip()
        if not content:
            continue
        rows.append({"role": role, "content": content[:6000]})
    return rows[-12:]


def _extract_openai_output_text(payload: Dict[str, Any]) -> str:
    direct = payload.get("output_text")
    if isinstance(direct, str) and direct.strip():
        return direct.strip()
    parts: List[str] = []
    for item in payload.get("output") if isinstance(payload.get("output"), list) else []:
        if not isinstance(item, dict):
            continue
        for content in item.get("content") if isinstance(item.get("content"), list) else []:
            if not isinstance(content, dict):
                continue
            text_value = content.get("text")
            if isinstance(text_value, str) and text_value.strip():
                parts.append(text_value.strip())
    return "\n\n".join(parts).strip()


def _extract_first_json_object(text: str) -> Optional[Dict[str, Any]]:
    raw = str(text or "").strip()
    if not raw:
        return None
    candidates = [raw]
    candidates.extend(re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.DOTALL | re.IGNORECASE))
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            parsed = json.loads(raw[start:end + 1])
            if isinstance(parsed, dict):
                return parsed
        except json.JSONDecodeError:
            return None
    return None


def _extract_sql_from_text(text: str) -> str:
    raw = str(text or "")
    fenced = re.findall(r"```sql\s*(.*?)```", raw, flags=re.DOTALL | re.IGNORECASE)
    if fenced:
        return fenced[0].strip()
    generic = re.findall(r"```(.*?)```", raw, flags=re.DOTALL)
    if generic:
        return generic[0].strip()
    return ""


def _short_text(value: Any, max_chars: int = 120) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[:max_chars].rstrip() + "..."


def _parse_qualified_table_name(value: str) -> Optional[Tuple[str, str]]:
    raw = str(value or "").strip()
    if not raw or "." not in raw:
        return None
    schema, table = raw.split(".", 1)
    schema = schema.strip()
    table = table.strip()
    if not schema or not table:
        return None
    return schema, table


def _table_schema_of(qualified_name: str) -> str:
    parsed = _parse_qualified_table_name(qualified_name)
    return parsed[0] if parsed else ""


def _same_datasource_tables(service: DataMapService, table_names: List[str], anchor_table: str) -> List[str]:
    anchor_schema = _table_schema_of(anchor_table)
    if not anchor_schema:
        return table_names[:]
    anchor_ds = service._datasource_for_schema(anchor_schema)
    anchor_key = str(anchor_ds.get("key") or anchor_ds.get("schema") or anchor_schema)
    filtered: List[str] = []
    for qualified_name in table_names:
        schema = _table_schema_of(qualified_name)
        if not schema:
            continue
        ds = service._datasource_for_schema(schema)
        ds_key = str(ds.get("key") or ds.get("schema") or schema)
        if ds_key == anchor_key:
            filtered.append(qualified_name)
    return filtered


def _has_cross_datasource_tables(service: DataMapService, table_names: List[str]) -> bool:
    ds_keys = set()
    for qualified_name in table_names:
        schema = _table_schema_of(qualified_name)
        if not schema:
            continue
        ds = service._datasource_for_schema(schema)
        ds_keys.add(str(ds.get("key") or ds.get("schema") or schema))
    return len(ds_keys) > 1


def _normalize_ai_context_table_inputs(value: Any, max_tables: int = 6) -> List[str]:
    if isinstance(value, list):
        raw_values = [str(item or "").strip() for item in value]
    else:
        raw_values = re.split(r"[\n,，;；、]+", str(value or ""))
    normalized: List[str] = []
    seen = set()
    for raw in raw_values:
        parsed = _parse_qualified_table_name(raw)
        if not parsed:
            continue
        qualified_name = ".".join(parsed)
        if qualified_name in seen:
            continue
        seen.add(qualified_name)
        normalized.append(qualified_name)
        if len(normalized) >= max_tables:
            break
    return normalized


def _extract_sql_context_tables(sql: str, max_tables: int = 6) -> List[str]:
    raw = str(sql or "").strip()
    if not raw:
        return []

    matches: List[str] = []
    seen = set()
    patterns = [
        r"\bfrom\s+[`\"]?([a-zA-Z_][\w]*)[`\"]?\s*\.\s*[`\"]?([a-zA-Z_][\w]*)[`\"]?",
        r"\bjoin\s+[`\"]?([a-zA-Z_][\w]*)[`\"]?\s*\.\s*[`\"]?([a-zA-Z_][\w]*)[`\"]?",
    ]
    for pattern in patterns:
        for schema, table in re.findall(pattern, raw, flags=re.I):
            qualified_name = f"{schema}.{table}"
            if qualified_name in seen:
                continue
            seen.add(qualified_name)
            matches.append(qualified_name)
            if len(matches) >= max_tables:
                return matches
    return matches


def _extract_ai_table_search_keywords(prompt: str, max_keywords: int = 12) -> List[str]:
    raw = str(prompt or "").strip().lower()
    if not raw:
        return []

    keywords: List[str] = []
    seen = set()

    def add(token: str) -> None:
        value = str(token or "").strip().strip("_")
        if len(value) < 2:
            return
        if value in AI_TABLE_SEARCH_STOPWORDS:
            return
        if value in seen:
            return
        seen.add(value)
        keywords.append(value)

    for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{1,40}", raw):
        add(token)

    for term in AI_DOMAIN_TERMS:
        if term in raw:
            add(term)

    for chunk in re.findall(r"[\u4e00-\u9fff]{2,24}", raw):
        for stopword in sorted(AI_TABLE_SEARCH_STOPWORDS, key=len, reverse=True):
            chunk = chunk.replace(stopword, " ")
        parts = [part.strip() for part in chunk.split() if part.strip()]
        for part in parts:
            if 2 <= len(part) <= 8:
                add(part)
            if len(part) <= 4:
                continue
            for size in (2, 3, 4):
                for index in range(0, len(part) - size + 1):
                    add(part[index:index + size])
                    if len(keywords) >= max_keywords:
                        return keywords[:max_keywords]

    return keywords[:max_keywords]


def _build_ai_table_candidate_payload(item: Dict[str, Any], score: int, matched_keywords: List[str], reason_codes: List[str]) -> Dict[str, Any]:
    reason_labels = [AI_TABLE_REASON_LABELS.get(code, code) for code in reason_codes if code]
    subtitle_parts = [
        item.get("table_comment") or item.get("description") or "",
        item.get("business_domain") or "",
        item.get("owner") or "",
    ]
    subtitle = " · ".join(part for part in subtitle_parts if part)
    return {
        "qualified_name": item.get("qualified_name") or "",
        "table_name": item.get("table_name") or "",
        "table_comment": item.get("table_comment") or "",
        "description": item.get("description") or "",
        "owner": item.get("owner") or "",
        "business_domain": item.get("business_domain") or "",
        "source_type": item.get("source_type") or "",
        "column_count": int(item.get("column_count") or 0),
        "score": int(score),
        "matched_keywords": matched_keywords[:4],
        "match_reasons": reason_codes,
        "reason_text": " / ".join(reason_labels[:3]),
        "subtitle": _short_text(subtitle, 120),
    }


def _search_ai_candidate_tables(service: DataMapService, prompt: str, limit: int = 6) -> List[Dict[str, Any]]:
    keywords = _extract_ai_table_search_keywords(prompt, max_keywords=12)
    aggregate: Dict[str, Dict[str, Any]] = {}

    for keyword_index, keyword in enumerate(keywords):
        try:
            payload = service.search_tables(keyword=keyword, limit=max(limit * 3, 12))
        except Exception:
            continue
        for rank, item in enumerate(payload.get("items") or []):
            qualified_name = str(item.get("qualified_name") or "").strip()
            if not qualified_name:
                continue
            entry = aggregate.setdefault(
                qualified_name,
                {
                    "item": item,
                    "score": 0,
                    "keywords": [],
                    "reasons": set(),
                },
            )
            entry["item"] = item
            entry["score"] += max(6, 36 - rank * 3) + max(0, 10 - keyword_index)
            lower_table = str(item.get("table_name") or "").lower()
            lower_qualified = qualified_name.lower()
            lower_comment = str(item.get("table_comment") or "").lower()
            if keyword == lower_table or keyword == lower_qualified:
                entry["score"] += 40
            elif keyword in lower_table or keyword in lower_qualified:
                entry["score"] += 18
            elif keyword in lower_comment:
                entry["score"] += 10
            if keyword not in entry["keywords"]:
                entry["keywords"].append(keyword)
            for reason in item.get("match_reasons") or []:
                entry["reasons"].add(str(reason))

    if aggregate:
        ranked = sorted(
            aggregate.values(),
            key=lambda entry: (
                -int(entry["score"]),
                -len(entry["keywords"]),
                entry["item"].get("qualified_name") or "",
            ),
        )
        return [
            _build_ai_table_candidate_payload(
                entry["item"],
                score=int(entry["score"]),
                matched_keywords=entry["keywords"],
                reason_codes=sorted(entry["reasons"]),
            )
            for entry in ranked[:limit]
        ]

    try:
        dashboard = service.get_dashboard(limit=limit)
    except Exception:
        return []

    fallback_rows = dashboard.get("recommended") or dashboard.get("recent") or dashboard.get("favorites") or []
    candidates: List[Dict[str, Any]] = []
    for index, item in enumerate(fallback_rows[:limit]):
        candidates.append(
            _build_ai_table_candidate_payload(
                item,
                score=max(1, limit - index),
                matched_keywords=[],
                reason_codes=[],
            )
        )
        candidates[-1]["reason_text"] = "常用推荐"
    return candidates


def _build_ai_table_search_response(service: DataMapService, prompt: str) -> Dict[str, Any]:
    candidates = _search_ai_candidate_tables(service, prompt, limit=6)
    if candidates:
        return {
            "reply": f"我先按你的问题找到了 {len(candidates)} 张更可能相关的表。你可以直接点候选表确认，也可以说“继续生成 SQL”，我会先用最可能的表继续往下走。",
            "sql": "",
            "title": "先选候选表",
            "follow_ups": [
                "点一张候选表后，我会自动继续判断关系并生成 SQL",
                "如果这些都不对，可以补充业务对象、指标名称或时间口径",
            ],
            "model": "",
            "linked_table": "",
            "mode": "table_search",
            "degraded": False,
            "table_candidates": candidates,
            "confirmed_tables": [],
            "guessed_relations": [],
            "need_confirmation": True,
            "next_actions": ["select_table", "continue_generate", "refine_prompt"],
        }
    return {
        "reply": "当前还没有指定关联表，但我暂时没从问题里匹配到明确候选表。你可以补充业务对象、核心指标或你怀疑的表名片段，我再帮你缩小范围。",
        "sql": "",
        "title": "请补充业务线索",
        "follow_ups": [
            "例如补一句：我要看订单、客户、合同、线索、金额、部门中的哪类数据",
            "也可以直接给我一个你怀疑的表名片段，我先帮你找表",
        ],
        "model": "",
        "linked_table": "",
        "mode": "table_search",
        "degraded": False,
        "table_candidates": [],
        "confirmed_tables": [],
        "guessed_relations": [],
        "need_confirmation": True,
        "next_actions": ["refine_prompt"],
    }


def _should_generate_sql_directly(prompt: str) -> bool:
    lowered = str(prompt or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in AI_SQL_DIRECT_TOKENS)


def _is_ai_confirmation_prompt(prompt: str) -> bool:
    lowered = str(prompt or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in AI_CONFIRM_TOKENS)


def _is_ai_refine_prompt(prompt: str) -> bool:
    lowered = str(prompt or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in AI_REFINE_TOKENS)


def _normalize_join_value(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    lowered = text.lower()
    if lowered in {"null", "none", "nan", "nat"}:
        return ""
    normalized = lowered.replace("-", "").replace("_", "").replace(" ", "")
    if not normalized:
        return ""
    if len(normalized) > 80:
        return ""
    return normalized


def _extract_column_comment(column: Dict[str, Any]) -> str:
    return str(column.get("column_comment") or column.get("business_def") or "").strip()


def _column_distinct_estimate(column: Dict[str, Any]) -> Optional[int]:
    value = column.get("distinct_estimate")
    if value is None:
        return None
    try:
        return max(0, int(value))
    except Exception:
        return None


def _column_uniqueness_ratio(column: Dict[str, Any]) -> Optional[float]:
    value = column.get("uniqueness_ratio")
    if value is None:
        return None
    try:
        ratio = float(value)
    except Exception:
        return None
    if ratio < 0:
        return 0.0
    if ratio > 1:
        return 1.0
    return ratio


def _column_null_rate(column: Dict[str, Any]) -> Optional[float]:
    value = column.get("null_rate")
    if value is None:
        return None
    try:
        ratio = float(value)
    except Exception:
        return None
    if ratio < 0:
        return 0.0
    if ratio > 1:
        return 1.0
    return ratio


def _column_business_tokens(column: Dict[str, Any]) -> Set[str]:
    name = _column_name(column).lower()
    comment = _extract_column_comment(column).lower()
    text = f"{name} {comment}"
    tokens: Set[str] = set()
    mapping = {
        "customer": ("customer", "cust", "客户"),
        "contract": ("contract", "agreement", "合同"),
        "order": ("order", "订单"),
        "opportunity": ("opportunity", "deal", "商机"),
        "clue": ("clue", "lead", "线索"),
        "user": ("user", "owner", "负责人", "员工", "销售"),
        "department": ("department", "dept", "部门"),
        "product": ("product", "sku", "物料", "产品"),
        "project": ("project", "项目"),
        "jdy": ("jdy", "简道云"),
        "kingdee": ("kingdee", "金蝶"),
        "uuid": ("uuid",),
        "number": ("number", "_no", "编号", "单号", "编码"),
        "id": ("_id", " id", "主键", "关联id", "关联客户id", "简道云id"),
    }
    for token, clues in mapping.items():
        if any(clue in text for clue in clues):
            tokens.add(token)
    return tokens


def _prompt_join_subjects(prompt: str) -> Set[str]:
    raw = str(prompt or "").strip().lower()
    subjects: Set[str] = set()
    mapping = {
        "customer": ("客户", "customer"),
        "contract": ("合同", "contract", "成单"),
        "order": ("订单", "order"),
        "opportunity": ("商机", "opportunity"),
        "clue": ("线索", "lead", "clue"),
        "user": ("负责人", "员工", "销售", "user"),
        "department": ("部门", "department", "dept"),
        "product": ("产品", "物料", "product"),
        "project": ("项目", "project"),
    }
    for subject, clues in mapping.items():
        if any(clue.lower() in raw for clue in clues):
            subjects.add(subject)
    return subjects


def _column_join_score(column: Dict[str, Any], primary_key_names: Set[str]) -> Tuple[int, List[str]]:
    name = _column_name(column)
    lowered = name.lower()
    comment = _extract_column_comment(column).lower()
    type_name = _column_type(column)
    uniqueness = _column_uniqueness_ratio(column)
    null_rate = _column_null_rate(column)
    score = 0
    reasons: List[str] = []

    if not name:
        return -999, ["空字段名"]
    if lowered in AI_JOIN_SKIP_EXACT_NAMES:
        return -999, ["非关联类字段"]
    if _is_temporal_column(column) or _is_numeric_column(column):
        return -999, ["时间/数值字段"]

    if name in primary_key_names:
        score += 80
        reasons.append("主键")
    if lowered == "id":
        score += 38
        reasons.append("标准主键名")
    if lowered.endswith("_id") or "_id_" in lowered:
        score += 34
        reasons.append("ID字段")
    if lowered.endswith("_number") or lowered.endswith("_no") or lowered.endswith("_code"):
        score += 30
        reasons.append("业务编号字段")
    if ("客户" in comment or "简道云" in comment or "金蝶" in comment) and ("number" in lowered or lowered.endswith("_no") or lowered.endswith("_code")):
        score += 24
        reasons.append("业务系统编号")
    if "uuid" in lowered or "uuid" in comment:
        score += 24
        reasons.append("UUID字段")
    if "编号" in comment or "编码" in comment or "单号" in comment:
        score += 26
        reasons.append("注释提示编号")
    if "主键" in comment:
        score += 22
        reasons.append("注释提示主键")
    if "关联" in comment:
        score += 20
        reasons.append("注释提示关联")
    if "简道云" in comment or "jdy" in lowered:
        score += 18
        reasons.append("简道云标识")
    if "金蝶" in comment or "kingdee" in lowered:
        score += 14
        reasons.append("金蝶标识")
    if any(pattern in lowered for pattern in AI_JOIN_SYSTEM_FIELD_PATTERNS):
        score -= 36
        reasons.append("系统审计字段")
    if lowered.endswith("_user_id") or lowered.endswith("_org_id") or lowered.endswith("_space_id"):
        score -= 22
        reasons.append("弱业务关联")

    subject_tokens = _column_business_tokens(column)
    shared_subjects = sorted(token for token in subject_tokens if token in AI_JOIN_SUBJECT_TAGS)
    if shared_subjects:
        score += min(len(shared_subjects) * 12, 36)
        reasons.append("业务对象明确")

    if uniqueness is not None:
        if uniqueness >= 0.95:
            score += 18
            reasons.append("高唯一")
        elif uniqueness >= 0.7:
            score += 10
            reasons.append("较高唯一")
    if null_rate is not None:
        if null_rate <= 0.05:
            score += 10
            reasons.append("低空值")
        elif null_rate >= 0.8:
            score -= 20
            reasons.append("高空值")

    distinct_estimate = _column_distinct_estimate(column)
    if distinct_estimate is not None:
        if distinct_estimate <= 1:
            score -= 60
            reasons.append("几乎无区分度")
        elif distinct_estimate < 10:
            score -= 18
            reasons.append("区分度偏低")

    return score, reasons


def _candidate_join_columns(profile: Dict[str, Any], max_candidates: int = AI_JOIN_MAX_CANDIDATES_PER_TABLE) -> List[Dict[str, Any]]:
    structure = profile.get("structure") if isinstance(profile.get("structure"), dict) else {}
    columns = structure.get("columns") if isinstance(structure.get("columns"), list) else []
    primary_keys = structure.get("primary_keys") if isinstance(structure.get("primary_keys"), list) else []
    primary_key_names = {str(item.get("column_name") or "").strip() for item in primary_keys if str(item.get("column_name") or "").strip()}

    qualified_name = str(profile.get("qualified_name") or "")
    ranked: List[Dict[str, Any]] = []
    for column in columns:
        if not isinstance(column, dict):
            continue
        name = _column_name(column)
        if not name:
            continue
        score, reasons = _column_join_score(column, primary_key_names)
        if score < 20:
            continue
        ranked.append({
            "column_name": name,
            "score": score,
            "reasons": reasons,
            "tokens": sorted(_column_business_tokens(column)),
            "uniqueness_ratio": _column_uniqueness_ratio(column),
            "null_rate": _column_null_rate(column),
            "distinct_estimate": _column_distinct_estimate(column),
            "comment": _extract_column_comment(column),
        })

    # 客户主数据里同时存在 number / jdy_number / jd_number 时，优先保留更像业务系统编号的字段。
    if qualified_name.endswith(".base_customer"):
        preferred_order = {"jdy_number": 3, "jd_number": 2, "number": 1}
        for item in ranked:
            name = str(item.get("column_name") or "").lower()
            if name in preferred_order:
                item["score"] = int(item.get("score") or 0) + preferred_order[name] * 24
                existing_reasons = item.get("reasons") or []
                item["reasons"] = list(dict.fromkeys(list(existing_reasons) + ["客户业务编号优先"]))

    ranked.sort(
        key=lambda item: (
            -int(item["score"]),
            -(item.get("uniqueness_ratio") or 0),
            item["column_name"],
        )
    )

    selected: List[Dict[str, Any]] = []
    seen = set()
    for item in ranked:
        name = item["column_name"]
        lowered = name.lower()
        normalized = lowered.replace("_id", "").replace("_number", "").replace("_no", "").replace("_code", "")
        dedupe_key = normalized or lowered
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        selected.append(item)
        if len(selected) >= max_candidates:
            break
    return selected


def _join_candidate_pair_score(left: Dict[str, Any], right: Dict[str, Any]) -> Tuple[int, List[str]]:
    left_name = str(left.get("column_name") or "")
    right_name = str(right.get("column_name") or "")
    left_lower = left_name.lower()
    right_lower = right_name.lower()
    left_tokens = set(left.get("tokens") or [])
    right_tokens = set(right.get("tokens") or [])
    score = int(left.get("score") or 0) + int(right.get("score") or 0)
    reasons: List[str] = []

    if left_lower == right_lower:
        score += 42
        reasons.append("同名字段")

    shared_subjects = sorted((left_tokens & right_tokens) & AI_JOIN_SUBJECT_TAGS)
    if shared_subjects:
        score += min(16 * len(shared_subjects), 48)
        reasons.append("业务对象一致")

    if "jdy" in left_tokens and "jdy" in right_tokens:
        score += 22
        reasons.append("同为简道云标识")
    if "kingdee" in left_tokens and "kingdee" in right_tokens:
        score += 16
        reasons.append("同为金蝶标识")
    if "number" in left_tokens and "number" in right_tokens:
        score += 20
        reasons.append("同为业务编号")
    if "customer" in shared_subjects and "number" in left_tokens and "number" in right_tokens:
        score += 36
        reasons.append("客户编号强匹配")
    if "contract" in shared_subjects and "number" in left_tokens and "number" in right_tokens:
        score += 24
        reasons.append("合同编号强匹配")
    if "id" in left_tokens and "id" in right_tokens:
        score += 18
        reasons.append("同为ID字段")
    if left_lower.endswith("_customer") and right_lower == "id":
        score += 16
        reasons.append("关联客户ID模式")
    if right_lower.endswith("_customer") and left_lower == "id":
        score += 16
        reasons.append("关联客户ID模式")

    left_comment = str(left.get("comment") or "").lower()
    right_comment = str(right.get("comment") or "").lower()
    if "客户编号" in left_comment and "客户编号" in right_comment:
        score += 30
        reasons.append("注释均提示客户编号")
    if "合同编号" in left_comment and "合同编号" in right_comment:
        score += 26
        reasons.append("注释均提示合同编号")
    if "关联客户" in left_comment and ("主键" in right_comment or right_lower == "id"):
        score += 28
        reasons.append("注释提示关联客户到主键")
    if "关联客户" in right_comment and ("主键" in left_comment or left_lower == "id"):
        score += 28
        reasons.append("注释提示关联客户到主键")

    if left_lower == "id" and right_lower == "id" and not shared_subjects:
        score -= 55
        reasons.append("纯ID对纯ID风险高")
    if left_lower == "id" or right_lower == "id":
        other_tokens = right_tokens if left_lower == "id" else left_tokens
        if "number" in other_tokens:
            score -= 48
            reasons.append("ID对编号风险高")
        if "jdy" in other_tokens:
            score -= 30
            reasons.append("主键对业务系统ID风险高")
    if any(pattern in left_lower for pattern in AI_JOIN_SYSTEM_FIELD_PATTERNS) or any(pattern in right_lower for pattern in AI_JOIN_SYSTEM_FIELD_PATTERNS):
        score -= 42
        reasons.append("系统字段配对降权")
    if left_lower.endswith("_name") or right_lower.endswith("_name"):
        score -= 30
        reasons.append("名称字段不稳定")

    return score, reasons


def _relation_confidence_from_overlap(overlap_ratio: float, matched_rows_ratio: float, pair_score: int) -> str:
    if overlap_ratio >= 0.65 and matched_rows_ratio >= 0.45 and pair_score >= 150:
        return "高"
    if overlap_ratio >= 0.3 and matched_rows_ratio >= 0.18 and pair_score >= 110:
        return "中"
    return "低"


def _relation_join_type(left_uniqueness: Optional[float], right_uniqueness: Optional[float]) -> str:
    left_high = (left_uniqueness or 0) >= 0.95
    right_high = (right_uniqueness or 0) >= 0.95
    if left_high and right_high:
        return "1-1"
    if left_high and not right_high:
        return "1-N"
    if not left_high and right_high:
        return "N-1"
    return "N-N"


def _build_relation_reason_text(
    pair_reasons: List[str],
    overlap_ratio: float,
    matched_rows_ratio: float,
    sample_size: int,
    method: str,
) -> str:
    reason_bits = list(dict.fromkeys(pair_reasons))[:3]
    reason_bits.append(f"样本重叠 {round(overlap_ratio * 100)}%")
    reason_bits.append(f"命中覆盖 {round(matched_rows_ratio * 100)}%")
    if sample_size > 0:
        reason_bits.append(f"样本数 {sample_size}")
    if method == "cross_source_sample":
        reason_bits.append("跨库样本校验")
    elif method == "same_source_sample":
        reason_bits.append("同库样本校验")
    return " / ".join(reason_bits)


def _query_join_sample_values(
    service: DataMapService,
    *,
    qualified_name: str,
    column_name: str,
    limit: int = AI_JOIN_SAMPLE_LIMIT,
) -> List[str]:
    parsed = _parse_qualified_table_name(qualified_name)
    if not parsed:
        return []
    schema, table = parsed
    ds = service._datasource_for_schema(schema)
    q = service._quote_ident_for
    sql = (
        f"SELECT DISTINCT {q(column_name, ds)} AS join_value "
        f"FROM {q(schema, ds)}.{q(table, ds)} "
        f"WHERE {q(column_name, ds)} IS NOT NULL "
        f"LIMIT {max(10, min(limit, AI_JOIN_SAMPLE_LIMIT))}"
    )
    try:
        rows = service._query(sql, None, ds)
    except Exception:
        return []
    values: List[str] = []
    seen = set()
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, dict):
            continue
        normalized = _normalize_join_value(row.get("join_value"))
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        values.append(normalized)
        if len(values) >= limit:
            break
    return values


def _probe_join_pair(
    service: DataMapService,
    *,
    left_table: str,
    right_table: str,
    left_candidate: Dict[str, Any],
    right_candidate: Dict[str, Any],
) -> Optional[Dict[str, Any]]:
    pair_score, pair_reasons = _join_candidate_pair_score(left_candidate, right_candidate)
    if pair_score < 90:
        return None

    left_values = _query_join_sample_values(service, qualified_name=left_table, column_name=str(left_candidate.get("column_name") or ""))
    right_values = _query_join_sample_values(service, qualified_name=right_table, column_name=str(right_candidate.get("column_name") or ""))
    if len(left_values) < 3 or len(right_values) < 3:
        return None

    left_set = set(left_values)
    right_set = set(right_values)
    overlap = left_set & right_set
    if len(overlap) < 2:
        return None

    overlap_ratio = len(overlap) / max(1, min(len(left_set), len(right_set)))
    matched_rows_ratio = max(len(overlap) / max(1, len(left_set)), len(overlap) / max(1, len(right_set)))
    if overlap_ratio < 0.08 and matched_rows_ratio < 0.12:
        return None

    left_parsed = _parse_qualified_table_name(left_table)
    right_parsed = _parse_qualified_table_name(right_table)
    method = "cross_source_sample"
    if left_parsed and right_parsed and left_parsed[0] == right_parsed[0]:
        method = "same_source_sample"

    confidence = _relation_confidence_from_overlap(overlap_ratio, matched_rows_ratio, pair_score)
    relation = {
        "left_table": left_table,
        "right_table": right_table,
        "left_column": left_candidate["column_name"],
        "right_column": right_candidate["column_name"],
        "join_type": _relation_join_type(
            left_candidate.get("uniqueness_ratio"),
            right_candidate.get("uniqueness_ratio"),
        ),
        "confidence": confidence,
        "reason": _build_relation_reason_text(
            pair_reasons,
            overlap_ratio=overlap_ratio,
            matched_rows_ratio=matched_rows_ratio,
            sample_size=min(len(left_set), len(right_set)),
            method=method,
        ),
        "expression": f"{left_table}.{left_candidate['column_name']} = {right_table}.{right_candidate['column_name']}",
        "overlap_ratio": round(overlap_ratio, 4),
        "matched_rows_ratio": round(matched_rows_ratio, 4),
        "sample_overlap_count": len(overlap),
        "sample_left_count": len(left_set),
        "sample_right_count": len(right_set),
        "method": method,
        "pair_score": pair_score,
    }
    return relation


def _relation_prompt_boost(prompt_subjects: Set[str], left_candidate: Dict[str, Any], right_candidate: Dict[str, Any]) -> int:
    if not prompt_subjects:
        return 0
    candidate_subjects = (set(left_candidate.get("tokens") or []) | set(right_candidate.get("tokens") or [])) & AI_JOIN_SUBJECT_TAGS
    if not candidate_subjects:
        return -20
    shared = prompt_subjects & candidate_subjects
    if not shared:
        return -30
    boost = len(shared) * 30
    if "customer" in shared and "number" in candidate_subjects:
        boost += 20
    if "contract" in prompt_subjects and "contract" in candidate_subjects:
        boost += 12
    if "order" in prompt_subjects and "order" in candidate_subjects:
        boost += 12
    return boost


def _is_system_relation(relation: Dict[str, Any]) -> bool:
    left_column = str(relation.get("left_column") or "").lower()
    right_column = str(relation.get("right_column") or "").lower()
    return any(pattern in left_column for pattern in AI_JOIN_SYSTEM_FIELD_PATTERNS) or any(
        pattern in right_column for pattern in AI_JOIN_SYSTEM_FIELD_PATTERNS
    )


def _relation_business_priority(relation: Dict[str, Any], prompt_subjects: Set[str]) -> int:
    left_column = str(relation.get("left_column") or "").lower()
    right_column = str(relation.get("right_column") or "").lower()
    reason = str(relation.get("reason") or "")
    expression = str(relation.get("expression") or "").lower()
    priority = 0
    if "客户编号强匹配" in reason:
        priority += 80
    if "合同编号强匹配" in reason:
        priority += 50
    if "同为业务编号" in reason:
        priority += 20
    if "同名字段" in reason and ("number" in left_column or "number" in right_column):
        priority += 12
    if "customer" in prompt_subjects and "customer" in expression:
        priority += 20
    if "contract" in prompt_subjects and "contract" in expression:
        priority += 16
    if _is_system_relation(relation):
        priority -= 120
    if (relation.get("overlap_ratio") or 0) < 0.05:
        priority -= 40
    if str(relation.get("confidence") or "") == "低":
        priority -= 20
    return priority


def _filter_ai_relation_hints(
    relations: List[Dict[str, Any]],
    *,
    prompt_subjects: Set[str],
    max_relations: int = AI_JOIN_MAX_RELATIONS,
) -> List[Dict[str, Any]]:
    filtered: List[Dict[str, Any]] = []
    for relation in relations:
        overlap_ratio = float(relation.get("overlap_ratio") or 0)
        confidence = str(relation.get("confidence") or "")
        business_priority = _relation_business_priority(relation, prompt_subjects)
        relation["business_priority"] = business_priority
        if _is_system_relation(relation) and confidence != "高":
            continue
        if overlap_ratio < 0.08 and confidence != "高":
            continue
        filtered.append(relation)

    filtered.sort(
        key=lambda item: (
            -(item.get("business_priority") or 0),
            {"高": 0, "中": 1, "低": 2}.get(str(item.get("confidence") or ""), 3),
            -(item.get("overlap_ratio") or 0),
            -(item.get("pair_score") or 0),
            item.get("expression") or "",
        )
    )
    return filtered[:max_relations]


def _guess_ai_relation_hints(service: DataMapService, profiles: List[Tuple[str, Dict[str, Any]]], prompt: str = "") -> List[Dict[str, Any]]:
    if len(profiles) < 2:
        return []

    prompt_subjects = _prompt_join_subjects(prompt)
    join_candidates: Dict[str, List[Dict[str, Any]]] = {}
    for qualified_name, profile in profiles:
        join_candidates[qualified_name] = _candidate_join_columns(profile)

    relations: List[Dict[str, Any]] = []
    seen_keys = set()
    for left_index, (left_name, _) in enumerate(profiles):
        left_candidates = join_candidates.get(left_name) or []
        if not left_candidates:
            continue
        for right_name, _ in profiles[left_index + 1:]:
            right_candidates = join_candidates.get(right_name) or []
            if not right_candidates:
                continue

            ranked_pairs: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
            for left_candidate in left_candidates:
                for right_candidate in right_candidates:
                    pair_score, _ = _join_candidate_pair_score(left_candidate, right_candidate)
                    pair_score += _relation_prompt_boost(prompt_subjects, left_candidate, right_candidate)
                    ranked_pairs.append((pair_score, left_candidate, right_candidate))
            ranked_pairs.sort(key=lambda item: (-item[0], item[1]["column_name"], item[2]["column_name"]))

            pair_count = 0
            for _, left_candidate, right_candidate in ranked_pairs:
                relation = _probe_join_pair(
                    service,
                    left_table=left_name,
                    right_table=right_name,
                    left_candidate=left_candidate,
                    right_candidate=right_candidate,
                )
                pair_count += 1
                if pair_count >= AI_JOIN_MAX_PAIRS_PER_TABLE_PAIR and relations:
                    break
                if not relation:
                    continue
                key = (
                    relation["left_table"],
                    relation["right_table"],
                    relation["left_column"],
                    relation["right_column"],
                )
                if key in seen_keys:
                    continue
                seen_keys.add(key)
                relations.append(relation)
                if len(relations) >= AI_JOIN_MAX_RELATIONS:
                    break
            if len(relations) >= AI_JOIN_MAX_RELATIONS:
                break
        if len(relations) >= AI_JOIN_MAX_RELATIONS:
            break

    table_rank = {name: index for index, (name, _) in enumerate(profiles)}
    relations.sort(
        key=lambda item: (
            table_rank.get(str(item.get("left_table") or ""), 99) + table_rank.get(str(item.get("right_table") or ""), 99),
            {"高": 0, "中": 1, "低": 2}.get(str(item.get("confidence") or ""), 3),
            -(item.get("overlap_ratio") or 0),
            -(item.get("pair_score") or 0),
            item.get("expression") or "",
        )
    )
    return _filter_ai_relation_hints(relations[: max(AI_JOIN_MAX_RELATIONS * 3, AI_JOIN_MAX_RELATIONS)], prompt_subjects=prompt_subjects, max_relations=AI_JOIN_MAX_RELATIONS)


def _build_ai_relation_confirm_response(
    *,
    prompt: str,
    linked_table: str,
    context_tables: List[str],
    candidates: List[Dict[str, Any]],
    guessed_relations: List[Dict[str, Any]],
    cross_datasource_blocked: bool = False,
) -> Dict[str, Any]:
    table_note = "、".join(context_tables[:4]) if context_tables else linked_table
    relation_note = "；".join(item["expression"] for item in guessed_relations[:2]) if guessed_relations else "暂未识别到明确关联键"
    if cross_datasource_blocked:
        reply = f"我准备先用 {table_note} 来回答这个问题。但你现在选到的表跨了不同数据库，当前工作台不能直接做跨库 JOIN。我会先按主表所在库继续生成 SQL；如果你要混合分析，需要先落一张中间表。"
    elif guessed_relations:
        reply = f"我准备先用 {table_note} 来回答这个问题。当前已探查到的主要关系是：{relation_note}。如果没问题，你可以直接点“继续生成 SQL”；如果不对，就改一下问题或重新选表。"
    else:
        reply = f"我准备先用 {table_note} 来回答这个问题。当前还没探查到稳定的直接关联键，我会先按同库范围生成更保守的 SQL，并把需要你确认的关联写清楚。"
    return {
        "reply": reply,
        "sql": "",
        "title": "先确认表和关系",
        "follow_ups": [
            "点“继续生成 SQL”后，我会按这些表先给你第一版 SQL",
            "如果表不对，可以点候选表，或者补一句更具体的业务口径",
        ],
        "model": "",
        "linked_table": linked_table,
        "mode": "relation_confirm",
        "degraded": False,
        "table_candidates": candidates,
        "confirmed_tables": context_tables,
        "context_tables": context_tables,
        "guessed_relations": guessed_relations,
        "need_confirmation": True,
        "next_actions": ["continue_generate", "select_table", "refine_prompt"],
    }


def _needs_multi_table_context(prompt: str) -> bool:
    lowered = str(prompt or "").strip().lower()
    if not lowered:
        return False
    return any(token in lowered for token in AI_MULTI_TABLE_PROMPT_HINTS)


def _resolve_ai_context_tables(
    service: DataMapService,
    prompt: str,
    linked_table: str,
    current_sql: str = "",
    extra_tables: Optional[List[str]] = None,
    max_tables: int = 4,
) -> List[str]:
    selected: List[str] = []
    primary = str(linked_table or "").strip()
    if primary:
        selected.append(primary)
    for qualified_name in extra_tables or []:
        if qualified_name not in selected:
            selected.append(qualified_name)
        if len(selected) >= max_tables:
            return selected[:max_tables]
    for qualified_name in _extract_sql_context_tables(current_sql, max_tables=max_tables):
        if qualified_name not in selected:
            selected.append(qualified_name)
        if len(selected) >= max_tables:
            return selected[:max_tables]
    if not _needs_multi_table_context(prompt):
        return selected[:max_tables]

    for item in _search_ai_candidate_tables(service, prompt, limit=8):
        qualified_name = str(item.get("qualified_name") or "").strip()
        if not qualified_name or qualified_name in selected:
            continue
        if int(item.get("score") or 0) < 80:
            continue
        selected.append(qualified_name)
        if len(selected) >= max_tables:
            break
    return selected[:max_tables]


def _build_ai_table_context_sections(profiles: List[Tuple[str, Dict[str, Any]]]) -> List[str]:
    sections: List[str] = []
    for index, (qualified_name, profile) in enumerate(profiles):
        role = "主表" if index == 0 else f"补充表{index}"
        columns = profile.get("structure", {}).get("columns", [])
        preview_rows = (profile.get("preview", {}) or {}).get("rows", []) if isinstance(profile.get("preview"), dict) else []
        ddl_text = profile.get("ddl", {}).get("text", "") if isinstance(profile.get("ddl"), dict) else ""

        column_lines: List[str] = []
        for column in columns[:80]:
            column_lines.append(
                f"- {column.get('column_name') or ''} | {column.get('data_type') or column.get('udt_name') or ''} | {column.get('column_comment') or column.get('business_def') or '暂无注释'}"
            )

        sections.extend([
            f"{role}：{qualified_name}",
            f"{role}说明：{_clip_text(profile.get('table_comment') or '', 400)}",
            f"{role}字段列表：\n{_clip_text(chr(10).join(column_lines), 4000)}",
            f"{role}DDL：\n{_clip_text(ddl_text, 5000)}",
            f"{role}样例数据：\n{_clip_text(json.dumps(preview_rows[:8], ensure_ascii=False, default=str), 2500)}",
        ])
    return sections


def _column_name(column: Dict[str, Any]) -> str:
    return str(column.get("column_name") or "").strip()


def _column_type(column: Dict[str, Any]) -> str:
    return str(column.get("data_type") or column.get("udt_name") or "").strip().lower()


def _matches_name_keywords(name: str, keywords: List[str]) -> bool:
    lowered = name.lower()
    return any(keyword in lowered for keyword in keywords)


def _is_numeric_column(column: Dict[str, Any]) -> bool:
    type_name = _column_type(column)
    return any(token in type_name for token in ("int", "numeric", "decimal", "double", "float", "real", "number"))


def _is_temporal_column(column: Dict[str, Any]) -> bool:
    type_name = _column_type(column)
    if any(token in type_name for token in ("date", "time", "timestamp")):
        return True
    return _matches_name_keywords(_column_name(column), ["date", "time", "_dt", "day", "month"])


def _pick_column(
    columns: List[Dict[str, Any]],
    keywords: List[str],
    predicate: Optional[Any] = None,
    exclude: Optional[List[str]] = None,
) -> str:
    excluded = {item for item in (exclude or []) if item}
    for keyword in keywords:
        for column in columns:
            name = _column_name(column)
            if not name or name in excluded:
                continue
            if keyword not in name.lower():
                continue
            if predicate is None or predicate(column):
                return name
    if predicate is not None:
        for column in columns:
            name = _column_name(column)
            if not name or name in excluded:
                continue
            if predicate(column):
                return name
    return ""


def _choose_fallback_focus_columns(columns: List[Dict[str, Any]]) -> Dict[str, str]:
    time_col = _pick_column(
        columns,
        ["created_at", "create_time", "created_time", "order_date", "biz_date", "date", "dt", "time"],
        predicate=_is_temporal_column,
    )
    metric_col = _pick_column(
        columns,
        ["amount", "amt", "price", "total", "fee", "cost", "gmv", "revenue", "quantity", "qty", "num"],
        predicate=_is_numeric_column,
        exclude=[time_col],
    )
    dim_col = _pick_column(
        columns,
        ["status", "type", "category", "owner", "department", "dept", "source", "channel", "project", "biz"],
        predicate=lambda column: not _is_numeric_column(column) and not _is_temporal_column(column),
        exclude=[time_col, metric_col],
    )
    id_col = _pick_column(
        columns,
        ["_id", "id", "code", "no", "number", "uuid"],
        predicate=lambda column: not _is_temporal_column(column),
        exclude=[time_col, metric_col, dim_col],
    )
    return {
        "time": time_col,
        "metric": metric_col,
        "dimension": dim_col,
        "id": id_col,
    }


def _build_fallback_understand_sql(table_ref: str, focus: Dict[str, str]) -> str:
    select_lines = ["COUNT(*) AS total_rows"]
    if focus["id"]:
        select_lines.append(f"COUNT(DISTINCT {focus['id']}) AS distinct_{focus['id']}")
    if focus["time"]:
        select_lines.append(f"MIN({focus['time']}) AS earliest_{focus['time']}")
        select_lines.append(f"MAX({focus['time']}) AS latest_{focus['time']}")
    if focus["metric"]:
        select_lines.append(f"SUM(COALESCE({focus['metric']}, 0)) AS total_{focus['metric']}")
        select_lines.append(f"AVG(COALESCE({focus['metric']}, 0)) AS avg_{focus['metric']}")
    return "SELECT\n  " + ",\n  ".join(select_lines) + f"\nFROM {table_ref};"


def _build_fallback_quality_sql(table_ref: str, columns: List[Dict[str, Any]], focus: Dict[str, str]) -> str:
    select_lines = ["COUNT(*) AS total_rows"]
    focus_columns: List[str] = []
    for name in [focus["id"], focus["time"], focus["dimension"], focus["metric"]]:
        if name and name not in focus_columns:
            focus_columns.append(name)
    for column in columns:
        name = _column_name(column)
        if not name or name in focus_columns:
            continue
        focus_columns.append(name)
        if len(focus_columns) >= 4:
            break
    for name in focus_columns[:4]:
        select_lines.append(f"SUM(CASE WHEN {name} IS NULL THEN 1 ELSE 0 END) AS null_{name}")
    if focus["id"]:
        select_lines.append(f"COUNT(DISTINCT {focus['id']}) AS distinct_{focus['id']}")
        select_lines.append(f"COUNT(*) - COUNT(DISTINCT {focus['id']}) AS duplicate_{focus['id']}_rows")
    return "SELECT\n  " + ",\n  ".join(select_lines) + f"\nFROM {table_ref};"


def _build_fallback_summary_sql(table_ref: str, focus: Dict[str, str]) -> str:
    time_col = focus["time"]
    metric_col = focus["metric"]
    dim_col = focus["dimension"]
    if time_col and metric_col:
        return (
            "SELECT\n"
            f"  CAST({time_col} AS DATE) AS stat_date,\n"
            "  COUNT(*) AS row_count,\n"
            f"  SUM(COALESCE({metric_col}, 0)) AS total_{metric_col}\n"
            f"FROM {table_ref}\n"
            f"GROUP BY CAST({time_col} AS DATE)\n"
            "ORDER BY stat_date DESC\n"
            "LIMIT 100;"
        )
    if dim_col and metric_col:
        return (
            "SELECT\n"
            f"  {dim_col},\n"
            "  COUNT(*) AS row_count,\n"
            f"  SUM(COALESCE({metric_col}, 0)) AS total_{metric_col}\n"
            f"FROM {table_ref}\n"
            f"GROUP BY {dim_col}\n"
            f"ORDER BY total_{metric_col} DESC\n"
            "LIMIT 50;"
        )
    if time_col:
        return (
            "SELECT\n"
            f"  CAST({time_col} AS DATE) AS stat_date,\n"
            "  COUNT(*) AS row_count\n"
            f"FROM {table_ref}\n"
            f"GROUP BY CAST({time_col} AS DATE)\n"
            "ORDER BY stat_date DESC\n"
            "LIMIT 100;"
        )
    if dim_col:
        return (
            "SELECT\n"
            f"  {dim_col},\n"
            "  COUNT(*) AS row_count\n"
            f"FROM {table_ref}\n"
            f"GROUP BY {dim_col}\n"
            "ORDER BY row_count DESC\n"
            "LIMIT 50;"
        )
    return "SELECT COUNT(*) AS total_rows\nFROM " + table_ref + "\nLIMIT 1;"


def _build_sql_assistant_fallback_response(
    *,
    schema: str,
    table: str,
    linked_table: str,
    context_tables: List[str],
    guessed_relations: List[Dict[str, Any]],
    prompt: str,
    current_sql: str,
    profile: Dict[str, Any],
) -> Dict[str, Any]:
    columns = profile.get("structure", {}).get("columns", [])
    focus = _choose_fallback_focus_columns(columns if isinstance(columns, list) else [])
    table_ref = ".".join([part for part in [schema.strip(), table.strip()] if part])
    prompt_lower = prompt.lower()
    focus_fields = [name for name in [focus["time"], focus["metric"], focus["dimension"], focus["id"]] if name]
    focus_summary = "、".join(focus_fields[:4]) if focus_fields else "字段列表和样例数据"

    if any(token in prompt_lower for token in ("质量", "空值", "重复", "异常")):
        title = "规则降级：数据质量探查"
        sql_text = _build_fallback_quality_sql(table_ref, columns if isinstance(columns, list) else [], focus)
        reply = (
            "当前未配置 OPENAI_API_KEY，已切换为规则降级模式。"
            f"我先按 {focus_summary} 给你生成一条质量探查起手 SQL，优先看总行数、空值和主键重复。"
        )
        follow_ups = ["跑完后把结果贴回来，我再帮你继续缩小问题范围", "如果需要，我可以继续给你补一条按日期或状态分组的异常定位 SQL"]
    elif any(token in prompt_lower for token in ("解释", "优化", "改写")) and current_sql.strip():
        title = "规则降级：当前 SQL 检查建议"
        sql_text = ""
        reply = (
            "当前未配置 OPENAI_API_KEY，已切换为规则降级模式。"
            "这类问题更适合模型解释，但你现在仍可以先人工检查 4 件事：筛选条件是否完整、聚合粒度是否一致、JOIN 是否会放大行数、排序和 LIMIT 是否符合预期。"
        )
        follow_ups = ["把当前 SQL 跑出来的结果贴回来，我可以继续按结果帮你定位问题", "如果你要，我也可以先给你补一条更稳妥的对照 SQL"]
    elif any(token in prompt_lower for token in ("理解", "这张表", "字段", "看这张表")):
        title = "规则降级：表概览 SQL"
        sql_text = _build_fallback_understand_sql(table_ref, focus)
        reply = (
            "当前未配置 OPENAI_API_KEY，已切换为规则降级模式。"
            f"我先按 {focus_summary} 给你一条表概览 SQL，方便先确认数据规模、时间范围和核心指标。"
        )
        follow_ups = ["看完总量后，可以继续按日期趋势或状态分布拆开分析", "如果你更关心质量问题，我可以直接再给你一条空值和重复探查 SQL"]
    else:
        title = "规则降级：分析起手 SQL"
        sql_text = _build_fallback_summary_sql(table_ref, focus)
        reply = (
            "当前未配置 OPENAI_API_KEY，已切换为规则降级模式。"
            f"我基于 {'、'.join(context_tables[:4]) or linked_table} 的上下文，按 {focus_summary} 先给你一条可直接执行的分析起手 SQL。"
        )
        follow_ups = ["如果你告诉我想看时间趋势、部门分布还是质量问题，我可以继续给你更具体的 SQL", "Key 配好后，这里会自动切回模型版助手"]

    return {
        "reply": reply,
        "sql": sql_text,
        "title": title,
        "follow_ups": follow_ups,
        "model": "",
        "linked_table": linked_table,
        "context_tables": context_tables or [linked_table],
        "confirmed_tables": context_tables or [linked_table],
        "guessed_relations": guessed_relations,
        "need_confirmation": False,
        "next_actions": ["write_sql", "run_sql", "refine_prompt"],
        "degraded": True,
        "mode": "sql_generate",
    }


HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>数据地图</title>
  <style>
    :root {
      /* ── 血缘图同款 Design Tokens ── */
      --nav-height: 52px;
      --sidebar-width: 320px;
      --brand: #1a73e8;
      --brand-light: #4a90e2;
      --bg-content: #f0f1f3;
      --bg-panel: #ffffff;
      --bg-hover: #e8eaed;
      --bg-soft: #f8f9fa;
      --bg-strong: #ffffff;
      --ink: #1a1d23;
      --ink-secondary: #5f6368;
      --muted: #9aa0a6;
      --line: #dadce0;
      --line-light: #e8eaed;
      --accent: #1a73e8;
      --accent-soft: rgba(26,115,232,0.08);
      --accent-teal: #0d9488;
      --accent-gold: #ea580c;
      --success: #16a34a;
      --danger: #dc2626;
      --shadow-sm: 0 1px 2px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.1);
      --shadow-md: 0 2px 6px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.06);
      --radius-panel: 8px;
      --radius-card: 8px;
      --radius-pill: 999px;
      --shadow-panel: var(--shadow-md);
      --shadow-soft: var(--shadow-sm);
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Helvetica Neue", sans-serif;
      font-size: 13px;
      color: var(--ink);
      background: var(--bg-content);
      min-height: 100vh;
    }
    input, select, button, textarea { font-family: inherit; font-size: inherit; }
    [hidden] { display: none !important; }
    /* ── 顶部导航栏 ── */
    .topbar { height: 48px; background: #1a1d23; display: flex; align-items: center; justify-content: space-between; padding: 0 20px; flex-shrink: 0; position: sticky; top: 0; z-index: 100; }
    .topbar-brand { display: flex; align-items: center; gap: 8px; color: #fff; font-weight: 700; font-size: 14px; }
    .dot { width: 7px; height: 7px; border-radius: 50%; background: #1a73e8; flex-shrink: 0; }
    .topbar-sub { color: rgba(255,255,255,0.35); font-size: 11px; font-weight: 400; margin-left: 6px; }
    .topbar-actions { display: flex; align-items: center; gap: 14px; }
    .ov-stat { color: rgba(255,255,255,0.5); font-size: 12px; }
    .ov-stat .ov-n { color: #fff; font-weight: 600; }
    .topbar-btn { background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.12); color: rgba(255,255,255,0.85); padding: 5px 12px; border-radius: 6px; cursor: pointer; font-size: 12px; font-family: inherit; transition: background 0.15s; }
    .topbar-btn:hover { background: rgba(255,255,255,0.15); }
    /* ── 搜索区 ── */
    .search-bar { background: #fff; border-bottom: 1px solid #dadce0; padding: 10px 20px; position: sticky; top: 48px; z-index: 90; }
    .search-form { display: flex; align-items: center; gap: 8px; }
    .search-input { flex: 1; max-width: 600px; height: 36px; border: 1px solid #dadce0; border-radius: 6px; padding: 0 12px; font-size: 13px; font-family: inherit; color: #1a1d23; background: #f8f9fa; outline: none; transition: border-color 0.15s; }
    .search-input:focus { border-color: #1a73e8; background: #fff; }
    .search-btn { height: 36px; padding: 0 16px; border: 0; border-radius: 6px; font-size: 13px; font-family: inherit; cursor: pointer; font-weight: 600; background: #1a73e8; color: #fff; transition: background 0.15s; }
    .search-btn:hover { background: #1557b0; }
    .search-btn.ghost { background: transparent; color: #9aa0a6; border: 1px solid #dadce0; }
    .search-btn.ghost:hover { background: #e8eaed; color: #1a1d23; }
    .filter-panel { padding-top: 10px; }
    .filter-row { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; }
    .filter-field { display: flex; flex-direction: column; gap: 3px; min-width: 120px; }
    .filter-field label { font-size: 11px; color: #9aa0a6; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
    .filter-field select { height: 30px; border: 1px solid #dadce0; border-radius: 6px; background: #f8f9fa; padding: 0 8px; font-size: 12px; font-family: inherit; color: #1a1d23; outline: none; }
    /* ── 页面：三栏数据目录 ── */
    .page { display: flex; flex-direction: column; height: 100vh; background: #f0f1f3; overflow: hidden; }
    .catalog-body { flex: 1; display: grid; --w-facets: 240px; --w-results: 380px; grid-template-columns: var(--w-facets) 8px var(--w-results) 8px minmax(0, 1fr); min-height: 0; overflow: hidden; position: relative; }
    /* 可拖拽分隔条 */
    .splitter { position: relative; cursor: col-resize; background: #f0f1f3; z-index: 6; user-select: none; }
    .splitter::after { content: ''; position: absolute; top: 0; bottom: 0; left: 50%; transform: translateX(-50%); width: 1px; background: #d3d6db; transition: background 0.12s, width 0.12s; }
    .splitter:hover::after, .splitter.dragging::after { background: #1a73e8; width: 2px; }
    .splitter .sp-grip { position: absolute; top: 50%; left: 50%; transform: translate(-50%, -50%); width: 4px; height: 32px; border-radius: 3px; background: #c9ccd1; pointer-events: none; transition: background 0.12s; }
    .splitter:hover .sp-grip { background: #1a73e8; }
    /* 浮动折叠按钮：JS 定位，跟着各自面板边走 */
    .col-toggle { position: absolute; top: 50%; margin-top: -16px; width: 22px; height: 32px; border: 1px solid #1a73e8; border-radius: 5px; background: #1a73e8; color: #fff; cursor: pointer; display: flex; align-items: center; justify-content: center; box-shadow: 0 2px 6px rgba(26,115,232,0.35); z-index: 12; padding: 0; transform: translateX(-50%); }
    .col-toggle:hover { background: #1557b0; border-color: #1557b0; }
    .col-toggle svg { display: block; width: 12px; height: 12px; transition: transform 0.18s; }
    /* 折叠后箭头反转方向，提示"再点回来" */
    .catalog-body.facets-collapsed .col-toggle[data-collapse="facets"] svg,
    .catalog-body.detail-collapsed .col-toggle[data-collapse="detail"] svg { transform: rotate(180deg); }
    body.col-resizing { cursor: col-resize !important; user-select: none !important; }
    /* 折叠态（栅格由 JS 内联设置，避免与响应式冲突） */
    .catalog-body.facets-collapsed .facets { display: none; }
    .catalog-body.detail-collapsed .detail-col { display: none; }
    .catalog-body.facets-collapsed .splitter[data-resize="facets"],
    .catalog-body.detail-collapsed .splitter[data-resize="results"] { cursor: default; }
    .catalog-body.facets-collapsed .splitter[data-resize="facets"] .sp-grip,
    .catalog-body.detail-collapsed .splitter[data-resize="results"] .sp-grip { display: none; }
    /* ── 左：分面导航 ── */
    .facets { background: #fff; border-right: 0; overflow-y: auto; padding: 12px 0 24px; grid-column: 1; grid-row: 1; min-width: 0; }
    .facet-head { display: flex; align-items: center; justify-content: space-between; padding: 4px 16px 10px; }
    .facet-head .ft { font-size: 12px; font-weight: 700; color: #1a1d23; letter-spacing: 0.02em; }
    .facet-clear { background: transparent; border: 0; color: #1a73e8; font-size: 11px; cursor: pointer; font-family: inherit; padding: 0; }
    .facet-clear:hover { text-decoration: underline; }
    .facet-clear[disabled] { color: #c9ccd1; cursor: default; text-decoration: none; }
    .facet-group { padding: 10px 16px; border-top: 1px solid #f1f3f4; }
    .facet-title { font-size: 10px; font-weight: 700; color: #9aa0a6; text-transform: uppercase; letter-spacing: 0.06em; margin-bottom: 7px; }
    .facet-item { display: flex; align-items: center; gap: 8px; padding: 4px 7px; margin: 1px -7px; border-radius: 6px; cursor: pointer; font-size: 12px; color: #5f6368; user-select: none; }
    .facet-item:hover { background: #f1f3f4; }
    .facet-item.on { background: rgba(26,115,232,0.08); color: #1a73e8; font-weight: 600; }
    .facet-item .fname { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; display: flex; align-items: center; gap: 6px; }
    .facet-item .fcount { color: #9aa0a6; font-size: 11px; font-variant-numeric: tabular-nums; flex-shrink: 0; }
    .facet-item.on .fcount { color: #1a73e8; }
    .tier-dot { width: 9px; height: 9px; border-radius: 2px; flex-shrink: 0; }
    /* ── 中：结果列 ── */
    .results-col { display: flex; flex-direction: column; min-width: 0; overflow: hidden; background: #f0f1f3; grid-column: 3; grid-row: 1; }
    .results-bar { display: flex; align-items: center; gap: 10px; padding: 9px 18px; border-bottom: 1px solid #e8eaed; background: #fff; flex-shrink: 0; }
    .results-bar .rcount { font-size: 13px; color: #1a1d23; font-weight: 600; }
    .results-bar .rcount b { color: #1a73e8; }
    .results-bar .rspace { flex: 1; }
    .results-bar label.sortlbl { font-size: 11px; color: #9aa0a6; }
    .results-bar select { height: 28px; border: 1px solid #dadce0; border-radius: 6px; background: #f8f9fa; padding: 0 8px; font-size: 12px; color: #5f6368; font-family: inherit; outline: none; cursor: pointer; }
    .results-bar .fav-toggle { display: inline-flex; align-items: center; gap: 5px; font-size: 12px; color: #5f6368; cursor: pointer; user-select: none; }
    /* 当前筛选条件面包屑 */
    .filter-crumbs { display: flex; flex-wrap: wrap; align-items: center; gap: 6px; padding: 6px 18px; background: #f8f9fa; border-bottom: 1px solid #e8eaed; font-size: 12px; color: #5f6368; }
    .filter-crumbs.hidden { display: none; }
    .filter-crumbs .label { color: #9aa0a6; font-size: 11px; }
    .filter-crumbs .crumb { display: inline-flex; align-items: center; gap: 4px; padding: 2px 6px 2px 8px; background: #fff; border: 1px solid #dadce0; border-radius: 12px; }
    .filter-crumbs .crumb b { color: #1a1d23; font-weight: 600; margin-left: 2px; }
    .filter-crumbs .crumb .x { display: inline-flex; align-items: center; justify-content: center; width: 14px; height: 14px; border-radius: 50%; color: #9aa0a6; cursor: pointer; font-size: 14px; line-height: 1; margin-left: 2px; }
    .filter-crumbs .crumb .x:hover { background: #1a73e8; color: #fff; }
    .filter-crumbs .clear-all { margin-left: auto; color: #1a73e8; font-size: 11px; cursor: pointer; padding: 2px 6px; }
    .filter-crumbs .clear-all:hover { text-decoration: underline; }
    .ghost-sm { background: transparent; border: 1px solid #dadce0; color: #9aa0a6; padding: 4px 10px; border-radius: 6px; font-size: 11px; cursor: pointer; font-family: inherit; }
    .ghost-sm:hover { background: #e8eaed; color: #1a1d23; }
    .results-scroll { flex: 1; overflow-y: auto; padding: 12px 16px; }
    /* ── 结果行（高信息密度） ── */
    .result-row { display: flex; align-items: stretch; background: #fff; border: 1px solid #e8eaed; border-radius: 8px; margin-bottom: 8px; cursor: pointer; overflow: hidden; transition: border-color 0.12s, box-shadow 0.12s; }
    .result-row:hover { border-color: #1a73e8; box-shadow: 0 0 0 3px rgba(26,115,232,0.07); }
    .result-row.active { border-color: #1a73e8; box-shadow: 0 0 0 3px rgba(26,115,232,0.12); background: rgba(26,115,232,0.02); }
    .rr-bar { width: 4px; flex-shrink: 0; }
    .rr-body { flex: 1; min-width: 0; padding: 11px 14px; display: grid; grid-template-columns: minmax(0, 1fr); gap: 5px; }
    .rr-top { display: flex; align-items: center; gap: 8px; min-width: 0; }
    .rr-tier { font-size: 10px; font-weight: 700; padding: 1px 6px; border-radius: 4px; color: #fff; flex-shrink: 0; letter-spacing: 0.02em; }
    .rr-name { font-size: 13.5px; font-weight: 600; color: #1a1d23; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; flex: 0 1 auto; min-width: 0; }
    .rr-qname { font-size: 11px; color: #9aa0a6; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; flex: 1 1 auto; min-width: 0; }
    .rr-fav { margin-left: 8px; background: transparent; border: 0; color: #c9ccd1; font-size: 16px; cursor: pointer; line-height: 1; padding: 0 2px; flex-shrink: 0; }
    .rr-fav:hover { color: #f59e0b; }
    .rr-fav.is-favorite { color: #f59e0b; }
    .rr-desc { font-size: 12px; color: #5f6368; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .rr-meta { display: flex; align-items: center; gap: 13px; font-size: 11.5px; color: #9aa0a6; flex-wrap: wrap; min-width: 0; }
    .rr-meta .m { display: inline-flex; align-items: center; gap: 3px; font-variant-numeric: tabular-nums; }
    .rr-meta .m b { color: #5f6368; font-weight: 600; }
    .rr-meta .hot b { color: #ea580c; }
    .chip { display: inline-flex; align-items: center; padding: 2px 7px; border-radius: 999px; font-size: 11px; font-weight: 500; background: #e8eaed; color: #5f6368; border: 1px solid #e8eaed; }
    .chip.match { background: rgba(26,115,232,0.08); color: #1a73e8; border-color: rgba(26,115,232,0.2); }
    /* ── 右：详情列（常驻） ── */
    .detail-col { background: #fff; border-left: 0; display: flex; flex-direction: column; min-height: 0; min-width: 0; overflow: hidden; grid-column: 5; grid-row: 1; }
    .splitter[data-resize="facets"] { grid-column: 2; grid-row: 1; }
    .splitter[data-resize="results"] { grid-column: 4; grid-row: 1; }
    .detail-empty { flex: 1; display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 8px; color: #9aa0a6; font-size: 13px; text-align: center; padding: 40px; }
    .detail-empty .de-ico { font-size: 30px; opacity: 0.5; }
    .detail-panel { display: flex; flex-direction: column; min-height: 0; flex: 1; overflow: hidden; background: #fff; }
    .detail-panel-header { padding: 14px 18px 12px; border-bottom: 1px solid #e8eaed; background: #fff; flex-shrink: 0; }
    /* 信任信号 hero */
    .detail-hero { display: flex; flex-wrap: wrap; align-items: center; gap: 6px 10px; margin-bottom: 8px; }
    .hero-tier { font-size: 10px; font-weight: 700; padding: 2px 7px; border-radius: 4px; color: #fff; letter-spacing: 0.02em; }
    .hero-badge { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; font-weight: 600; padding: 2px 8px; border-radius: 999px; }
    .hero-badge.cert { background: rgba(22,163,74,0.1); color: #16a34a; border: 1px solid rgba(22,163,74,0.22); }
    .hero-badge.fresh { background: rgba(26,115,232,0.08); color: #1a73e8; border: 1px solid rgba(26,115,232,0.2); }
    .hero-badge.hot { background: rgba(234,88,12,0.1); color: #ea580c; border: 1px solid rgba(234,88,12,0.22); }
    .hero-badge.owner { background: #f1f3f4; color: #5f6368; border: 1px solid #e8eaed; }
    .detail-panel-title-wrap { display: flex; align-items: flex-start; gap: 12px; min-width: 0; }
    .back-btn { background: transparent; border: 1px solid #dadce0; color: #9aa0a6; padding: 4px 10px; border-radius: 6px; font-size: 12px; cursor: pointer; font-family: inherit; white-space: nowrap; flex-shrink: 0; margin-top: 3px; }
    .back-btn:hover { background: #e8eaed; color: #1a1d23; }
    .detail-title-row { display: flex; align-items: center; gap: 8px; min-width: 0; flex-wrap: wrap; }
    .detail-title { font-size: 17px; font-weight: 700; color: #1a1d23; line-height: 1.25; min-width: 0; word-break: break-word; }
    .detail-ddl-toggle { display: inline-flex; align-items: center; justify-content: center; height: 24px; padding: 0 10px; border-radius: 999px; border: 1px solid rgba(26,115,232,0.2); background: rgba(26,115,232,0.08); color: #1a73e8; font-size: 11px; font-weight: 700; cursor: pointer; font-family: inherit; flex-shrink: 0; }
    .detail-ddl-toggle:hover { background: rgba(26,115,232,0.14); }
    .detail-ddl-toggle.active { background: #1a73e8; border-color: #1a73e8; color: #fff; }
    .detail-subtitle { color: #9aa0a6; font-size: 12px; margin-top: 2px; line-height: 1.5; }
    .detail-ddl-inline { margin-top: 10px; border: 1px solid #e8eaed; border-radius: 8px; background: #f8f9fa; padding: 12px 14px 14px; display: grid; gap: 10px; }
    .detail-ddl-inline-head { display: flex; align-items: flex-start; justify-content: space-between; gap: 12px; flex-wrap: wrap; }
    .detail-ddl-inline-main { display: grid; gap: 4px; min-width: 0; }
    .detail-ddl-inline-title { font-size: 13px; font-weight: 600; color: #1a1d23; }
    .detail-ddl-inline-meta { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .detail-ddl-inline-note { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .detail-ddl-inline .ddl-code { margin: 0; max-height: 320px; }
    .detail-panel-actions { display: flex; gap: 8px; flex-shrink: 0; }
    .favorite-btn { background: transparent; border: 1px solid #dadce0; color: #9aa0a6; padding: 5px 12px; border-radius: 6px; font-size: 12px; cursor: pointer; font-family: inherit; }
    .favorite-btn:hover { background: #e8eaed; }
    .favorite-btn.is-favorite { color: #b45309; background: #fffbeb; border-color: #fde68a; }
    .stat-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 1px; background: #e8eaed; border-bottom: 1px solid #e8eaed; flex-shrink: 0; }
    .stat-card { background: #f8f9fa; padding: 10px 16px; display: grid; gap: 2px; }
    .stat-label { color: #9aa0a6; font-size: 10px; letter-spacing: 0.04em; text-transform: uppercase; font-weight: 600; }
    .stat-value { color: #1a1d23; font-size: 15px; font-weight: 700; word-break: break-word; }
    /* ── Tab ── */
    .detail-tabs { display: flex; padding: 0 18px; border-bottom: 1px solid #e8eaed; background: #fff; flex-shrink: 0; }
    .detail-tab { padding: 10px 16px; font-size: 13px; font-weight: 500; color: #9aa0a6; cursor: pointer; border-bottom: 2px solid transparent; margin-bottom: -1px; user-select: none; transition: color 0.12s; }
    .detail-tab:hover { color: #1a1d23; }
    .detail-tab.active { color: #1a73e8; border-bottom-color: #1a73e8; font-weight: 600; }
    .tab-pane { display: none; }
    .tab-pane.active { display: block; }
    /* ── 详情内容 ── */
    .detail-content { padding: 14px 18px; display: grid; gap: 12px; overflow-y: auto; flex: 1; min-height: 0; align-content: start; }
    .load-more { width: 100%; margin-top: 4px; padding: 9px; border: 1px solid #dadce0; border-radius: 8px; background: #fff; color: #1a73e8; font-size: 12px; font-weight: 600; cursor: pointer; font-family: inherit; }
    .load-more:hover { background: #f1f3f4; }
    .list-end { text-align: center; color: #c9ccd1; font-size: 11px; padding: 8px 0 4px; }
    .section { background: #fff; border: 1px solid #e8eaed; border-radius: 8px; padding: 14px 16px; display: grid; gap: 10px; container-type: inline-size; }
    .section-head { display: flex; align-items: flex-end; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .section-title { font-size: 13px; font-weight: 600; color: #1a1d23; }
    .section-copy { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .field-stack { display: grid; gap: 10px; }
    .field-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .field { display: grid; gap: 3px; }
    .field label { color: #9aa0a6; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.03em; }
    .field input, .field select, .field textarea { width: 100%; border: 1px solid #dadce0; border-radius: 6px; background: #f8f9fa; padding: 6px 10px; color: #1a1d23; font-size: 13px; font-family: inherit; outline: none; transition: border-color 0.15s; }
    .field input:focus, .field select:focus, .field textarea:focus { border-color: #1a73e8; background: #fff; }
    .field textarea { min-height: 60px; resize: vertical; line-height: 1.5; }
    .checkbox-row { display: flex; flex-wrap: wrap; align-items: center; gap: 12px; color: #5f6368; font-size: 12px; }
    .actions { display: flex; flex-wrap: wrap; gap: 8px; }
    .btn-primary { border: 0; border-radius: 6px; padding: 6px 14px; font-size: 13px; font-weight: 600; cursor: pointer; background: #1a73e8; color: #fff; font-family: inherit; transition: background 0.15s; }
    .btn-primary:hover { background: #1557b0; }
    .meta-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .meta-card { border-radius: 6px; border: 1px solid #e8eaed; background: #f8f9fa; padding: 9px 12px; display: grid; gap: 2px; }
    .meta-label { color: #9aa0a6; font-size: 10px; letter-spacing: 0.04em; text-transform: uppercase; font-weight: 600; }
    .meta-value { color: #1a1d23; font-size: 13px; font-weight: 600; word-break: break-word; line-height: 1.4; }
    .preview-shell { display: grid; gap: 10px; }
    .preview-toolbar { display: grid; gap: 8px; }
    .preview-form { display: grid; grid-template-columns: minmax(140px, 220px) minmax(180px, 1fr); align-items: center; gap: 8px; min-width: 0; }
    .preview-actions { grid-column: 1 / -1; display: flex; align-items: center; gap: 8px; white-space: nowrap; }
    .preview-select, .preview-input { height: 32px; border: 1px solid #dadce0; border-radius: 6px; background: #f8f9fa; padding: 0 10px; color: #1a1d23; font-size: 12px; font-family: inherit; outline: none; transition: border-color 0.15s, background 0.15s; }
    .preview-select:focus, .preview-input:focus { border-color: #1a73e8; background: #fff; }
    .preview-select, .preview-input { width: 100%; min-width: 0; }
    .preview-note, .preview-meta { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .preview-meta b { color: #1a1d23; }
    .sql-shell { display: grid; grid-template-columns: minmax(0, 1.5fr) minmax(280px, 0.9fr); gap: 12px; min-height: 0; align-items: start; }
    .sql-shell[data-layout="wide"] { grid-template-columns: minmax(0, 1.9fr) minmax(240px, 0.7fr); }
    .sql-shell[data-layout="full"] { grid-template-columns: minmax(0, 1fr); }
    .sql-shell[data-layout="full"] .sql-side { display: none; }
    .sql-main, .sql-side { min-width: 0; display: grid; gap: 10px; align-content: start; }
    .sql-editor-card, .sql-field-card, .sql-result-card { min-width: 0; max-width: 100%; border: 1px solid #e8eaed; border-radius: 8px; background: #f8f9fa; padding: 12px; display: grid; gap: 10px; overflow: hidden; }
    .sql-toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .sql-toolbar .spacer { flex: 1; }
    .sql-layout-switch { display: inline-flex; align-items: center; gap: 4px; padding: 3px; border: 1px solid #dadce0; border-radius: 999px; background: #fff; }
    .sql-layout-btn { height: 28px; padding: 0 10px; border: 0; border-radius: 999px; background: transparent; color: #5f6368; font-size: 11px; font-weight: 600; cursor: pointer; font-family: inherit; }
    .sql-layout-btn.active { background: rgba(26,115,232,0.1); color: #1a73e8; }
    .sql-limit { display: inline-flex; align-items: center; gap: 6px; color: #9aa0a6; font-size: 11px; }
    .sql-limit select { height: 30px; border: 1px solid #dadce0; border-radius: 6px; background: #fff; padding: 0 8px; color: #1a1d23; font-size: 12px; font-family: inherit; }
    .sql-editor-wrap { position: relative; display: grid; gap: 8px; min-width: 0; max-width: 100%; overflow: hidden; }
    .sql-editor { display: block; width: 100%; min-width: 0; max-width: 100%; min-height: 220px; border: 1px solid #dadce0; border-radius: 8px; background: #101318; color: #e8eaed; padding: 14px; font: 12px/1.6 "SF Mono", ui-monospace, monospace; resize: vertical; outline: none; overflow: auto; }
    .sql-editor:focus { border-color: #1a73e8; }
    .sql-editor::placeholder { color: rgba(232,234,237,0.42); }
    .sql-hint { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .sql-status { color: #9aa0a6; font-size: 11px; line-height: 1.5; min-height: 16px; }
    .sql-status.ok { color: #0d9488; }
    .sql-status.error { color: #dc2626; }
    .sql-result-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .sql-result-meta { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .sql-field-top { display: grid; gap: 8px; }
    .sql-field-search { width: 100%; height: 32px; border: 1px solid #dadce0; border-radius: 6px; background: #fff; padding: 0 10px; color: #1a1d23; font-size: 12px; font-family: inherit; outline: none; }
    .sql-field-search:focus { border-color: #1a73e8; }
    .sql-field-list { display: grid; gap: 8px; max-height: 520px; overflow: auto; padding-right: 2px; min-width: 0; }
    .sql-field-item { border: 1px solid #e8eaed; border-radius: 8px; background: #fff; padding: 9px 10px; display: grid; gap: 5px; cursor: pointer; transition: border-color 0.12s, box-shadow 0.12s; }
    .sql-field-item:hover { border-color: #1a73e8; box-shadow: 0 0 0 2px rgba(26,115,232,0.08); }
    .sql-field-row { display: flex; align-items: center; gap: 6px; min-width: 0; flex-wrap: wrap; }
    .sql-field-name { font-size: 12px; font-weight: 700; color: #1a1d23; }
    .sql-field-comment { color: #5f6368; font-size: 11px; line-height: 1.5; }
    .sql-field-empty { color: #9aa0a6; font-size: 12px; text-align: center; padding: 20px 10px; border: 1px dashed #dadce0; border-radius: 8px; background: #fff; }
    .sql-suggest { position: absolute; left: 0; top: 100%; margin-top: 4px; width: min(360px, 100%); max-height: 240px; overflow: auto; border: 1px solid #dadce0; border-radius: 8px; background: #fff; box-shadow: 0 10px 28px rgba(15,23,42,0.14); z-index: 8; }
    .sql-suggest-item { padding: 9px 10px; display: grid; gap: 3px; cursor: pointer; border-bottom: 1px solid #f1f3f4; }
    .sql-suggest-item:last-child { border-bottom: 0; }
    .sql-suggest-item:hover, .sql-suggest-item.active { background: rgba(26,115,232,0.08); }
    .sql-suggest-name { font-size: 12px; font-weight: 700; color: #1a1d23; }
    .sql-suggest-meta { color: #9aa0a6; font-size: 11px; line-height: 1.4; }
    .ddl-card { border: 1px solid #e8eaed; border-radius: 8px; background: #f8f9fa; overflow: hidden; }
    .ddl-card[open] { background: #fff; }
    .ddl-summary { list-style: none; cursor: pointer; display: flex; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 14px; }
    .ddl-summary::-webkit-details-marker { display: none; }
    .ddl-summary-main { display: grid; gap: 3px; min-width: 0; }
    .ddl-title-row { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .ddl-title { font-size: 13px; font-weight: 600; color: #1a1d23; }
    .ddl-copy { flex-shrink: 0; }
    .ddl-copy.done { color: #0d9488; border-color: rgba(13,148,136,0.24); background: rgba(13,148,136,0.08); }
    .ddl-meta { color: #9aa0a6; font-size: 11px; line-height: 1.4; }
    .ddl-body { border-top: 1px solid #e8eaed; display: grid; gap: 10px; padding: 12px 14px 14px; }
    .ddl-notice { color: #9aa0a6; font-size: 11px; line-height: 1.5; }
    .ddl-code { margin: 0; border: 1px solid #e8eaed; border-radius: 6px; background: #101318; color: #e8eaed; padding: 14px; overflow: auto; font: 12px/1.6 "SF Mono", ui-monospace, monospace; white-space: pre; }
    .table-wrap { overflow: auto; border-radius: 6px; border: 1px solid #e8eaed; }
    .columns-table-wrap { overflow: auto; }
    .columns-table th:nth-child(1), .columns-table td:nth-child(1) { min-width: 168px; }
    .columns-table th:nth-child(2), .columns-table td:nth-child(2) { min-width: 112px; }
    .columns-table th:nth-child(3), .columns-table td:nth-child(3) { min-width: 156px; }
    .columns-table th:nth-child(4), .columns-table td:nth-child(4) { min-width: 220px; }
    .columns-table th:nth-child(5), .columns-table td:nth-child(5),
    .columns-table th:nth-child(6), .columns-table td:nth-child(6),
    .columns-table th:nth-child(7), .columns-table td:nth-child(7) { min-width: 92px; }
    .columns-table th:nth-child(8), .columns-table td:nth-child(8) { min-width: 200px; }
    table { width: 100%; border-collapse: collapse; min-width: 800px; }
    th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid #e8eaed; vertical-align: top; font-size: 12px; line-height: 1.5; }
    th { position: sticky; top: 0; background: #f8f9fa; color: #9aa0a6; font-size: 11px; letter-spacing: 0.04em; text-transform: uppercase; font-weight: 600; z-index: 1; }
    tbody tr:hover td { background: rgba(26,115,232,0.025); }
    tbody tr:last-child td { border-bottom: 0; }
    .list-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 8px; }
    .list-card { border-radius: 6px; border: 1px solid #e8eaed; background: #f8f9fa; padding: 10px 12px; display: grid; gap: 6px; }
    .list-card-title { font-size: 12px; font-weight: 600; color: #1a1d23; }
    .list-card-copy { color: #5f6368; font-size: 12px; line-height: 1.5; word-break: break-word; }
    .quality-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 8px; }
    .metric-card { border-radius: 6px; border: 1px solid #e8eaed; background: #f8f9fa; padding: 10px 12px; display: grid; gap: 3px; }
    .metric-label { color: #9aa0a6; font-size: 10px; letter-spacing: 0.04em; text-transform: uppercase; font-weight: 600; }
    .metric-value { color: #1a1d23; font-size: 20px; font-weight: 700; line-height: 1; }
    .metric-note { color: #9aa0a6; font-size: 11px; line-height: 1.4; }
    .item-subtitle { color: #9aa0a6; font-size: 11px; line-height: 1.4; word-break: break-word; }
    .empty { padding: 20px; border-radius: 6px; border: 1px dashed #dadce0; background: #f8f9fa; color: #9aa0a6; font-size: 13px; text-align: center; }
    .error { padding: 8px 12px; border-radius: 6px; border: 1px solid rgba(220,38,38,0.2); background: rgba(220,38,38,0.06); color: #dc2626; font-size: 12px; }
    .hidden { display: none !important; }
    /* ── 血缘图 ── */
    .lineage-wrap { background: #fafafa; }
    .lineage-toolbar { display: flex; align-items: center; gap: 8px; padding: 8px 16px; border-bottom: 1px solid #e8eaed; background: #fff; }
    .lineage-toolbar button { height: 28px; padding: 0 10px; border: 1px solid #dadce0; border-radius: 5px; background: #f8f9fa; color: #5f6368; font-size: 12px; cursor: pointer; font-family: inherit; transition: background 0.12s; }
    .lineage-toolbar button:hover { background: #e8eaed; }
    .lineage-toolbar .l-info { color: #9aa0a6; font-size: 12px; margin-right: auto; }
    .lineage-body { display: flex; min-height: 400px; }
    .lineage-canvas { flex: 1; min-width: 0; overflow: auto; position: relative; }
    .lineage-stage { width: 100%; height: 460px; overflow: auto; position: relative; }
    .lineage-stage svg { display: block; transform-origin: top left; }
    .lineage-empty { display: flex; align-items: center; justify-content: center; height: 240px; color: #9aa0a6; font-size: 13px; }
    .lineage-loading { text-align: center; padding: 60px 0; color: #9aa0a6; font-size: 13px; }
    .l-edge { fill: none; stroke: #9aa0a6; stroke-width: 1.5; }
    .l-edge-label { font-size: 10px; fill: #9aa0a6; font-family: inherit; }
    .l-node-group { cursor: pointer; }
    .l-node-group:hover .l-node-rect { opacity: 0.85; }
    .l-node-selected .l-node-rect { stroke: #fff; stroke-width: 2.5; filter: drop-shadow(0 0 8px rgba(26,115,232,0.5)); }
    .l-node-label { font-size: 12px; font-weight: 600; fill: #fff; font-family: inherit; }
    .l-node-meta { font-size: 10px; fill: rgba(255,255,255,0.72); font-family: inherit; }
    /* ── 血缘图侧边详情栏 ── */
    .lineage-sidebar { width: 0; overflow: hidden; transition: width 0.2s; border-left: 1px solid #e8eaed; background: #fff; display: flex; flex-direction: column; }
    .lineage-sidebar.open { width: 320px; overflow-y: auto; }
    .ls-section { padding: 12px 14px; border-bottom: 1px solid #e8eaed; }
    .ls-section:last-child { border-bottom: none; flex: 1; }
    .ls-title { font-size: 11px; font-weight: 600; color: #9aa0a6; text-transform: uppercase; letter-spacing: 0.04em; margin-bottom: 8px; }
    .ls-close { float: right; background: transparent; border: 0; color: #9aa0a6; cursor: pointer; font-size: 16px; line-height: 1; padding: 0 2px; }
    .ls-close:hover { color: #1a1d23; }
    .ls-row { display: flex; font-size: 12px; padding: 3px 0; }
    .ls-row .lbl { color: #9aa0a6; width: 64px; flex-shrink: 0; }
    .ls-row .val { color: #1a1d23; word-break: break-word; }
    .ls-type-badge { display: inline-block; font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 999px; color: #fff; }
    .ls-relation { font-size: 12px; color: #5f6368; padding: 4px 0; border-bottom: 1px solid #e8eaed; display: flex; gap: 6px; align-items: center; }
    .ls-relation:last-child { border-bottom: 0; }
    .ls-conn { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
    /* ── 调度信息区 ── */
    .lineage-bottom { border-top: 1px solid #e8eaed; background: #fff; }
    .lb-tabs { display: flex; padding: 0 16px; border-bottom: 1px solid #e8eaed; background: #fafafa; }
    .lb-tab { padding: 9px 14px; font-size: 12px; font-weight: 500; color: #9aa0a6; cursor: pointer; border-bottom: 2px solid transparent; margin-bottom: -1px; user-select: none; }
    .lb-tab:hover { color: #5f6368; }
    .lb-tab.active { color: #1a73e8; border-bottom-color: #1a73e8; }
    .lb-content { padding: 12px 16px; max-height: 200px; overflow-y: auto; font-size: 12px; color: #5f6368; }
    .lb-empty { padding: 16px 0; text-align: center; color: #9aa0a6; }
    .lb-item { display: flex; gap: 8px; align-items: flex-start; padding: 6px 0; border-bottom: 1px solid #e8eaed; }
    .lb-item:last-child { border-bottom: 0; }
    .lb-item .stat { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; color: #5f6368; }
    .lb-item .stat.ok { color: #16a34a; }
    .lb-item .stat.fail { color: #dc2626; }
    .lb-item .stat.run { color: #1a73e8; }
    /* ── 字典 ── */
    .dict-cell { min-width: 130px; max-width: 220px; vertical-align: top; }
    .dict-display { cursor: pointer; padding: 2px 0; }
    .dict-display:hover .dict-hint { opacity: 1; }
    .dict-def { color: #1a1d23; line-height: 1.4; font-size: 12px; }
    .dict-enum { color: #9aa0a6; font-size: 11px; margin-top: 2px; line-height: 1.3; }
    .dict-empty { color: #9aa0a6; opacity: 0.35; font-size: 11px; }
    .dict-hint { opacity: 0; font-size: 11px; color: #1a73e8; margin-left: 4px; transition: opacity 0.15s; }
    .dict-editor { display: flex; flex-direction: column; gap: 4px; }
    .dict-editor input, .dict-editor textarea { font-size: 12px; padding: 4px 7px; border: 1px solid #1a73e8; border-radius: 5px; font-family: inherit; background: white; color: #1a1d23; width: 100%; outline: none; }
    .dict-editor textarea { resize: vertical; min-height: 32px; }
    .dict-editor-row { display: flex; gap: 4px; margin-top: 2px; }
    .dict-editor-row button { font-size: 11px; padding: 2px 9px; border-radius: 5px; cursor: pointer; border: 1px solid #dadce0; background: #f8f9fa; font-family: inherit; }
    .dict-editor-row button.save { background: #1a73e8; color: white; border-color: #1a73e8; }
    .dict-complete-badge { display: inline-flex; align-items: center; gap: 4px; font-size: 11px; color: #9aa0a6; }
    .dict-complete-badge .pct { color: #0d9488; font-weight: 600; }
    /* ── 字段徽章 ── */
    .type-badge { font-family: "SF Mono", ui-monospace, monospace; font-size: 11px; background: rgba(26,115,232,0.06); color: #1a73e8; padding: 1px 6px; border-radius: 4px; border: 1px solid rgba(26,115,232,0.12); white-space: nowrap; }
    .nullable-pill { display: inline-block; font-size: 10px; font-weight: 600; padding: 1px 6px; border-radius: 999px; white-space: nowrap; }
    .nullable-yes { background: rgba(220,38,38,0.08); color: #dc2626; border: 1px solid rgba(220,38,38,0.18); }
    .nullable-no { background: rgba(22,163,74,0.08); color: #16a34a; border: 1px solid rgba(22,163,74,0.18); }
    .miss-high { color: #dc2626; font-weight: 600; }
    .miss-mid { color: #ea580c; font-weight: 500; }
    .col-name-wrap { display: flex; align-items: center; gap: 4px; }
    .pk-badge { font-size: 10px; font-weight: 700; color: #b45309; background: #fffbeb; border: 1px solid #fde68a; padding: 1px 5px; border-radius: 4px; }
    .fk-badge { font-size: 10px; font-weight: 700; color: #1a73e8; background: rgba(26,115,232,0.06); border: 1px solid rgba(26,115,232,0.15); padding: 1px 5px; border-radius: 4px; }
    @media (max-width: 1100px) {
      .catalog-body { grid-template-columns: var(--w-results) 8px minmax(0, 1fr); }
      .facets, .splitter[data-resize="facets"] { display: none; }
      .results-col { grid-column: 1; }
      .splitter[data-resize="results"] { grid-column: 2; }
      .detail-col { grid-column: 3; }
      .sql-shell { grid-template-columns: 1fr; }
      .sql-field-list { max-height: 260px; }
    }
    @container (max-width: 520px) {
      .preview-form { grid-template-columns: 1fr; }
      .preview-actions { justify-content: flex-start; }
    }
  </style>
</head>
<body>
  <div class="page">

    <!-- ── 顶部导航 ── -->
    <header class="topbar">
      <div class="topbar-brand">
        <div class="dot"></div>
        数据智知中心
        <span class="topbar-sub">Data Map · Dictionary · Lineage</span>
      </div>
      <div class="topbar-actions">
        <span id="ovTables" class="ov-stat"><span class="ov-n">—</span> 张表</span>
        <span id="ovColumns" class="ov-stat"><span class="ov-n">—</span> 个字段</span>
        <button type="button" class="topbar-btn" id="openDatasets">数据集</button>
        <button type="button" class="topbar-btn" id="openDashboards">看板</button>
        <button type="button" class="topbar-btn" id="refreshCatalog">刷新目录</button>
      </div>
    </header>

    <!-- ── 全局搜索 ── -->
    <div class="search-bar">
      <form id="searchForm" class="search-form">
        <input id="keyword" name="keyword" placeholder="搜索表名 / 字段名 / 业务术语 / 负责人…" class="search-input" autocomplete="off" />
        <button type="submit" class="search-btn">搜索</button>
        <button type="button" class="search-btn ghost" id="clearFilters">重置</button>
      </form>
      <!-- 兼容隐藏输入：供脚本读取，避免空引用 -->
      <div hidden>
        <input id="exactMatch" type="checkbox" />
        <input id="favoritesOnly" type="checkbox" />
        <select id="ownerFilter"></select>
        <select id="domainFilter"></select>
        <select id="projectFilter"></select>
        <select id="storageFilter"></select>
        <select id="sourceTypeFilter"></select>
        <select id="limitFilter"><option value="1000" selected>1000</option></select>
      </div>
    </div>

    <!-- ── 三栏数据目录 ── -->
    <div class="catalog-body">

      <!-- 左：分面导航 -->
      <aside class="facets" id="facets">
        <div class="facet-head">
          <span class="ft">筛选</span>
          <button type="button" class="facet-clear" id="facetClear" disabled>清空</button>
        </div>
        <div id="facetGroups"></div>
      </aside>

      <!-- 分隔条：分面 ↔ 列表 -->
      <div class="splitter" data-resize="facets" title="拖拽调整宽度，双击重置">
        <span class="sp-grip"></span>
      </div>

      <!-- 中：结果列表 -->
      <main class="results-col">
        <div class="results-bar">
          <span class="rcount" id="resultSummary">共 <b>0</b> 张表</span>
          <span class="rspace"></span>
          <label class="fav-toggle"><input type="checkbox" id="favToggle" /> 只看收藏</label>
          <label class="sortlbl">排序</label>
          <select id="sortSelect">
            <option value="relevance">默认</option>
            <option value="hot">热度</option>
            <option value="size">物理大小</option>
            <option value="columns">字段数</option>
            <option value="rows">估算行数</option>
            <option value="name">表名</option>
          </select>
          <button type="button" class="ghost-sm" id="copyCurrentLink">复制链接</button>
        </div>
        <div id="filterCrumbs" class="filter-crumbs hidden"></div>
        <div id="searchError" class="error hidden" style="margin:10px 16px 0"></div>
        <div class="results-scroll">
          <div id="resultsList"></div>
        </div>
      </main>

      <!-- 分隔条：列表 ↔ 详情 -->
      <div class="splitter" data-resize="results" title="拖拽调整宽度，双击重置">
        <span class="sp-grip"></span>
      </div>

      <!-- 右：详情预览（常驻） -->
      <aside class="detail-col">
        <div id="detailEmpty" class="detail-empty">
          <div class="de-ico">🗂️</div>
          <div>从左侧选择一张表<br/>查看结构、血缘与业务元数据</div>
        </div>
        <div id="detailPanel" class="detail-panel hidden">
          <div class="detail-panel-header">
            <div id="detailHero" class="detail-hero"></div>
            <div style="display:flex;align-items:flex-start;justify-content:space-between;gap:10px">
              <div style="min-width:0">
                <div class="detail-title-row">
                  <div id="detailTitle" class="detail-title">—</div>
                  <button type="button" class="detail-ddl-toggle hidden" id="detailDdlShortcut">DDL</button>
                </div>
                <div id="detailSubtitle" class="detail-subtitle"></div>
                <div id="detailDdlInline" class="detail-ddl-inline hidden"></div>
              </div>
              <button type="button" class="favorite-btn" id="favoriteToggle">☆ 收藏</button>
            </div>
          </div>
          <div id="detailStats" class="stat-grid"></div>

          <!-- Tab 切换 -->
          <div class="detail-tabs" id="detailTabs">
            <div class="detail-tab active" data-tab="structure">表结构</div>
            <div class="detail-tab" data-tab="sql">SQL 试跑</div>
            <div class="detail-tab" data-tab="lineage">血缘图</div>
          </div>

          <div id="detailContent" class="detail-content"></div>
        </div>
      </aside>

      <!-- 浮动折叠按钮（JS 定位，跟着各自面板边走） -->
      <button type="button" class="col-toggle" data-collapse="facets" aria-label="收起/展开 筛选栏" title="收起/展开 筛选栏">
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="10 4 6 8 10 12"></polyline></svg>
      </button>
      <button type="button" class="col-toggle" data-collapse="detail" aria-label="收起/展开 详情栏" title="收起/展开 详情栏">
        <svg viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="6 4 10 8 6 12"></polyline></svg>
      </button>

    </div>

  </div>

  <script>
    const params = new URLSearchParams(window.location.search);
    const searchForm = document.getElementById('searchForm');
    const keywordInput = document.getElementById('keyword');
    const exactMatchInput = document.getElementById('exactMatch');
    const favoritesOnlyInput = document.getElementById('favoritesOnly');
    const ownerFilter = document.getElementById('ownerFilter');
    const domainFilter = document.getElementById('domainFilter');
    const projectFilter = document.getElementById('projectFilter');
    const storageFilter = document.getElementById('storageFilter');
    const sourceTypeFilter = document.getElementById('sourceTypeFilter');
    const limitFilter = document.getElementById('limitFilter');
    const resultSummary = document.getElementById('resultSummary');
    const resultsList = document.getElementById('resultsList');
    const searchError = document.getElementById('searchError');
    const resultsMeta = document.getElementById('resultsMeta');
    const detailPanel = document.getElementById('detailPanel');
    const detailTitle = document.getElementById('detailTitle');
    const detailSubtitle = document.getElementById('detailSubtitle');
    const detailStats = document.getElementById('detailStats');
    const detailContent = document.getElementById('detailContent');
    const favoriteToggle = document.getElementById('favoriteToggle');
    const detailDdlShortcut = document.getElementById('detailDdlShortcut');
    const detailDdlInline = document.getElementById('detailDdlInline');
    const refreshCatalogButton = document.getElementById('refreshCatalog');
    const clearFiltersButton = document.getElementById('clearFilters');
    const copyCurrentLinkButton = document.getElementById('copyCurrentLink');
    const backBtn = document.getElementById('backBtn');
    const toggleFiltersBtn = document.getElementById('toggleFilters');
    const filterPanel = document.getElementById('filterPanel');
    const facetGroupsEl = document.getElementById('facetGroups');
    const facetClearBtn = document.getElementById('facetClear');
    const sortSelect = document.getElementById('sortSelect');
    const favToggle = document.getElementById('favToggle');
    const detailEmpty = document.getElementById('detailEmpty');
    const detailHero = document.getElementById('detailHero');

    // 数仓分层定义（用于分面与徽章配色）
    const TIER_DEFS = [
      { key: 'ODS', name: 'ODS 贴源层', color: '#64748b' },
      { key: 'DWD', name: 'DWD 明细层', color: '#1a73e8' },
      { key: 'DWS', name: 'DWS 汇总层', color: '#0d9488' },
      { key: 'DIM', name: 'DIM 维度层', color: '#7c3aed' },
      { key: 'ADS', name: 'ADS 应用层', color: '#ea580c' },
      { key: 'TMP', name: 'TMP 临时层', color: '#9aa0a6' },
    ];
    function tierOf(item) {
      const c = String(item.table_comment || item.description || '').toUpperCase().trim();
      const n = String(item.table_name || '').toUpperCase();
      for (const t of TIER_DEFS) {
        if (c.startsWith(t.key) || n.startsWith(t.key + '_') || n.startsWith(t.key)) return t.key;
      }
      return '';
    }
    function tierColor(key) { const t = TIER_DEFS.find((x) => x.key === key); return t ? t.color : '#9aa0a6'; }
    function tierName(key) { const t = TIER_DEFS.find((x) => x.key === key); return t ? t.name : '未分层'; }

    const state = {
      dashboard: null,
      results: [],
      allResults: [],
      selectedKey: '',
      profile: null,
      filters: null,
      previewRequestId: 0,
      sqlRequestId: 0,
    };
    let activeDetailTab = 'structure';
    let sqlOutsideClickBound = false;
    const SQL_LAYOUTS = ['split', 'wide', 'full'];

    // 分面选中态：每个维度一个 Set（组内 OR，组间 AND）
    const facetState = { tier: new Set(), database: new Set(), owner: new Set(), source_type: new Set(), business_domain: new Set(), storage_type: new Set() };
    function facetActiveCount() { return Object.values(facetState).reduce((s, set) => s + set.size, 0); }

    function escapeHtml(text) {
      return String(text == null ? '' : text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function truncateMiddle(text, maxLength = 60) {
      const value = String(text || '');
      if (!value || value.length <= maxLength) return value;
      const head = Math.max(16, Math.floor((maxLength - 1) * 0.6));
      const tail = Math.max(10, maxLength - head - 1);
      return `${value.slice(0, head)}…${value.slice(-tail)}`;
    }

    function renderEmpty(text) {
      return `<div class="empty">${escapeHtml(text)}</div>`;
    }

    function withoutPipelineTasks(tasks) {
      return Array.isArray(tasks) ? tasks.filter(task => task?.resource_type !== 'PIPELINE') : [];
    }

    function renderChip(text, kind = '') {
      return `<span class="chip ${kind}">${escapeHtml(text)}</span>`;
    }

    // ══════════════════════════════════════════════════

    async function fetchJson(url, options = {}) {
      const response = await fetch(url, options);
      const payload = await response.json();
      if (!response.ok) {
        throw new Error(payload.error || '请求失败');
      }
      return payload;
    }

    function buildSearchParams() {
      const search = new URLSearchParams();
      const keyword = keywordInput.value.trim();
      if (keyword) search.set('keyword', keyword);
      if (favToggle && favToggle.checked) search.set('favorites_only', 'true');
      // 一次性取回全量匹配，分面/排序在前端完成
      search.set('limit', '1000');
      return search;
    }

    function buildPageParams() {
      const next = buildSearchParams();
      if (state.selectedKey) {
        const [schema, table] = state.selectedKey.split('.');
        next.set('schema', schema);
        next.set('table', table);
      }
      return next;
    }

    function syncPageUrl() {
      const next = buildPageParams();
      const url = `${window.location.pathname}?${next.toString()}`;
      window.history.replaceState({}, '', url);
    }

    function initFormFromParams() {
      keywordInput.value = params.get('keyword') || '';
      if (favToggle) favToggle.checked = params.get('favorites_only') === 'true';
    }

    function setSelectOptions(selectEl, values, selectedValue) {
      if (!selectEl) return;
      const baseOption = '<option value="">全部</option>';
      selectEl.innerHTML = baseOption + (values || []).map((item) => `<option value="${escapeHtml(item)}">${escapeHtml(item)}</option>`).join('');
      selectEl.value = selectedValue || '';
    }

    function applyFilters(filters) {
      state.filters = filters || state.filters;
    }

    function renderMiniList(container, items, emptyText) {
      if (!items || !items.length) {
        container.innerHTML = renderEmpty(emptyText);
        return;
      }
      container.innerHTML = items.map((item) => `
        <button type="button" class="mini-item" data-key="${escapeHtml(item.qualified_name)}" style="text-align:left; border:1px solid rgba(221,212,197,0.92); cursor:pointer;">
          <div class="item-title">${escapeHtml(item.table_name)}</div>
          <div class="item-subtitle">${escapeHtml(item.qualified_name)}</div>
          <div class="chip-row">
            ${item.favorite ? renderChip('已收藏') : ''}
            ${item.business_domain ? renderChip(item.business_domain) : ''}
            ${item.owner ? renderChip(item.owner) : ''}
          </div>
        </button>
      `).join('');
      container.querySelectorAll('[data-key]').forEach((node) => {
        node.addEventListener('click', () => {
          selectTableByKey(node.dataset.key);
        });
      });
    }

    function renderDashboard(dashboard) {
      state.dashboard = dashboard;
      const summary = dashboard.summary || {};
      const ovTables = document.getElementById('ovTables');
      const ovColumns = document.getElementById('ovColumns');
      if (ovTables) ovTables.querySelector('.ov-n').textContent = summary.table_count || 0;
      if (ovColumns) ovColumns.querySelector('.ov-n').textContent = summary.column_count || 0;
      if (!state.filters) applyFilters(dashboard.filters || {});
    }

    function fmtInt(n) {
      const v = Number(n) || 0;
      if (v >= 10000) return (v / 10000).toFixed(v >= 100000 ? 0 : 1) + '万';
      return v.toLocaleString('en-US');
    }

    function renderResults(payload) {
      state.allResults = payload.items || [];
      searchError.classList.add('hidden');
      searchError.textContent = '';
      applyFilters(payload.filters || {});
      computeAndRenderFacets();
      applyFacetsAndRender();

      // 自动选中 URL 参数里的表
      const requestedKey = params.get('schema') && params.get('table')
        ? `${params.get('schema')}.${params.get('table')}` : '';
      if (!state.selectedKey && requestedKey && state.allResults.some((i) => i.qualified_name === requestedKey)) {
        selectTableByKey(requestedKey);
      }
    }

    // ── 分面计算与渲染 ──
    function countBy(rows, keyFn) {
      const m = new Map();
      rows.forEach((r) => { const k = keyFn(r); if (k === '' || k == null) return; m.set(k, (m.get(k) || 0) + 1); });
      return m;
    }

    function computeAndRenderFacets() {
      const rows = state.allResults;
      const groups = [];
      // 数仓分层（按 TIER_DEFS 顺序）
      const tierCount = countBy(rows, (r) => tierOf(r));
      const untiered = rows.filter((r) => !tierOf(r)).length;
      const tierItems = TIER_DEFS.filter((t) => tierCount.get(t.key)).map((t) => ({ value: t.key, label: t.key, sub: t.name.replace(t.key + ' ', ''), count: tierCount.get(t.key), color: t.color }));
      if (untiered) tierItems.push({ value: '__none__', label: '未分层', sub: '', count: untiered, color: '#cbd5e1' });
      if (tierItems.length >= 2) groups.push({ dim: 'tier', title: '数仓分层', items: tierItems, tier: true });
      // 负责人
      const ownerItems = [...countBy(rows, (r) => r.owner)].sort((a, b) => b[1] - a[1]).map(([v, c]) => ({ value: v, label: v, count: c }));
      if (ownerItems.length >= 2) groups.push({ dim: 'owner', title: '负责人', items: ownerItems });
      // 数据源
      const srcItems = [...countBy(rows, (r) => r.source_type)].sort((a, b) => b[1] - a[1]).map(([v, c]) => ({ value: v, label: v, count: c }));
      if (srcItems.length >= 2) groups.push({ dim: 'source_type', title: '数据源', items: srcItems });
      // 库（数据库）
      const dbItems = [...countBy(rows, (r) => r.database)].sort((a, b) => b[1] - a[1]).map(([v, c]) => ({ value: v, label: v, count: c }));
      if (dbItems.length >= 2) groups.push({ dim: 'database', title: '库', items: dbItems });
      // 业务域
      const domItems = [...countBy(rows, (r) => r.business_domain)].sort((a, b) => b[1] - a[1]).map(([v, c]) => ({ value: v, label: v, count: c }));
      if (domItems.length >= 2) groups.push({ dim: 'business_domain', title: '业务域', items: domItems });
      // 存储类型
      const stItems = [...countBy(rows, (r) => r.storage_type)].sort((a, b) => b[1] - a[1]).map(([v, c]) => ({ value: v, label: v, count: c }));
      if (stItems.length >= 2) groups.push({ dim: 'storage_type', title: '存储类型', items: stItems });

      facetGroupsEl.innerHTML = groups.map((g) => `
        <div class="facet-group">
          <div class="facet-title">${escapeHtml(g.title)}</div>
          ${g.items.map((it) => {
            const on = facetState[g.dim].has(it.value);
            return `<div class="facet-item ${on ? 'on' : ''}" data-dim="${g.dim}" data-value="${escapeHtml(it.value)}">
              <span class="fname">${g.tier ? `<span class="tier-dot" style="background:${it.color}"></span>` : ''}${escapeHtml(it.label)}${it.sub ? `<span style="color:#9aa0a6;font-weight:400">·${escapeHtml(it.sub)}</span>` : ''}</span>
              <span class="fcount">${it.count}</span>
            </div>`;
          }).join('')}
        </div>
      `).join('') || '<div class="facet-group"><div class="facet-title" style="color:#c9ccd1">暂无可筛选维度</div></div>';

      facetGroupsEl.querySelectorAll('.facet-item').forEach((node) => {
        node.addEventListener('click', () => {
          const dim = node.dataset.dim, val = node.dataset.value;
          const set = facetState[dim];
          if (set.has(val)) set.delete(val); else set.add(val);
          computeAndRenderFacets();
          applyFacetsAndRender();
        });
      });
      if (facetClearBtn) facetClearBtn.disabled = facetActiveCount() === 0;
    }

    function getFilteredResults() {
      return state.allResults.filter((r) => {
        for (const dim of Object.keys(facetState)) {
          const set = facetState[dim];
          if (!set.size) continue;
          let v;
          if (dim === 'tier') { const t = tierOf(r); v = t || '__none__'; }
          else v = r[dim] || '';
          if (!set.has(v)) return false;
        }
        return true;
      });
    }

    function sortResults(rows) {
      const mode = sortSelect ? sortSelect.value : 'relevance';
      const arr = rows.slice();
      const cmp = {
        hot: (a, b) => (b.view_count || 0) - (a.view_count || 0),
        size: (a, b) => (b.total_size_bytes || 0) - (a.total_size_bytes || 0),
        columns: (a, b) => (b.column_count || 0) - (a.column_count || 0),
        rows: (a, b) => (b.estimated_rows || 0) - (a.estimated_rows || 0),
        name: (a, b) => String(a.table_name).localeCompare(String(b.table_name), 'zh-CN'),
      }[mode];
      if (cmp) arr.sort(cmp);
      return arr;
    }

    const PAGE_SIZE = 50;
    let renderLimit = PAGE_SIZE;

    function applyFacetsAndRender() {
      state.results = sortResults(getFilteredResults());
      renderLimit = PAGE_SIZE;            // 每次筛选/排序/检索都重置分页
      const total = state.allResults.length;
      resultSummary.innerHTML = state.results.length === total
        ? `共 <b>${total}</b> 张表`
        : `<b>${state.results.length}</b> / ${total} 张表`;
      renderFilterCrumbs();
      renderRows();
    }

    // 当前生效筛选条件面包屑
    const FACET_TITLES = { tier: '分层', database: '库', owner: '负责人', source_type: '数据源', business_domain: '业务域', storage_type: '存储' };
    function renderFilterCrumbs() {
      const el = document.getElementById('filterCrumbs');
      if (!el) return;
      const parts = [];
      const kw = keywordInput.value.trim();
      if (kw) parts.push({ kind: 'keyword', label: '关键词', value: kw });
      if (favToggle && favToggle.checked) parts.push({ kind: 'favorite' });
      Object.keys(facetState).forEach((dim) => {
        facetState[dim].forEach((v) => parts.push({ kind: 'facet', dim, value: v }));
      });
      if (!parts.length) { el.classList.add('hidden'); el.innerHTML = ''; return; }
      el.classList.remove('hidden');
      el.innerHTML = '<span class="label">当前筛选</span>' +
        parts.map((p, i) => {
          if (p.kind === 'keyword') return `<span class="crumb">关键词:<b>${escapeHtml(p.value)}</b><span class="x" data-act="kw">×</span></span>`;
          if (p.kind === 'favorite') return `<span class="crumb">仅看收藏<span class="x" data-act="fav">×</span></span>`;
          const title = FACET_TITLES[p.dim] || p.dim;
          const lbl = p.value === '__none__' ? '未分层' : p.value;
          return `<span class="crumb">${escapeHtml(title)}:<b>${escapeHtml(lbl)}</b><span class="x" data-act="facet" data-dim="${escapeHtml(p.dim)}" data-val="${escapeHtml(p.value)}">×</span></span>`;
        }).join('') +
        '<span class="clear-all" data-act="clear">全部清除</span>';
      el.querySelectorAll('[data-act]').forEach((node) => {
        node.addEventListener('click', () => {
          const act = node.dataset.act;
          if (act === 'kw') { keywordInput.value = ''; searchTables(); return; }
          if (act === 'fav') { favToggle.checked = false; searchTables(); return; }
          if (act === 'facet') { facetState[node.dataset.dim].delete(node.dataset.val); computeAndRenderFacets(); applyFacetsAndRender(); return; }
          if (act === 'clear') {
            keywordInput.value = '';
            if (favToggle) favToggle.checked = false;
            Object.values(facetState).forEach((set) => set.clear());
            searchTables();
          }
        });
      });
    }

    function renderRows() {
      const filtered = state.results;
      if (!filtered.length) {
        resultsList.innerHTML = renderEmpty('没有匹配的表，试试调整筛选或关键词');
        return;
      }
      const shown = filtered.slice(0, renderLimit);
      const rowsHtml = shown.map((item) => {
        const tier = tierOf(item);
        const color = tier ? tierColor(tier) : '#e0e3e7';
        const desc = item.table_comment || item.description || '';
        const out = item.foreign_key_outgoing_count || 0;
        const inc = item.foreign_key_incoming_count || 0;
        return `
        <div class="result-row ${item.qualified_name === state.selectedKey ? 'active' : ''}" data-key="${escapeHtml(item.qualified_name)}">
          <div class="rr-bar" style="background:${color}"></div>
          <div class="rr-body">
            <div class="rr-top">
              ${tier ? `<span class="rr-tier" style="background:${tierColor(tier)}">${tier}</span>` : ''}
              <span class="rr-name">${escapeHtml(item.table_name)}</span>
              <span class="rr-qname">${escapeHtml(item.qualified_name)}</span>
              <button type="button" class="rr-fav ${item.favorite ? 'is-favorite' : ''}" data-favorite-key="${escapeHtml(item.qualified_name)}" title="收藏">${item.favorite ? '★' : '☆'}</button>
            </div>
            ${desc ? `<div class="rr-desc">${escapeHtml(desc)}</div>` : ''}
            <div class="rr-meta">
              ${item.owner ? `<span class="m">👤 <b>${escapeHtml(item.owner)}</b></span>` : ''}
              <span class="m"><b>${escapeHtml(item.column_count || 0)}</b> 字段</span>
              ${item.estimated_rows > 0 ? `<span class="m"><b>${fmtInt(item.estimated_rows)}</b> 行</span>` : ''}
              ${item.total_size_pretty ? `<span class="m"><b>${escapeHtml(item.total_size_pretty)}</b></span>` : ''}
              ${item.view_count ? `<span class="m hot">🔥 <b>${escapeHtml(item.view_count)}</b></span>` : ''}
              ${(out + inc) ? `<span class="m" title="外键出向/入向">🔗 ${out ? `↑${out}` : ''}${inc ? ` ↓${inc}` : ''}</span>` : ''}
              ${keywordInput.value.trim() ? (item.match_reasons || []).map((r) => `<span class="chip match">${escapeHtml(r)}</span>`).join('') : ''}
            </div>
          </div>
        </div>`;
      }).join('');
      const footer = filtered.length > shown.length
        ? `<button type="button" class="load-more" id="loadMore">加载更多（已显示 ${shown.length} / ${filtered.length}）</button>`
        : (filtered.length > PAGE_SIZE ? `<div class="list-end">已全部显示 ${filtered.length} 张</div>` : '');
      resultsList.innerHTML = rowsHtml + footer;

      resultsList.querySelectorAll('.result-row').forEach((node) => {
        node.addEventListener('click', (event) => {
          if (event.target.closest('[data-favorite-key]')) return;
          selectTableByKey(node.dataset.key);
        });
      });
      resultsList.querySelectorAll('[data-favorite-key]').forEach((node) => {
        node.addEventListener('click', async (event) => {
          event.stopPropagation();
          await toggleFavoriteByKey(node.dataset.favoriteKey);
        });
      });
      const moreBtn = document.getElementById('loadMore');
      if (moreBtn) moreBtn.addEventListener('click', () => { renderLimit += PAGE_SIZE; renderRows(); });
    }

    function markActiveResult(key) {
      resultsList.querySelectorAll('.result-row').forEach((node) => {
        node.classList.toggle('active', node.dataset.key === key);
      });
    }

    // 详情接口返回的最新 view_count 回写到列表，使两处数字保持一致
    function syncRowViewCount(qname, viewCount) {
      if (!qname || viewCount == null) return;
      const v = Number(viewCount);
      // 1) 更新内存数据
      [state.allResults, state.results].forEach((arr) => {
        const item = (arr || []).find((r) => r.qualified_name === qname);
        if (item) item.view_count = v;
      });
      // 2) 更新已渲染卡片里的 🔥 数字（不重渲整页，不抢焦点）
      const row = resultsList.querySelector(`.result-row[data-key="${CSS.escape(qname)}"]`);
      if (row) {
        const meta = row.querySelector('.rr-meta');
        if (!meta) return;
        let hot = meta.querySelector('.m.hot');
        if (v > 0) {
          if (hot) {
            const b = hot.querySelector('b');
            if (b) b.textContent = String(v);
          } else {
            const span = document.createElement('span');
            span.className = 'm hot';
            span.innerHTML = `🔥 <b>${v}</b>`;
            meta.appendChild(span);
          }
        } else if (hot) {
          hot.remove();
        }
      }
    }

    function renderProfilePlaceholder() {
      detailPanel.classList.add('hidden');
      if (detailEmpty) detailEmpty.style.display = '';
    }

    function renderMetaCards(items) {
      const valid = (items || []).filter((item) => item && item.value !== undefined && item.value !== null && String(item.value).trim() !== '');
      if (!valid.length) return renderEmpty('暂无元数据');
      return `
        <div class="meta-grid">
          ${valid.map((item) => `
            <div class="meta-card">
              <div class="meta-label">${escapeHtml(item.label)}</div>
              <div class="meta-value">${escapeHtml(item.value)}</div>
            </div>
          `).join('')}
        </div>
      `;
    }

    function renderKeyList(title, items, formatter, emptyText) {
      return `
        <div class="list-card">
          <div class="list-card-title">${escapeHtml(title)}</div>
          ${(items || []).length ? items.map(formatter).join('') : `<div class="list-card-copy">${escapeHtml(emptyText)}</div>`}
        </div>
      `;
    }

    function renderPreviewMeta(preview) {
      const rows = preview.rows || [];
      const filter = preview.filter || {};
      const applied = !!filter.applied;
      const scope = filter.column ? `字段 <b>${escapeHtml(filter.column)}</b>` : '全部字段';
      return applied
        ? `<div class="preview-meta">当前按 ${scope} 筛选：<b>${escapeHtml(filter.keyword || '')}</b>，返回 <b>${rows.length}</b> 行样本</div>`
        : `<div class="preview-meta">展示最近取回的 <b>${rows.length}</b> 行样本；可按单列或全字段关键词筛选</div>`;
    }

    function renderPreviewTable(preview) {
      if (preview.error) return `<div class="error">${escapeHtml(preview.error)}</div>`;
      const rows = preview.rows || [];
      const filter = preview.filter || {};
      if (!rows.length) return renderEmpty(filter.applied ? '没有命中筛选条件的样本数据' : '没有可预览的样本数据');
      const columns = Object.keys(rows[0] || {});
      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('')}</tr>
            </thead>
            <tbody>
              ${rows.map((row) => `
                <tr>
                  ${columns.map((column) => `<td>${escapeHtml(truncateMiddle(row[column], 120))}</td>`).join('')}
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderPreviewSection(preview, columns) {
      const rows = preview?.rows || [];
      const filter = preview?.filter || {};
      const options = ['<option value="">全部字段</option>'].concat(
        (columns || []).map((column) => `<option value="${escapeHtml(column)}" ${filter.column === column ? 'selected' : ''}>${escapeHtml(column)}</option>`)
      ).join('');
      return `
        <div class="preview-shell">
          <div class="preview-toolbar">
            <form class="preview-form" id="previewFilterForm">
              <select id="previewFilterColumn" class="preview-select">${options}</select>
              <input id="previewFilterKeyword" class="preview-input" type="text" value="${escapeHtml(filter.keyword || '')}" placeholder="输入关键词，支持模糊匹配" />
              <div class="preview-actions">
                <button type="submit" class="btn-primary">筛选</button>
                <button type="button" class="ghost-sm" id="previewFilterReset">清空</button>
              </div>
            </form>
            <div class="preview-note">样本上限 ${escapeHtml(preview?.sample_limit || 20)} 行</div>
          </div>
          ${renderPreviewMeta(preview || {})}
          <div id="previewTableWrap">${renderPreviewTable(preview || {})}</div>
        </div>
      `;
    }

    function defaultSqlForTable(qualifiedName, sampleLimit = 20) {
      return `SELECT\n  *\nFROM ${qualifiedName}\nLIMIT ${sampleLimit}`;
    }

    function renderSqlFieldList(columns, keyword = '') {
      const rows = Array.isArray(columns) ? columns : [];
      const needle = String(keyword || '').trim().toLowerCase();
      const filtered = !needle
        ? rows
        : rows.filter((column) => {
            const hay = [
              column.column_name,
              prettyType(column.data_type, column.udt_name),
              column.column_comment,
              column.business_def,
              column.enum_values,
            ].join(' ').toLowerCase();
            return hay.includes(needle);
          });
      if (!filtered.length) return '<div class="sql-field-empty">没有匹配的字段</div>';
      return filtered.map((column) => `
        <button type="button" class="sql-field-item" data-insert-column="${escapeHtml(column.column_name)}">
          <div class="sql-field-row">
            <span class="sql-field-name">${escapeHtml(column.column_name)}</span>
            <code class="type-badge">${escapeHtml(prettyType(column.data_type, column.udt_name))}</code>
            ${column.is_nullable ? '<span class="nullable-pill nullable-yes">可空</span>' : '<span class="nullable-pill nullable-no">非空</span>'}
          </div>
          <div class="sql-field-comment">${escapeHtml(column.column_comment || column.business_def || '暂无注释')}</div>
        </button>
      `).join('');
    }

    function renderSqlResultTable(result) {
      if (!result) return '<div class="sql-field-empty">执行后在这里查看结果</div>';
      const rows = result.rows || [];
      const columns = result.columns || [];
      if (!rows.length) return '<div class="sql-field-empty">查询执行成功，但没有返回数据</div>';
      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join('')}</tr>
            </thead>
            <tbody>
              ${rows.map((row) => `
                <tr>
                  ${columns.map((column) => `<td>${escapeHtml(truncateMiddle(row[column], 160))}</td>`).join('')}
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderSqlSection(profile) {
      const table = profile?.table || {};
      const structure = profile?.structure || {};
      const columns = structure.columns || [];
      const qname = table.qualified_name || '';
      const sqlText = profile?.sql?.text || defaultSqlForTable(qname, 20);
      const rowLimit = profile?.sql?.row_limit || 200;
      const result = profile?.sql?.result || null;
      const fieldKeyword = profile?.sql?.fieldKeyword || '';
      const layout = SQL_LAYOUTS.includes(profile?.sql?.layout) ? profile.sql.layout : 'split';
      return `
        <div class="tab-pane" id="tabSql">
          <div class="sql-shell" id="sqlShell" data-layout="${escapeHtml(layout)}">
            <div class="sql-main">
              <section class="sql-editor-card">
                <div class="section-head">
                  <div>
                    <div class="section-title">SQL 试跑</div>
                    <div class="section-copy">当前只支持 SELECT / WITH，只读执行；未写 LIMIT 时会自动补上。</div>
                  </div>
                </div>
                <div class="sql-toolbar">
                  <button type="button" class="btn-primary" id="runSqlBtn">执行</button>
                  <button type="button" class="ghost-sm" id="resetSqlBtn">重置</button>
                  <button type="button" class="ghost-sm" id="openSqlWorkbenchBtn">在工作台打开</button>
                  <div class="spacer"></div>
                  <div class="sql-layout-switch" id="sqlLayoutSwitch">
                    <button type="button" class="sql-layout-btn ${layout === 'split' ? 'active' : ''}" data-sql-layout="split">标准</button>
                    <button type="button" class="sql-layout-btn ${layout === 'wide' ? 'active' : ''}" data-sql-layout="wide">加宽</button>
                    <button type="button" class="sql-layout-btn ${layout === 'full' ? 'active' : ''}" data-sql-layout="full">全宽</button>
                  </div>
                  <label class="sql-limit">结果上限
                    <select id="sqlRowLimit">
                      ${[20, 50, 100, 200].map((value) => `<option value="${value}" ${Number(rowLimit) === value ? 'selected' : ''}>${value}</option>`).join('')}
                    </select>
                  </label>
                </div>
                <div class="sql-editor-wrap">
                  <textarea id="sqlEditor" class="sql-editor" spellcheck="false" placeholder="请输入 SELECT 或 WITH 查询">${escapeHtml(sqlText)}</textarea>
                  <div id="sqlSuggest" class="sql-suggest hidden"></div>
                </div>
                <div class="sql-hint">当前表：<b>${escapeHtml(qname)}</b>。字段面板在右侧，点击即可插入。</div>
                <div id="sqlStatus" class="sql-status"></div>
              </section>

              <section class="sql-result-card">
                <div class="sql-result-head">
                  <div>
                    <div class="section-title">执行结果</div>
                    <div class="sql-result-meta">${result ? `返回 <b>${escapeHtml(result.row_count || 0)}</b> 行，方言 <b>${escapeHtml(result.dialect || '')}</b>` : '尚未执行'}</div>
                  </div>
                </div>
                <div id="sqlResultWrap">${renderSqlResultTable(result)}</div>
              </section>
            </div>

            <aside class="sql-side">
              <section class="sql-field-card">
                <div class="sql-field-top">
                  <div>
                    <div class="section-title">字段面板</div>
                    <div class="section-copy">搜索字段、查看类型和注释，点击即可插入到当前光标位置。</div>
                  </div>
                  <input id="sqlFieldSearch" class="sql-field-search" type="text" value="${escapeHtml(fieldKeyword)}" placeholder="搜索字段 / 注释 / 业务定义" />
                </div>
                <div id="sqlFieldList" class="sql-field-list">${renderSqlFieldList(columns, fieldKeyword)}</div>
              </section>
            </aside>
          </div>
        </div>
      `;
    }

    function getDdlMeta(ddl) {
      const sourceMap = { native: '原始 DDL', generated: '生成 DDL' };
      const dialectMap = { mysql: 'MySQL', postgresql: 'PostgreSQL' };
      return {
        sourceLabel: sourceMap[ddl?.source] || ddl?.source || 'DDL',
        dialectLabel: dialectMap[ddl?.dialect] || ddl?.dialect || '',
      };
    }

    function renderHeaderDdlInline(ddl) {
      if (!ddl || !ddl.text) return '';
      const { sourceLabel, dialectLabel } = getDdlMeta(ddl);
      return `
        <div class="detail-ddl-inline-head">
          <div class="detail-ddl-inline-main">
            <div class="detail-ddl-inline-title">表定义</div>
            <div class="detail-ddl-inline-meta">
              ${dialectLabel ? `<span class="chip">${escapeHtml(dialectLabel)}</span>` : ''}
              ${sourceLabel ? `<span class="chip">${escapeHtml(sourceLabel)}</span>` : ''}
            </div>
            <div class="detail-ddl-inline-note">${escapeHtml(ddl.notice || '直接在表头查看建表语句，方便核对字段定义、主键和注释。')}</div>
          </div>
          <button type="button" class="ghost-sm ddl-copy" data-copy-ddl data-copy-label="复制 DDL">复制 DDL</button>
        </div>
        <pre class="ddl-code"><code>${escapeHtml(ddl.text)}</code></pre>
      `;
    }

    function renderDdlSection(ddl) {
      if (!ddl || !ddl.text) return '';
      const { sourceLabel, dialectLabel } = getDdlMeta(ddl);
      return `
        <details class="ddl-card">
          <summary class="ddl-summary">
            <div class="ddl-summary-main">
              <div class="ddl-title-row">
                <span class="ddl-title">DDL 语句</span>
                ${dialectLabel ? `<span class="chip">${escapeHtml(dialectLabel)}</span>` : ''}
                ${sourceLabel ? `<span class="chip">${escapeHtml(sourceLabel)}</span>` : ''}
              </div>
              <div class="ddl-meta">默认收起，需要时展开查看表定义、主键和注释</div>
            </div>
            <button type="button" class="ghost-sm ddl-copy" data-copy-ddl data-copy-label="复制 DDL">复制 DDL</button>
          </summary>
          <div class="ddl-body">
            ${ddl.notice ? `<div class="ddl-notice">${escapeHtml(ddl.notice)}</div>` : ''}
            <pre class="ddl-code"><code id="ddlCodeBlock">${escapeHtml(ddl.text)}</code></pre>
          </div>
        </details>
      `;
    }

    function renderQualitySummary(summary) {
      const cards = [
        { label: '高缺失字段', value: summary.high_missing_count || 0, note: '缺失率 >= 50%' },
        { label: '高唯一字段', value: summary.high_unique_count || 0, note: '唯一度 >= 95%' },
        { label: '质量覆盖字段', value: summary.column_count || 0, note: '来自 pg_stats 的统计信息' },
      ];
      return `
        <div class="quality-grid">
          ${cards.map((card) => `
            <div class="metric-card">
              <div class="metric-label">${escapeHtml(card.label)}</div>
              <div class="metric-value">${escapeHtml(card.value)}</div>
              <div class="metric-note">${escapeHtml(card.note)}</div>
            </div>
          `).join('')}
        </div>
      `;
    }

    function prettyType(dataType, udtName) {
      const u = (udtName || '').trim();
      if (u) return u;
      const map = {
        'character varying': 'varchar',
        'timestamp without time zone': 'timestamp',
        'timestamp with time zone': 'timestamptz',
        'double precision': 'float8',
        'character': 'char',
        'boolean': 'bool',
      };
      return map[(dataType || '').trim()] || (dataType || '');
    }

    function missingRateClass(text) {
      if (!text || text === '-') return '';
      const n = parseFloat(text);
      if (n >= 50) return 'miss-high';
      if (n >= 20) return 'miss-mid';
      return '';
    }

    function renderColumnsTable(columns, qualifiedName, pkCols, fkCols) {
      if (!columns || !columns.length) return renderEmpty('没有字段信息');
      const defined = columns.filter((c) => c.business_def).length;
      const pctText = columns.length ? Math.round(defined / columns.length * 100) + '%' : '0%';
      const badge = `<span class="dict-complete-badge">字典完整度 <span class="pct">${pctText}</span>（${defined}/${columns.length}）</span>`;
      const parts = (qualifiedName || '').split('.');
      const schemaName = parts[0] || 'public';
      const tableName = parts.slice(1).join('.') || '';
      const pkSet = new Set((pkCols || []).map((c) => c.column_name));
      const fkSet = new Set((fkCols || []).map((c) => c.column_name));
      return `
        <div style="margin-bottom:8px">${badge}</div>
        <div class="table-wrap columns-table-wrap">
          <table class="columns-table">
            <thead>
              <tr>
                <th>字段</th>
                <th>类型</th>
                <th>数据库注释</th>
                <th>业务定义</th>
                <th>允许空值</th>
                <th>缺失率</th>
                <th>唯一度</th>
                <th>分布提示</th>
              </tr>
            </thead>
            <tbody>
              ${columns.map((column) => `
                <tr>
                  <td data-label="字段">
                    <div class="col-name-wrap">
                      <strong>${escapeHtml(column.column_name)}</strong>
                      ${pkSet.has(column.column_name) ? '<span class="pk-badge">PK</span>' : ''}
                      ${fkSet.has(column.column_name) ? '<span class="fk-badge">FK</span>' : ''}
                    </div>
                  </td>
                  <td data-label="类型"><code class="type-badge">${escapeHtml(prettyType(column.data_type, column.udt_name))}</code></td>
                  <td data-label="数据库注释">${escapeHtml(column.column_comment || '') || '<span style="color:var(--muted);opacity:0.45">—</span>'}</td>
                  <td class="dict-cell"
                      data-label="业务定义"
                      data-schema="${escapeHtml(schemaName)}"
                      data-table="${escapeHtml(tableName)}"
                      data-col="${escapeHtml(column.column_name)}"
                      data-def="${escapeHtml(column.business_def || '')}"
                      data-enum="${escapeHtml(column.enum_values || '')}">
                    <div class="dict-display">
                      ${column.business_def
                        ? `<div class="dict-def">${escapeHtml(column.business_def)}<span class="dict-hint">✏</span></div>${column.enum_values ? `<div class="dict-enum">${escapeHtml(column.enum_values)}</div>` : ''}`
                        : `<span class="dict-empty">— 点击定义</span>`
                      }
                    </div>
                  </td>
                  <td data-label="允许空值">${column.is_nullable
                    ? '<span class="nullable-pill nullable-yes">可空</span>'
                    : '<span class="nullable-pill nullable-no">非空</span>'}</td>
                  <td data-label="缺失率"><span class="${missingRateClass(column.missing_rate_text)}">${escapeHtml(column.missing_rate_text || '—')}</span></td>
                  <td data-label="唯一度">${escapeHtml(column.uniqueness_text || '—')}</td>
                  <td data-label="分布提示" style="color:var(--muted);font-size:0.85em">${escapeHtml(truncateMiddle(column.distribution_hint || '', 72)) || '<span style="opacity:0.4">—</span>'}</td>
                </tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderProfile(profile) {
      state.profile = profile;
      const table = profile.table || {};
      const manual = profile.manual_metadata || {};
      const structure = profile.structure || {};
      const details = profile.details || {};
      const quality = profile.quality || {};
      const summary = quality.summary || {};

      const tier = tierOf(table);
      const hot = Number(table.view_count || 0);
      const owner = details.responsible_owner || details.db_owner || table.owner || '';
      const complete = !!(owner && (table.table_comment || manual.description));
      const freshText = details.last_modified_time_text || table.last_modified_time_text || '';
      const ddl = profile.ddl || {};
      if (detailHero) {
        detailHero.innerHTML = [
          tier ? `<span class="hero-tier" style="background:${tierColor(tier)}">${tier} · ${escapeHtml(tierName(tier).replace(tier + ' ', ''))}</span>` : '',
          complete ? `<span class="hero-badge cert">✓ 元数据完整</span>` : '',
          owner ? `<span class="hero-badge owner">👤 ${escapeHtml(owner)}</span>` : '',
          hot ? `<span class="hero-badge hot">🔥 ${hot} 次浏览</span>` : '',
          freshText ? `<span class="hero-badge fresh">🕑 ${escapeHtml(freshText)}</span>` : '',
        ].filter(Boolean).join('');
      }
      detailTitle.textContent = table.table_name || table.qualified_name || '未命名表';
      detailSubtitle.textContent = [table.qualified_name, table.table_comment || manual.description].filter(Boolean).join(' · ') || '暂无说明';
      if (detailDdlShortcut && detailDdlInline) {
        const hasDdl = !!ddl.text;
        detailDdlShortcut.classList.toggle('hidden', !hasDdl);
        detailDdlShortcut.classList.remove('active');
        detailDdlShortcut.setAttribute('aria-expanded', 'false');
        detailDdlInline.classList.add('hidden');
        detailDdlInline.innerHTML = hasDdl ? renderHeaderDdlInline(ddl) : '';
      }
      detailStats.innerHTML = `
        <div class="stat-card">
          <div class="stat-label">物理大小</div>
          <div class="stat-value">${escapeHtml(details.physical_size_text || '0 bytes')}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">估算行数</div>
          <div class="stat-value">${(details.estimated_rows && details.estimated_rows > 0) ? escapeHtml(fmtInt(details.estimated_rows)) : '<span style="color:#9aa0a6;font-size:14px">未统计</span>'}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">字段数</div>
          <div class="stat-value">${escapeHtml(structure.column_count || 0)}</div>
        </div>
        <div class="stat-card">
          <div class="stat-label">主键 / 外键</div>
          <div class="stat-value">${escapeHtml((structure.primary_keys || []).length)} / ${escapeHtml((table.foreign_key_outgoing_count || 0))}</div>
        </div>
      `;
      favoriteToggle.disabled = false;
      favoriteToggle.textContent = table.favorite ? '★ 已收藏' : '☆ 收藏';
      favoriteToggle.classList.toggle('is-favorite', !!table.favorite);
      if (detailEmpty) detailEmpty.style.display = 'none';
      detailPanel.classList.remove('hidden');
      markActiveResult(state.selectedKey);

      // 把详情接口返回的最新 view_count 同步回左侧卡片（消除两边数字不一致）
      syncRowViewCount(table.qualified_name, table.view_count);

      detailContent.innerHTML = `
        <div class="tab-pane active" id="tabStructure">
        <section class="section">
          <div class="section-head">
            <div>
              <div class="section-title">结构与明细属性</div>
              <div class="section-copy">包含表别名、业务术语、负责人、大小、生命周期、主外键和维护状态。</div>
            </div>
          </div>
          ${renderMetaCards([
            { label: '表别名', value: table.alias || '' },
            { label: '业务术语', value: (table.business_terms || []).join('、') },
            { label: '业务板块', value: table.business_domain || '' },
            { label: '所属项目', value: table.project || '' },
            { label: '数据源类型', value: table.source_type || '' },
            { label: '存储类型', value: table.storage_type || '' },
            { label: '分区信息', value: structure.partition_info && structure.partition_info.is_partitioned ? structure.partition_info.partition_key || '已分区' : '非分区表' },
            { label: '生命周期 TTL', value: table.ttl_days ? `${table.ttl_days} 天` : '' },
            { label: '创建时间', value: details.created_time_text || '' },
            { label: '最后修改时间', value: details.last_modified_time_text || '' },
            { label: '数据库 Owner', value: details.db_owner || '' },
            { label: '最后 Analyze', value: (details.maintenance || {}).last_autoanalyze || (details.maintenance || {}).last_analyze || '' },
          ])}
        </section>

        <section class="section">
          <div class="section-head">
            <div>
              <div class="section-title">字段结构</div>
              <div class="section-copy">查看字段名称、类型、注释、缺失率、唯一度和分布提示。</div>
            </div>
          </div>
          ${renderColumnsTable(structure.columns || [], table.qualified_name, structure.primary_keys, structure.foreign_keys_outgoing)}
        </section>

        <section class="section">
          <div class="section-head">
            <div>
              <div class="section-title">主外键关系</div>
              <div class="section-copy">快速判断这张表是事实表、维表还是桥接表，以及它的上下游引用关系。</div>
            </div>
          </div>
          <div class="list-grid">
            ${renderKeyList('主键', structure.primary_keys || [], (item) => `<div class="list-card-copy">${escapeHtml(item.column_name)} <span class="item-subtitle">(${escapeHtml(item.constraint_name)})</span></div>`, '未识别到主键')}
            ${renderKeyList('外键出向', structure.foreign_keys_outgoing || [], (item) => `<div class="list-card-copy">${escapeHtml(item.column_name)} → ${escapeHtml(item.foreign_qualified_name)}.${escapeHtml(item.foreign_column_name)}</div>`, '未识别到外键')}
            ${renderKeyList('外键入向', structure.foreign_keys_incoming || [], (item) => `<div class="list-card-copy">${escapeHtml(item.source_qualified_name)}.${escapeHtml(item.source_column_name)} → ${escapeHtml(item.column_name)}</div>`, '没有其他表引用它')}
            ${renderKeyList('相关推荐', profile.recommendations || [], (item) => `<button type="button" class="list-card-copy" data-related-key="${escapeHtml(item.qualified_name)}" style="text-align:left; background:none; border:0; padding:0; cursor:pointer; color:var(--ink);">${escapeHtml(item.qualified_name)}</button>`, '暂无相关推荐')}
          </div>
        </section>

        <section class="section">
          <div class="section-head">
            <div>
              <div class="section-title">数据预览</div>
              <div class="section-copy">在当前权限下，展示部分样本数据，帮助快速确认字段语义和数据形态。</div>
            </div>
          </div>
          ${renderPreviewSection(profile.preview || {}, (structure.columns || []).map((column) => column.column_name))}
        </section>

        <section class="section">
          <div class="section-head">
            <div>
              <div class="section-title">基础质量探查</div>
              <div class="section-copy">${escapeHtml(quality.note || '基于系统统计信息和样本进行基础质量判断。')}</div>
            </div>
          </div>
          ${renderQualitySummary(summary)}
          <div class="list-grid">
            ${renderKeyList('高缺失字段', summary.top_missing_columns || [], (item) => `<div class="list-card-copy">${escapeHtml(item.column_name)} · ${escapeHtml(item.missing_rate_text)}</div>`, '暂无高缺失字段')}
            ${renderKeyList('高唯一字段', summary.top_unique_columns || [], (item) => `<div class="list-card-copy">${escapeHtml(item.column_name)} · ${escapeHtml(item.uniqueness_text)}</div>`, '暂无高唯一字段')}
          </div>
        </section>
        </div>

        ${renderSqlSection(profile)}

        <div class="tab-pane" id="tabLineage">
          <div class="lineage-wrap">
            <div class="lineage-toolbar">
              <span class="l-info" id="lineageInfo">—</span>
              <span style="font-size:12px;color:#9aa0a6" id="lineageSummary"></span>
              <button id="lineageFit">适应窗口</button>
              <button id="lineageZoomIn">＋</button>
              <button id="lineageZoomOut">−</button>
              <a id="lineageOpenFull" href="#" target="_blank" rel="noopener noreferrer" style="font-size:12px;color:#1a73e8;margin-left:4px;text-decoration:none">完整 ↗</a>
            </div>
            <div class="lineage-body">
              <div class="lineage-canvas" id="lineageCanvas"><div class="lineage-loading">点击「血缘图」Tab 即可加载</div></div>
              <div class="lineage-sidebar" id="lineageSidebar">
                <div class="ls-section">
                  <div class="ls-title">节点详情 <button class="ls-close" id="lsClose">×</button></div>
                  <div id="lsDetailBody"></div>
                </div>
                <div class="ls-section" style="flex:1;overflow-y:auto">
                  <div class="ls-title">影响分析</div>
                  <div id="lsImpactBody"><div style="color:#9aa0a6;font-size:12px">选中节点查看上下游影响</div></div>
                </div>
              </div>
            </div>
            <div class="lineage-bottom">
              <div class="lb-tabs">
                <div class="lb-tab active" data-lb="overview">概览</div>
                <div class="lb-tab" data-lb="producers" title="谁写入这张表">上游 ↑</div>
                <div class="lb-tab" data-lb="runs">运行记录</div>
                <div class="lb-tab" data-lb="consumers" title="谁读取这张表">下游 ↓</div>
              </div>
              <div class="lb-content" id="lbContent"><div class="lb-empty">选中节点查看调度信息</div></div>
            </div>
          </div>
        </div>
      `;

      detailContent.querySelectorAll('[data-related-key]').forEach((node) => {
        node.addEventListener('click', () => {
          selectTableByKey(node.dataset.relatedKey);
        });
      });

      detailContent.querySelectorAll('.dict-cell').forEach((cell) => {
        cell.addEventListener('click', (e) => {
          if (cell.querySelector('.dict-editor')) return;
          openDictEditor(cell);
        });
      });

      if (detailDdlShortcut) detailDdlShortcut.onclick = () => {
        if (!detailDdlInline || !profile?.ddl?.text) return;
        const willOpen = detailDdlInline.classList.contains('hidden');
        detailDdlInline.classList.toggle('hidden', !willOpen);
        detailDdlShortcut.classList.toggle('active', willOpen);
        detailDdlShortcut.setAttribute('aria-expanded', willOpen ? 'true' : 'false');
      };

      const bindDdlCopyButtons = (root) => {
        root?.querySelectorAll?.('[data-copy-ddl]')?.forEach((button) => {
          button.addEventListener('click', async (event) => {
            event.preventDefault();
            event.stopPropagation();
            const btn = event.currentTarget;
            const ddlText = profile?.ddl?.text || '';
            const defaultLabel = btn.dataset.copyLabel || '复制 DDL';
            if (!ddlText) return;
            try {
              await navigator.clipboard.writeText(ddlText);
              btn.textContent = '已复制';
              btn.classList.add('done');
              setTimeout(() => {
                btn.textContent = defaultLabel;
                btn.classList.remove('done');
              }, 1500);
            } catch (error) {
              btn.textContent = '复制失败';
            }
          });
        });
      };

      bindDdlCopyButtons(detailDdlInline);
      bindDdlCopyButtons(detailContent);

      bindPreviewFilterEvents((structure.columns || []).map((column) => column.column_name));
      bindSqlEvents(profile);

      activeDetailTab = 'structure';
      setDetailTab('structure');
    }

    function setDetailTab(tab) {
      activeDetailTab = tab || 'structure';
      document.getElementById('detailTabs')?.querySelectorAll('.detail-tab').forEach((t) => {
        t.classList.toggle('active', t.dataset.tab === activeDetailTab);
      });
      detailContent.querySelectorAll('.tab-pane').forEach((p) => p.classList.remove('active'));
      const paneId = activeDetailTab === 'lineage' ? 'tabLineage' : (activeDetailTab === 'sql' ? 'tabSql' : 'tabStructure');
      document.getElementById(paneId)?.classList.add('active');
      if (activeDetailTab === 'lineage') loadLineageForCurrentTable();
    }

    function insertTextAtCursor(textarea, text) {
      if (!textarea) return;
      const start = textarea.selectionStart ?? textarea.value.length;
      const end = textarea.selectionEnd ?? textarea.value.length;
      const before = textarea.value.slice(0, start);
      const after = textarea.value.slice(end);
      textarea.value = `${before}${text}${after}`;
      const next = start + text.length;
      textarea.focus();
      textarea.setSelectionRange(next, next);
    }

    function getSqlFieldSuggestions(columns, prefix) {
      const needle = String(prefix || '').trim().toLowerCase();
      if (!needle) return [];
      return (columns || [])
        .filter((column) => String(column.column_name || '').toLowerCase().includes(needle))
        .slice(0, 8);
    }

    function renderSqlSuggestions(columns) {
      if (!columns.length) return '';
      return columns.map((column, index) => `
        <button type="button" class="sql-suggest-item ${index === 0 ? 'active' : ''}" data-suggest-column="${escapeHtml(column.column_name)}">
          <div class="sql-suggest-name">${escapeHtml(column.column_name)}</div>
          <div class="sql-suggest-meta">${escapeHtml(prettyType(column.data_type, column.udt_name))}${column.column_comment ? ` · ${escapeHtml(truncateMiddle(column.column_comment, 44))}` : ''}</div>
        </button>
      `).join('');
    }

    function currentSqlToken(textarea) {
      if (!textarea) return '';
      const pos = textarea.selectionStart ?? textarea.value.length;
      const head = textarea.value.slice(0, pos);
      const match = head.match(/([a-zA-Z_][a-zA-Z0-9_]*)$/);
      return match ? match[1] : '';
    }

    function bindSqlEvents(profile) {
      const sqlEditor = document.getElementById('sqlEditor');
      const runBtn = document.getElementById('runSqlBtn');
      const resetBtn = document.getElementById('resetSqlBtn');
      const openWorkbenchBtn = document.getElementById('openSqlWorkbenchBtn');
      const rowLimitEl = document.getElementById('sqlRowLimit');
      const statusEl = document.getElementById('sqlStatus');
      const resultWrap = document.getElementById('sqlResultWrap');
      const fieldSearch = document.getElementById('sqlFieldSearch');
      const fieldList = document.getElementById('sqlFieldList');
      const suggest = document.getElementById('sqlSuggest');
      const sqlShell = document.getElementById('sqlShell');
      const columns = profile?.structure?.columns || [];
      const qname = profile?.table?.qualified_name || '';
      if (!sqlEditor) return;

      const applySqlLayout = (layout) => {
        const nextLayout = SQL_LAYOUTS.includes(layout) ? layout : 'split';
        if (sqlShell) sqlShell.dataset.layout = nextLayout;
        document.querySelectorAll('[data-sql-layout]').forEach((btn) => {
          btn.classList.toggle('active', btn.dataset.sqlLayout === nextLayout);
        });
        if (state.profile) {
          state.profile.sql = {
            ...(state.profile.sql || {}),
            layout: nextLayout,
          };
        }
      };

      const refreshFieldList = () => {
        if (!fieldList) return;
        fieldList.innerHTML = renderSqlFieldList(columns, fieldSearch?.value || '');
        fieldList.querySelectorAll('[data-insert-column]').forEach((btn) => {
          btn.addEventListener('click', () => insertTextAtCursor(sqlEditor, btn.dataset.insertColumn));
        });
      };

      const hideSuggest = () => {
        if (!suggest) return;
        suggest.classList.add('hidden');
        suggest.innerHTML = '';
      };

      const showSuggest = () => {
        const token = currentSqlToken(sqlEditor);
        const matches = getSqlFieldSuggestions(columns, token);
        if (!suggest || !token || !matches.length) {
          hideSuggest();
          return;
        }
        suggest.innerHTML = renderSqlSuggestions(matches);
        suggest.classList.remove('hidden');
        suggest.querySelectorAll('[data-suggest-column]').forEach((btn) => {
          btn.addEventListener('click', () => {
            const replace = btn.dataset.suggestColumn || '';
            const tokenNow = currentSqlToken(sqlEditor);
            if (!tokenNow) return;
            const start = (sqlEditor.selectionStart ?? 0) - tokenNow.length;
            const before = sqlEditor.value.slice(0, start);
            const after = sqlEditor.value.slice(sqlEditor.selectionStart ?? 0);
            sqlEditor.value = `${before}${replace}${after}`;
            const next = before.length + replace.length;
            sqlEditor.focus();
            sqlEditor.setSelectionRange(next, next);
            hideSuggest();
          });
        });
      };

      const setStatus = (text, kind = '') => {
        if (!statusEl) return;
        statusEl.textContent = text || '';
        statusEl.className = `sql-status${kind ? ` ${kind}` : ''}`;
      };

      const runSql = async () => {
        if (!state.selectedKey) return;
        const [schema, table] = state.selectedKey.split('.');
        const rowLimit = Number(rowLimitEl?.value || 200);
        const sql = sqlEditor.value.trim();
        const requestId = ++state.sqlRequestId;
        setStatus('正在执行查询…');
        if (resultWrap) resultWrap.innerHTML = '<div class="sql-field-empty">正在执行，请稍候…</div>';
        try {
          const result = await fetchJson('/api/table-sql', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ schema, table, sql, row_limit: rowLimit }),
          });
          if (requestId !== state.sqlRequestId) return;
          if (!state.profile) return;
          state.profile.sql = {
            text: sqlEditor.value,
            row_limit: rowLimit,
            result,
            fieldKeyword: fieldSearch?.value || '',
            layout: state.profile?.sql?.layout || 'split',
          };
          const meta = detailContent.querySelector('.sql-result-meta');
          if (meta) meta.innerHTML = `返回 <b>${escapeHtml(result.row_count || 0)}</b> 行，方言 <b>${escapeHtml(result.dialect || '')}</b>`;
          if (resultWrap) resultWrap.innerHTML = renderSqlResultTable(result);
          setStatus(`执行完成，实际返回 ${result.row_count || 0} 行。`, 'ok');
        } catch (error) {
          if (requestId !== state.sqlRequestId) return;
          const meta = detailContent.querySelector('.sql-result-meta');
          if (meta) meta.textContent = '执行失败';
          if (resultWrap) resultWrap.innerHTML = `<div class="error">${escapeHtml(error.message || 'SQL 执行失败')}</div>`;
          setStatus(error.message || 'SQL 执行失败', 'error');
        }
      };

      runBtn?.addEventListener('click', runSql);
      resetBtn?.addEventListener('click', () => {
        sqlEditor.value = defaultSqlForTable(qname, 20);
        hideSuggest();
        setStatus('');
      });
      openWorkbenchBtn?.addEventListener('click', () => {
        const [schema, table] = qname.split('.');
        const next = new URLSearchParams();
        if (schema) next.set('schema', schema);
        if (table) next.set('table', table);
        window.location.href = `/sql-workbench?${next.toString()}`;
      });
      document.querySelectorAll('[data-sql-layout]').forEach((btn) => {
        btn.addEventListener('click', () => applySqlLayout(btn.dataset.sqlLayout || 'split'));
      });
      fieldSearch?.addEventListener('input', () => {
        if (state.profile?.sql) state.profile.sql.fieldKeyword = fieldSearch.value;
        refreshFieldList();
      });
      sqlEditor.addEventListener('input', () => {
        if (state.profile?.sql) state.profile.sql.text = sqlEditor.value;
        showSuggest();
      });
      sqlEditor.addEventListener('click', showSuggest);
      sqlEditor.addEventListener('keyup', (event) => {
        if (event.key === 'Escape') hideSuggest();
      });
      sqlEditor.addEventListener('keydown', (event) => {
        if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
          event.preventDefault();
          runSql();
          return;
        }
        if (event.key === 'Tab' && !suggest?.classList.contains('hidden')) {
          const active = suggest.querySelector('.sql-suggest-item.active') || suggest.querySelector('.sql-suggest-item');
          if (active) {
            event.preventDefault();
            active.click();
          }
        }
      });
      if (!sqlOutsideClickBound) {
        document.addEventListener('click', (event) => {
          if (!event.target.closest('.sql-editor-wrap')) {
            document.getElementById('sqlSuggest')?.classList.add('hidden');
            const suggestEl = document.getElementById('sqlSuggest');
            if (suggestEl) suggestEl.innerHTML = '';
          }
        });
        sqlOutsideClickBound = true;
      }
      applySqlLayout(state.profile?.sql?.layout || 'split');
      refreshFieldList();
    }

    function bindPreviewFilterEvents(columnNames) {
      const form = document.getElementById('previewFilterForm');
      const resetBtn = document.getElementById('previewFilterReset');
      if (!form) return;
      form.addEventListener('submit', async (event) => {
        event.preventDefault();
        await applyPreviewFilter(columnNames);
      });
      resetBtn?.addEventListener('click', async () => {
        const keywordEl = document.getElementById('previewFilterKeyword');
        const columnEl = document.getElementById('previewFilterColumn');
        if (keywordEl) keywordEl.value = '';
        if (columnEl) columnEl.value = '';
        await applyPreviewFilter(columnNames);
      });
    }

    async function applyPreviewFilter(columnNames) {
      if (!state.selectedKey) return;
      const tableWrap = document.getElementById('previewTableWrap');
      const metaEl = detailContent.querySelector('.preview-meta');
      const columnEl = document.getElementById('previewFilterColumn');
      const keywordEl = document.getElementById('previewFilterKeyword');
      const previewColumn = columnEl ? columnEl.value.trim() : '';
      const previewKeyword = keywordEl ? keywordEl.value.trim() : '';
      const requestId = ++state.previewRequestId;
      if (tableWrap) tableWrap.innerHTML = '<div class="empty">正在筛选样本数据…</div>';
      if (metaEl) metaEl.textContent = '正在更新预览样本…';

      const [schema, table] = state.selectedKey.split('.');
      const query = new URLSearchParams({
        schema: schema || 'public',
        table: table || '',
        sample_limit: String((state.profile?.preview || {}).sample_limit || 20),
      });
      if (previewColumn) query.set('preview_column', previewColumn);
      if (previewKeyword) query.set('preview_keyword', previewKeyword);

      try {
        const preview = await fetchJson(`/api/table-preview?${query.toString()}`);
        if (requestId !== state.previewRequestId) return;
        if (state.profile) state.profile.preview = preview;
        if (metaEl) metaEl.outerHTML = renderPreviewMeta(preview);
        if (tableWrap) tableWrap.innerHTML = renderPreviewTable(preview);
        const nextColumnEl = document.getElementById('previewFilterColumn');
        const nextKeywordEl = document.getElementById('previewFilterKeyword');
        if (nextColumnEl) nextColumnEl.value = preview.filter?.column || '';
        if (nextKeywordEl) nextKeywordEl.value = preview.filter?.keyword || '';
      } catch (error) {
        if (requestId !== state.previewRequestId) return;
        if (metaEl) metaEl.textContent = '预览筛选失败';
        if (tableWrap) tableWrap.innerHTML = `<div class="error">${escapeHtml(error.message || '预览筛选失败')}</div>`;
      }
    }

    // ══════════════════════════════════════════════════
    // 血缘图渲染引擎
    // ══════════════════════════════════════════════════
    const lineagePalette = {
      target_table: '#0d9488',
      pipeline: '#ea580c',
      graph_table: '#1a73e8',
      graph_task: '#7c3aed',
      source_table_config: '#475569',
    };
    let lineageZoom = 1;
    let lineagePositions = null;
    let lineageSelectedNodeId = null;

    function escapeXml(t) {
      return String(t == null ? '' : t).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&apos;');
    }

    function getLineageMetaLine(node) {
      const m = node.meta || {};
      const parts = [m.connection_name, m.database_name || m.fdl_database, m.schema_name || m.schema, m.table_name || m.fdl_table].filter(Boolean);
      return truncateMiddle(parts.join('.') || m.resource_id || m.full_name || [m.work_name, m.node_name].filter(Boolean).join(' / ') || '', 42);
    }

    function buildLineageLayout(graph) {
      const incoming = new Map(), outgoing = new Map();
      graph.nodes.forEach(n => { incoming.set(n.id, []); outgoing.set(n.id, []); });
      graph.edges.forEach(e => {
        if (incoming.has(e.target)) incoming.get(e.target).push(e.source);
        if (outgoing.has(e.source)) outgoing.get(e.source).push(e.target);
      });
      const upDist = new Map([[graph.root_node_id, 0]]), upQ = [graph.root_node_id];
      while (upQ.length) { const c = upQ.shift(); for (const p of (incoming.get(c)||[])) { if (!upDist.has(p)) { upDist.set(p, upDist.get(c)+1); upQ.push(p); } } }
      const downDist = new Map([[graph.root_node_id, 0]]), downQ = [graph.root_node_id];
      while (downQ.length) { const c = downQ.shift(); for (const n of (outgoing.get(c)||[])) { if (!downDist.has(n)) { downDist.set(n, downDist.get(c)+1); downQ.push(n); } } }
      const level = new Map(); let minL = 0, maxL = 0;
      graph.nodes.forEach(n => {
        let lv = 0;
        if (n.id !== graph.root_node_id && downDist.has(n.id)) lv = downDist.get(n.id);
        else if (n.id !== graph.root_node_id && upDist.has(n.id)) lv = -upDist.get(n.id);
        level.set(n.id, lv); minL = Math.min(minL, lv); maxL = Math.max(maxL, lv);
      });
      const layers = new Map();
      graph.nodes.forEach(n => { const l = level.get(n.id)-minL; if (!layers.has(l)) layers.set(l,[]); layers.get(l).push(n); });
      const layerKeys = Array.from(layers.keys()).sort((a,b)=>a-b);
      const colGap=120, rowGap=28, topPad=40, leftPad=30;
      const positions=new Map(), metrics=new Map();
      layerKeys.forEach(layer => {
        const nodes = layers.get(layer).sort((a,b)=>a.label.localeCompare(b.label,'zh-CN'));
        let cw=0, ch=0;
        const mn = nodes.map(n=>{const ml=getLineageMetaLine(n); const w=Math.min(300,Math.max(190,n.label.length*12+56,ml.length*6+40)); cw=Math.max(cw,w); ch+=64; return {node:n,width:w,height:64};});
        ch += Math.max(0, mn.length-1)*rowGap;
        metrics.set(layer, {nodes:mn, width:cw, height:ch});
      });
      const totalW = layerKeys.reduce((s,lk,i)=>s+metrics.get(lk).width+(i>0?colGap:0),0);
      const maxH = Math.max(...Array.from(metrics.values()).map(m=>m.height),0);
      let cx = leftPad;
      layerKeys.forEach(layer => {
        const m=metrics.get(layer), sy=topPad+Math.max(0,(maxH-m.height)/2); let cy=sy;
        m.nodes.forEach(({node,width,height})=>{positions.set(node.id,{x:cx,y:cy,width,height}); cy+=height+rowGap;});
        cx += m.width + colGap;
      });
      return {positions, width:Math.max(totalW+leftPad*2,600), height:Math.max(maxH+topPad*2,260)};
    }

    function renderLineageGraph(graph) {
      const canvasEl = document.getElementById('lineageCanvas');
      const stageEl = document.getElementById('lineageCanvas');
      if (!canvasEl) return;
      if (!graph.nodes || !graph.nodes.length) { canvasEl.innerHTML = '<div class="lineage-empty">该表未找到血缘关系</div>'; return; }
      const {positions,width,height} = buildLineageLayout(graph);
      lineagePositions = positions; lineageZoom = 1; lineageSelectedNodeId = graph.root_node_id;
      const defs = `<defs><marker id="l-arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto"><path d="M0,0 L0,6 L7,3 z" fill="#9aa0a6"/></marker></defs>`;
      const edges = graph.edges.map(edge => {
        const from=positions.get(edge.source), to=positions.get(edge.target);
        if (!from||!to) return '';
        const x1=from.x+from.width, y1=from.y+from.height/2, x2=to.x, y2=to.y+to.height/2;
        const dx=Math.max(30,(x2-x1)/2);
        return `<path class="l-edge" d="M ${x1} ${y1} C ${x1+dx} ${y1}, ${x2-dx} ${y2}, ${x2} ${y2}" marker-end="url(#l-arrow)"/><text class="l-edge-label" x="${(x1+x2)/2}" y="${(y1+y2)/2-6}" text-anchor="middle">${escapeXml(edge.relation||'')}</text>`;
      }).join('');
      const nodes = graph.nodes.map(node => {
        const box=positions.get(node.id); if (!box) return '';
        const fill=lineagePalette[node.type]||'#334155', ml=getLineageMetaLine(node);
        const isRoot = node.id === graph.root_node_id;
        return `<g class="l-node-group${isRoot?' l-node-selected':''}" data-node-id="${escapeXml(node.id)}"><rect class="l-node-rect" x="${box.x}" y="${box.y}" width="${box.width}" height="${box.height}" fill="${fill}" rx="6" ry="6"/><text class="l-node-label" x="${box.x+14}" y="${box.y+26}">${escapeXml(node.label)}</text><text class="l-node-meta" x="${box.x+14}" y="${box.y+46}">${escapeXml(ml)}</text></g>`;
      }).join('');
      canvasEl.innerHTML = `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMinYMin meet">${defs}${edges}${nodes}</svg>`;
      // 节点点击 → 高亮 + 显示详情
      canvasEl.querySelectorAll('.l-node-group').forEach(el => {
        el.addEventListener('click', () => {
          canvasEl.querySelectorAll('.l-node-group').forEach(g=>g.classList.remove('l-node-selected'));
          el.classList.add('l-node-selected');
          lineageSelectedNodeId = el.dataset.nodeId;
          selectLineageNode(lineageSelectedNodeId, graph);
        });
      });
      // 自动选中根节点并加载调度信息
      selectLineageNode(lineageSelectedNodeId, graph);
      requestAnimationFrame(() => fitLineageToScreen());
    }

    function applyLineageZoom() {
      const svg = document.querySelector('#lineageCanvas svg');
      if (svg) { svg.style.transform = `scale(${lineageZoom})`; svg.style.transformOrigin = 'top left'; }
    }

    function fitLineageToScreen() {
      const stage = document.getElementById('lineageCanvas');
      if (!stage||!lineagePositions) return;
      const svg = stage.querySelector('svg');
      if (!svg) return;
      let minX=Infinity,minY=Infinity,maxX=0,maxY=0;
      lineagePositions.forEach(b=>{minX=Math.min(minX,b.x);minY=Math.min(minY,b.y);maxX=Math.max(maxX,b.x+b.width);maxY=Math.max(maxY,b.y+b.height);});
      const pad=30, contentW=maxX-minX+pad*2, contentH=maxY-minY+pad*2;
      const stageW=stage.clientWidth||600, stageH=stage.clientHeight||460;
      lineageZoom=Math.min(stageW/contentW,stageH/contentH,1); lineageZoom=Math.max(0.2,lineageZoom);
      applyLineageZoom(); stage.scrollTo({left:Math.max(0,minX-pad),top:0});
    }

    // ── 节点详情 ──
    function selectLineageNode(nodeId, graph) {
      const node = graph.nodes.find(n=>n.id===nodeId);
      if (!node) return;
      document.getElementById('lineageSidebar').classList.add('open');
      const meta = node.meta||{};
      window._lineageMeta = meta; // 缓存 meta 供 Tab 切换时使用
      const body = document.getElementById('lsDetailBody');
      const typeColors = {target_table:'#0d9488',pipeline:'#ea580c',graph_table:'#1a73e8',graph_task:'#7c3aed',source_table_config:'#475569'};
      const typeName = {target_table:'目标表',pipeline:'管道',graph_table:'表节点',graph_task:'任务节点',source_table_config:'源配置'};
      body.innerHTML = `
        <div class="ls-row"><span class="lbl">类型</span><span class="val"><span class="ls-type-badge" style="background:${typeColors[node.type]||'#334155'}">${escapeXml(typeName[node.type]||node.type)}</span></span></div>
        <div class="ls-row"><span class="lbl">名称</span><span class="val">${escapeXml(node.label)}</span></div>
        ${meta.full_name?`<div class="ls-row"><span class="lbl">全名</span><span class="val">${escapeXml(meta.full_name)}</span></div>`:''}
        ${meta.connection_name?`<div class="ls-row"><span class="lbl">连接</span><span class="val">${escapeXml(meta.connection_name)}</span></div>`:''}
        ${meta.database_name||meta.fdl_database?`<div class="ls-row"><span class="lbl">数据库</span><span class="val">${escapeXml(meta.database_name||meta.fdl_database)}</span></div>`:''}
        ${meta.schema_name||meta.schema?`<div class="ls-row"><span class="lbl">Schema</span><span class="val">${escapeXml(meta.schema_name||meta.schema)}</span></div>`:''}
        ${meta.table_name||meta.fdl_table?`<div class="ls-row"><span class="lbl">表名</span><span class="val">${escapeXml(meta.table_name||meta.fdl_table)}</span></div>`:''}
        ${meta.work_name?`<div class="ls-row"><span class="lbl">任务</span><span class="val">${escapeXml(meta.work_name)}</span></div>`:''}
      `;
      // 影响分析
      const impactBody = document.getElementById('lsImpactBody');
      const upstreamEdges = graph.edges.filter(e=>e.target===nodeId);
      const downstreamEdges = graph.edges.filter(e=>e.source===nodeId);
      const upstreamNodes = upstreamEdges.map(e=>graph.nodes.find(n=>n.id===e.source)).filter(Boolean);
      const downstreamNodes = downstreamEdges.map(e=>graph.nodes.find(n=>n.id===e.target)).filter(Boolean);
      let impactHtml = '';
      if (upstreamNodes.length) {
        impactHtml += `<div style="font-weight:600;margin-bottom:4px;font-size:12px">上游 (${upstreamNodes.length})</div>`;
        upstreamNodes.forEach(n => { impactHtml += `<div class="ls-relation"><span class="ls-conn" style="background:${lineagePalette[n.type]||'#334155'}"></span>${escapeXml(n.label)}</div>`; });
      }
      if (downstreamNodes.length) {
        impactHtml += `<div style="font-weight:600;margin:8px 0 4px;font-size:12px">下游 (${downstreamNodes.length})</div>`;
        downstreamNodes.forEach(n => { impactHtml += `<div class="ls-relation"><span class="ls-conn" style="background:${lineagePalette[n.type]||'#334155'}"></span>${escapeXml(n.label)}</div>`; });
      }
      impactBody.innerHTML = impactHtml || '<div style="color:#9aa0a6;font-size:12px">无上下游关系</div>';
      // 加载调度信息（传元数据避开 UUID 解析问题）
      loadNodeSchedule(nodeId, meta);
    }

    // ── 调度信息 ──
    async function loadNodeSchedule(nodeId, meta) {
      const content = document.getElementById('lbContent');
      content.innerHTML = '<div style="padding:16px;text-align:center;color:#9aa0a6">加载中…</div>';
      meta = meta || {};
      const params = new URLSearchParams({ node_id: nodeId, run_limit: '10' });
      if (meta.target_table_id) params.set('target_table_id', meta.target_table_id);
      if (meta.connection_name) params.set('connection_name', meta.connection_name);
      if (meta.schema_name || meta.schema) params.set('schema', meta.schema_name || meta.schema);
      if (meta.table_name || meta.fdl_table || meta.table) params.set('table', meta.table_name || meta.fdl_table || meta.table);
      if (meta.database_name || meta.fdl_database) params.set('database_name', meta.database_name || meta.fdl_database);
      try {
        const resp = await fetch(`/api/lineage-node-activity?${params.toString()}`);
        const data = await resp.json();
        if (!resp.ok) throw new Error(data.error);
        renderNodeSchedule(nodeId, data);
      } catch(e) {
        content.innerHTML = `<div class="lb-empty">${escapeHtml(e.message)}</div>`;
      }
    }

    // 资源类型 → 业务可读标签（PIPELINE 在前端被过滤掉，不会出现）
    function taskTypeLabel(rt) {
      return ({
        DEV_DATA_SYNC: '同步任务',
        DEV_DATA_FLOW: '转换任务',
        DEV_PARAM_ASSIGN: '参数任务',
      })[rt] || '任务';
    }

    // 把节点级数据按 task_name 聚合到任务级；同名归到一组，节点合并展示
    function aggregateByTask(items) {
      const groups = new Map();
      (items || []).forEach((it) => {
        const taskName = it.task_name || '(未命名)';
        const key = `task::${taskName}::${it.resource_type || ''}`;
        if (!groups.has(key)) {
          groups.set(key, {
            taskName,
            resource_type: it.resource_type,
            schedule_plan_name: it.schedule_plan_name || '',
            schedule_cycle_text: it.schedule_cycle_text || '',
            nodeNames: [],
            downstreamTables: [],
          });
        }
        const g = groups.get(key);
        const nn = (it.node_name || '').trim();
        if (nn && !g.nodeNames.includes(nn)) g.nodeNames.push(nn);
        (it.downstream_tables || []).forEach((t) => { if (t && !g.downstreamTables.includes(t)) g.downstreamTables.push(t); });
      });
      return [...groups.values()];
    }

    // 上下游通用渲染（PIPELINE 类型不直接展示，但用于推导"下游表"）
    function renderLineageList(opts) {
      const { kind, items, isSourceFallback, data } = opts;
      const isUpstream = kind === 'upstream';
      const arrowLabel = isUpstream ? '生产' : '消费';
      // 直接过滤掉 PIPELINE，从用户视野中彻底消失
      const tasks = aggregateByTask((items || []).filter(it => it.resource_type !== 'PIPELINE'));

      const fallbackHint = isSourceFallback
        ? `<div style="color:#9aa0a6;font-size:11px;margin-bottom:6px">仅识别到任务级归属，未定位到目标侧具体产出节点</div>`
        : '';

      // 任务条目：任务名 + 类型 · 节点 · 调度计划 · 周期
      const renderGroup = (g) => {
        const typeLabel = taskTypeLabel(g.resource_type);
        const nodesText = g.nodeNames.length
          ? (g.nodeNames.length > 3
              ? `节点: ${g.nodeNames.slice(0, 3).map(escapeXml).join('、')}…（共 ${g.nodeNames.length} 个）`
              : `节点: ${g.nodeNames.map(escapeXml).join('、')}`)
          : '';
        const flowText = (g.downstreamTables && g.downstreamTables.length)
          ? `→ 流向 ${g.downstreamTables.map(escapeXml).join('、')}`
          : '';
        const scheduleParts = [];
        if (g.schedule_plan_name) scheduleParts.push(`调度计划: ${escapeXml(g.schedule_plan_name)}`);
        if (g.schedule_cycle_text) scheduleParts.push(`周期: ${escapeXml(g.schedule_cycle_text)}`);
        return `<div class="lb-item" style="flex-direction:column;gap:3px">
          <div style="display:flex;align-items:center;gap:8px;width:100%">
            <span style="font-weight:600;font-size:13px">${escapeXml(g.taskName)}</span>
            <span style="font-size:11px;color:#9aa0a6">${typeLabel}</span>
          </div>
          ${nodesText ? `<div style="color:#5f6368;font-size:11px">${nodesText}</div>` : ''}
          ${flowText ? `<div style="color:#0d9488;font-size:11px">${flowText}</div>` : ''}
          ${scheduleParts.length ? `<div style="color:#9aa0a6;font-size:11px">${scheduleParts.join(' · ')}</div>` : ''}
        </div>`;
      };

      // ── 下游表区块（下游 Tab 专用）──
      // 已被"真实下游任务"覆盖的下游表不再单列；只列出"知道流向、但搬运任务未采集到"的表
      let downstreamTablesHtml = '';
      if (!isUpstream && data) {
        const coveredTables = new Set();
        tasks.forEach(t => (t.downstreamTables || []).forEach(x => coveredTables.add(x)));
        const targets = [...new Set((items || [])
          .filter(it => it.resource_type === 'PIPELINE' && it.pipeline_target)
          .map(it => it.pipeline_target))].filter(t => !coveredTables.has(t));
        if (targets.length) {
          downstreamTablesHtml = `<div style="margin-top:${tasks.length ? '10px' : '0'}">
            <div style="color:#9aa0a6;font-size:11px;margin-bottom:6px">数据流向下游表（搬运任务未采集到）</div>
            ${targets.map(t => `<div class="lb-item" style="flex-direction:column;gap:3px">
              <div style="display:flex;align-items:center;gap:8px;width:100%">
                <span style="font-weight:600;font-size:13px">📄 ${escapeXml(t)}</span>
                <span style="font-size:11px;color:#9aa0a6">下游表</span>
              </div>
            </div>`).join('')}
          </div>`;
        }
      }

      const totalShown = tasks.length + (downstreamTablesHtml ? 1 : 0);
      const toolbar = totalShown
        ? `<div style="padding-bottom:6px;color:#9aa0a6;font-size:11px;border-bottom:1px solid #e8eaed;margin-bottom:6px">${
            isUpstream
              ? `共 <b style="color:#1a1d23">${tasks.length}</b> 个任务写入这张表`
              : (tasks.length
                  ? `共 <b style="color:#1a1d23">${tasks.length}</b> 个任务读取这张表`
                  : `本表数据流向下游`)
          }</div>`
        : '';

      const emptyHint = isUpstream
        ? `<div class="lb-empty" style="text-align:left;padding:10px 0">
             <div style="color:#5f6368;margin-bottom:4px">未识别到任务${arrowLabel}这张表</div>
             <div style="color:#9aa0a6;font-size:11px">可能原因：① 数据来自外部库；② 该表的写入 SQL 暂未被 fdl_lineage_node 采集</div>
           </div>`
        : `<div class="lb-empty" style="text-align:left;padding:10px 0">
             <div style="color:#5f6368;margin-bottom:4px">未识别到任务${arrowLabel}这张表</div>
             <div style="color:#9aa0a6;font-size:11px">可能原因：① 这是终点表（直接给业务系统/报表使用）；② 下游 SQL 暂未被 fdl_lineage_node 采集</div>
           </div>`;

      const taskBody = tasks.length ? tasks.map(renderGroup).join('') : '';
      const body = (taskBody || downstreamTablesHtml)
        ? (taskBody + downstreamTablesHtml)
        : emptyHint;
      return fallbackHint + toolbar + body;
    }

    function renderNodeSchedule(nodeId, data) {
      const content = document.getElementById('lbContent');
      const activeTab = document.querySelector('.lb-tab.active');
      const tab = activeTab ? activeTab.dataset.lb : 'overview';
      const recentRuns = data.recent_runs || [];
      const latestRun = recentRuns[0] || null;
      const producers = withoutPipelineTasks(data.producers);
      const consumers = Array.isArray(data.consumers) ? data.consumers : [];
      const summary = data.schedule_summary || {};
      const producerCount = data.producer_count ?? producers.length ?? summary.task_count ?? 0;
      const consumerCount = data.consumer_count ?? consumers.length ?? 0;
      const overviewNote = data.overview_note || '';
      const primaryProducer = producers[0] || {};
      const taskDisplay = summary.latest_task_name || primaryProducer.task_name || '';
      const cycleDisplay = primaryProducer.schedule_cycle_text || summary.schedule_cycle_text || '';
      const sourceTableNames = Array.isArray(data.source_table_names) ? data.source_table_names : [];
      const isSourceFallback = !!data.is_source_fallback;
      const lineageVerified = !!data.lineage_verified;
      const verifiedBadge = lineageVerified
        ? `<div style="color:#16a34a;font-size:11px;margin-top:2px">✓ 已由连接级血缘佐证（fdl_connection_lineage）</div>`
        : '';
      const firstRunTaskName = recentRuns[0]?.task_name || recentRuns[0]?.node_name || '';
      const allRunsSameTask = !!firstRunTaskName && recentRuns.every(r => (r.task_name || r.node_name || '') === firstRunTaskName);
      const spotlightHTML = taskDisplay
        ? `<div class="lb-item" style="flex-direction:column;gap:4px;margin-top:8px">
            <div style="font-weight:600;font-size:13px">当前产出任务: ${escapeXml(taskDisplay)}${cycleDisplay ? ` · ${escapeXml(cycleDisplay)}` : ''}</div>
            ${primaryProducer.node_name ? `<div style="color:#9aa0a6;font-size:11px">节点: ${escapeXml(primaryProducer.node_name)}</div>` : ''}
            ${verifiedBadge}
            ${overviewNote ? `<div style="color:#9aa0a6;font-size:11px">${escapeXml(overviewNote)}</div>` : ''}
          </div>`
        : (overviewNote ? `<div class="lb-item" style="margin-top:8px;color:#9aa0a6;font-size:11px">${escapeXml(overviewNote)}</div>` : '');

      switch(tab) {
        case 'overview':
          content.innerHTML = `
            <div style="display:flex;gap:20px;margin-bottom:12px">
              <div><span style="color:#9aa0a6;font-size:11px">产出任务</span><br><span style="font-size:18px;font-weight:700">${producerCount}</span></div>
              <div><span style="color:#9aa0a6;font-size:11px">消费者</span><br><span style="font-size:18px;font-weight:700">${consumerCount}</span></div>
              <div><span style="color:#9aa0a6;font-size:11px">运行次数</span><br><span style="font-size:18px;font-weight:700">${summary.run_count||0}</span></div>
              <div><span style="color:#9aa0a6;font-size:11px">状态</span><br><span style="font-size:14px;font-weight:600">${escapeXml(summary.status_label||'—')}</span></div>
            </div>${spotlightHTML}` + (latestRun ? `<div style="font-weight:600;font-size:12px;margin:10px 0 6px">最新运行</div>
            ${[latestRun].map(r => `
              <div class="lb-item">
                <div style="display:flex;align-items:center;gap:8px;width:100%">
                  <span class="stat ${(r.task_status||r.status||'').toLowerCase()}">${(r.task_status||r.status||'')==='SUCCESS'?'✓':(r.task_status||r.status||'')==='FAILED'?'✗':'◉'}</span>
                  <span>${escapeXml(r.task_name||r.node_name||'')}</span>
                  <span style="margin-left:auto;color:#5f6368;font-size:11px">${escapeXml(r.duration_text||'')}</span>
                </div>
                <div style="color:#9aa0a6;font-size:11px">${escapeXml(r.start_time_text||'')}</div>
              </div>`).join('')}
            <div style="color:#9aa0a6;font-size:11px;margin-top:6px">最近 10 次明细见「运行记录」页签</div>` : '');
          break;
        case 'producers':
          content.innerHTML = renderLineageList({
            kind: 'upstream',
            items: producers,
            nodeId, data,
            isSourceFallback,
          });
          break;
        case 'runs':
          content.innerHTML = `${isSourceFallback && sourceTableNames.length ? `<div style="color:#9aa0a6;font-size:11px;margin-bottom:8px">以下展示任务级归属对应的最近运行记录，来源表为 ${escapeXml(sourceTableNames.join(', '))}。</div>` : ''}${allRunsSameTask ? `<div style="font-weight:600;font-size:12px;margin-bottom:8px">${escapeXml(firstRunTaskName)} · 最近 10 次运行</div>` : ''}` + (recentRuns.length
            ? recentRuns.map(r =>
              `<div class="lb-item">
                <div style="display:flex;align-items:center;gap:8px;width:100%">
                  <span class="stat ${(r.task_status||r.status||'').toLowerCase()}">${(r.task_status||r.status||'')==='SUCCESS'?'✓':(r.task_status||r.status||'')==='FAILED'?'✗':'◉'}</span>
                  <span style="font-weight:500">${escapeXml(allRunsSameTask ? (r.start_time_text||r.finish_time_text||'') : (r.task_name||r.node_name||''))}</span>
                  <span style="margin-left:auto;color:#5f6368;font-size:11px">${escapeXml(r.duration_text||'')}</span>
                </div>
                <div style="color:#9aa0a6;font-size:11px">${escapeXml(r.start_time_text||'')} → ${escapeXml(r.finish_time_text||'')}</div>
                ${r.trigger_plan_name ? `<div style="color:#9aa0a6;font-size:11px">触发器: ${escapeXml(r.trigger_plan_name)}</div>` : ''}
              </div>`).join('')
            : '<div class="lb-empty">暂无运行记录</div>');
          break;
        case 'consumers': {
          content.innerHTML = renderLineageList({
            kind: 'downstream',
            items: consumers,
            nodeId, data,
            isSourceFallback: false,
          });
          break;
        }
        default:
          content.innerHTML = '<div class="lb-empty">暂无数据</div>';
      }
    }

    async function loadLineageForCurrentTable() {
      if (!state.profile) return;
      const tbl = state.profile.table || {};
      const qname = tbl.qualified_name || '';
      const parts = qname.split('.');
      const schema = parts[0] || 'public';
      const table = parts.slice(1).join('.') || '';
      if (!table) return;
      const conn = tbl.lineage_connection || 'PG';
      const kind = (tbl.source_type === 'PostgreSQL') ? 'target' : 'graph';

      const infoEl = document.getElementById('lineageInfo');
      const summaryEl = document.getElementById('lineageSummary');
      const openFullLink = document.getElementById('lineageOpenFull');
      const canvasEl = document.getElementById('lineageCanvas');
      if (infoEl) infoEl.textContent = '加载中…';
      if (summaryEl) summaryEl.textContent = '';
      document.getElementById('lineageSidebar')?.classList.remove('open');

      let lineageOk = false;
      try {
        const resp = await fetch(`/api/lineage-graph?connection_name=${encodeURIComponent(conn)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&upstream_depth=12&lineage_kind=${kind}`);
        const graph = await resp.json();
        if (resp.ok && graph.nodes && graph.nodes.length) {
          if (infoEl) infoEl.textContent = `血缘图 · ${graph.nodes.length} 个节点, ${graph.edges.length} 条关系`;
          if (openFullLink) openFullLink.href = `http://127.0.0.1:8766/viewer?mode=target_table&connection_name=${encodeURIComponent(conn)}&schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&upstream_depth=12`;
          renderLineageGraph(graph);
          lineageOk = true;
        }
      } catch(e) {/* 不报错，回退到调度信息 */}

      if (!lineageOk) {
        // 表不在血缘里：回退到"任务级调度信息"
        try {
          const comment = (state.profile.table || {}).table_comment || (state.profile.table_comment || '');
          const params = new URLSearchParams({ connection_name: conn, schema, table, run_limit: '10' });
          if (comment) params.set('table_comment', comment);
          const sresp = await fetch(`/api/table-schedule?${params.toString()}`);
          const sched = await sresp.json();
          if (!sresp.ok) throw new Error(sched.error || '加载调度信息失败');
          renderTableScheduleFallback(sched, canvasEl, infoEl);
        } catch(e2) {
          if (canvasEl) canvasEl.innerHTML = `<div class="lineage-empty">${escapeHtml(e2.message)}</div>`;
          if (infoEl) infoEl.textContent = '无血缘 / 无调度信息';
        }
      }
    }

    // 血缘空时的回退渲染：直接展示任务+调度+运行
    function renderTableScheduleFallback(data, canvasEl, infoEl) {
      const producers = data.producers || [];
      const runs = data.recent_runs || [];
      if (!producers.length) {
        if (canvasEl) canvasEl.innerHTML = `<div class="lineage-empty" style="flex-direction:column;gap:6px;text-align:center">
          <div>该表未在血缘图中建模</div>
          <div style="font-size:11px;color:#9aa0a6">未通过任务名匹配到相关任务（候选 ${data.candidate_work_count||0} 个）</div>
        </div>`;
        if (infoEl) infoEl.textContent = '无血缘信息';
        return;
      }
      if (infoEl) infoEl.textContent = `任务级调度 · ${producers.length} 个相关任务, ${runs.length} 条运行记录`;
      const taskListHtml = producers.map(p => {
        const sched = [];
        if (p.schedule_plan_name) sched.push(`调度计划: ${escapeHtml(p.schedule_plan_name)}`);
        if (p.schedule_cycle_text) sched.push(`周期: ${escapeHtml(p.schedule_cycle_text)}`);
        if (p.schedule_start_time_text) sched.push(`起点: ${escapeHtml(p.schedule_start_time_text)}`);
        const ops = (p.operator_types || []).map(o => o === 'DB_WRITE' ? '写入' : o === 'DB_READ' ? '读取' : o).join('/');
        const summary = p.schedule_summary || {};
        const taskRuns = (p.recent_runs || []).slice(0, 5);
        return `<div class="lb-item" style="flex-direction:column;gap:4px;margin-bottom:8px">
          <div style="display:flex;align-items:center;gap:8px;flex-wrap:wrap">
            <span style="font-weight:600;font-size:13px">${escapeHtml(p.task_name)}</span>
            ${p.resource_type ? `<span style="font-size:11px;color:#9aa0a6">${escapeHtml(taskTypeLabel(p.resource_type))}</span>` : ''}
            ${ops ? `<span style="font-size:11px;color:#0d9488">${escapeHtml(ops)}</span>` : ''}
            ${p.node_name ? `<span style="font-size:11px;color:#9aa0a6">节点: ${escapeHtml(p.node_name)}</span>` : ''}
          </div>
          ${sched.length ? `<div style="color:#5f6368;font-size:11px">${sched.join(' · ')}</div>` : ''}
          ${summary.status_label ? `<div style="font-size:11px">最近状态: <span class="stat ${(summary.latest_status||'').toLowerCase()}">${escapeHtml(summary.status_label)}</span> · ${escapeHtml(summary.latest_time_text||'')}</div>` : ''}
          ${taskRuns.length ? `<div style="margin-top:4px;border-top:1px dashed #e8eaed;padding-top:4px">
            ${taskRuns.map(r => `<div style="display:flex;gap:8px;font-size:11px;color:#5f6368;align-items:center">
              <span class="stat ${(r.task_status||'').toLowerCase()}" style="font-size:11px">${(r.task_status==='SUCCESS'?'✓':r.task_status==='FAILED'?'✗':'◉')}</span>
              <span>${escapeHtml(r.start_time_text||'')}</span>
              <span style="color:#9aa0a6">${escapeHtml(r.duration_text||'')}</span>
              <span style="color:#9aa0a6">${escapeHtml(r.trigger_method||'')}</span>
            </div>`).join('')}
          </div>` : ''}
        </div>`;
      }).join('');
      if (canvasEl) {
        canvasEl.innerHTML = `<div style="padding:14px 18px;overflow:auto;height:100%">
          <div style="background:#fffbe6;border:1px solid #ffe58f;color:#7a5b00;padding:6px 10px;border-radius:6px;font-size:11px;margin-bottom:10px">
            ⚠ 该表未在血缘图中建模，以下为通过任务名匹配找到的相关任务和调度信息（精度仅到任务级）
          </div>
          ${taskListHtml}
        </div>`;
      }
    }

    function openDictEditor(cell) {
      const defVal = cell.dataset.def || '';
      const enumVal = cell.dataset.enum || '';
      cell.innerHTML = `
        <div class="dict-editor">
          <input class="dict-input-def" placeholder="业务定义（必填）" value="${escapeHtml(defVal)}" />
          <textarea class="dict-input-enum" placeholder="枚举值说明（选填）如：1=待处理, 2=已完成">${escapeHtml(enumVal)}</textarea>
          <div class="dict-editor-row">
            <button class="save" type="button">保存</button>
            <button class="cancel" type="button">取消</button>
          </div>
        </div>
      `;
      const inputDef = cell.querySelector('.dict-input-def');
      const inputEnum = cell.querySelector('.dict-input-enum');
      inputDef.focus();
      cell.querySelector('.save').addEventListener('click', async (e) => {
        e.stopPropagation();
        const newDef = inputDef.value.trim();
        const newEnum = inputEnum.value.trim();
        cell.dataset.def = newDef;
        cell.dataset.enum = newEnum;
        await saveDictEntry(cell.dataset.schema, cell.dataset.table, cell.dataset.col, newDef, newEnum, cell);
      });
      cell.querySelector('.cancel').addEventListener('click', (e) => {
        e.stopPropagation();
        restoreDictDisplay(cell, cell.dataset.def, cell.dataset.enum);
      });
    }

    function restoreDictDisplay(cell, defVal, enumVal) {
      cell.innerHTML = `
        <div class="dict-display">
          ${defVal
            ? `<div class="dict-def">${escapeHtml(defVal)}<span class="dict-hint">✏</span></div>${enumVal ? `<div class="dict-enum">${escapeHtml(enumVal)}</div>` : ''}`
            : `<span class="dict-empty">— 点击定义</span>`
          }
        </div>
      `;
      cell.addEventListener('click', (e) => {
        if (cell.querySelector('.dict-editor')) return;
        openDictEditor(cell);
      }, { once: true });
    }

    async function saveDictEntry(schema, table, column, businessDef, enumValues, cell) {
      try {
        await fetchJson('/api/update-column-dict', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({ schema, table: table, column, business_def: businessDef, enum_values: enumValues }),
        });
        restoreDictDisplay(cell, businessDef, enumValues);
        refreshDictBadge();
      } catch (err) {
        cell.querySelector('.dict-editor-row')?.insertAdjacentHTML('afterend', `<div style="color:var(--danger);font-size:0.8em;margin-top:4px">${escapeHtml(err.message)}</div>`);
      }
    }

    function refreshDictBadge() {
      const cells = detailContent.querySelectorAll('.dict-cell');
      if (!cells.length) return;
      const total = cells.length;
      const defined = [...cells].filter((c) => c.dataset.def).length;
      const pct = Math.round(defined / total * 100) + '%';
      const badge = detailContent.querySelector('.dict-complete-badge');
      if (badge) badge.innerHTML = `字典完整度 <span class="pct">${pct}</span>（${defined}/${total}）`;
    }

    async function loadDashboard() {
      const dashboard = await fetchJson('/api/dashboard');
      renderDashboard(dashboard);
    }

    async function searchTables() {
      try {
        const search = buildSearchParams();
        const payload = await fetchJson(`/api/search?${search.toString()}`);
        renderResults(payload);
        syncPageUrl();
      } catch (error) {
        console.error('searchTables failed', error);
        searchError.classList.remove('hidden');
        searchError.textContent = error.message || '搜索失败';
        // 同时把错误直接显示在结果区，避免空白
        if (resultsList) resultsList.innerHTML = `<div class="error" style="margin:12px 0">渲染失败：${escapeHtml(error.message || String(error))}</div>`;
      }
    }

    async function selectTable(schema, table) {
      const key = `${schema}.${table}`;
      state.selectedKey = key;
      markActiveResult(key);
      syncPageUrl();
      // 切表前清空血缘图相关残留，避免上一张表的"概览/上游/运行记录/下游"内容串联到下一张
      resetLineagePanel();
      try {
        const profile = await fetchJson(`/api/table-profile?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&sample_limit=20`);
        renderProfile(profile);
      } catch (error) {
        if (detailEmpty) detailEmpty.style.display = 'none';
        detailPanel.classList.remove('hidden');
        detailHero && (detailHero.innerHTML = '');
        detailStats.innerHTML = '';
        detailContent.innerHTML = `<div class="error" style="margin:16px 20px">${escapeHtml(error.message || '加载表详情失败')}</div>`;
      }
    }

    // 清空血缘图相关的 DOM 与状态（lbContent / lsDetailBody / lsImpactBody / 画布等）
    function resetLineagePanel() {
      lineageSelectedNodeId = null;
      window._lineageMeta = null;
      const ids = ['lbContent', 'lsDetailBody', 'lsImpactBody'];
      ids.forEach((id) => { const el = document.getElementById(id); if (el) el.innerHTML = ''; });
      const canvas = document.getElementById('lineageCanvas');
      if (canvas) canvas.innerHTML = '<div class="lineage-empty">点击"血缘图"标签加载</div>';
      const info = document.getElementById('lineageInfo'); if (info) info.textContent = '';
      const summary = document.getElementById('lineageSummary'); if (summary) summary.textContent = '';
      document.getElementById('lineageSidebar')?.classList.remove('open');
    }

    function selectTableByKey(key) {
      if (!key) return;
      const [schema, table] = key.split('.');
      if (!schema || !table) return;
      selectTable(schema, table);
    }

    async function toggleFavoriteByKey(key) {
      const [schema, table] = key.split('.');
      if (!schema || !table) return;
      await fetchJson('/api/toggle-favorite', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ schema, table }),
      });
      await loadDashboard();
      await searchTables();
      if (state.selectedKey === key) {
        await selectTable(schema, table);
      }
    }

    async function saveMetadata() {
      if (!state.selectedKey) return;
      const [schema, table] = state.selectedKey.split('.');
      const feedback = document.getElementById('metadataFeedback');
      try {
        feedback.innerHTML = '';
        await fetchJson('/api/update-table-metadata', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            schema,
            table,
            alias: document.getElementById('metaAlias').value.trim(),
            owner: document.getElementById('metaOwner').value.trim(),
            business_terms: document.getElementById('metaTerms').value.trim(),
            business_domain: document.getElementById('metaDomain').value.trim(),
            project: document.getElementById('metaProject').value.trim(),
            storage_type: document.getElementById('metaStorage').value.trim(),
            ttl_days: document.getElementById('metaTtl').value.trim(),
            source_type: document.getElementById('metaSourceType').value.trim(),
            created_time_text: document.getElementById('metaCreatedTime').value.trim(),
            last_modified_time_text: document.getElementById('metaUpdatedTime').value.trim(),
            description: document.getElementById('metaDescription').value.trim(),
          }),
        });
        feedback.innerHTML = '<div class="pill">元数据已保存</div>';
        await loadDashboard();
        await searchTables();
        await selectTable(schema, table);
      } catch (error) {
        feedback.innerHTML = `<div class="error">${escapeHtml(error.message || '保存失败')}</div>`;
      }
    }

    searchForm.addEventListener('submit', async (event) => {
      event.preventDefault();
      await searchTables();
    });

    clearFiltersButton.addEventListener('click', async () => {
      keywordInput.value = '';
      if (favToggle) favToggle.checked = false;
      Object.values(facetState).forEach((set) => set.clear());
      if (sortSelect) sortSelect.value = 'relevance';
      await searchTables();
    });

    // 分面：清空
    facetClearBtn?.addEventListener('click', () => {
      if (facetActiveCount() === 0) return;
      Object.values(facetState).forEach((set) => set.clear());
      computeAndRenderFacets();
      applyFacetsAndRender();
    });

    // 排序切换（纯前端）
    sortSelect?.addEventListener('change', () => applyFacetsAndRender());

    // 只看收藏（后端过滤，需重新检索）
    favToggle?.addEventListener('change', async () => { await searchTables(); });

    favoriteToggle.addEventListener('click', async () => {
      if (!state.selectedKey) return;
      await toggleFavoriteByKey(state.selectedKey);
    });

    refreshCatalogButton.addEventListener('click', async () => {
      await fetchJson('/api/refresh');
      await loadDashboard();
      await searchTables();
      if (state.selectedKey) selectTableByKey(state.selectedKey);
    });
    document.getElementById('openDatasets')?.addEventListener('click', () => {
      window.location.href = '/datasets';
    });
    document.getElementById('openDashboards')?.addEventListener('click', () => {
      window.location.href = '/dashboards';
    });

    copyCurrentLinkButton?.addEventListener('click', async () => {
      await navigator.clipboard.writeText(window.location.href);
    });

    backBtn?.addEventListener('click', () => {
      state.selectedKey = '';
      detailPanel.classList.add('hidden');
      markActiveResult('');
      syncPageUrl();
    });

    toggleFiltersBtn?.addEventListener('click', () => {
      filterPanel.classList.toggle('hidden');
      toggleFiltersBtn.textContent = filterPanel.classList.contains('hidden') ? '筛选 ▾' : '筛选 ▴';
    });

    // ── 三栏可拖拽分隔条 ──
    const SPLIT_DEFAULTS = { facets: 240, results: 380 };
    const SPLIT_MIN = { facets: 180, results: 300 };
    const SPLIT_MAX = { facets: 340, results: 600 };
    const DETAIL_MIN = 400;
    const catalogBody = document.querySelector('.catalog-body');

    function getColWidth(which) {
      const v = getComputedStyle(catalogBody).getPropertyValue('--w-' + which);
      return parseFloat(v) || SPLIT_DEFAULTS[which];
    }
    function setColWidth(which, px) { catalogBody.style.setProperty('--w-' + which, Math.round(px) + 'px'); }
    function clampWidth(which, px) {
      let v = Math.max(SPLIT_MIN[which], Math.min(SPLIT_MAX[which], px));
      const total = catalogBody.clientWidth;
      const facetsVisible = getComputedStyle(document.getElementById('facets')).display !== 'none';
      const splitterPx = facetsVisible ? 16 : 8;
      const otherW = which === 'results' ? (facetsVisible ? getColWidth('facets') : 0) : getColWidth('results');
      const maxForDetail = total - otherW - splitterPx - DETAIL_MIN;
      if (maxForDetail > SPLIT_MIN[which]) v = Math.min(v, maxForDetail);
      return Math.round(v);
    }
    // 用内联栅格统一处理 折叠 + 响应式，优先级最高，避免规则打架
    function recomputeGrid() {
      if (!catalogBody) return;
      const fC = catalogBody.classList.contains('facets-collapsed');
      const dC = catalogBody.classList.contains('detail-collapsed');
      const narrow = catalogBody.clientWidth <= 1100;
      const rTrack = dC ? 'minmax(0, 1fr)' : 'var(--w-results)';
      const dTrack = dC ? '0px' : 'minmax(0, 1fr)';
      if (narrow) {
        catalogBody.style.gridTemplateColumns = rTrack + ' 8px ' + dTrack;
      } else {
        const fTrack = fC ? '0px' : 'var(--w-facets)';
        catalogBody.style.gridTemplateColumns = fTrack + ' 8px ' + rTrack + ' 8px ' + dTrack;
      }
      // 重新定位折叠按钮（栅格变了，边界跟着变）
      positionToggles();
    }

    // 把折叠按钮钉在它控制的那块面板的内侧边上（用栅格变量直接算，零时序问题）
    function positionToggles() {
      if (!catalogBody) return;
      const total = catalogBody.clientWidth;
      const facetsBtn = document.querySelector('.col-toggle[data-collapse="facets"]');
      const detailBtn = document.querySelector('.col-toggle[data-collapse="detail"]');
      const fC = catalogBody.classList.contains('facets-collapsed');
      const dC = catalogBody.classList.contains('detail-collapsed');
      const narrow = total <= 1100;
      const wFacets = narrow || fC ? 0 : getColWidth('facets');
      const wResults = dC ? (total - (narrow ? 8 : 8 + wFacets + 8)) : getColWidth('results');
      const SPL = 8;

      if (facetsBtn) {
        if (narrow) { facetsBtn.style.display = 'none'; }
        else {
          facetsBtn.style.display = '';
          // 折叠时按钮一半要落在容器内（避免被裁），所以最少 10px
          facetsBtn.style.left = Math.max(10, wFacets) + 'px';
        }
      }
      if (detailBtn) {
        detailBtn.style.display = '';
        const facetsPart = narrow ? 0 : (wFacets + SPL);
        const xRaw = dC ? total : (facetsPart + wResults + SPL);
        // 折叠时按钮一半要落在容器内，所以最多 total-10
        detailBtn.style.left = Math.min(total - 10, xRaw) + 'px';
      }
    }
    function collapseClass(kind) { return kind === 'facets' ? 'facets-collapsed' : 'detail-collapsed'; }
    function applyCollapse(kind, collapsed, save) {
      catalogBody.classList.toggle(collapseClass(kind), collapsed);
      // 折叠态不持久化，刷新即恢复；按钮箭头由 CSS 自动旋转
      recomputeGrid();
    }

    function setupSplitters() {
      if (!catalogBody) return;
      try {
        // URL 上加 ?reset=layout 可一键清除布局记忆
        if (params.get('reset') === 'layout') {
          ['dm_w_facets','dm_w_results','dm_collapse_facets','dm_collapse_detail'].forEach(k => localStorage.removeItem(k));
        }
        // 清掉历史的折叠记忆（折叠态只在当前会话内生效，刷新即恢复展开）
        localStorage.removeItem('dm_collapse_facets');
        localStorage.removeItem('dm_collapse_detail');
        // 恢复保存的宽度
        ['facets', 'results'].forEach((w) => {
          const saved = parseFloat(localStorage.getItem('dm_w_' + w));
          if (saved) setColWidth(w, clampWidth(w, saved));
        });
      } catch (e) { console.warn('splitter restore failed', e); }

      document.querySelectorAll('.splitter').forEach((sp) => {
        const which = sp.dataset.resize;
        const cls = which === 'facets' ? 'facets-collapsed' : 'detail-collapsed';
        sp.addEventListener('mousedown', (e) => {
          if (catalogBody.classList.contains(cls)) return;     // 已折叠则不可拖
          e.preventDefault();
          const startX = e.clientX;
          const startW = getColWidth(which);
          sp.classList.add('dragging');
          document.body.classList.add('col-resizing');
          const onMove = (ev) => { setColWidth(which, clampWidth(which, startW + (ev.clientX - startX))); positionToggles(); };
          const onUp = () => {
            document.removeEventListener('mousemove', onMove);
            document.removeEventListener('mouseup', onUp);
            sp.classList.remove('dragging');
            document.body.classList.remove('col-resizing');
            localStorage.setItem('dm_w_' + which, getColWidth(which));
            positionToggles();
          };
          document.addEventListener('mousemove', onMove);
          document.addEventListener('mouseup', onUp);
        });
        sp.addEventListener('dblclick', () => {
          if (catalogBody.classList.contains(cls)) return;
          setColWidth(which, SPLIT_DEFAULTS[which]);
          localStorage.removeItem('dm_w_' + which);
          positionToggles();
        });
      });

      // 折叠按钮（顶层浮动元素）
      document.querySelectorAll('.col-toggle').forEach((btn) => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const kind = btn.dataset.collapse;
          applyCollapse(kind, !catalogBody.classList.contains(collapseClass(kind)));
        });
      });

      // 初始套用 + 窗口缩放跟随
      recomputeGrid();
      let _rt; window.addEventListener('resize', () => { clearTimeout(_rt); _rt = setTimeout(recomputeGrid, 80); });
    }

    async function bootstrap() {
      initFormFromParams();
      renderProfilePlaceholder();
      try { setupSplitters(); } catch (e) { console.error('setupSplitters error', e); }

      // Tab 事件只绑一次（Tab 在静态 HTML 里）
      document.getElementById('detailTabs')?.querySelectorAll('.detail-tab').forEach((tab) => {
        tab.addEventListener('click', () => {
          setDetailTab(tab.dataset.tab || 'structure');
        });
      });

      // 血缘工具栏
      document.getElementById('lineageFit')?.addEventListener('click', fitLineageToScreen);
      document.getElementById('lineageZoomIn')?.addEventListener('click', () => { lineageZoom = Math.min(2, lineageZoom + 0.15); applyLineageZoom(); });
      document.getElementById('lineageZoomOut')?.addEventListener('click', () => { lineageZoom = Math.max(0.2, lineageZoom - 0.15); applyLineageZoom(); });

      // 侧边栏关闭
      document.getElementById('lsClose')?.addEventListener('click', () => document.getElementById('lineageSidebar').classList.remove('open'));

      // 调度 Tab 切换
      // 调度 Tab 切换（事件委托，因为 tab 是动态生成的）
      document.addEventListener('click', (e) => {
        const tab = e.target.closest('.lb-tab');
        if (!tab) return;
        document.querySelectorAll('.lb-tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        if (lineageSelectedNodeId) loadNodeSchedule(lineageSelectedNodeId, window._lineageMeta);
      });

      await loadDashboard();
      await searchTables();
    }

    bootstrap();
  </script>
</body>
</html>
"""

SQL_WORKBENCH_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>SQL 工作台</title>
  <style>
    :root {
      --bg: #eef1f5;
      --surface: #ffffff;
      --surface-soft: #f8fafc;
      --ink: #111827;
      --muted: #6b7280;
      --line: #dbe2ea;
      --brand: #1a73e8;
      --brand-soft: rgba(26,115,232,0.08);
      --danger: #dc2626;
      --ok: #0d9488;
      --radius: 10px;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Helvetica Neue", sans-serif; font-size: 13px; color: var(--ink); background: var(--bg); min-height: 100vh; overflow: hidden; }
    button, input, textarea, select { font: inherit; }
    .page { --wb-result-height: 320px; display: grid; grid-template-rows: 48px minmax(0, 1fr) 10px var(--wb-result-height); height: 100vh; overflow: hidden; }
    .topbar { display: flex; align-items: center; justify-content: space-between; gap: 12px; background: #111827; color: #fff; padding: 0 18px; }
    .brand { display: flex; align-items: center; gap: 8px; font-weight: 700; }
    .dot { width: 8px; height: 8px; border-radius: 999px; background: #1a73e8; }
    .topbar-actions { display: flex; align-items: center; gap: 8px; }
    .topbar-btn { height: 30px; padding: 0 12px; border: 1px solid rgba(255,255,255,0.14); border-radius: 8px; background: rgba(255,255,255,0.08); color: rgba(255,255,255,0.92); cursor: pointer; }
    .topbar-btn:hover { background: rgba(255,255,255,0.14); }
    .shell { display: grid; grid-template-columns: 220px minmax(0, 1fr) 480px; gap: 1px; min-height: 0; background: var(--line); }
    .shell.meta-collapsed { grid-template-columns: 220px minmax(0, 1fr) 54px; }
    .shell.meta-collapsed .panel.meta { display: none; }
    .meta-rail { display: none; min-width: 0; min-height: 0; background: var(--surface); align-items: center; justify-content: center; border-left: 1px solid var(--line); }
    .shell.meta-collapsed .meta-rail { display: flex; }
    .meta-rail-btn { width: 36px; min-height: 180px; border: 1px solid var(--line); border-radius: 12px; background: #fff; color: var(--muted); cursor: pointer; writing-mode: vertical-rl; text-orientation: mixed; letter-spacing: 0.08em; padding: 12px 8px; }
    .meta-rail-btn:hover { background: var(--brand-soft); color: var(--brand); border-color: rgba(26,115,232,0.24); }
    .panel { min-width: 0; min-height: 0; background: var(--surface); display: flex; flex-direction: column; }
    .panel.meta { min-width: 380px; }
    .panel.result { border-top: 1px solid var(--line); overflow: hidden; }
    .result-splitter { position: relative; background: linear-gradient(180deg, rgba(148,163,184,0.14), rgba(148,163,184,0.24)); cursor: ns-resize; touch-action: none; }
    .result-splitter::before { content: ""; position: absolute; left: 50%; top: 50%; width: 72px; height: 4px; border-radius: 999px; background: rgba(148,163,184,0.72); transform: translate(-50%, -50%); transition: background 0.18s ease, width 0.18s ease; }
    .result-splitter:hover::before, .result-splitter.dragging::before { width: 96px; background: rgba(26,115,232,0.92); }
    body.result-resizing, body.result-resizing * { cursor: ns-resize !important; user-select: none !important; }
    .panel-head { padding: 14px 16px 12px; border-bottom: 1px solid var(--line); display: grid; gap: 8px; }
    .panel-head.meta-head { grid-template-columns: minmax(0, 1fr) auto; align-items: start; }
    .panel-title { font-size: 18px; font-weight: 700; }
    .panel-sub { color: var(--muted); font-size: 12px; line-height: 1.5; }
    .tree-actions, .editor-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .btn-primary, .btn-ghost { height: 32px; padding: 0 12px; border-radius: 8px; cursor: pointer; }
    .btn-primary { border: 0; background: var(--brand); color: #fff; font-weight: 600; }
    .btn-ghost { border: 1px solid var(--line); background: var(--surface); color: var(--muted); }
    .tree-body, .meta-body, .editor-body, .result-body { min-height: 0; overflow: auto; }
    .tree-body { padding: 10px 10px 16px; }
    .tree-node { display: grid; gap: 4px; }
    .tree-row { display: grid; grid-template-columns: minmax(0, 1fr) auto; gap: 6px; align-items: center; }
    .tree-folder-label, .tree-file-label { width: 100%; text-align: left; border: 0; background: transparent; cursor: pointer; border-radius: 8px; padding: 8px 10px; color: var(--ink); min-width: 0; }
    .tree-folder-label:hover, .tree-file-label:hover { background: var(--brand-soft); }
    .tree-folder-label.active { background: rgba(180,83,9,0.10); color: #92400e; font-weight: 700; }
    .tree-file-label.active { background: var(--brand-soft); color: var(--brand); font-weight: 700; }
    .tree-item-title { display: inline-block; max-width: 100%; overflow: hidden; text-overflow: ellipsis; vertical-align: bottom; }
    .tree-item-menu-btn { width: 28px; height: 28px; border: 1px solid transparent; border-radius: 8px; background: transparent; color: #94a3b8; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; }
    .tree-item-menu-btn:hover { background: rgba(26,115,232,0.08); color: var(--brand); border-color: rgba(26,115,232,0.14); }
    .tree-item-menu-btn.hidden { visibility: hidden; }
    .tree-children { padding-left: 12px; display: grid; gap: 4px; }
    .folder-mark { color: #b45309; margin-right: 6px; }
    .file-mark { color: var(--brand); margin-right: 6px; }
    .tree-selection { color: var(--muted); font-size: 12px; line-height: 1.5; }
    .tree-menu { position: fixed; z-index: 90; min-width: 148px; border: 1px solid rgba(148,163,184,0.22); border-radius: 12px; background: #fff; box-shadow: 0 18px 48px rgba(15,23,42,0.20); padding: 6px; display: grid; gap: 4px; }
    .tree-menu.hidden { display: none; }
    .tree-menu button { border: 0; background: transparent; text-align: left; border-radius: 8px; padding: 8px 10px; font-size: 12px; color: var(--ink); cursor: pointer; }
    .tree-menu button:hover { background: rgba(26,115,232,0.08); color: var(--brand); }
    .tree-menu button.danger:hover { background: rgba(220,38,38,0.08); color: var(--danger); }
    .editor-head-row { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .editor-meta { color: var(--muted); font-size: 12px; }
    .linked-table-card { display: grid; gap: 8px; padding: 12px; border: 1px solid var(--line); border-radius: 10px; background: var(--surface-soft); }
    .linked-table-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
    .linked-table-label { font-size: 12px; color: var(--muted); }
    .linked-table-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .editor-body { padding: 14px 16px 18px; display: grid; grid-template-rows: auto minmax(0, 1fr) auto; gap: 12px; }
    .field-inline { width: 100%; height: 36px; border: 1px solid var(--line); border-radius: 8px; background: var(--surface-soft); padding: 0 12px; }
    .sql-editor { width: 100%; min-height: 420px; height: clamp(420px, 48vh, 720px); border: 1px solid var(--line); border-radius: 10px; background: #0f141b; color: #e5e7eb; padding: 16px; font: 13px/1.65 "SF Mono", ui-monospace, monospace; resize: vertical; outline: none; }
    .editor-toolbar { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .editor-toolbar .spacer { flex: 1; }
    .editor-note { color: var(--muted); font-size: 12px; }
    .status { min-height: 18px; font-size: 12px; color: var(--muted); }
    .status.ok { color: var(--ok); }
    .status.error { color: var(--danger); }
    .result-body { flex: 1 1 auto; padding: 14px 16px 18px; }
    .result-card { border: 1px solid var(--line); border-radius: 10px; background: var(--surface-soft); padding: 12px; display: grid; gap: 10px; }
    .result-meta { color: var(--muted); font-size: 12px; }
    .result-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .result-stack { display: grid; gap: 10px; }
    .result-notices { display: grid; gap: 6px; }
    .result-notice { padding: 8px 10px; border-radius: 8px; background: rgba(26,115,232,0.08); color: #1d4ed8; font-size: 12px; line-height: 1.5; border: 1px solid rgba(26,115,232,0.14); }
    .pager { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; color: var(--muted); font-size: 12px; }
    .pager input { width: 76px; }
    .table-wrap { overflow: auto; border: 1px solid var(--line); border-radius: 8px; background: #fff; }
    table { width: 100%; border-collapse: collapse; min-width: 680px; }
    th, td { padding: 8px 12px; border-bottom: 1px solid var(--line); text-align: left; vertical-align: top; font-size: 12px; line-height: 1.5; }
    th:not(:last-child), td:not(:last-child) { border-right: 1px solid rgba(148,163,184,0.16); }
    th { position: sticky; top: 0; background: #f8fafc; color: #6b7280; font-size: 11px; text-transform: uppercase; letter-spacing: 0.04em; }
    .th-head { display: flex; align-items: center; justify-content: space-between; gap: 8px; min-width: 0; }
    .th-title { min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .th-filter-btn { width: 22px; height: 22px; border: 1px solid transparent; border-radius: 6px; background: transparent; color: #94a3b8; cursor: pointer; display: inline-flex; align-items: center; justify-content: center; flex: 0 0 auto; }
    .th-filter-btn:hover { background: rgba(26,115,232,0.08); color: var(--brand); border-color: rgba(26,115,232,0.14); }
    .th-filter-btn.active { background: rgba(26,115,232,0.10); color: var(--brand); border-color: rgba(26,115,232,0.18); }
    .filter-popover { position: fixed; z-index: 80; width: min(280px, calc(100vw - 24px)); max-height: min(420px, calc(100vh - 24px)); border: 1px solid rgba(148,163,184,0.22); border-radius: 12px; background: #fff; box-shadow: 0 20px 56px rgba(15,23,42,0.22); display: grid; overflow: hidden; }
    .filter-popover.hidden { display: none; }
    .filter-popover-head { padding: 12px 14px 10px; border-bottom: 1px solid var(--line); display: grid; gap: 4px; }
    .filter-popover-title { font-size: 13px; font-weight: 700; color: var(--ink); }
    .filter-popover-sub { font-size: 11px; color: var(--muted); }
    .filter-popover-search { margin: 10px 14px 0; width: calc(100% - 28px); height: 32px; border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 0 10px; font-size: 12px; }
    .filter-popover-body { padding: 10px 14px; overflow: auto; display: grid; gap: 6px; align-content: start; }
    .filter-popover-option { display: flex; align-items: center; gap: 8px; font-size: 12px; color: var(--ink); }
    .filter-popover-option input { margin: 0; }
    .filter-popover-option .label { flex: 1; min-width: 0; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
    .filter-popover-option .count { color: var(--muted); font-size: 11px; }
    .filter-popover-empty { padding: 12px 0; color: var(--muted); font-size: 12px; text-align: center; }
    .filter-popover-actions { padding: 12px 14px 14px; border-top: 1px solid var(--line); display: flex; justify-content: space-between; gap: 8px; }
    .result-filter-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .result-filter-hint { color: var(--muted); font-size: 12px; }
    tbody tr:hover td { background: rgba(26,115,232,0.035); }
    .empty, .error { padding: 18px; border-radius: 10px; font-size: 12px; }
    .empty { border: 1px dashed var(--line); background: var(--surface-soft); color: var(--muted); text-align: center; }
    .error { border: 1px solid rgba(220,38,38,0.18); background: rgba(220,38,38,0.06); color: var(--danger); }
    .modal-backdrop { position: fixed; inset: 0; background: rgba(15,23,42,0.42); display: flex; align-items: center; justify-content: center; padding: 24px; z-index: 50; }
    .modal-backdrop.hidden { display: none; }
    .modal-card { width: min(720px, 100%); max-height: calc(100vh - 48px); overflow: auto; border-radius: 16px; background: #fff; box-shadow: 0 28px 80px rgba(15,23,42,0.28); border: 1px solid rgba(148,163,184,0.24); }
    .modal-head { padding: 18px 20px 12px; border-bottom: 1px solid var(--line); display: grid; gap: 6px; }
    .modal-title { font-size: 20px; font-weight: 700; }
    .modal-sub { color: var(--muted); font-size: 12px; line-height: 1.6; }
    .modal-body { padding: 18px 20px; display: grid; gap: 14px; }
    .modal-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; }
    .modal-field { display: grid; gap: 6px; }
    .modal-field label { font-size: 12px; color: var(--muted); }
    .modal-field textarea { min-height: 180px; border: 1px solid var(--line); border-radius: 10px; background: #fff; padding: 12px; font: 13px/1.6 "SF Mono", ui-monospace, monospace; resize: vertical; }
    .modal-actions { padding: 0 20px 20px; display: flex; justify-content: flex-end; gap: 10px; }
    .ai-modal-card { width: min(1160px, 100%); }
    .ai-layout { display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(340px, 0.85fr); gap: 16px; }
    .ai-column { display: grid; gap: 12px; min-width: 0; }
    .ai-card { border: 1px solid var(--line); border-radius: 12px; background: var(--surface-soft); padding: 14px; display: grid; gap: 10px; min-width: 0; }
    .ai-card h4 { font-size: 14px; }
    .ai-sub { color: var(--muted); font-size: 12px; line-height: 1.6; }
    .ai-chip-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .ai-chip { height: 30px; padding: 0 12px; border-radius: 999px; border: 1px solid rgba(26,115,232,0.18); background: rgba(26,115,232,0.08); color: var(--brand); cursor: pointer; }
    .ai-card-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .ai-thread { min-height: 380px; max-height: min(52vh, 640px); overflow: auto; display: grid; gap: 12px; align-content: start; }
    .ai-empty { padding: 18px; border: 1px dashed rgba(148,163,184,0.45); border-radius: 12px; color: var(--muted); background: rgba(255,255,255,0.78); text-align: center; line-height: 1.6; }
    .ai-msg { display: grid; gap: 6px; padding: 12px 14px; border-radius: 12px; border: 1px solid var(--line); background: #fff; }
    .ai-msg.user { background: rgba(26,115,232,0.08); border-color: rgba(26,115,232,0.16); }
    .ai-msg.assistant { background: #fff; }
    .ai-role { font-size: 11px; font-weight: 700; letter-spacing: 0.04em; text-transform: uppercase; color: var(--muted); }
    .ai-msg.user .ai-role { color: var(--brand); }
    .ai-body { white-space: pre-wrap; word-break: break-word; line-height: 1.65; font-size: 13px; color: var(--ink); }
    .ai-prompt { width: 100%; min-height: 128px; border: 1px solid var(--line); border-radius: 10px; background: #fff; padding: 12px; font: 13px/1.65 inherit; resize: vertical; }
    .ai-context-input { width: 100%; min-height: 86px; border: 1px solid var(--line); border-radius: 10px; background: #fff; padding: 10px 12px; font: 12px/1.65 "SF Mono", ui-monospace, monospace; resize: vertical; }
    .ai-advanced { border-top: 1px dashed rgba(148,163,184,0.5); padding-top: 10px; display: grid; gap: 10px; }
    .ai-advanced summary { cursor: pointer; color: var(--muted); font-size: 12px; font-weight: 600; }
    .ai-stage-card { border: 1px solid rgba(26,115,232,0.14); border-radius: 12px; background: rgba(26,115,232,0.05); padding: 12px 14px; display: grid; gap: 8px; }
    .ai-stage-title { font-size: 13px; font-weight: 700; color: var(--ink); }
    .ai-stage-copy { font-size: 12px; color: var(--muted); line-height: 1.65; }
    .ai-stage-actions { display: flex; gap: 8px; flex-wrap: wrap; }
    .ai-rel-list { display: grid; gap: 8px; }
    .ai-rel-item { border: 1px solid rgba(148,163,184,0.2); border-radius: 10px; background: #fff; padding: 10px 12px; display: grid; gap: 5px; }
    .ai-rel-main { font-size: 12px; color: var(--ink); word-break: break-word; }
    .ai-rel-meta { display: flex; gap: 8px; flex-wrap: wrap; color: var(--muted); font-size: 11px; }
    .ai-rel-meta span { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 999px; background: rgba(148,163,184,0.12); }
    .ai-toolbar { display: flex; align-items: center; justify-content: space-between; gap: 8px; flex-wrap: wrap; }
    .ai-status { min-height: 18px; font-size: 12px; color: var(--muted); }
    .ai-status.ok { color: var(--ok); }
    .ai-status.error { color: var(--danger); }
    .ai-generated-sql { width: 100%; min-height: 260px; border: 1px solid #1f2937; border-radius: 12px; background: #0f141b; color: #e5e7eb; padding: 14px; font: 12px/1.65 "SF Mono", ui-monospace, monospace; resize: vertical; }
    .ai-context-list { display: grid; gap: 6px; font-size: 12px; color: var(--muted); line-height: 1.6; }
    .ai-follow-list { display: grid; gap: 6px; color: var(--muted); font-size: 12px; line-height: 1.6; }
    .ai-candidate-list { display: grid; gap: 10px; }
    .ai-candidate-item { width: 100%; text-align: left; border: 1px solid rgba(26,115,232,0.16); border-radius: 12px; background: #fff; padding: 12px 14px; display: grid; gap: 7px; cursor: pointer; transition: border-color 0.12s, background 0.12s, transform 0.12s; }
    .ai-candidate-item:hover { border-color: rgba(26,115,232,0.42); background: rgba(26,115,232,0.05); transform: translateY(-1px); }
    .ai-candidate-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }
    .ai-candidate-name { font-size: 13px; font-weight: 700; color: var(--ink); word-break: break-word; }
    .ai-candidate-score { font-size: 11px; color: var(--brand); border: 1px solid rgba(26,115,232,0.18); border-radius: 999px; padding: 2px 8px; background: rgba(26,115,232,0.08); }
    .ai-candidate-sub { font-size: 12px; color: var(--muted); line-height: 1.6; word-break: break-word; }
    .ai-candidate-meta { display: flex; gap: 8px; flex-wrap: wrap; color: var(--muted); font-size: 11px; }
    .ai-candidate-meta span { display: inline-flex; align-items: center; padding: 2px 8px; border-radius: 999px; background: rgba(148,163,184,0.12); }
    @media (max-width: 1200px) {
      .page { --wb-result-height: 300px; }
      .ai-layout { grid-template-columns: 1fr; }
    }
    .wb-meta-body { padding: 14px 16px 18px; display: grid; gap: 10px; min-height: 0; }
    .wb-meta-card { border: 1px solid var(--line); border-radius: 10px; background: var(--surface-soft); padding: 12px; display: grid; gap: 8px; min-width: 0; min-height: 0; }
    .wb-meta-card h4 { font-size: 13px; }
    .wb-meta-tabs { display: flex; gap: 8px; flex-wrap: wrap; }
    .wb-meta-tab { height: 30px; padding: 0 12px; border-radius: 999px; border: 1px solid var(--line); background: #fff; color: var(--muted); cursor: pointer; }
    .wb-meta-tab.active { background: var(--brand); border-color: var(--brand); color: #fff; font-weight: 600; }
    .wb-field-search { width: 100%; height: 34px; border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 0 12px; }
    .wb-field-list { display: grid; gap: 8px; max-height: 520px; overflow: auto; align-content: start; min-width: 0; }
    .wb-field-item { width: 100%; border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 9px 10px; cursor: pointer; display: grid; gap: 5px; text-align: left; }
    .wb-field-item:hover { border-color: #a7c7f2; background: var(--brand-soft); }
    .wb-field-row { display: flex; align-items: center; gap: 6px; flex-wrap: wrap; min-width: 0; }
    .wb-field-name { font-weight: 700; color: var(--ink); word-break: break-all; }
    .wb-field-meta { color: var(--muted); font-size: 11px; line-height: 1.4; word-break: break-word; }
    .wb-type-badge { display: inline-flex; align-items: center; padding: 2px 7px; border-radius: 999px; background: rgba(26,115,232,0.08); color: var(--brand); font-size: 11px; font-family: "SF Mono", ui-monospace, monospace; border: 1px solid rgba(26,115,232,0.12); white-space: nowrap; }
    .wb-ddl-box { margin: 0; border: 1px solid #dbe2ea; border-radius: 8px; background: #0f141b; color: #e5e7eb; padding: 12px; overflow: auto; min-height: 260px; max-height: 520px; max-width: 100%; font: 12px/1.55 "SF Mono", ui-monospace, monospace; white-space: pre-wrap; overflow-wrap: anywhere; word-break: break-word; }
    .hidden { display: none !important; }
    .prompt-row { display: flex; gap: 8px; flex-wrap: wrap; }
    .prompt-row input { flex: 1; min-width: 180px; height: 34px; border: 1px solid var(--line); border-radius: 8px; background: #fff; padding: 0 12px; }
    @media (max-width: 1200px) {
      .shell { grid-template-columns: 200px minmax(0, 1fr); }
      .panel.meta { grid-column: 1 / -1; border-top: 1px solid var(--line); }
    }
    @media (max-width: 860px) {
      .shell { grid-template-columns: 1fr; }
      .panel.tree, .panel.meta { min-height: 220px; }
      .page { --wb-result-height: 240px; }
      .result-splitter::before { width: 56px; }
      .modal-grid { grid-template-columns: 1fr; }
      .ai-modal-card { width: 100%; }
    }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span>SQL 工作台</div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="openFromTableBtn">回到数据地图</button>
        <button class="topbar-btn" type="button" id="openDatasetsFromWorkbenchBtn">数据集</button>
        <button class="topbar-btn" type="button" id="openDashboardsFromWorkbenchBtn">看板</button>
      </div>
    </header>
    <div class="shell">
      <aside class="panel tree">
        <div class="panel-head">
          <div class="panel-title" style="font-size:16px">查询目录</div>
          <div class="panel-sub">建文件夹、建 SQL 文件，按主题沉淀查询。</div>
          <div class="tree-actions">
            <button type="button" class="btn-primary" id="newSqlFileBtn">新建 SQL</button>
            <button type="button" class="btn-ghost" id="newFolderBtn">新建文件夹</button>
          </div>
          <div class="tree-selection" id="treeSelectionHint">当前新建位置：个人查询</div>
        </div>
        <div class="tree-body" id="treeBody"></div>
      </aside>

      <main class="panel editor">
        <div class="panel-head">
          <div class="editor-head-row">
            <div>
              <div class="panel-title" id="editorTitle">未选择 SQL 文件</div>
              <div class="editor-meta" id="editorMeta">从左侧打开或新建一个 SQL 文件开始。</div>
            </div>
          </div>
          <div class="linked-table-card">
            <div class="linked-table-head">
              <div class="linked-table-label">关联表</div>
              <div class="linked-table-actions">
                <button type="button" class="btn-ghost" id="applyLinkedTableBtn">加载字段</button>
              </div>
            </div>
            <input id="linkedTableInput" class="field-inline" type="text" placeholder="关联表，例如 public.sales_order" />
            <div id="linkedTableStatus" class="status">关联表会驱动右侧字段和 DDL。</div>
          </div>
        </div>
        <div class="editor-body">
          <div class="editor-toolbar">
            <button type="button" class="btn-primary" id="runWorkbenchSqlBtn">执行</button>
            <button type="button" class="btn-ghost" id="saveWorkbenchSqlBtn">保存</button>
            <button type="button" class="btn-ghost" id="saveAsDatasetBtn">保存为数据集</button>
            <button type="button" class="btn-ghost" id="openAiWorkbenchBtn">AI 助手</button>
            <div class="spacer"></div>
            <label class="editor-note">默认每页条数（1-500）
              <input id="workbenchRowLimit" class="field-inline" type="number" min="1" max="500" value="200" style="width:108px;height:32px;display:inline-flex" />
            </label>
          </div>
          <textarea id="workbenchSqlEditor" class="sql-editor" spellcheck="false" placeholder="请选择或新建一个 SQL 文件"></textarea>
          <div id="workbenchStatus" class="status"></div>
        </div>
      </main>

      <aside class="meta-rail">
        <button type="button" class="meta-rail-btn" id="toggleMetaPanelBtn">查看字段</button>
      </aside>

      <aside class="panel meta">
        <div class="panel-head meta-head">
          <div>
            <div class="panel-title" style="font-size:16px">元数据面板</div>
            <div class="panel-sub">用于查看当前关联表的字段清单和建表 DDL，也可点击字段名直接插入到 SQL 编辑器。</div>
          </div>
          <button type="button" class="btn-ghost" id="collapseMetaPanelBtn">收起</button>
        </div>
        <div class="wb-meta-body">
          <div class="wb-meta-card">
            <div class="wb-meta-tabs">
              <button type="button" class="wb-meta-tab active" id="metaFieldsTabBtn">字段</button>
              <button type="button" class="wb-meta-tab" id="metaDdlTabBtn">DDL</button>
            </div>
          </div>
          <div class="wb-meta-card" id="metaFieldsPanel">
            <h4>字段搜索</h4>
            <input id="metaFieldSearch" class="wb-field-search" type="text" placeholder="搜索字段 / 注释" />
            <div id="metaFieldList" class="wb-field-list"></div>
          </div>
          <div class="wb-meta-card hidden" id="metaDdlPanel">
            <h4>表定义</h4>
            <div id="metaDdlWrap" class="empty">选择关联表后可查看 DDL</div>
          </div>
        </div>
      </aside>
    </div>
    <div class="result-splitter" id="workbenchResultSplitter" role="separator" aria-orientation="horizontal" title="拖动调整执行结果区域高度"></div>
    <section class="panel result">
      <div class="panel-head">
        <div class="result-stack">
          <div class="result-toolbar">
            <div>
              <div class="panel-title" style="font-size:16px">执行结果</div>
              <div class="panel-sub" id="workbenchResultMeta">尚未执行</div>
            </div>
            <div class="pager">
              <label>每页
                <input id="workbenchPageSize" class="field-inline" type="number" min="1" max="500" value="200" />
              </label>
              <button type="button" class="btn-ghost" id="prevWorkbenchPageBtn">上一页</button>
              <span id="workbenchPageInfo">第 1 / 1 页</span>
              <button type="button" class="btn-ghost" id="nextWorkbenchPageBtn">下一页</button>
            </div>
          </div>
          <div class="result-filter-actions">
            <button type="button" class="btn-primary" id="applyWorkbenchFilterBtn">应用字段筛选</button>
            <button type="button" class="btn-ghost" id="clearWorkbenchFilterBtn">清空筛选</button>
            <div class="result-filter-hint">点击列头右侧筛选图标，像 Excel 一样勾选值；多个字段会同时生效。</div>
          </div>
          <div id="workbenchResultNotices" class="result-notices hidden"></div>
        </div>
      </div>
      <div class="result-body">
        <div id="workbenchResultWrap" class="empty">执行后在这里查看结果</div>
      </div>
    </section>
  </div>

  <div id="treeItemMenu" class="tree-menu hidden">
    <button type="button" id="renameTreeItemBtn">重命名</button>
    <button type="button" id="moveTreeItemBtn">移动</button>
    <button type="button" class="danger" id="deleteTreeItemBtn">删除</button>
  </div>

  <div id="workbenchFilterPopover" class="filter-popover hidden">
    <div class="filter-popover-head">
      <div class="filter-popover-title" id="workbenchFilterPopoverTitle">字段筛选</div>
      <div class="filter-popover-sub" id="workbenchFilterPopoverSub">勾选要保留的值</div>
    </div>
    <input id="workbenchFilterPopoverSearch" class="filter-popover-search" type="text" placeholder="搜索候选值" />
    <div id="workbenchFilterPopoverBody" class="filter-popover-body"></div>
    <div class="filter-popover-actions">
      <button type="button" class="btn-ghost" id="workbenchFilterPopoverClearBtn">清空本列</button>
      <button type="button" class="btn-primary" id="workbenchFilterPopoverApplyBtn">确定</button>
    </div>
  </div>

  <div class="modal-backdrop hidden" id="newSqlModal">
    <div class="modal-card" role="dialog" aria-modal="true" aria-labelledby="newSqlModalTitle">
      <div class="modal-head">
        <div class="modal-title" id="newSqlModalTitle">新建 SQL 文件</div>
        <div class="modal-sub">先把名称、目录、关联表和初始 SQL 一次填好，避免小弹框来回输。</div>
      </div>
      <div class="modal-body">
        <div class="modal-grid">
          <div class="modal-field">
            <label for="newSqlFileNameInput">文件名</label>
            <input id="newSqlFileNameInput" class="field-inline" type="text" placeholder="例如 线索转化分析" />
          </div>
          <div class="modal-field">
            <label for="newSqlFolderInput">保存目录</label>
            <input id="newSqlFolderInput" class="field-inline" type="text" placeholder="例如 个人查询/销售分析" />
          </div>
        </div>
        <div class="modal-field">
          <label for="newSqlLinkedTableInput">关联表</label>
          <input id="newSqlLinkedTableInput" class="field-inline" type="text" placeholder="例如 public.sales_order" />
        </div>
        <div class="modal-field">
          <label for="newSqlContentInput">初始 SQL</label>
          <textarea id="newSqlContentInput" spellcheck="false"></textarea>
        </div>
        <div id="newSqlModalStatus" class="status"></div>
      </div>
      <div class="modal-actions">
        <button type="button" class="btn-ghost" id="cancelNewSqlBtn">取消</button>
        <button type="button" class="btn-primary" id="confirmNewSqlBtn">创建并打开</button>
      </div>
    </div>
  </div>

  <div class="modal-backdrop hidden" id="aiWorkbenchModal">
    <div class="modal-card ai-modal-card" role="dialog" aria-modal="true" aria-labelledby="aiWorkbenchModalTitle">
      <div class="modal-head">
        <div class="modal-title" id="aiWorkbenchModalTitle">AI SQL 助手</div>
        <div class="modal-sub">基于当前关联表、字段、DDL、当前 SQL 和最近一次执行结果对话，帮你生成 SQL 和探查建议。</div>
      </div>
      <div class="modal-body">
        <div class="ai-layout">
          <div class="ai-column">
            <div class="ai-card">
              <h4>快捷问题</h4>
              <div class="ai-chip-row">
                <button type="button" class="ai-chip" data-ai-prompt="先帮我快速理解这张表，列出最值得分析的字段。">理解这张表</button>
                <button type="button" class="ai-chip" data-ai-prompt="基于当前表给我一条数据质量探查 SQL，优先看空值、重复和异常。">质量探查</button>
                <button type="button" class="ai-chip" data-ai-prompt="帮我写一条适合这张表的汇总分析 SQL，并说明看什么。">生成分析 SQL</button>
                <button type="button" class="ai-chip" data-ai-prompt="解释当前编辑器里的 SQL 在做什么，并指出还能怎么优化。">解释当前 SQL</button>
              </div>
            </div>
            <div class="ai-card">
              <h4>对话</h4>
              <div class="ai-sub">你可以直接说业务问题，AI 会尽量给出可执行 SQL；如果上下文不够，它会明确告诉你缺什么。</div>
              <div id="aiWorkbenchThread" class="ai-thread">
                <div class="ai-empty">还没有对话。可以先问“帮我快速看这张表能分析什么”。</div>
              </div>
            </div>
            <div class="ai-card">
              <div class="ai-toolbar">
                <h4>输入问题</h4>
                <button type="button" class="btn-ghost" id="clearAiWorkbenchBtn">清空对话</button>
              </div>
              <textarea id="aiWorkbenchPrompt" class="ai-prompt" spellcheck="false" placeholder="例如：按订单创建月份统计成交金额，并区分负责部门；再告诉我还应该补哪些筛选条件"></textarea>
              <div id="aiWorkbenchStatus" class="ai-status"></div>
            </div>
          </div>
          <div class="ai-column">
            <div class="ai-card">
              <div class="ai-card-head">
                <h4>当前上下文</h4>
                <button type="button" class="btn-ghost" id="switchAiTableSearchBtn">重新找表</button>
              </div>
              <div id="aiWorkbenchStage" class="ai-stage-card hidden"></div>
              <details class="ai-advanced">
                <summary>高级模式：手动补充上下文表</summary>
                <div class="modal-field">
                  <label for="aiExtraContextTablesInput">补充上下文表</label>
                  <textarea id="aiExtraContextTablesInput" class="ai-context-input" spellcheck="false" placeholder="可填多个表，用逗号或换行分隔，例如&#10;public.sales_clue&#10;public.crm_sales_opportunity"></textarea>
                </div>
                <div class="ai-sub">第一张仍建议放在“关联表”里用于执行 SQL；这里的多张表只补给 AI 做联表分析、找关系、写 JOIN。</div>
              </details>
              <div id="aiWorkbenchContext" class="ai-context-list"></div>
            </div>
            <div class="ai-card">
              <div class="ai-toolbar">
                <h4 id="aiGeneratedSqlTitle">AI 生成 SQL</h4>
                <button type="button" class="btn-primary" id="sendAiWorkbenchBtn">发送问题</button>
              </div>
              <textarea id="aiGeneratedSql" class="ai-generated-sql" spellcheck="false" placeholder="AI 生成的 SQL 会显示在这里"></textarea>
              <div class="ai-sub">AI 不会自动执行。你可以先看 SQL，再决定写回编辑器还是直接试跑。</div>
              <div id="aiWorkbenchFollowUps" class="ai-follow-list hidden"></div>
              <div id="aiWorkbenchCandidates" class="ai-candidate-list hidden"></div>
            </div>
          </div>
        </div>
      </div>
      <div class="modal-actions">
        <button type="button" class="btn-ghost" id="closeAiWorkbenchBtn">关闭</button>
        <button type="button" class="btn-ghost" id="insertAiSqlBtn">写入编辑器</button>
        <button type="button" class="btn-primary" id="runAiSqlBtn">写入并试跑</button>
      </div>
    </div>
  </div>

  <script>
    const wbParams = new URLSearchParams(window.location.search);
    const WORKBENCH_RESULT_HEIGHT_KEY = 'sql_workbench_result_height';
    const WORKBENCH_META_COLLAPSED_KEY = 'sql_workbench_meta_collapsed';
    const WORKBENCH_RESULT_DEFAULT = 320;
    const wbState = {
      tree: null,
      currentFilePath: wbParams.get('file') || '',
      currentFile: null,
      profile: null,
      runRequestId: 0,
      selectedFolderPath: '个人查询',
      bootstrapDraftDone: false,
      resultPage: 1,
      resultPageSize: 200,
      metaTab: 'fields',
      resultColumns: [],
      resultFilters: {},
      lastRunResult: null,
      selectedEntryPath: '',
      selectedEntryType: 'folder',
      treeMenu: {
        path: '',
        type: '',
        anchor: null,
      },
      filterPopover: {
        column: '',
        options: [],
        filteredOptions: [],
        draftValues: [],
        anchor: null,
        requestId: 0,
        keyword: '',
        searchTimer: null,
      },
      aiAssistant: {
        history: [],
        generatedSql: '',
        generatedTitle: '',
        followUps: [],
        tableCandidates: [],
        contextTables: [],
        confirmedTables: [],
        guessedRelations: [],
        mode: 'idle',
        needConfirmation: false,
        manualContextTables: [],
        pending: false,
      },
    };

    function escapeHtml(text) {
      return String(text == null ? '' : text)
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#39;');
    }

    function truncateMiddle(text, maxLength = 88) {
      const value = String(text || '');
      if (!value || value.length <= maxLength) return value;
      const head = Math.max(18, Math.floor((maxLength - 1) * 0.65));
      const tail = Math.max(10, maxLength - head - 1);
      return `${value.slice(0, head)}…${value.slice(-tail)}`;
    }

    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const payload = await resp.json();
      if (!resp.ok) throw new Error(payload.error || '请求失败');
      return payload;
    }

    function renderResultTable(result) {
      if (!result) return '<div class="empty">执行后在这里查看结果</div>';
      const rows = result.rows || [];
      const columns = result.columns || [];
      if (!columns.length) return '<div class="empty">执行成功，但没有识别到结果字段</div>';
      if (!rows.length) {
        return `
          <div class="table-wrap">
            <table>
              <thead>
                <tr>${columns.map(renderResultHeaderCell).join('')}</tr>
              </thead>
              <tbody>
                <tr><td colspan="${columns.length}"><div class="empty" style="margin:8px">执行成功，但没有返回数据</div></td></tr>
              </tbody>
            </table>
          </div>
        `;
      }
      return `
        <div class="table-wrap">
          <table>
            <thead>
              <tr>${columns.map(renderResultHeaderCell).join('')}</tr>
            </thead>
            <tbody>
              ${rows.map((row) => `
                <tr>${columns.map((column) => `<td>${escapeHtml(truncateMiddle(row[column], 140))}</td>`).join('')}</tr>
              `).join('')}
            </tbody>
          </table>
        </div>
      `;
    }

    function renderResultHeaderCell(column) {
      const active = Array.isArray(wbState.resultFilters[column]) && wbState.resultFilters[column].length > 0;
      return `
        <th>
          <div class="th-head">
            <span class="th-title" title="${escapeHtml(column)}">${escapeHtml(column)}</span>
            <button
              type="button"
              class="th-filter-btn ${active ? 'active' : ''}"
              data-filter-column="${escapeHtml(column)}"
              title="筛选 ${escapeHtml(column)}"
            >▾</button>
          </div>
        </th>
      `;
    }

    function normalizeWorkbenchFilterValues(raw) {
      if (!Array.isArray(raw)) return [];
      const seen = new Set();
      const values = [];
      raw.forEach((item) => {
        const value = String(item == null ? '' : item).trim();
        if (!value || seen.has(value)) return;
        seen.add(value);
        values.push(value);
      });
      return values;
    }

    function currentWorkbenchColumnFilters() {
      const next = {};
      Object.entries(wbState.resultFilters || {}).forEach(([column, values]) => {
        const normalized = normalizeWorkbenchFilterValues(values);
        if (normalized.length) {
          next[column] = normalized;
        }
      });
      wbState.resultFilters = next;
      return next;
    }

    function renderResultNotices(notices = []) {
      const el = document.getElementById('workbenchResultNotices');
      if (!el) return;
      const valid = (notices || []).filter(Boolean);
      if (!valid.length) {
        el.innerHTML = '';
        el.classList.add('hidden');
        return;
      }
      el.innerHTML = valid.map((notice) => `<div class="result-notice">${escapeHtml(notice)}</div>`).join('');
      el.classList.remove('hidden');
    }

    function renderTreeNode(node) {
      if (!node) return '';
      const isLockedFolder = !node.path || node.path === '个人查询' || node.path === '临时查询';
      const menuBtn = `
        <button
          type="button"
          class="tree-item-menu-btn ${isLockedFolder ? 'hidden' : ''}"
          data-tree-menu-path="${escapeHtml(node.path || '')}"
          data-tree-menu-type="${escapeHtml(node.type || '')}"
          title="更多操作"
        >⋯</button>
      `;
      if (node.type === 'file') {
        return `
          <div class="tree-node">
            <div class="tree-row">
              <button type="button" class="tree-file-label ${wbState.currentFilePath === node.path ? 'active' : ''}" data-file-path="${escapeHtml(node.path)}">
                <span class="file-mark">SQL</span><span class="tree-item-title">${escapeHtml(node.name)}</span>
              </button>
              ${menuBtn}
            </div>
          </div>
        `;
      }
      const children = (node.children || []).map(renderTreeNode).join('');
      return `
        <div class="tree-node">
          <div class="tree-row">
            <button type="button" class="tree-folder-label ${wbState.selectedFolderPath === node.path ? 'active' : ''}" data-folder-path="${escapeHtml(node.path)}">
              <span class="folder-mark">▸</span><span class="tree-item-title">${escapeHtml(node.name)}</span>
            </button>
            ${menuBtn}
          </div>
          <div class="tree-children">${children}</div>
        </div>
      `;
    }

    function defaultLinkedTable() {
      return wbParams.get('schema') && wbParams.get('table')
        ? `${wbParams.get('schema')}.${wbParams.get('table')}`
        : '';
    }

    function defaultSqlForLinkedTable(linkedTable) {
      return linkedTable
        ? `SELECT\n  *\nFROM ${linkedTable}\nLIMIT 20`
        : `SELECT\n  *\nFROM public.sales_order\nLIMIT 20`;
    }

    function normalizeRowLimit(value) {
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return 200;
      return Math.max(1, Math.min(500, Math.floor(numeric)));
    }

    function syncWorkbenchPageSize(value, source = '') {
      const normalized = normalizeRowLimit(value);
      wbState.resultPageSize = normalized;
      const rowLimitInput = document.getElementById('workbenchRowLimit');
      const pageSizeInput = document.getElementById('workbenchPageSize');
      if (rowLimitInput && source !== 'rowLimit') {
        rowLimitInput.value = String(normalized);
      }
      if (pageSizeInput && source !== 'pageSize') {
        pageSizeInput.value = String(normalized);
      }
      return normalized;
    }

    function getWorkbenchResultMinHeight() {
      return window.innerWidth <= 860 ? 180 : 220;
    }

    function getWorkbenchResultMaxHeight() {
      const page = document.querySelector('.page');
      const topbar = document.querySelector('.topbar');
      const pageHeight = page ? page.clientHeight : window.innerHeight;
      const topbarHeight = topbar ? topbar.offsetHeight : 48;
      const splitterHeight = 10;
      const minShellHeight = window.innerWidth <= 860 ? 160 : 240;
      const maxByViewport = Math.floor((pageHeight - topbarHeight) * 0.78);
      const maxByShell = pageHeight - topbarHeight - splitterHeight - minShellHeight;
      return Math.max(getWorkbenchResultMinHeight(), Math.min(maxByViewport, maxByShell));
    }

    function clampWorkbenchResultHeight(value) {
      const minHeight = getWorkbenchResultMinHeight();
      const maxHeight = getWorkbenchResultMaxHeight();
      const fallback = Math.min(Math.max(WORKBENCH_RESULT_DEFAULT, minHeight), maxHeight);
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) return fallback;
      return Math.round(Math.max(minHeight, Math.min(maxHeight, numeric)));
    }

    function currentWorkbenchResultHeight() {
      const panel = document.querySelector('.panel.result');
      if (!panel) return WORKBENCH_RESULT_DEFAULT;
      return panel.getBoundingClientRect().height || WORKBENCH_RESULT_DEFAULT;
    }

    function applyWorkbenchResultHeight(value, options = {}) {
      const page = document.querySelector('.page');
      if (!page) return clampWorkbenchResultHeight(value);
      const height = clampWorkbenchResultHeight(value);
      page.style.setProperty('--wb-result-height', `${height}px`);
      if (options.persist !== false) {
        try {
          localStorage.setItem(WORKBENCH_RESULT_HEIGHT_KEY, String(height));
        } catch (error) {
          console.warn('save workbench result height failed', error);
        }
      }
      return height;
    }

    function restoreWorkbenchResultHeight() {
      try {
        const saved = localStorage.getItem(WORKBENCH_RESULT_HEIGHT_KEY);
        applyWorkbenchResultHeight(saved || WORKBENCH_RESULT_DEFAULT, { persist: false });
      } catch (error) {
        console.warn('restore workbench result height failed', error);
        applyWorkbenchResultHeight(WORKBENCH_RESULT_DEFAULT, { persist: false });
      }
    }

    function syncWorkbenchResultHeightToViewport() {
      applyWorkbenchResultHeight(currentWorkbenchResultHeight(), { persist: false });
    }

    function applyWorkbenchMetaCollapsed(collapsed, options = {}) {
      const shell = document.querySelector('.shell');
      const toggleBtn = document.getElementById('toggleMetaPanelBtn');
      const nextCollapsed = collapsed !== false;
      if (shell) {
        shell.classList.toggle('meta-collapsed', nextCollapsed);
      }
      if (toggleBtn) {
        const count = Number(wbState.profile?.structure?.column_count || wbState.profile?.structure?.columns?.length || 0);
        toggleBtn.textContent = `${nextCollapsed ? '查看字段' : '收起字段'}${count ? `（${count}）` : ''}`;
      }
      if (options.persist !== false) {
        try {
          localStorage.setItem(WORKBENCH_META_COLLAPSED_KEY, nextCollapsed ? '1' : '0');
        } catch (error) {
          console.warn('save meta panel state failed', error);
        }
      }
      return nextCollapsed;
    }

    function restoreWorkbenchMetaCollapsed() {
      try {
        const saved = localStorage.getItem(WORKBENCH_META_COLLAPSED_KEY);
        if (saved == null) {
          applyWorkbenchMetaCollapsed(true, { persist: false });
          return;
        }
        applyWorkbenchMetaCollapsed(saved !== '0', { persist: false });
      } catch (error) {
        console.warn('restore meta panel state failed', error);
        applyWorkbenchMetaCollapsed(true, { persist: false });
      }
    }

    function setupWorkbenchResultSplitter() {
      const splitter = document.getElementById('workbenchResultSplitter');
      if (!splitter) return;
      restoreWorkbenchResultHeight();
      splitter.addEventListener('mousedown', (event) => {
        event.preventDefault();
        const startY = event.clientY;
        const startHeight = currentWorkbenchResultHeight();
        splitter.classList.add('dragging');
        document.body.classList.add('result-resizing');
        const onMove = (moveEvent) => {
          const delta = startY - moveEvent.clientY;
          applyWorkbenchResultHeight(startHeight + delta, { persist: false });
        };
        const onUp = () => {
          document.removeEventListener('mousemove', onMove);
          document.removeEventListener('mouseup', onUp);
          splitter.classList.remove('dragging');
          document.body.classList.remove('result-resizing');
          applyWorkbenchResultHeight(currentWorkbenchResultHeight());
        };
        document.addEventListener('mousemove', onMove);
        document.addEventListener('mouseup', onUp);
      });
      splitter.addEventListener('dblclick', () => {
        try {
          localStorage.removeItem(WORKBENCH_RESULT_HEIGHT_KEY);
        } catch (error) {
          console.warn('reset workbench result height failed', error);
        }
        applyWorkbenchResultHeight(WORKBENCH_RESULT_DEFAULT);
      });
    }

    function updateWorkbenchPager(result = null) {
      const pageInfo = document.getElementById('workbenchPageInfo');
      const prevBtn = document.getElementById('prevWorkbenchPageBtn');
      const nextBtn = document.getElementById('nextWorkbenchPageBtn');
      const pageSizeInput = document.getElementById('workbenchPageSize');
      pageSizeInput.value = String(wbState.resultPageSize);
      if (!result) {
        pageInfo.textContent = '第 1 / 1 页';
        prevBtn.disabled = true;
        nextBtn.disabled = true;
        return;
      }
      pageInfo.textContent = `第 ${result.page || 1} / ${result.total_pages || 1} 页`;
      prevBtn.disabled = !result.has_prev;
      nextBtn.disabled = !result.has_next;
    }

    function setMetaTab(tab) {
      wbState.metaTab = tab === 'ddl' ? 'ddl' : 'fields';
      const fieldsPanel = document.getElementById('metaFieldsPanel');
      const ddlPanel = document.getElementById('metaDdlPanel');
      const fieldsBtn = document.getElementById('metaFieldsTabBtn');
      const ddlBtn = document.getElementById('metaDdlTabBtn');
      fieldsPanel.classList.toggle('hidden', wbState.metaTab !== 'fields');
      ddlPanel.classList.toggle('hidden', wbState.metaTab !== 'ddl');
      fieldsBtn.classList.toggle('active', wbState.metaTab === 'fields');
      ddlBtn.classList.toggle('active', wbState.metaTab === 'ddl');
    }

    function normalizeFolderPath(path) {
      const value = String(path || '').trim();
      return value || '个人查询';
    }

    function setSelectedFolder(path) {
      wbState.selectedFolderPath = normalizeFolderPath(path);
      wbState.selectedEntryPath = wbState.selectedFolderPath;
      wbState.selectedEntryType = 'folder';
      const hint = document.getElementById('treeSelectionHint');
      if (hint) {
        hint.textContent = `当前新建位置：${wbState.selectedFolderPath}`;
      }
    }

    function setLinkedTableStatus(message, kind = '') {
      const el = document.getElementById('linkedTableStatus');
      if (!el) return;
      el.textContent = message;
      el.className = kind ? `status ${kind}` : 'status';
    }

    function refreshWorkbenchHeader(file) {
      if (!file) {
        document.getElementById('editorTitle').textContent = '未选择 SQL 文件';
        document.getElementById('editorMeta').textContent = '从左侧打开或新建一个 SQL 文件开始。';
        return;
      }
      document.getElementById('editorTitle').textContent = file.name;
      document.getElementById('editorMeta').textContent = `文件路径：${file.path}${file.updated_at ? ` · 更新于 ${file.updated_at}` : ''}`;
    }

    function resetWorkbenchResult() {
      document.getElementById('workbenchStatus').textContent = '';
      document.getElementById('workbenchStatus').className = 'status';
      document.getElementById('workbenchResultMeta').textContent = '尚未执行';
      document.getElementById('workbenchResultWrap').innerHTML = '<div class="empty">执行后在这里查看结果</div>';
      wbState.resultPage = 1;
      wbState.resultColumns = [];
      wbState.resultFilters = {};
      wbState.lastRunResult = null;
      closeWorkbenchFilterPopover();
      syncWorkbenchPageSize(wbState.resultPageSize || 200);
      renderResultNotices([]);
      updateWorkbenchPager(null);
    }

    function setAiWorkbenchStatus(message, kind = '') {
      const el = document.getElementById('aiWorkbenchStatus');
      if (!el) return;
      el.textContent = message;
      el.className = kind ? `ai-status ${kind}` : 'ai-status';
    }

    function parseAiContextTablesInput(value) {
      const raw = String(value || '');
      const parts = raw.split(/[\\n,，;；、]+/);
      const seen = new Set();
      const tables = [];
      parts.forEach((item) => {
        const qualified = String(item || '').trim();
        if (!qualified || !qualified.includes('.')) return;
        if (seen.has(qualified)) return;
        seen.add(qualified);
        tables.push(qualified);
      });
      return tables.slice(0, 6);
    }

    function syncAiManualContextTables() {
      const input = document.getElementById('aiExtraContextTablesInput');
      const tables = parseAiContextTablesInput(input?.value || '');
      wbState.aiAssistant.manualContextTables = tables;
      return tables;
    }

    function syncAiSearchModeUi() {
      const linked = document.getElementById('linkedTableInput')?.value.trim() || '';
      const promptEl = document.getElementById('aiWorkbenchPrompt');
      const switchBtn = document.getElementById('switchAiTableSearchBtn');
      if (promptEl) {
        promptEl.placeholder = linked
          ? '例如：按订单创建月份统计成交金额，并区分负责部门；再告诉我还应该补哪些筛选条件'
          : '例如：我还没确定表，想看销售订单按月份的成交金额和负责人分布，先帮我找表';
      }
      if (switchBtn) {
        switchBtn.textContent = linked ? '重新找表' : '当前已是找表模式';
      }
    }

    function renderAiWorkbenchStage() {
      const stageEl = document.getElementById('aiWorkbenchStage');
      if (!stageEl) return;
      const mode = wbState.aiAssistant.mode || 'idle';
      const confirmedTables = wbState.aiAssistant.confirmedTables || [];
      const guessedRelations = wbState.aiAssistant.guessedRelations || [];
      if (mode === 'idle') {
        stageEl.innerHTML = `
          <div class="ai-stage-title">第一步：描述你要看什么数据</div>
          <div class="ai-stage-copy">直接说业务问题即可。AI 会先找表，再让你确认关系，最后生成 SQL。</div>
        `;
        stageEl.classList.remove('hidden');
        return;
      }
      if (mode === 'table_search') {
        stageEl.innerHTML = `
          <div class="ai-stage-title">第一步：先确认候选表</div>
          <div class="ai-stage-copy">AI 会先根据你的业务问题找表。你可以直接点下面的候选表，也可以补充更具体的业务口径。</div>
        `;
        stageEl.classList.remove('hidden');
        return;
      }
      if (mode === 'relation_confirm') {
        stageEl.innerHTML = `
          <div class="ai-stage-title">第二步：确认要用哪些表</div>
          <div class="ai-stage-copy">本轮准备使用：${escapeHtml(confirmedTables.join('、') || '未识别')}。如果关系没问题，点“继续生成 SQL”；不对就换候选表或补充描述。</div>
          ${guessedRelations.length ? `<div class="ai-rel-list">${guessedRelations.map((item) => `
            <div class="ai-rel-item">
              <div class="ai-rel-main">${escapeHtml(item.expression || '')}</div>
              <div class="ai-rel-meta">
                ${(item.reason ? `<span>${escapeHtml(item.reason)}</span>` : '')}
                ${(item.confidence ? `<span>置信度：${escapeHtml(item.confidence)}</span>` : '')}
              </div>
            </div>
          `).join('')}</div>` : '<div class="ai-stage-copy">暂时还没识别出明确关联键，生成 SQL 时会尽量保守处理，并在说明里提示你确认。</div>'}
          <div class="ai-stage-actions">
            <button type="button" class="btn-primary" id="continueAiGenerateBtn">继续生成 SQL</button>
          </div>
        `;
        stageEl.classList.remove('hidden');
        document.getElementById('continueAiGenerateBtn')?.addEventListener('click', async () => {
          const latestUserPrompt = [...(wbState.aiAssistant.history || [])].reverse().find((row) => row.role === 'user')?.content || '';
          await askAiWorkbench(latestUserPrompt || '继续生成 SQL', { forceGenerate: true, skipHistory: true });
        });
        return;
      }
      stageEl.innerHTML = `
        <div class="ai-stage-title">第三步：生成 SQL</div>
        <div class="ai-stage-copy">已确认表和关系。下面是当前可执行的 SQL，你可以直接写入编辑器或试跑。</div>
      `;
      stageEl.classList.remove('hidden');
    }

    function renderAiWorkbenchContext() {
      const contextEl = document.getElementById('aiWorkbenchContext');
      if (!contextEl) return;
      const linked = document.getElementById('linkedTableInput').value.trim() || '未填写';
      const manualTables = syncAiManualContextTables();
      const sql = document.getElementById('workbenchSqlEditor').value.trim();
      const sqlPreview = sql ? `${sql.split('\\n').length} 行 SQL` : '当前编辑器为空';
      const rows = wbState.lastRunResult?.rows?.length || 0;
      const columns = wbState.lastRunResult?.columns?.length || 0;
      const fieldCount = wbState.profile?.structure?.columns?.length || 0;
      const ddlReady = wbState.profile?.ddl?.text ? '已加载' : '未加载';
      const isTableSearchMode = linked === '未填写';
      const contextTables = wbState.aiAssistant.contextTables || [];
      contextEl.innerHTML = [
        `当前模式：<b>${isTableSearchMode ? '找表模式' : '已绑定主表'}</b>`,
        `关联表：<b>${escapeHtml(linked)}</b>`,
        `手动补充表：${escapeHtml(manualTables.length ? manualTables.join('、') : '未填写')}`,
        `AI 上下文表：${escapeHtml(contextTables.length ? contextTables.join('、') : linked)}`,
        `当前 SQL：${escapeHtml(sqlPreview)}`,
        `字段上下文：${escapeHtml(fieldCount)} 个字段`,
        `DDL：${escapeHtml(ddlReady)}`,
        `最近一次执行结果：${rows ? `${rows} 行 / ${columns} 列（已作为 AI 上下文）` : '还没有执行结果'}`,
        `当前文件：${escapeHtml(wbState.currentFile?.path || '未保存到 SQL 文件')}`,
      ].map((line) => `<div>${line}</div>`).join('');
      renderAiWorkbenchStage();
      syncAiSearchModeUi();
    }

    function renderAiWorkbenchThread() {
      const threadEl = document.getElementById('aiWorkbenchThread');
      if (!threadEl) return;
      const history = wbState.aiAssistant.history || [];
      if (!history.length) {
        threadEl.innerHTML = '<div class="ai-empty">还没有对话。可以先问“帮我快速看这张表能分析什么”。</div>';
        return;
      }
      threadEl.innerHTML = history.map((item) => `
        <div class="ai-msg ${escapeHtml(item.role)}">
          <div class="ai-role">${item.role === 'user' ? '你' : 'AI 助手'}</div>
          <div class="ai-body">${escapeHtml(item.content || '')}</div>
        </div>
      `).join('');
      threadEl.scrollTop = threadEl.scrollHeight;
    }

    function renderAiWorkbenchOutput() {
      const sqlEl = document.getElementById('aiGeneratedSql');
      const titleEl = document.getElementById('aiGeneratedSqlTitle');
      const followEl = document.getElementById('aiWorkbenchFollowUps');
      const candidateEl = document.getElementById('aiWorkbenchCandidates');
      if (!sqlEl || !titleEl || !followEl || !candidateEl) return;
      sqlEl.value = wbState.aiAssistant.generatedSql || '';
      titleEl.textContent = wbState.aiAssistant.generatedTitle || 'AI 生成 SQL';
      const followUps = wbState.aiAssistant.followUps || [];
      const candidates = wbState.aiAssistant.tableCandidates || [];
      if (!followUps.length) {
        followEl.innerHTML = '';
        followEl.classList.add('hidden');
      } else {
        followEl.innerHTML = `<div><b>下一步建议</b></div>${followUps.map((item) => `<div>• ${escapeHtml(item)}</div>`).join('')}`;
        followEl.classList.remove('hidden');
      }
      if (!candidates.length) {
        candidateEl.innerHTML = '';
        candidateEl.classList.add('hidden');
        return;
      }
      const mode = wbState.aiAssistant.mode || 'table_search';
      candidateEl.innerHTML = `
        <div><b>${mode === 'table_search' ? '候选表' : '可切换候选表'}</b></div>
        ${candidates.map((item, index) => `
          <button type="button" class="ai-candidate-item" data-ai-candidate-index="${index}">
            <div class="ai-candidate-head">
              <div class="ai-candidate-name">${escapeHtml(item.qualified_name || item.table_name || '未命名表')}</div>
              <div class="ai-candidate-score">候选 ${index + 1}</div>
            </div>
            <div class="ai-candidate-sub">${escapeHtml(item.subtitle || item.table_comment || item.description || '暂无描述')}</div>
            <div class="ai-candidate-meta">
              ${(item.reason_text ? `<span>${escapeHtml(item.reason_text)}</span>` : '')}
              ${(item.owner ? `<span>负责人：${escapeHtml(item.owner)}</span>` : '')}
              ${(item.business_domain ? `<span>业务域：${escapeHtml(item.business_domain)}</span>` : '')}
              ${(item.column_count ? `<span>${escapeHtml(item.column_count)} 个字段</span>` : '')}
            </div>
          </button>
        `).join('')}
      `;
      candidateEl.classList.remove('hidden');
      candidateEl.querySelectorAll('[data-ai-candidate-index]').forEach((btn) => {
        btn.addEventListener('click', async () => {
          const item = candidates[Number(btn.dataset.aiCandidateIndex)];
          if (!item?.qualified_name) return;
          await chooseAiCandidateTable(item);
        });
      });
    }

    function openAiWorkbenchModal() {
      if (!wbState.aiAssistant.mode) {
        wbState.aiAssistant.mode = 'idle';
      }
      renderAiWorkbenchContext();
      renderAiWorkbenchThread();
      renderAiWorkbenchOutput();
      syncAiSearchModeUi();
      setAiWorkbenchStatus('');
      document.getElementById('aiWorkbenchModal').classList.remove('hidden');
      window.setTimeout(() => document.getElementById('aiWorkbenchPrompt').focus(), 30);
    }

    function closeAiWorkbenchModal() {
      document.getElementById('aiWorkbenchModal').classList.add('hidden');
      setAiWorkbenchStatus('');
    }

    function clearAiWorkbenchConversation() {
      wbState.aiAssistant.history = [];
      wbState.aiAssistant.generatedSql = '';
      wbState.aiAssistant.generatedTitle = '';
      wbState.aiAssistant.followUps = [];
      wbState.aiAssistant.tableCandidates = [];
      wbState.aiAssistant.contextTables = [];
      wbState.aiAssistant.confirmedTables = [];
      wbState.aiAssistant.guessedRelations = [];
      wbState.aiAssistant.mode = 'idle';
      wbState.aiAssistant.needConfirmation = false;
      wbState.aiAssistant.manualContextTables = [];
      document.getElementById('aiWorkbenchPrompt').value = '';
      document.getElementById('aiExtraContextTablesInput').value = '';
      renderAiWorkbenchThread();
      renderAiWorkbenchOutput();
      renderAiWorkbenchContext();
      setAiWorkbenchStatus('已清空对话', 'ok');
    }

    async function switchAiToTableSearchMode() {
      const linkedInput = document.getElementById('linkedTableInput');
      if (!linkedInput) return;
      linkedInput.value = '';
      await loadLinkedTableProfile('', { silent: true, reason: 'ai_table_search' });
      wbState.aiAssistant.generatedSql = '';
      wbState.aiAssistant.generatedTitle = '先选候选表';
      wbState.aiAssistant.followUps = [];
      wbState.aiAssistant.tableCandidates = [];
      wbState.aiAssistant.contextTables = [];
      wbState.aiAssistant.confirmedTables = [];
      wbState.aiAssistant.guessedRelations = [];
      wbState.aiAssistant.mode = 'table_search';
      wbState.aiAssistant.needConfirmation = true;
      wbState.aiAssistant.manualContextTables = [];
      document.getElementById('aiExtraContextTablesInput').value = '';
      renderAiWorkbenchContext();
      renderAiWorkbenchOutput();
      const prompt = document.getElementById('aiWorkbenchPrompt')?.value.trim() || '';
      if (prompt) {
        setAiWorkbenchStatus('已切到找表模式，正在根据当前问题找候选表…');
        await askAiWorkbench(prompt);
        return;
      }
      setAiWorkbenchStatus('已切到找表模式。描述业务问题后点击“发送问题”，我会先给你候选表。', 'ok');
    }

    async function chooseAiCandidateTable(item) {
      const qualifiedName = String(item?.qualified_name || '').trim();
      if (!qualifiedName) return;
      const promptEl = document.getElementById('aiWorkbenchPrompt');
      const latestUserPrompt = [...(wbState.aiAssistant.history || [])].reverse().find((row) => row.role === 'user')?.content || '';
      document.getElementById('linkedTableInput').value = qualifiedName;
      await loadLinkedTableProfile(qualifiedName);
      wbState.aiAssistant.tableCandidates = [];
      wbState.aiAssistant.mode = 'relation_confirm';
      renderAiWorkbenchContext();
      renderAiWorkbenchOutput();
      setAiWorkbenchStatus(`已选中 ${qualifiedName}，正在重新确认表关系…`);
      if (promptEl && !promptEl.value.trim() && latestUserPrompt) {
        promptEl.value = latestUserPrompt;
      }
      await askAiWorkbench(promptEl?.value.trim() || latestUserPrompt, { skipHistory: true });
    }

    async function askAiWorkbench(promptOverride = '', options = {}) {
      if (wbState.aiAssistant.pending) return;
      const promptEl = document.getElementById('aiWorkbenchPrompt');
      const prompt = String(promptOverride || promptEl.value || '').trim();
      const manualTables = syncAiManualContextTables();
      const linkedInput = document.getElementById('linkedTableInput');
      let linked = linkedInput.value.trim();
      if (!linked && manualTables.length) {
        linked = manualTables[0];
        linkedInput.value = linked;
        await loadLinkedTableProfile(linked, { silent: true });
        renderAiWorkbenchContext();
      }
      if (!prompt) {
        setAiWorkbenchStatus('先输入你想让 AI 做什么。', 'error');
        return;
      }
      const hasLinkedTable = linked && linked.includes('.');
      const [schema, ...rest] = hasLinkedTable ? linked.split('.') : ['public'];
      const table = hasLinkedTable ? rest.join('.') : '';
      const extraContextTables = manualTables.filter((item) => item !== linked);
      const forceGenerate = options.forceGenerate === true;
      const skipHistory = options.skipHistory === true;
      if (!skipHistory) {
        wbState.aiAssistant.history.push({ role: 'user', content: prompt });
      }
      wbState.aiAssistant.pending = true;
      renderAiWorkbenchThread();
      const requestMode = forceGenerate ? 'sql_generate' : 'auto';
      setAiWorkbenchStatus(hasLinkedTable ? 'AI 正在整理表关系和 SQL 上下文，请稍候…' : 'AI 正在根据你的问题先找候选表，请稍候…');
      if (!skipHistory) {
        promptEl.value = '';
      }
      try {
        const payload = await fetchJson('/api/sql-workspace/assistant', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            mode: requestMode,
            schema,
            table,
            linked_table: linked,
            extra_context_tables: extraContextTables,
            prompt,
            sql: document.getElementById('workbenchSqlEditor').value.trim(),
            file_path: wbState.currentFile?.path || '',
            conversation: wbState.aiAssistant.history,
            result_preview: wbState.lastRunResult || null,
          }),
        });
        if (payload.reply) {
          wbState.aiAssistant.history.push({ role: 'assistant', content: payload.reply || 'AI 没有返回说明。' });
        }
        wbState.aiAssistant.generatedSql = payload.sql || '';
        wbState.aiAssistant.generatedTitle = payload.title || 'AI 生成 SQL';
        wbState.aiAssistant.followUps = Array.isArray(payload.follow_ups) ? payload.follow_ups : [];
        wbState.aiAssistant.tableCandidates = Array.isArray(payload.table_candidates) ? payload.table_candidates : [];
        wbState.aiAssistant.contextTables = Array.isArray(payload.context_tables) ? payload.context_tables : (hasLinkedTable ? [linked] : []);
        wbState.aiAssistant.confirmedTables = Array.isArray(payload.confirmed_tables) ? payload.confirmed_tables : wbState.aiAssistant.contextTables;
        wbState.aiAssistant.guessedRelations = Array.isArray(payload.guessed_relations) ? payload.guessed_relations : [];
        wbState.aiAssistant.mode = payload.mode || 'sql_generate';
        wbState.aiAssistant.needConfirmation = payload.need_confirmation === true;
        renderAiWorkbenchThread();
        renderAiWorkbenchContext();
        renderAiWorkbenchOutput();
        if (payload.mode === 'table_search') {
          setAiWorkbenchStatus('已找到候选表，先确认范围再继续', 'ok');
        } else if (payload.mode === 'relation_confirm') {
          setAiWorkbenchStatus('已识别候选表和关系，确认后继续生成 SQL', 'ok');
        } else if (payload.degraded) {
          setAiWorkbenchStatus('已生成，当前为无 Key 降级模式', 'ok');
        } else {
          setAiWorkbenchStatus(payload.model ? `已生成，模型：${payload.model}` : '已生成', 'ok');
        }
      } catch (error) {
        setAiWorkbenchStatus(error.message || 'AI 助手调用失败', 'error');
      } finally {
        wbState.aiAssistant.pending = false;
      }
    }

    async function applyAiGeneratedSql(runAfterWrite = false) {
      const sql = String(wbState.aiAssistant.generatedSql || '').trim();
      if (!sql) {
        setAiWorkbenchStatus('当前还没有可写入的 AI SQL。', 'error');
        return;
      }
      document.getElementById('workbenchSqlEditor').value = sql;
      document.getElementById('workbenchStatus').textContent = '已将 AI SQL 写入编辑器';
      document.getElementById('workbenchStatus').className = 'status ok';
      if (!runAfterWrite) {
        setAiWorkbenchStatus('已写入编辑器', 'ok');
        return;
      }
      closeAiWorkbenchModal();
      await runCurrentSql(1);
    }

    function setNewSqlModalStatus(message, kind = '') {
      const el = document.getElementById('newSqlModalStatus');
      if (!el) return;
      el.textContent = message;
      el.className = kind ? `status ${kind}` : 'status';
    }

    function closeNewSqlModal() {
      document.getElementById('newSqlModal').classList.add('hidden');
      setNewSqlModalStatus('');
    }

    function openNewSqlModal() {
      const folderPath = normalizeFolderPath(wbState.selectedFolderPath);
      const linkedTable = document.getElementById('linkedTableInput').value.trim() || defaultLinkedTable();
      document.getElementById('newSqlFileNameInput').value = '';
      document.getElementById('newSqlFolderInput').value = folderPath;
      document.getElementById('newSqlLinkedTableInput').value = linkedTable;
      document.getElementById('newSqlLinkedTableInput').dataset.lastLinked = linkedTable;
      document.getElementById('newSqlContentInput').value = defaultSqlForLinkedTable(linkedTable);
      setNewSqlModalStatus('');
      document.getElementById('newSqlModal').classList.remove('hidden');
      window.setTimeout(() => document.getElementById('newSqlFileNameInput').focus(), 30);
    }

    async function ensureWorkbenchDraftFromLinkedTable() {
      const linkedTable = defaultLinkedTable();
      if (wbState.bootstrapDraftDone || !linkedTable || wbState.currentFilePath) return false;
      wbState.bootstrapDraftDone = true;
      const draftPath = `个人查询/${linkedTable.replaceAll('.', '_')}_草稿.sql`;
      try {
        const file = await fetchJson('/api/sql-workspace/file', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            create: true,
            folder_path: '个人查询',
            file_name: `${linkedTable.replaceAll('.', '_')}_草稿`,
            content: defaultSqlForLinkedTable(linkedTable),
            linked_table: linkedTable,
          }),
        });
        wbState.currentFilePath = file.path;
        wbState.currentFile = file;
        setSelectedFolder(file.path.includes('/') ? file.path.slice(0, file.path.lastIndexOf('/')) : '个人查询');
        return true;
      } catch (error) {
        if (!String(error.message || '').includes('已存在')) {
          throw error;
        }
        wbState.currentFilePath = draftPath;
        setSelectedFolder('个人查询');
      }
      return true;
    }

    async function renderWorkspaceTree(payload) {
      wbState.tree = payload.tree;
      document.getElementById('treeBody').innerHTML = renderTreeNode(payload.tree);
      bindTreeEvents();
    }

    function bindTreeEvents() {
      document.querySelectorAll('[data-file-path]').forEach((btn) => {
        btn.addEventListener('click', () => {
          wbState.selectedEntryPath = btn.dataset.filePath || '';
          wbState.selectedEntryType = 'file';
          openSqlFile(btn.dataset.filePath || '');
        });
      });
      document.querySelectorAll('[data-folder-path]').forEach((btn) => {
        btn.addEventListener('click', () => {
          setSelectedFolder(btn.dataset.folderPath || '个人查询');
          renderWorkspaceTree({ tree: wbState.tree || { type: 'folder', name: 'SQL 工作台', path: '', children: [] } });
        });
      });
      document.querySelectorAll('[data-tree-menu-path]').forEach((btn) => {
        btn.addEventListener('click', (event) => {
          event.stopPropagation();
          openTreeItemMenu(btn.dataset.treeMenuPath || '', btn.dataset.treeMenuType || '', btn);
        });
      });
    }

    async function loadWorkspaceTree() {
      const payload = await fetchJson('/api/sql-workspace');
      await renderWorkspaceTree(payload);
      return payload;
    }

    async function loadWorkspace() {
      await ensureWorkbenchDraftFromLinkedTable();
      const payload = await loadWorkspaceTree();
      const target = wbState.currentFilePath || payload.recent_files?.[0] || '';
      if (target) {
        await openSqlFile(target, { refreshTree: false });
        return;
      }
      setSelectedFolder(defaultLinkedTable() ? '个人查询' : wbState.selectedFolderPath);
      refreshWorkbenchHeader(null);
      document.getElementById('linkedTableInput').value = defaultLinkedTable();
      document.getElementById('workbenchSqlEditor').value = '';
      resetWorkbenchResult();
      await loadLinkedTableProfile(defaultLinkedTable(), { silent: true });
    }

    async function openSqlFile(path, options = {}) {
      if (!path) return;
      const refreshTree = options.refreshTree !== false;
      const file = await fetchJson(`/api/sql-workspace/file?path=${encodeURIComponent(path)}`);
      wbState.currentFilePath = file.path;
      wbState.currentFile = file;
      wbState.selectedEntryPath = file.path;
      wbState.selectedEntryType = 'file';
      setSelectedFolder(file.path.includes('/') ? file.path.slice(0, file.path.lastIndexOf('/')) : '个人查询');
      refreshWorkbenchHeader(file);
      document.getElementById('workbenchSqlEditor').value = file.content || '';
      document.getElementById('linkedTableInput').value = file.linked_table || '';
      resetWorkbenchResult();
      if (refreshTree) {
        await loadWorkspaceTree();
      }
      await loadLinkedTableProfile(file.linked_table || '', { silent: true });
    }

    async function loadLinkedTableProfile(qualifiedName, options = {}) {
      const silent = options.silent === true;
      const reason = String(options.reason || '').trim();
      const listEl = document.getElementById('metaFieldList');
      const ddlEl = document.getElementById('metaDdlWrap');
      wbState.profile = null;
      if (!qualifiedName || !qualifiedName.includes('.')) {
        listEl.innerHTML = '<div class="empty">先填写或带入一个关联表，右侧才会显示字段清单。</div>';
        ddlEl.innerHTML = '<div class="empty">先填写或带入一个关联表，这里才会显示建表 DDL。</div>';
        setMetaTab('fields');
        if (reason == 'ai_table_search') {
          setLinkedTableStatus('已切到找表模式。发送业务问题后，AI 会先给你候选表。', 'ok');
        } else {
          setLinkedTableStatus('请先指定关联表，例如 public.sales_order。', 'error');
        }
        return;
      }
      const [schema, ...rest] = qualifiedName.split('.');
      const table = rest.join('.');
      try {
        if (!silent) {
          setLinkedTableStatus(`正在加载 ${qualifiedName} 的字段和 DDL…`);
        }
        const profile = await fetchJson(`/api/table-profile?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&sample_limit=20`);
        wbState.profile = profile;
        renderMetaFields('');
        ddlEl.innerHTML = profile.ddl?.text
          ? `<pre class="wb-ddl-box">${escapeHtml(profile.ddl.text)}</pre>`
          : '<div class="empty">当前表没有可用 DDL</div>';
        setMetaTab(wbState.metaTab);
        setLinkedTableStatus(`已加载 ${qualifiedName}，右侧可直接查看字段和 DDL。`, 'ok');
        const collapsed = document.querySelector('.shell')?.classList.contains('meta-collapsed');
        applyWorkbenchMetaCollapsed(Boolean(collapsed), { persist: false });
      } catch (error) {
        listEl.innerHTML = `<div class="error">${escapeHtml(error.message || '加载字段失败')}</div>`;
        ddlEl.innerHTML = `<div class="error">${escapeHtml(error.message || '加载 DDL 失败')}</div>`;
        setMetaTab('fields');
        setLinkedTableStatus(error.message || '关联表加载失败', 'error');
      }
    }

    function renderMetaFields(keyword) {
      const listEl = document.getElementById('metaFieldList');
      const columns = wbState.profile?.structure?.columns || [];
      const needle = String(keyword || '').trim().toLowerCase();
      const filtered = !needle ? columns : columns.filter((column) => {
        const hay = [column.column_name, column.column_comment, column.business_def, column.udt_name, column.data_type].join(' ').toLowerCase();
        return hay.includes(needle);
      });
      if (!filtered.length) {
        listEl.innerHTML = '<div class="empty">没有匹配的字段</div>';
        return;
      }
      listEl.innerHTML = filtered.map((column) => `
        <button type="button" class="wb-field-item" data-insert-column="${escapeHtml(column.column_name)}">
          <div class="wb-field-row"><span class="wb-field-name">${escapeHtml(column.column_name)}</span> <span class="wb-type-badge">${escapeHtml(column.udt_name || column.data_type || '')}</span></div>
          <div class="wb-field-meta">${escapeHtml(column.column_comment || column.business_def || '暂无注释')}</div>
        </button>
      `).join('');
      listEl.querySelectorAll('[data-insert-column]').forEach((btn) => {
        btn.addEventListener('click', () => insertTextAtCursor(document.getElementById('workbenchSqlEditor'), btn.dataset.insertColumn || ''));
      });
    }

    function insertTextAtCursor(textarea, text) {
      if (!textarea) return;
      const start = textarea.selectionStart ?? textarea.value.length;
      const end = textarea.selectionEnd ?? textarea.value.length;
      const before = textarea.value.slice(0, start);
      const after = textarea.value.slice(end);
      textarea.value = `${before}${text}${after}`;
      const next = start + text.length;
      textarea.focus();
      textarea.setSelectionRange(next, next);
    }

    async function saveCurrentSqlFile() {
      if (!wbState.currentFilePath) {
        document.getElementById('workbenchStatus').textContent = '请先从左侧新建或打开一个 SQL 文件';
        document.getElementById('workbenchStatus').className = 'status error';
        return;
      }
      const payload = await fetchJson('/api/sql-workspace/file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          path: wbState.currentFilePath,
          content: document.getElementById('workbenchSqlEditor').value,
          linked_table: document.getElementById('linkedTableInput').value.trim(),
        }),
      });
      wbState.currentFile = payload;
      document.getElementById('workbenchStatus').textContent = '已保存';
      document.getElementById('workbenchStatus').className = 'status ok';
      refreshWorkbenchHeader(payload);
      await loadWorkspaceTree();
      await loadLinkedTableProfile(payload.linked_table || '');
    }

    async function runCurrentSql(page = 1) {
      const linked = document.getElementById('linkedTableInput').value.trim();
      if (!linked || !linked.includes('.')) {
        document.getElementById('workbenchStatus').textContent = '请先填写关联表，例如 public.sales_order';
        document.getElementById('workbenchStatus').className = 'status error';
        return;
      }
      const [schema, ...rest] = linked.split('.');
      const table = rest.join('.');
      const sql = document.getElementById('workbenchSqlEditor').value.trim();
      const pageSizeInput = document.getElementById('workbenchPageSize');
      const rowLimitInput = document.getElementById('workbenchRowLimit');
      const pageSize = syncWorkbenchPageSize(pageSizeInput.value || rowLimitInput.value || wbState.resultPageSize || 200);
      const rowLimit = pageSize;
      const columnFilters = currentWorkbenchColumnFilters();
      wbState.resultPage = Math.max(1, Number(page || 1));
      const requestId = ++wbState.runRequestId;
      document.getElementById('workbenchStatus').textContent = '正在执行查询…';
      document.getElementById('workbenchStatus').className = 'status';
      document.getElementById('workbenchResultWrap').innerHTML = '<div class="empty">正在执行，请稍候…</div>';
      renderResultNotices([]);
      closeWorkbenchFilterPopover();
      try {
        const result = await fetchJson('/api/sql-workspace/run', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            schema,
            table,
            sql,
            row_limit: rowLimit,
            page: wbState.resultPage,
            page_size: pageSize,
            column_filters: columnFilters,
          }),
        });
        if (requestId !== wbState.runRequestId) return;
        wbState.resultPage = result.page || 1;
        wbState.resultColumns = result.columns || [];
        wbState.resultFilters = result.filter?.values || {};
        wbState.lastRunResult = {
          columns: result.columns || [],
          rows: result.rows || [],
          row_count: result.row_count || 0,
          total_count: result.total_count || 0,
          notices: result.notices || [],
        };
        const filterText = result.filter?.applied
          ? `；已筛 <b>${escapeHtml(result.filter.count || 0)}</b> 个字段`
          : '';
        document.getElementById('workbenchResultMeta').innerHTML = `关联表 <b>${escapeHtml(linked)}</b>；执行库 <b>${escapeHtml(result.resolved_schema || linked.split('.')[0] || '')}</b>；共 <b>${escapeHtml(result.total_count || 0)}</b> 行；当前第 <b>${escapeHtml(result.page || 1)}</b> / <b>${escapeHtml(result.total_pages || 1)}</b> 页；本页返回 <b>${escapeHtml(result.row_count || 0)}</b> 行；方言 <b>${escapeHtml(result.dialect || '')}</b>${filterText}`;
        document.getElementById('workbenchResultWrap').innerHTML = renderResultTable(result);
        bindWorkbenchHeaderFilters();
        renderResultNotices(result.notices || []);
        updateWorkbenchPager(result);
        document.getElementById('workbenchStatus').textContent = '执行完成';
        document.getElementById('workbenchStatus').className = 'status ok';
      } catch (error) {
        if (requestId !== wbState.runRequestId) return;
        wbState.lastRunResult = null;
        document.getElementById('workbenchResultMeta').textContent = '执行失败';
        document.getElementById('workbenchResultWrap').innerHTML = `<div class="error">${escapeHtml(error.message || '执行失败')}</div>`;
        renderResultNotices([]);
        updateWorkbenchPager(null);
        document.getElementById('workbenchStatus').textContent = error.message || '执行失败';
        document.getElementById('workbenchStatus').className = 'status error';
      }
    }

    function positionWorkbenchFilterPopover() {
      const popover = document.getElementById('workbenchFilterPopover');
      const anchor = wbState.filterPopover.anchor;
      if (!popover || !anchor) return;
      const rect = anchor.getBoundingClientRect();
      const width = Math.min(280, window.innerWidth - 24);
      let left = rect.right - width;
      if (left < 12) left = 12;
      if (left + width > window.innerWidth - 12) {
        left = Math.max(12, window.innerWidth - width - 12);
      }
      const top = Math.min(rect.bottom + 8, window.innerHeight - 440);
      popover.style.left = `${Math.max(12, left)}px`;
      popover.style.top = `${Math.max(12, top)}px`;
    }

    function closeWorkbenchFilterPopover() {
      const popover = document.getElementById('workbenchFilterPopover');
      if (!popover) return;
      if (wbState.filterPopover.searchTimer) {
        window.clearTimeout(wbState.filterPopover.searchTimer);
      }
      popover.classList.add('hidden');
      wbState.filterPopover = {
        column: '',
        options: [],
        filteredOptions: [],
        draftValues: [],
        anchor: null,
        requestId: 0,
        keyword: '',
        searchTimer: null,
      };
    }

    function renderWorkbenchFilterPopoverOptions() {
      const body = document.getElementById('workbenchFilterPopoverBody');
      if (!body) return;
      const column = wbState.filterPopover.column;
      const draftSet = new Set(wbState.filterPopover.draftValues || []);
      const options = wbState.filterPopover.filteredOptions || [];
      if (!options.length) {
        body.innerHTML = '<div class="filter-popover-empty">没有匹配的候选值</div>';
        return;
      }
      body.innerHTML = options.map((option, index) => `
        <label class="filter-popover-option">
          <input type="checkbox" data-filter-option-index="${index}" ${draftSet.has(String(option.value)) ? 'checked' : ''} />
          <span class="label" title="${escapeHtml(option.label)}">${escapeHtml(option.label)}</span>
          <span class="count">${escapeHtml(option.count)}</span>
        </label>
      `).join('');
      body.querySelectorAll('[data-filter-option-index]').forEach((input) => {
        input.addEventListener('change', (event) => {
          const option = options[Number(event.target.dataset.filterOptionIndex)];
          if (!option) return;
          const next = new Set(wbState.filterPopover.draftValues || []);
          if (event.target.checked) {
            next.add(String(option.value));
          } else {
            next.delete(String(option.value));
          }
          wbState.filterPopover.draftValues = Array.from(next);
        });
      });
    }

    async function loadWorkbenchFilterOptions(keyword = '') {
      const column = wbState.filterPopover.column;
      if (!column) return;
      const linked = document.getElementById('linkedTableInput').value.trim();
      if (!linked || !linked.includes('.')) return;
      const [schema, ...rest] = linked.split('.');
      const table = rest.join('.');
      const sql = document.getElementById('workbenchSqlEditor').value.trim();
      const bodyEl = document.getElementById('workbenchFilterPopoverBody');
      const requestId = (wbState.filterPopover.requestId || 0) + 1;
      wbState.filterPopover.requestId = requestId;
      wbState.filterPopover.keyword = String(keyword || '');
      bodyEl.innerHTML = '<div class="filter-popover-empty">正在加载候选值…</div>';
      try {
        const payload = await fetchJson('/api/sql-workspace/filter-options', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            schema,
            table,
            sql,
            column,
            keyword: wbState.filterPopover.keyword,
            column_filters: currentWorkbenchColumnFilters(),
          }),
        });
        if (wbState.filterPopover.column !== column || wbState.filterPopover.requestId !== requestId) return;
        wbState.filterPopover.options = payload.options || [];
        wbState.filterPopover.filteredOptions = payload.options || [];
        wbState.filterPopover.draftValues = [...(payload.active_values || wbState.filterPopover.draftValues || [])];
        renderWorkbenchFilterPopoverOptions();
        positionWorkbenchFilterPopover();
      } catch (error) {
        if (wbState.filterPopover.column !== column || wbState.filterPopover.requestId !== requestId) return;
        bodyEl.innerHTML = `<div class="filter-popover-empty">${escapeHtml(error.message || '加载候选值失败')}</div>`;
      }
    }

    async function openWorkbenchFilterPopover(column, anchor) {
      if (!column || !anchor) return;
      const popover = document.getElementById('workbenchFilterPopover');
      const titleEl = document.getElementById('workbenchFilterPopoverTitle');
      const subEl = document.getElementById('workbenchFilterPopoverSub');
      const searchEl = document.getElementById('workbenchFilterPopoverSearch');
      const bodyEl = document.getElementById('workbenchFilterPopoverBody');
      wbState.filterPopover = {
        column,
        options: [],
        filteredOptions: [],
        draftValues: [...(wbState.resultFilters[column] || [])],
        anchor,
        requestId: 0,
        keyword: '',
        searchTimer: null,
      };
      titleEl.textContent = `筛选 ${column}`;
      subEl.textContent = '勾选要保留的值，输入关键字会到全量候选值里搜索';
      searchEl.value = '';
      bodyEl.innerHTML = '<div class="filter-popover-empty">正在加载候选值…</div>';
      popover.classList.remove('hidden');
      positionWorkbenchFilterPopover();
      await loadWorkbenchFilterOptions('');
    }

    function applyWorkbenchFilterPopover() {
      const column = wbState.filterPopover.column;
      if (!column) return;
      const next = currentWorkbenchColumnFilters();
      const values = normalizeWorkbenchFilterValues(wbState.filterPopover.draftValues || []);
      if (values.length) {
        next[column] = values;
      } else {
        delete next[column];
      }
      wbState.resultFilters = next;
      closeWorkbenchFilterPopover();
      runCurrentSql(1);
    }

    function clearWorkbenchFilterPopoverColumn() {
      const column = wbState.filterPopover.column;
      if (!column) return;
      wbState.filterPopover.draftValues = [];
      const next = currentWorkbenchColumnFilters();
      delete next[column];
      wbState.resultFilters = next;
      closeWorkbenchFilterPopover();
      runCurrentSql(1);
    }

    function bindWorkbenchHeaderFilters() {
      document.querySelectorAll('[data-filter-column]').forEach((btn) => {
        btn.addEventListener('click', async (event) => {
          event.stopPropagation();
          const column = btn.dataset.filterColumn || '';
          if (wbState.filterPopover.column === column && !document.getElementById('workbenchFilterPopover').classList.contains('hidden')) {
            closeWorkbenchFilterPopover();
            return;
          }
          await openWorkbenchFilterPopover(column, btn);
        });
      });
    }

    async function createFolder() {
      const value = window.prompt('请输入文件夹名称');
      if (!value) return;
      const base = wbState.selectedFolderPath && wbState.selectedFolderPath !== 'SQL 工作台' ? wbState.selectedFolderPath : '';
      const nextPath = [base, value.trim()].filter(Boolean).join('/');
      await fetchJson('/api/sql-workspace/folder', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path: nextPath }),
      });
      setSelectedFolder(nextPath);
      await loadWorkspaceTree();
    }

    async function createSqlFile() {
      const fileName = document.getElementById('newSqlFileNameInput').value.trim();
      const folderPath = normalizeFolderPath(document.getElementById('newSqlFolderInput').value);
      const linkedTable = document.getElementById('newSqlLinkedTableInput').value.trim();
      const defaultSql = document.getElementById('newSqlContentInput').value;
      if (!fileName) {
        setNewSqlModalStatus('请先填写 SQL 文件名', 'error');
        return;
      }
      const file = await fetchJson('/api/sql-workspace/file', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          create: true,
          folder_path: folderPath,
          file_name: fileName.trim(),
          content: defaultSql,
          linked_table: linkedTable,
        }),
      });
      wbState.currentFilePath = file.path;
      wbState.selectedEntryPath = file.path;
      wbState.selectedEntryType = 'file';
      closeNewSqlModal();
      await openSqlFile(file.path);
    }

    function flattenWorkspaceFolders(node, target = []) {
      if (!node) return target;
      if (node.type === 'folder') {
        target.push(node.path || '');
        (node.children || []).forEach((child) => flattenWorkspaceFolders(child, target));
      }
      return target;
    }

    function closeTreeItemMenu() {
      const menu = document.getElementById('treeItemMenu');
      if (!menu) return;
      menu.classList.add('hidden');
      wbState.treeMenu = { path: '', type: '', anchor: null };
    }

    function positionTreeItemMenu() {
      const menu = document.getElementById('treeItemMenu');
      const anchor = wbState.treeMenu.anchor;
      if (!menu || !anchor) return;
      const rect = anchor.getBoundingClientRect();
      let left = rect.right - 148;
      if (left < 12) left = 12;
      if (left + 148 > window.innerWidth - 12) {
        left = Math.max(12, window.innerWidth - 160);
      }
      let top = rect.bottom + 6;
      if (top + 132 > window.innerHeight - 12) {
        top = Math.max(12, rect.top - 132);
      }
      menu.style.left = `${left}px`;
      menu.style.top = `${top}px`;
    }

    function openTreeItemMenu(path, type, anchor) {
      wbState.treeMenu = { path, type, anchor };
      const menu = document.getElementById('treeItemMenu');
      menu.classList.remove('hidden');
      positionTreeItemMenu();
    }

    async function renameTreeItem() {
      const path = wbState.treeMenu.path;
      if (!path) return;
      const currentName = path.split('/').pop() || '';
      const nextName = window.prompt('请输入新名称', currentName);
      closeTreeItemMenu();
      if (!nextName || nextName.trim() === currentName) return;
      const payload = await fetchJson('/api/sql-workspace/rename', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, new_name: nextName.trim() }),
      });
      await handleWorkspaceMutationPayload(payload);
    }

    async function moveTreeItem() {
      const path = wbState.treeMenu.path;
      if (!path) return;
      const folders = flattenWorkspaceFolders(wbState.tree || {}).filter((item) => item !== path && !item.startsWith(`${path}/`));
      const currentFolder = path.includes('/') ? path.slice(0, path.lastIndexOf('/')) : '';
      const suggested = currentFolder || '个人查询';
      const nextFolder = window.prompt(`请输入目标目录，可选：${folders.map((item) => item || '根目录').join('、')}`, suggested);
      closeTreeItemMenu();
      if (nextFolder == null) return;
      const folderPath = String(nextFolder).trim();
      const payload = await fetchJson('/api/sql-workspace/move', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path, target_folder_path: folderPath }),
      });
      await handleWorkspaceMutationPayload(payload);
    }

    async function deleteTreeItem() {
      const path = wbState.treeMenu.path;
      const type = wbState.treeMenu.type;
      if (!path) return;
      const name = path.split('/').pop() || path;
      closeTreeItemMenu();
      const ok = window.confirm(`确定删除${type === 'folder' ? '文件夹' : '文件'}“${name}”吗？${type === 'folder' ? ' 文件夹下内容也会一起删除。' : ''}`);
      if (!ok) return;
      const payload = await fetchJson('/api/sql-workspace/delete', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ path }),
      });
      await handleWorkspaceMutationPayload(payload);
    }

    async function handleWorkspaceMutationPayload(payload) {
      if (payload?.workspace?.tree) {
        wbState.tree = payload.workspace.tree;
        await renderWorkspaceTree(payload.workspace);
      } else {
        await loadWorkspaceTree();
      }
      if (payload?.entry_type === 'file' && payload?.file) {
        wbState.currentFilePath = payload.file.path;
        wbState.currentFile = payload.file;
        wbState.selectedEntryPath = payload.file.path;
        wbState.selectedEntryType = 'file';
        refreshWorkbenchHeader(payload.file);
        document.getElementById('workbenchSqlEditor').value = payload.file.content || '';
        document.getElementById('linkedTableInput').value = payload.file.linked_table || '';
        await loadLinkedTableProfile(payload.file.linked_table || '', { silent: true });
        return;
      }
      if (payload?.action === 'delete' && payload?.old_path === wbState.currentFilePath) {
        wbState.currentFilePath = '';
        wbState.currentFile = null;
        refreshWorkbenchHeader(null);
        document.getElementById('workbenchSqlEditor').value = '';
        document.getElementById('linkedTableInput').value = '';
        resetWorkbenchResult();
        await loadLinkedTableProfile('', { silent: true });
        return;
      }
      if (payload?.entry_type === 'folder' && payload?.new_path) {
        setSelectedFolder(payload.new_path);
      }
      if (payload?.action === 'delete' && payload?.entry_type === 'folder') {
        setSelectedFolder('个人查询');
      }
    }

    document.getElementById('newFolderBtn').addEventListener('click', createFolder);
    document.getElementById('renameTreeItemBtn').addEventListener('click', renameTreeItem);
    document.getElementById('moveTreeItemBtn').addEventListener('click', moveTreeItem);
    document.getElementById('deleteTreeItemBtn').addEventListener('click', deleteTreeItem);
    document.getElementById('metaFieldsTabBtn').addEventListener('click', () => setMetaTab('fields'));
    document.getElementById('metaDdlTabBtn').addEventListener('click', () => setMetaTab('ddl'));
    document.getElementById('newSqlFileBtn').addEventListener('click', openNewSqlModal);
    document.getElementById('cancelNewSqlBtn').addEventListener('click', closeNewSqlModal);
    document.getElementById('confirmNewSqlBtn').addEventListener('click', async () => {
      try {
        await createSqlFile();
      } catch (error) {
        setNewSqlModalStatus(error.message || '创建 SQL 文件失败', 'error');
      }
    });
    document.getElementById('newSqlLinkedTableInput').addEventListener('change', (event) => {
      const linked = event.target.value.trim();
      const editor = document.getElementById('newSqlContentInput');
      const currentLinked = document.getElementById('newSqlLinkedTableInput').dataset.lastLinked || '';
      const oldTemplate = defaultSqlForLinkedTable(currentLinked);
      if (!editor.value.trim() || editor.value === oldTemplate) {
        editor.value = defaultSqlForLinkedTable(linked);
      }
      document.getElementById('newSqlLinkedTableInput').dataset.lastLinked = linked;
    });
    document.getElementById('newSqlModal').addEventListener('click', (event) => {
      if (event.target && event.target.id === 'newSqlModal') {
        closeNewSqlModal();
      }
    });
    document.getElementById('aiWorkbenchModal').addEventListener('click', (event) => {
      if (event.target && event.target.id === 'aiWorkbenchModal') {
        closeAiWorkbenchModal();
      }
    });
    document.getElementById('openAiWorkbenchBtn').addEventListener('click', openAiWorkbenchModal);
    document.getElementById('closeAiWorkbenchBtn').addEventListener('click', closeAiWorkbenchModal);
    document.getElementById('clearAiWorkbenchBtn').addEventListener('click', clearAiWorkbenchConversation);
    document.getElementById('switchAiTableSearchBtn').addEventListener('click', async () => {
      await switchAiToTableSearchMode();
    });
    document.getElementById('sendAiWorkbenchBtn').addEventListener('click', () => askAiWorkbench());
    document.getElementById('insertAiSqlBtn').addEventListener('click', async () => {
      await applyAiGeneratedSql(false);
    });
    document.getElementById('runAiSqlBtn').addEventListener('click', async () => {
      await applyAiGeneratedSql(true);
    });
    document.querySelectorAll('[data-ai-prompt]').forEach((btn) => {
      btn.addEventListener('click', async () => {
        const prompt = btn.dataset.aiPrompt || '';
        document.getElementById('aiWorkbenchPrompt').value = prompt;
        await askAiWorkbench(prompt);
      });
    });
    document.getElementById('aiWorkbenchPrompt').addEventListener('keydown', async (event) => {
      if ((event.metaKey || event.ctrlKey) && event.key === 'Enter') {
        event.preventDefault();
        await askAiWorkbench();
      }
    });
    document.getElementById('saveWorkbenchSqlBtn').addEventListener('click', saveCurrentSqlFile);
    document.getElementById('saveAsDatasetBtn').addEventListener('click', async () => {
      const sql = document.getElementById('workbenchSqlEditor').value.trim();
      const linked = document.getElementById('linkedTableInput').value.trim();
      if (!sql && !linked) {
        document.getElementById('workbenchStatus').textContent = '请先选择关联表或输入 SQL 再保存为数据集';
        document.getElementById('workbenchStatus').className = 'status error';
        return;
      }
      const name = window.prompt('请输入数据集名称');
      if (!name) return;
      const sourceType = sql ? 'sql' : 'table';
      const sourceRef = sourceType === 'sql' ? (wbState.currentFilePath || '') : linked;
      try {
        const payload = await fetchJson('/api/datasets', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json' },
          body: JSON.stringify({
            name,
            source_type: sourceType,
            source_ref: sourceRef,
            datasource_key: linked.split('.')[0] || '',
            linked_table: linked,
            description: sourceType === 'sql' ? '由 SQL 工作台沉淀' : '由关联表沉淀',
          }),
        });
        document.getElementById('workbenchStatus').textContent = `已创建数据集 ${payload.name || payload.id}`;
        document.getElementById('workbenchStatus').className = 'status ok';
      } catch (error) {
        document.getElementById('workbenchStatus').textContent = error.message || '保存数据集失败';
        document.getElementById('workbenchStatus').className = 'status error';
      }
    });
    document.getElementById('runWorkbenchSqlBtn').addEventListener('click', () => runCurrentSql(1));
    document.getElementById('prevWorkbenchPageBtn').addEventListener('click', () => runCurrentSql(Math.max(1, wbState.resultPage - 1)));
    document.getElementById('nextWorkbenchPageBtn').addEventListener('click', () => runCurrentSql(wbState.resultPage + 1));
    document.getElementById('workbenchRowLimit').addEventListener('change', (event) => {
      syncWorkbenchPageSize(event.target.value, 'rowLimit');
    });
    document.getElementById('workbenchPageSize').addEventListener('change', (event) => {
      syncWorkbenchPageSize(event.target.value, 'pageSize');
      runCurrentSql(1);
    });
    document.getElementById('applyWorkbenchFilterBtn').addEventListener('click', () => runCurrentSql(1));
    document.getElementById('clearWorkbenchFilterBtn').addEventListener('click', () => {
      wbState.resultFilters = {};
      closeWorkbenchFilterPopover();
      runCurrentSql(1);
    });
    document.getElementById('workbenchFilterPopoverApplyBtn').addEventListener('click', applyWorkbenchFilterPopover);
    document.getElementById('workbenchFilterPopoverClearBtn').addEventListener('click', clearWorkbenchFilterPopoverColumn);
    document.getElementById('workbenchFilterPopoverSearch').addEventListener('input', (event) => {
      const keyword = String(event.target.value || '');
      if (wbState.filterPopover.searchTimer) {
        window.clearTimeout(wbState.filterPopover.searchTimer);
      }
      wbState.filterPopover.searchTimer = window.setTimeout(() => {
        loadWorkbenchFilterOptions(keyword);
      }, 180);
    });
    document.addEventListener('click', (event) => {
      const popover = document.getElementById('workbenchFilterPopover');
      if (popover.classList.contains('hidden')) return;
      const target = event.target;
      if (popover.contains(target)) return;
      if (target.closest('[data-filter-column]')) return;
      closeWorkbenchFilterPopover();
    });
    document.addEventListener('click', (event) => {
      const menu = document.getElementById('treeItemMenu');
      if (menu.classList.contains('hidden')) return;
      const target = event.target;
      if (menu.contains(target)) return;
      if (target.closest('[data-tree-menu-path]')) return;
      closeTreeItemMenu();
    });
    window.addEventListener('resize', positionWorkbenchFilterPopover);
    window.addEventListener('scroll', positionWorkbenchFilterPopover, true);
    window.addEventListener('resize', positionTreeItemMenu);
    window.addEventListener('scroll', positionTreeItemMenu, true);
    window.addEventListener('resize', syncWorkbenchResultHeightToViewport);
    document.getElementById('applyLinkedTableBtn').addEventListener('click', async () => {
      await loadLinkedTableProfile(document.getElementById('linkedTableInput').value.trim());
    });
    document.getElementById('toggleMetaPanelBtn').addEventListener('click', () => {
      const shell = document.querySelector('.shell');
      const isCollapsed = shell?.classList.contains('meta-collapsed');
      applyWorkbenchMetaCollapsed(!isCollapsed);
    });
    document.getElementById('collapseMetaPanelBtn').addEventListener('click', () => {
      applyWorkbenchMetaCollapsed(true);
    });
    document.getElementById('metaFieldSearch').addEventListener('input', (event) => renderMetaFields(event.target.value));
    document.getElementById('linkedTableInput').addEventListener('change', async (event) => {
      await loadLinkedTableProfile(event.target.value.trim());
    });
    document.getElementById('aiExtraContextTablesInput').addEventListener('input', () => {
      syncAiManualContextTables();
      renderAiWorkbenchContext();
    });
    document.getElementById('openFromTableBtn').addEventListener('click', () => {
      const linked = document.getElementById('linkedTableInput').value.trim();
      if (linked && linked.includes('.')) {
        const [schema, ...rest] = linked.split('.');
        const table = rest.join('.');
        window.location.href = `/map?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}`;
        return;
      }
      window.location.href = '/map';
    });
    document.getElementById('openDatasetsFromWorkbenchBtn').addEventListener('click', () => {
      window.location.href = '/datasets';
    });
    document.getElementById('openDashboardsFromWorkbenchBtn').addEventListener('click', () => {
      window.location.href = '/dashboards';
    });

    setupWorkbenchResultSplitter();
    restoreWorkbenchMetaCollapsed();
    setMetaTab('fields');

    loadWorkspace().catch((error) => {
      document.getElementById('treeBody').innerHTML = `<div class="error">${escapeHtml(error.message || '加载工作台失败')}</div>`;
    });
  </script>
</body>
</html>
"""

DATASETS_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>数据集中心</title>
  <style>
    :root { --bg:#eef1f5; --surface:#fff; --surface-soft:#f8fafc; --ink:#111827; --muted:#6b7280; --line:#dbe2ea; --brand:#1a73e8; --ok:#0d9488; --radius:12px; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Helvetica Neue",sans-serif; font-size:13px; color:var(--ink); background:var(--bg); min-height:100vh; }
    button,input,select,textarea { font:inherit; }
    .page { min-height:100vh; display:grid; grid-template-rows:48px auto minmax(0,1fr); }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; background:#111827; color:#fff; padding:0 18px; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:700; }
    .dot { width:8px; height:8px; border-radius:999px; background:#1a73e8; }
    .topbar-actions { display:flex; align-items:center; gap:8px; }
    .topbar-btn { height:30px; padding:0 12px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.92); cursor:pointer; }
    .hero { padding:18px 22px 10px; display:grid; gap:12px; }
    .hero-title { font-size:24px; font-weight:800; }
    .hero-sub { color:var(--muted); line-height:1.7; }
    .hero-actions { display:flex; gap:10px; flex-wrap:wrap; }
    .btn-primary,.btn-ghost { height:34px; padding:0 14px; border-radius:10px; cursor:pointer; }
    .btn-primary { border:0; background:var(--brand); color:#fff; font-weight:600; }
    .btn-ghost { border:1px solid var(--line); background:#fff; color:var(--muted); }
    .content { padding:0 22px 22px; display:grid; gap:14px; }
    .toolbar { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
    .toolbar input, .toolbar select { height:36px; border:1px solid var(--line); border-radius:10px; background:#fff; padding:0 12px; }
    .toolbar input { min-width:260px; flex:1; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(280px,1fr)); gap:14px; }
    .card { border:1px solid var(--line); border-radius:14px; background:#fff; padding:16px; display:grid; gap:10px; box-shadow:0 8px 24px rgba(15,23,42,0.04); }
    .card-main { display:grid; gap:10px; cursor:pointer; }
    .card-main:hover .card-title { color:var(--brand); }
    .card-head { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; }
    .card-title { font-size:15px; font-weight:700; }
    .badge { display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px; font-size:11px; border:1px solid rgba(26,115,232,0.16); background:rgba(26,115,232,0.08); color:var(--brand); }
    .meta { display:flex; gap:8px; flex-wrap:wrap; color:var(--muted); font-size:12px; }
    .meta span { display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px; background:var(--surface-soft); }
    .desc { color:var(--muted); line-height:1.6; min-height:38px; }
    .card-actions { display:flex; gap:8px; flex-wrap:wrap; padding-top:2px; }
    .mini-btn,.mini-danger { height:30px; padding:0 12px; border-radius:9px; cursor:pointer; font-size:12px; }
    .mini-btn { border:1px solid var(--line); background:#fff; color:var(--ink); }
    .mini-danger { border:1px solid rgba(220,38,38,0.16); background:rgba(220,38,38,0.08); color:#dc2626; }
    .empty { padding:28px; border:1px dashed var(--line); border-radius:14px; background:var(--surface-soft); color:var(--muted); text-align:center; }
    .status { min-height:18px; color:var(--muted); font-size:12px; }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span>数据集中心</div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="openMapBtn">数据地图</button>
        <button class="topbar-btn" type="button" id="openWorkbenchBtn">SQL 工作台</button>
        <button class="topbar-btn" type="button" id="openDashboardsBtn">看板</button>
      </div>
    </header>
    <section class="hero">
      <div class="hero-title">数据集</div>
      <div class="hero-sub">把宽表或保存好的 SQL 沉淀成可复用的分析入口。第一版先支持基于表或 SQL 文件创建数据集，后续再补字段角色配置。</div>
      <div class="hero-actions">
        <button class="btn-primary" type="button" id="newTableDatasetBtn">新建表数据集</button>
        <button class="btn-ghost" type="button" id="newSqlDatasetBtn">从 SQL 创建</button>
      </div>
    </section>
    <main class="content">
      <div class="toolbar">
        <input id="datasetSearchInput" placeholder="搜索数据集名称 / 来源表 / SQL 路径" />
        <select id="datasetStatusFilter"><option value="">全部状态</option><option value="draft">草稿</option><option value="published">已发布</option></select>
      </div>
      <div id="datasetStatus" class="status"></div>
      <div id="datasetGrid" class="grid"></div>
    </main>
  </div>
  <script>
    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `请求失败(${resp.status})`);
      return data;
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>\"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));
    }
    function renderDatasets(items) {
      const host = document.getElementById('datasetGrid');
      if (!items.length) {
        host.innerHTML = '<div class="empty">还没有数据集。可以先从宽表或 SQL 工作台开始沉淀。</div>';
        return;
      }
      host.innerHTML = items.map((item) => `
        <article class="card">
          <div class="card-main" data-action="open" data-id="${escapeHtml(item.id)}">
            <div class="card-head">
              <div class="card-title">${escapeHtml(item.name || item.id)}</div>
              <span class="badge">${escapeHtml(item.status || 'draft')}</span>
            </div>
            <div class="desc">${escapeHtml(item.description || '暂无说明')}</div>
            <div class="meta">
              <span>来源：${escapeHtml(item.source_type || '-')}</span>
              <span>${escapeHtml(item.source_ref || '-')}</span>
            </div>
            <div class="meta">
              <span>数据源：${escapeHtml(item.datasource_key || '-')}</span>
              <span>更新：${escapeHtml(item.updated_at || '-')}</span>
            </div>
          </div>
          <div class="card-actions">
            <button class="mini-btn" type="button" data-action="open" data-id="${escapeHtml(item.id)}">查看详情</button>
            <button class="mini-danger" type="button" data-action="delete" data-id="${escapeHtml(item.id)}" data-name="${escapeHtml(item.name || item.id)}">删除</button>
          </div>
        </article>
      `).join('');
    }
    async function loadDatasets() {
      const statusEl = document.getElementById('datasetStatus');
      statusEl.textContent = '正在加载数据集…';
      const payload = await fetchJson('/api/datasets');
      window.__datasets = Array.isArray(payload.items) ? payload.items : [];
      applyDatasetFilters();
      statusEl.textContent = `已加载 ${window.__datasets.length} 个数据集`;
    }
    function applyDatasetFilters() {
      const keyword = document.getElementById('datasetSearchInput').value.trim().toLowerCase();
      const status = document.getElementById('datasetStatusFilter').value.trim();
      const rows = (window.__datasets || []).filter((item) => {
        if (status && String(item.status || '') !== status) return false;
        if (!keyword) return true;
        const hay = [item.name, item.description, item.source_ref, item.datasource_key].join(' ').toLowerCase();
        return hay.includes(keyword);
      });
      renderDatasets(rows);
    }
    async function createDataset(sourceType) {
      const name = window.prompt(sourceType === 'table' ? '请输入数据集名称' : '请输入 SQL 数据集名称');
      if (!name) return;
      const sourceRef = window.prompt(sourceType === 'table' ? '请输入来源表，例如 public.sales_order' : '请输入 SQL 文件路径，例如 个人查询/demo.sql');
      if (!sourceRef) return;
      await fetchJson('/api/datasets', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, source_type: sourceType, source_ref: sourceRef, datasource_key: sourceRef.split('.')[0] || '' }),
      });
      await loadDatasets();
    }
    document.getElementById('datasetSearchInput').addEventListener('input', applyDatasetFilters);
    document.getElementById('datasetStatusFilter').addEventListener('change', applyDatasetFilters);
    document.getElementById('datasetGrid').addEventListener('click', async (event) => {
      const target = event.target.closest('[data-action]');
      if (!target) return;
      const action = target.dataset.action || '';
      const datasetId = target.dataset.id || '';
      if (!datasetId) return;
      if (action === 'open') {
        window.location.href = `/datasets/${encodeURIComponent(datasetId)}`;
        return;
      }
      if (action === 'delete') {
        const datasetName = target.dataset.name || datasetId;
        const ok = window.confirm(`确定删除数据集“${datasetName}”吗？`);
        if (!ok) return;
        document.getElementById('datasetStatus').textContent = '正在删除数据集…';
        await fetchJson(`/api/datasets/${encodeURIComponent(datasetId)}/delete`, { method: 'POST' });
        await loadDatasets();
      }
    });
    document.getElementById('newTableDatasetBtn').addEventListener('click', () => createDataset('table'));
    document.getElementById('newSqlDatasetBtn').addEventListener('click', () => createDataset('sql'));
    document.getElementById('openMapBtn').addEventListener('click', () => { window.location.href = '/map'; });
    document.getElementById('openWorkbenchBtn').addEventListener('click', () => {
      const payload = window.__dataset || {};
      const linked = String(payload.linked_table || payload.source_ref || '').trim();
      if (!linked || !linked.includes('.')) {
        window.location.href = '/sql-workbench';
        return;
      }
      const [schema, ...rest] = linked.split('.');
      const table = rest.join('.');
      const next = new URLSearchParams();
      if (schema) next.set('schema', schema);
      if (table) next.set('table', table);
      window.location.href = `/sql-workbench?${next.toString()}`;
    });
    document.getElementById('openDashboardsBtn').addEventListener('click', () => { window.location.href = '/dashboards'; });
    loadDatasets().catch((error) => {
      document.getElementById('datasetStatus').textContent = error.message || '数据集加载失败';
    });
  </script>
</body>
</html>
"""

DATASET_DETAIL_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>数据集详情</title>
  <style>
    :root { --bg:#eef1f5; --surface:#fff; --surface-soft:#f8fafc; --ink:#111827; --muted:#6b7280; --line:#dbe2ea; --brand:#1a73e8; --danger:#dc2626; --ok:#0d9488; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Helvetica Neue",sans-serif; font-size:13px; color:var(--ink); background:var(--bg); min-height:100vh; }
    button,input,textarea,select { font:inherit; }
    .page { min-height:100vh; display:grid; grid-template-rows:48px auto minmax(0,1fr); }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; background:#111827; color:#fff; padding:0 18px; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:700; }
    .dot { width:8px; height:8px; border-radius:999px; background:#1a73e8; }
    .topbar-actions { display:flex; align-items:center; gap:8px; }
    .topbar-btn { height:30px; padding:0 12px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.92); cursor:pointer; }
    .hero { padding:20px 22px 12px; display:grid; gap:10px; }
    .title { font-size:28px; font-weight:800; }
    .sub { color:var(--muted); line-height:1.7; }
    .meta { display:flex; gap:8px; flex-wrap:wrap; }
    .meta span { display:inline-flex; align-items:center; padding:6px 10px; border-radius:999px; background:#fff; border:1px solid var(--line); color:var(--muted); }
    .content { padding:0 22px 22px; display:grid; grid-template-columns:minmax(0,1.45fr) 380px; gap:14px; }
    .stack { display:grid; gap:14px; }
    .card { border:1px solid var(--line); border-radius:14px; background:#fff; padding:16px; display:grid; gap:12px; }
    .card h3 { font-size:15px; }
    .kv { display:grid; gap:10px; }
    .kv-row { display:grid; grid-template-columns:120px minmax(0,1fr); gap:10px; align-items:start; }
    .kv-row label { color:var(--muted); }
    .mono { font-family:"SF Mono",ui-monospace,monospace; word-break:break-all; }
    .field { display:grid; gap:6px; }
    .field label { font-size:12px; color:var(--muted); }
    .field input, .field textarea, .field select { width:100%; border:1px solid var(--line); border-radius:10px; background:#fff; padding:10px 12px; }
    .field textarea { min-height:88px; resize:vertical; }
    .field select { height:40px; padding:0 12px; }
    .field-inline-actions { display:flex; gap:8px; }
    .field-inline-actions input { flex:1; }
    .actions { display:flex; gap:8px; flex-wrap:wrap; }
    .btn-primary,.btn-ghost,.btn-danger,.mini-btn { height:34px; padding:0 14px; border-radius:10px; cursor:pointer; }
    .btn-primary { border:0; background:var(--brand); color:#fff; font-weight:600; }
    .btn-ghost, .mini-btn { border:1px solid var(--line); background:#fff; color:var(--muted); }
    .btn-danger { border:1px solid rgba(220,38,38,0.16); background:rgba(220,38,38,0.08); color:var(--danger); }
    .status { min-height:18px; color:var(--muted); font-size:12px; }
    .status.ok { color:var(--ok); }
    .status.error { color:var(--danger); }
    .model-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; }
    .model-hint { color:var(--muted); font-size:12px; line-height:1.6; }
    .field-list { border:1px solid var(--line); border-radius:12px; background:var(--surface-soft); padding:10px; display:grid; gap:8px; max-height:320px; overflow:auto; }
    .field-item { display:grid; grid-template-columns:auto minmax(0,1fr); gap:10px; align-items:start; border:1px solid rgba(148,163,184,0.18); border-radius:10px; background:#fff; padding:10px 12px; }
    .field-item input { margin-top:2px; }
    .field-name { font-weight:700; word-break:break-all; }
    .field-meta { color:var(--muted); font-size:12px; line-height:1.5; }
    .metric-list { display:grid; gap:10px; }
    .metric-row { border:1px solid var(--line); border-radius:12px; background:var(--surface-soft); padding:10px; display:grid; gap:10px; }
    .metric-grid { display:grid; grid-template-columns:1.1fr 0.8fr 1fr auto; gap:8px; align-items:end; }
    .pill-list { display:flex; flex-wrap:wrap; gap:8px; }
    .pill { display:inline-flex; align-items:center; padding:5px 10px; border-radius:999px; background:rgba(26,115,232,0.08); color:var(--brand); border:1px solid rgba(26,115,232,0.14); font-size:12px; }
    .empty { padding:16px; border:1px dashed var(--line); border-radius:12px; background:var(--surface-soft); color:var(--muted); text-align:center; }
    @media (max-width: 1200px) { .content { grid-template-columns:1fr; } }
    @media (max-width: 720px) {
      .model-grid { grid-template-columns:1fr; }
      .metric-grid { grid-template-columns:1fr; }
    }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span>数据集详情</div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="backDatasetsBtn">返回列表</button>
        <button class="topbar-btn" type="button" id="openDashboardsBtn">看板</button>
      </div>
    </header>
    <section class="hero">
      <div class="title" id="datasetTitle">数据集</div>
      <div class="sub" id="datasetDesc">用于沉淀一张分析表或一条保存 SQL，后续给看板和 AI 复用。</div>
      <div class="meta" id="datasetMeta"></div>
    </section>
    <main class="content">
      <section class="stack">
        <section class="card">
          <h3>数据集信息</h3>
          <div class="kv" id="datasetInfo"></div>
        </section>
        <section class="card">
          <h3>字段建模</h3>
          <div class="model-hint">先把主表、时间字段、维度和指标沉淀成数据集，后面看板编辑器会直接复用这些配置。</div>
          <div class="model-grid">
            <div class="field">
              <label>关联表</label>
              <div class="field-inline-actions">
                <input id="linkedTableInput" placeholder="例如 public.sales_order" />
                <button class="btn-ghost" type="button" id="reloadColumnsBtn">读取字段</button>
              </div>
            </div>
            <div class="field">
              <label>分析粒度</label>
              <input id="datasetGrainInput" placeholder="例如 一行一笔订单" />
            </div>
          </div>
          <div class="field">
            <label>时间字段</label>
            <select id="timeFieldSelect"></select>
          </div>
          <div class="field">
            <label>维度字段</label>
            <div id="dimensionList" class="field-list"></div>
          </div>
          <div class="field">
            <label>指标定义</label>
            <div id="metricList" class="metric-list"></div>
            <div class="actions">
              <button class="btn-ghost" type="button" id="addMetricBtn">新增指标</button>
            </div>
          </div>
          <div class="field">
            <label>建模摘要</label>
            <div id="datasetModelSummary" class="pill-list"></div>
          </div>
          <div class="actions">
            <button class="btn-primary" type="button" id="saveDatasetModelBtn">保存建模</button>
          </div>
          <div class="status" id="modelStatus"></div>
        </section>
      </section>
      <aside class="card">
        <h3>操作</h3>
        <div class="field">
          <label>名称</label>
          <input id="datasetNameInput" />
        </div>
        <div class="field">
          <label>说明</label>
          <textarea id="datasetDescInput"></textarea>
        </div>
        <div class="actions">
          <button class="btn-primary" type="button" id="createDashboardBtn">基于它建看板</button>
          <button class="btn-ghost" type="button" id="openWorkbenchBtn">去工作台</button>
          <button class="btn-danger" type="button" id="deleteDatasetBtn">删除数据集</button>
        </div>
        <div class="status" id="datasetStatus"></div>
      </aside>
    </main>
  </div>
  <script>
    const datasetState = { dataset: null, profile: null };
    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `请求失败(${resp.status})`);
      return data;
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>\"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));
    }
    function availableColumns() {
      return datasetState.profile?.structure?.columns || [];
    }
    function renderTimeFieldSelect(value = '') {
      const select = document.getElementById('timeFieldSelect');
      const columns = availableColumns();
      if (!columns.length) {
        select.innerHTML = '<option value="">先读取字段</option>';
        return;
      }
      select.innerHTML = ['<option value="">不设置</option>', ...columns.map((column) => `<option value="${escapeHtml(column.column_name)}" ${column.column_name === value ? 'selected' : ''}>${escapeHtml(column.column_name)}</option>`)].join('');
    }
    function renderDimensionList(selectedFields = []) {
      const host = document.getElementById('dimensionList');
      const columns = availableColumns();
      if (!columns.length) {
        host.innerHTML = '<div class="empty">先读取主表字段，再配置维度。</div>';
        return;
      }
      const picked = new Set(selectedFields);
      host.innerHTML = columns.map((column) => `
        <label class="field-item">
          <input type="checkbox" value="${escapeHtml(column.column_name)}" ${picked.has(column.column_name) ? 'checked' : ''} />
          <div>
            <div class="field-name">${escapeHtml(column.column_name)}</div>
            <div class="field-meta">${escapeHtml(column.column_comment || column.data_type || column.udt_name || '暂无说明')}</div>
          </div>
        </label>
      `).join('');
    }
    function renderMetricList(metrics = []) {
      const host = document.getElementById('metricList');
      const columns = availableColumns();
      if (!metrics.length) {
        host.innerHTML = '<div class="empty">还没有指标。建议至少配置 1 个 KPI 指标。</div>';
        return;
      }
      host.innerHTML = metrics.map((metric, index) => `
        <div class="metric-row">
          <div class="metric-grid">
            <div class="field">
              <label>字段</label>
              <select data-metric-field>
                <option value="">仅 count 可留空</option>
                ${columns.map((column) => `<option value="${escapeHtml(column.column_name)}" ${column.column_name === (metric.field || '') ? 'selected' : ''}>${escapeHtml(column.column_name)}</option>`).join('')}
              </select>
            </div>
            <div class="field">
              <label>聚合</label>
              <select data-metric-agg>
                ${['count','count_distinct','sum','avg','max','min'].map((agg) => `<option value="${agg}" ${agg === (metric.agg || 'sum') ? 'selected' : ''}>${escapeHtml(agg)}</option>`).join('')}
              </select>
            </div>
            <div class="field">
              <label>指标名称</label>
              <input data-metric-label value="${escapeHtml(metric.label || '')}" placeholder="例如 成交金额" />
            </div>
            <button class="mini-btn" type="button" data-remove-metric="${index}">删除</button>
          </div>
        </div>
      `).join('');
      host.querySelectorAll('[data-remove-metric]').forEach((btn) => {
        btn.addEventListener('click', () => {
          const metrics = collectMetrics();
          metrics.splice(Number(btn.dataset.removeMetric), 1);
          renderMetricList(metrics);
        });
      });
    }
    function renderModelSummary(payload) {
      const host = document.getElementById('datasetModelSummary');
      const dimensions = Array.isArray(payload.dimensions) ? payload.dimensions : [];
      const metrics = Array.isArray(payload.metrics) ? payload.metrics : [];
      const chips = [
        `<span class="pill">时间：${escapeHtml(payload.time_field || '未设置')}</span>`,
        `<span class="pill">维度：${escapeHtml(dimensions.length)}</span>`,
        `<span class="pill">指标：${escapeHtml(metrics.length)}</span>`,
      ];
      dimensions.slice(0, 4).forEach((item) => chips.push(`<span class="pill">${escapeHtml(item.label || item.field || '')}</span>`));
      metrics.slice(0, 4).forEach((item) => chips.push(`<span class="pill">${escapeHtml(item.label || '')}</span>`));
      host.innerHTML = chips.join('');
    }
    function renderModelEditor(payload) {
      document.getElementById('linkedTableInput').value = payload.linked_table || payload.source_ref || '';
      document.getElementById('datasetGrainInput').value = payload.grain || '';
      renderTimeFieldSelect(payload.time_field || '');
      renderDimensionList((payload.dimensions || []).map((item) => item.field || ''));
      renderMetricList(payload.metrics || []);
      renderModelSummary(payload);
    }
    function renderDataset(payload) {
      datasetState.dataset = payload;
      document.getElementById('datasetTitle').textContent = payload.name || payload.id || '数据集';
      document.getElementById('datasetDesc').textContent = payload.description || '暂无说明';
      document.getElementById('datasetNameInput').value = payload.name || '';
      document.getElementById('datasetDescInput').value = payload.description || '';
      document.getElementById('datasetMeta').innerHTML = [
        `状态：${escapeHtml(payload.status || 'draft')}`,
        `来源：${escapeHtml(payload.source_type || '-')}`,
        `数据源：${escapeHtml(payload.datasource_key || '-')}`,
        `更新时间：${escapeHtml(payload.updated_at || '-')}`
      ].map((text) => `<span>${text}</span>`).join('');
      document.getElementById('datasetInfo').innerHTML = `
        <div class="kv-row"><label>ID</label><div class="mono">${escapeHtml(payload.id || '-')}</div></div>
        <div class="kv-row"><label>来源类型</label><div>${escapeHtml(payload.source_type || '-')}</div></div>
        <div class="kv-row"><label>来源引用</label><div class="mono">${escapeHtml(payload.source_ref || '-')}</div></div>
        <div class="kv-row"><label>关联表</label><div class="mono">${escapeHtml(payload.linked_table || '-')}</div></div>
        <div class="kv-row"><label>粒度</label><div>${escapeHtml(payload.grain || '未填写')}</div></div>
        <div class="kv-row"><label>时间字段</label><div>${escapeHtml(payload.time_field || '未填写')}</div></div>
        <div class="kv-row"><label>维度数</label><div>${escapeHtml((payload.dimensions || []).length)}</div></div>
        <div class="kv-row"><label>指标数</label><div>${escapeHtml((payload.metrics || []).length)}</div></div>
      `;
      renderModelEditor(payload);
    }
    function collectDimensions() {
      return Array.from(document.querySelectorAll('#dimensionList input[type="checkbox"]:checked')).map((input) => ({
        field: input.value,
        label: input.value,
      }));
    }
    function collectMetrics() {
      return Array.from(document.querySelectorAll('.metric-row')).map((row) => {
        const field = row.querySelector('[data-metric-field]')?.value.trim() || '';
        const agg = row.querySelector('[data-metric-agg]')?.value.trim() || 'sum';
        const label = row.querySelector('[data-metric-label]')?.value.trim() || '';
        return { field, agg, label: label || (field ? `${agg.toUpperCase()}_${field}` : 'COUNT') };
      }).filter((item) => item.agg === 'count' || item.field);
    }
    async function loadLinkedTableProfile(qualifiedName) {
      const statusEl = document.getElementById('modelStatus');
      const linked = String(qualifiedName || '').trim();
      if (!linked || !linked.includes('.')) {
        datasetState.profile = null;
        renderModelEditor(datasetState.dataset || {});
        statusEl.textContent = '请先填写关联表，例如 public.sales_order';
        statusEl.className = 'status error';
        return;
      }
      const [schema, ...rest] = linked.split('.');
      const table = rest.join('.');
      statusEl.textContent = `正在读取 ${linked} 的字段…`;
      statusEl.className = 'status';
      const profile = await fetchJson(`/api/table-profile?schema=${encodeURIComponent(schema)}&table=${encodeURIComponent(table)}&sample_limit=20`);
      datasetState.profile = profile;
      datasetState.dataset = {
        ...(datasetState.dataset || {}),
        linked_table: linked,
        source_ref: datasetState.dataset?.source_ref || linked,
      };
      renderModelEditor(datasetState.dataset || {});
      statusEl.textContent = `已读取 ${linked}，共 ${profile.structure?.column_count || 0} 个字段`;
      statusEl.className = 'status ok';
    }
    async function saveDatasetModel() {
      const payload = datasetState.dataset;
      if (!payload?.id) return;
      const linkedTable = document.getElementById('linkedTableInput').value.trim();
      const body = {
        id: payload.id,
        name: document.getElementById('datasetNameInput').value.trim() || payload.name,
        description: document.getElementById('datasetDescInput').value.trim(),
        source_type: payload.source_type || 'table',
        source_ref: linkedTable || payload.source_ref || '',
        datasource_key: linkedTable.split('.')[0] || payload.datasource_key || '',
        linked_table: linkedTable,
        grain: document.getElementById('datasetGrainInput').value.trim(),
        time_field: document.getElementById('timeFieldSelect').value.trim(),
        dimensions: collectDimensions(),
        metrics: collectMetrics(),
        default_filters: payload.default_filters || [],
        status: payload.status || 'draft',
      };
      const updated = await fetchJson(`/api/datasets/${encodeURIComponent(payload.id)}/update`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      });
      window.__dataset = updated;
      renderDataset(updated);
      document.getElementById('modelStatus').textContent = '建模已保存';
      document.getElementById('modelStatus').className = 'status ok';
    }
    async function loadDataset() {
      const parts = window.location.pathname.split('/').filter(Boolean);
      const datasetId = parts[1];
      const payload = await fetchJson(`/api/datasets/${encodeURIComponent(datasetId)}`);
      window.__dataset = payload;
      renderDataset(payload);
      const linked = payload.linked_table || (payload.source_type === 'table' ? payload.source_ref : '');
      if (linked) {
        try { await loadLinkedTableProfile(linked); } catch (error) {
          document.getElementById('modelStatus').textContent = error.message || '字段读取失败';
          document.getElementById('modelStatus').className = 'status error';
        }
      }
    }
    document.getElementById('backDatasetsBtn').addEventListener('click', () => { window.location.href = '/datasets'; });
    document.getElementById('openDashboardsBtn').addEventListener('click', () => { window.location.href = '/dashboards'; });
    document.getElementById('reloadColumnsBtn').addEventListener('click', async () => {
      try { await loadLinkedTableProfile(document.getElementById('linkedTableInput').value.trim()); } catch (error) {
        document.getElementById('modelStatus').textContent = error.message || '字段读取失败';
        document.getElementById('modelStatus').className = 'status error';
      }
    });
    document.getElementById('addMetricBtn').addEventListener('click', () => {
      const metrics = collectMetrics();
      metrics.push({ field: '', agg: 'sum', label: '' });
      renderMetricList(metrics);
    });
    document.getElementById('saveDatasetModelBtn').addEventListener('click', () => {
      saveDatasetModel().catch((error) => {
        document.getElementById('modelStatus').textContent = error.message || '保存建模失败';
        document.getElementById('modelStatus').className = 'status error';
      });
    });
    document.getElementById('openWorkbenchBtn').addEventListener('click', () => {
      const payload = window.__dataset || {};
      const linked = String(payload.linked_table || payload.source_ref || '').trim();
      if (!linked || !linked.includes('.')) {
        window.location.href = '/sql-workbench';
        return;
      }
      const [schema, ...rest] = linked.split('.');
      const table = rest.join('.');
      const next = new URLSearchParams();
      if (schema) next.set('schema', schema);
      if (table) next.set('table', table);
      window.location.href = `/sql-workbench?${next.toString()}`;
    });
    document.getElementById('createDashboardBtn').addEventListener('click', async () => {
      const payload = window.__dataset;
      if (!payload) return;
      if (!(payload.metrics || []).length) {
        document.getElementById('datasetStatus').textContent = '请先保存至少 1 个指标，再基于它建看板';
        document.getElementById('datasetStatus').className = 'status error';
        return;
      }
      const created = await fetchJson('/api/dashboards', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: `${payload.name || payload.id}_看板`,
          dataset_id: payload.id,
          description: '由数据集详情页创建',
          layout_mode: 'overview',
        }),
      });
      window.location.href = `/dashboards/${encodeURIComponent(created.id)}/edit`;
    });
    document.getElementById('deleteDatasetBtn').addEventListener('click', async () => {
      const payload = window.__dataset;
      if (!payload) return;
      const ok = window.confirm(`确定删除数据集“${payload.name || payload.id}”吗？`);
      if (!ok) return;
      await fetchJson(`/api/datasets/${encodeURIComponent(payload.id)}/delete`, { method: 'POST' });
      window.location.href = '/datasets';
    });
    loadDataset().catch((error) => {
      document.getElementById('datasetStatus').textContent = error.message || '数据集加载失败';
      document.getElementById('datasetStatus').className = 'status error';
    });
  </script>
</body>
</html>
"""

DASHBOARDS_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>看板中心</title>
  <style>
    :root { --bg:#eef1f5; --surface:#fff; --surface-soft:#f8fafc; --ink:#111827; --muted:#6b7280; --line:#dbe2ea; --brand:#1a73e8; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Helvetica Neue",sans-serif; font-size:13px; color:var(--ink); background:var(--bg); min-height:100vh; }
    button,input,select { font:inherit; }
    .page { min-height:100vh; display:grid; grid-template-rows:48px auto minmax(0,1fr); }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; background:#111827; color:#fff; padding:0 18px; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:700; }
    .dot { width:8px; height:8px; border-radius:999px; background:#1a73e8; }
    .topbar-actions { display:flex; align-items:center; gap:8px; }
    .topbar-btn { height:30px; padding:0 12px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.92); cursor:pointer; }
    .hero { padding:18px 22px 10px; display:grid; gap:12px; }
    .hero-title { font-size:24px; font-weight:800; }
    .hero-sub { color:var(--muted); line-height:1.7; }
    .hero-actions { display:flex; gap:10px; flex-wrap:wrap; }
    .btn-primary,.btn-ghost { height:34px; padding:0 14px; border-radius:10px; cursor:pointer; }
    .btn-primary { border:0; background:var(--brand); color:#fff; font-weight:600; }
    .btn-ghost { border:1px solid var(--line); background:#fff; color:var(--muted); }
    .content { padding:0 22px 22px; display:grid; gap:14px; }
    .toolbar { display:flex; gap:10px; flex-wrap:wrap; align-items:center; }
    .toolbar input, .toolbar select { height:36px; border:1px solid var(--line); border-radius:10px; background:#fff; padding:0 12px; }
    .toolbar input { min-width:260px; flex:1; }
    .grid { display:grid; grid-template-columns:repeat(auto-fill,minmax(300px,1fr)); gap:14px; }
    .card { border:1px solid var(--line); border-radius:14px; background:#fff; padding:16px; display:grid; gap:10px; box-shadow:0 8px 24px rgba(15,23,42,0.04); }
    .card-head { display:flex; align-items:flex-start; justify-content:space-between; gap:10px; }
    .card-title { font-size:15px; font-weight:700; }
    .badge { display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px; font-size:11px; border:1px solid rgba(26,115,232,0.16); background:rgba(26,115,232,0.08); color:var(--brand); }
    .meta { display:flex; gap:8px; flex-wrap:wrap; color:var(--muted); font-size:12px; }
    .meta span { display:inline-flex; align-items:center; padding:2px 8px; border-radius:999px; background:var(--surface-soft); }
    .desc { color:var(--muted); line-height:1.6; min-height:38px; }
    .card-actions { display:flex; gap:8px; flex-wrap:wrap; }
    .empty { padding:28px; border:1px dashed var(--line); border-radius:14px; background:var(--surface-soft); color:var(--muted); text-align:center; }
    .status { min-height:18px; color:var(--muted); font-size:12px; }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span>看板中心</div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="openMapBtn">数据地图</button>
        <button class="topbar-btn" type="button" id="openWorkbenchBtn">SQL 工作台</button>
        <button class="topbar-btn" type="button" id="openDatasetsBtn">数据集</button>
      </div>
    </header>
    <section class="hero">
      <div class="hero-title">看板</div>
      <div class="hero-sub">第一版先提供模板化看板骨架。一个看板绑定一个主数据集，不做自由拖拽，先把视觉和结构做稳。</div>
      <div class="hero-actions">
        <button class="btn-primary" type="button" id="newDashboardBtn">新建看板</button>
        <button class="btn-ghost" type="button" id="aiDraftBtn">AI 生成草稿</button>
      </div>
    </section>
    <main class="content">
      <div class="toolbar">
        <input id="dashboardSearchInput" placeholder="搜索看板名称 / 描述 / 数据集" />
        <select id="dashboardStatusFilter"><option value="">全部状态</option><option value="draft">草稿</option><option value="published">已发布</option></select>
      </div>
      <div id="dashboardStatus" class="status"></div>
      <div id="dashboardGrid" class="grid"></div>
    </main>
  </div>
  <script>
    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `请求失败(${resp.status})`);
      return data;
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>\"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',\"'\":'&#39;'}[c]));
    }
    function renderDashboards(items) {
      const host = document.getElementById('dashboardGrid');
      if (!items.length) {
        host.innerHTML = '<div class="empty">还没有看板。可以先创建数据集，再新建一个模板化看板。</div>';
        return;
      }
      host.innerHTML = items.map((item) => `
        <article class="card">
          <div class="card-head">
            <div class="card-title">${escapeHtml(item.name || item.id)}</div>
            <span class="badge">${escapeHtml(item.status || 'draft')}</span>
          </div>
          <div class="desc">${escapeHtml(item.description || '暂无说明')}</div>
          <div class="meta">
            <span>数据集：${escapeHtml(item.dataset_id || '-')}</span>
            <span>更新：${escapeHtml(item.updated_at || '-')}</span>
          </div>
          <div class="card-actions">
            <button class="btn-primary" type="button" data-open-edit="${escapeHtml(item.id)}">编辑</button>
            <button class="btn-ghost" type="button" data-open-view="${escapeHtml(item.id)}">预览</button>
          </div>
        </article>
      `).join('');
      host.querySelectorAll('[data-open-edit]').forEach((btn) => {
        btn.addEventListener('click', () => { window.location.href = `/dashboards/${encodeURIComponent(btn.dataset.openEdit)}/edit`; });
      });
      host.querySelectorAll('[data-open-view]').forEach((btn) => {
        btn.addEventListener('click', () => { window.location.href = `/dashboards/${encodeURIComponent(btn.dataset.openView)}`; });
      });
    }
    async function loadDashboards() {
      const statusEl = document.getElementById('dashboardStatus');
      statusEl.textContent = '正在加载看板…';
      const payload = await fetchJson('/api/dashboards');
      window.__dashboards = Array.isArray(payload.items) ? payload.items : [];
      applyDashboardFilters();
      statusEl.textContent = `已加载 ${window.__dashboards.length} 个看板`;
    }
    function applyDashboardFilters() {
      const keyword = document.getElementById('dashboardSearchInput').value.trim().toLowerCase();
      const status = document.getElementById('dashboardStatusFilter').value.trim();
      const rows = (window.__dashboards || []).filter((item) => {
        if (status && String(item.status || '') !== status) return false;
        if (!keyword) return true;
        const hay = [item.name, item.description, item.dataset_id].join(' ').toLowerCase();
        return hay.includes(keyword);
      });
      renderDashboards(rows);
    }
    async function createDashboard() {
      const datasetId = window.prompt('请输入要绑定的数据集 ID');
      if (!datasetId) return;
      const name = window.prompt('请输入看板名称');
      if (!name) return;
      const payload = await fetchJson('/api/dashboards', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name,
          dataset_id: datasetId,
          description: '基于模板化画布创建',
        }),
      });
      window.location.href = `/dashboards/${encodeURIComponent(payload.id)}/edit`;
    }
    document.getElementById('dashboardSearchInput').addEventListener('input', applyDashboardFilters);
    document.getElementById('dashboardStatusFilter').addEventListener('change', applyDashboardFilters);
    document.getElementById('newDashboardBtn').addEventListener('click', createDashboard);
    document.getElementById('aiDraftBtn').addEventListener('click', () => window.alert('AI 生成草稿将在下一版接入，这一版先固定模板化画布。'));
    document.getElementById('openMapBtn').addEventListener('click', () => { window.location.href = '/map'; });
    document.getElementById('openWorkbenchBtn').addEventListener('click', () => { window.location.href = '/sql-workbench'; });
    document.getElementById('openDatasetsBtn').addEventListener('click', () => { window.location.href = '/datasets'; });
    loadDashboards().catch((error) => {
      document.getElementById('dashboardStatus').textContent = error.message || '看板加载失败';
    });
  </script>
</body>
</html>
"""

DASHBOARD_EDITOR_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>看板编辑器</title>
  <style>
    :root { --bg:#eef1f5; --surface:#fff; --surface-soft:#f8fafc; --ink:#111827; --muted:#6b7280; --line:#dbe2ea; --brand:#1a73e8; --radius:12px; --ok:#0d9488; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Helvetica Neue",sans-serif; font-size:13px; color:var(--ink); background:var(--bg); min-height:100vh; }
    button,input,select,textarea { font:inherit; }
    .page { min-height:100vh; display:grid; grid-template-rows:48px minmax(0,1fr); }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; background:#111827; color:#fff; padding:0 18px; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:700; }
    .dot { width:8px; height:8px; border-radius:999px; background:#1a73e8; }
    .topbar-actions { display:flex; align-items:center; gap:8px; flex-wrap:wrap; }
    .topbar-btn { height:30px; padding:0 12px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.92); cursor:pointer; }
    .topbar-btn.primary { background:#1a73e8; border-color:#1a73e8; color:#fff; }
    .shell { display:grid; grid-template-columns:240px minmax(0,1fr) 360px; gap:1px; background:var(--line); min-height:0; }
    .panel { background:var(--surface); min-width:0; min-height:0; display:flex; flex-direction:column; }
    .panel-head { padding:14px 16px 12px; border-bottom:1px solid var(--line); display:grid; gap:8px; }
    .panel-title { font-size:16px; font-weight:700; }
    .panel-sub { color:var(--muted); font-size:12px; line-height:1.6; }
    .panel-body { padding:14px 16px 18px; overflow:auto; min-height:0; }
    .group { display:grid; gap:8px; }
    .group + .group { margin-top:14px; }
    .group h4 { font-size:12px; color:var(--muted); text-transform:uppercase; letter-spacing:.04em; }
    .chip-list { display:flex; gap:8px; flex-wrap:wrap; }
    .chip { display:inline-flex; align-items:center; padding:6px 10px; border-radius:999px; background:rgba(26,115,232,0.08); border:1px solid rgba(26,115,232,0.14); color:var(--brand); font-size:12px; }
    .tool-btn { width:100%; text-align:left; border:1px solid var(--line); border-radius:10px; background:#fff; padding:10px 12px; color:var(--ink); cursor:pointer; }
    .tool-btn:hover { border-color:rgba(26,115,232,0.24); background:rgba(26,115,232,0.04); }
    .canvas-wrap { padding:18px; overflow:auto; min-height:0; background:linear-gradient(180deg,#f8fafc 0%, #eef2f7 100%); }
    .canvas-head { display:flex; align-items:center; justify-content:space-between; gap:10px; margin-bottom:14px; flex-wrap:wrap; }
    .canvas-title { font-size:22px; font-weight:800; }
    .canvas-sub { color:var(--muted); font-size:12px; }
    .canvas-grid { display:grid; grid-template-columns:repeat(12, minmax(0,1fr)); gap:12px; align-content:start; }
    .widget { background:#fff; border:1px solid var(--line); border-radius:16px; box-shadow:0 10px 30px rgba(15,23,42,0.05); padding:14px; display:grid; gap:8px; min-height:120px; cursor:pointer; }
    .widget.active { border-color:rgba(26,115,232,0.4); box-shadow:0 12px 32px rgba(26,115,232,0.12); }
    .widget .title { font-size:13px; font-weight:700; }
    .widget .sub { color:var(--muted); font-size:12px; line-height:1.6; }
    .widget.kpi .value { font-size:28px; font-weight:800; color:var(--brand); }
    .widget.empty { display:flex; align-items:center; justify-content:center; border-style:dashed; background:rgba(255,255,255,0.7); color:var(--muted); }
    .widget-table { border:1px solid var(--line); border-radius:10px; overflow:hidden; }
    .widget-row { display:grid; grid-template-columns:1fr 1fr 1fr; }
    .widget-row span { padding:8px 10px; border-bottom:1px solid var(--line); font-size:12px; }
    .widget-row.head span { background:var(--surface-soft); color:var(--muted); font-weight:700; }
    .chart-shell { border:1px solid rgba(148,163,184,0.18); border-radius:14px; background:linear-gradient(180deg,#ffffff 0%, #f8fbff 100%); padding:12px 12px 8px; display:grid; gap:10px; min-height:260px; }
    .chart-shell.rank { min-height:240px; }
    .chart-meta { display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap; color:var(--muted); font-size:11px; }
    .chart-svg { width:100%; height:220px; display:block; }
    .chart-bars { display:grid; gap:8px; }
    .chart-bar-row { display:grid; grid-template-columns:minmax(72px, 120px) minmax(0,1fr) 64px; gap:10px; align-items:center; }
    .chart-bar-label,.chart-bar-value { font-size:11px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .chart-bar-track { height:10px; border-radius:999px; background:rgba(148,163,184,0.16); overflow:hidden; position:relative; }
    .chart-bar-fill { height:100%; border-radius:999px; background:linear-gradient(90deg,#1a73e8 0%, #3b82f6 100%); }
    .chart-empty { min-height:220px; display:flex; align-items:center; justify-content:center; border:1px dashed rgba(148,163,184,0.35); border-radius:12px; color:var(--muted); background:rgba(255,255,255,0.75); }
    .chart-axis-label { fill:#94a3b8; font-size:10px; }
    .chart-axis-line { stroke:rgba(148,163,184,0.35); stroke-width:1; }
    .chart-grid-line { stroke:rgba(148,163,184,0.18); stroke-width:1; }
    .chart-line { fill:none; stroke:#1a73e8; stroke-width:3; stroke-linecap:round; stroke-linejoin:round; }
    .chart-area { fill:url(#chartAreaFill); opacity:0.22; }
    .chart-point { fill:#1a73e8; stroke:#fff; stroke-width:2; }
    .chart-column { fill:url(#chartBarFill); }
    .field { display:grid; gap:6px; }
    .field label { font-size:12px; color:var(--muted); }
    .field input, .field select, .field textarea { width:100%; border:1px solid var(--line); border-radius:10px; background:#fff; padding:10px 12px; }
    .field textarea { min-height:92px; resize:vertical; }
    .status { min-height:18px; color:var(--muted); font-size:12px; }
    .status.ok { color:var(--ok); }
    .bind-note { padding:10px 12px; border-radius:10px; background:var(--surface-soft); color:var(--muted); line-height:1.6; font-size:12px; }
    .actions { display:flex; gap:8px; flex-wrap:wrap; }
    .empty { padding:18px; border:1px dashed var(--line); border-radius:12px; background:var(--surface-soft); color:var(--muted); text-align:center; }
    @media (max-width: 1200px) { .shell { grid-template-columns:220px minmax(0,1fr); } .panel.props { grid-column:1 / -1; border-top:1px solid var(--line); } }
    @media (max-width: 860px) { .shell { grid-template-columns:1fr; } }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span><span id="dashBrandTitle">看板编辑器</span></div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="backDashboardsBtn">返回列表</button>
        <button class="topbar-btn" type="button" id="openDatasetsBtn">数据集</button>
        <button class="topbar-btn" type="button" id="previewDashboardBtn">预览</button>
        <button class="topbar-btn primary" type="button" id="saveDashboardBtn">保存</button>
      </div>
    </header>
    <div class="shell">
      <aside class="panel">
        <div class="panel-head">
          <div class="panel-title">组件库</div>
          <div class="panel-sub">第一版先支持 KPI、趋势图、明细表三种核心组件，并直接复用数据集里定义好的维度和指标。</div>
        </div>
        <div class="panel-body">
          <div class="group"><h4>指标</h4><button class="tool-btn" type="button" data-add-widget="kpi">新增 KPI 卡</button></div>
          <div class="group"><h4>趋势</h4><button class="tool-btn" type="button" data-add-widget="line">新增趋势图</button></div>
          <div class="group"><h4>明细</h4><button class="tool-btn" type="button" data-add-widget="table">新增明细表</button></div>
        </div>
      </aside>
      <main class="panel">
        <div class="canvas-wrap">
          <div class="canvas-head">
            <div>
              <div class="canvas-title" id="dashboardTitle">看板</div>
              <div class="canvas-sub" id="dashboardDatasetHint">数据集：-</div>
            </div>
            <div class="chip-list" id="dashboardSummary"></div>
          </div>
          <div class="canvas-grid" id="dashboardCanvas"></div>
        </div>
      </main>
      <aside class="panel props">
        <div class="panel-head">
          <div class="panel-title">属性</div>
          <div class="panel-sub">先选中一个组件，再给它绑定指标、时间字段或明细字段。看板预览会直接跑真实数据。</div>
        </div>
        <div class="panel-body">
          <div class="field">
            <label>看板名称</label>
            <input id="dashboardNameInput" />
          </div>
          <div class="field">
            <label>描述</label>
            <textarea id="dashboardDescInput"></textarea>
          </div>
          <div class="field">
            <label>数据集</label>
            <input id="dashboardDatasetInput" disabled />
          </div>
          <div class="field">
            <label>布局模式</label>
            <select id="dashboardLayoutMode">
              <option value="overview">经营总览</option>
              <option value="conversion">销售转化</option>
              <option value="customer">客户分析</option>
            </select>
          </div>
          <div class="field">
            <label>当前组件</label>
            <select id="widgetSelect"></select>
          </div>
          <div class="field">
            <label>组件标题</label>
            <input id="widgetTitleInput" />
          </div>
          <div class="field" id="metricBindingField">
            <label>绑定指标</label>
            <select id="widgetMetricSelect"></select>
          </div>
          <div class="field" id="timeBindingField">
            <label>时间字段</label>
            <select id="widgetTimeFieldSelect"></select>
          </div>
          <div class="field" id="seriesTypeField">
            <label>图表类型</label>
            <select id="widgetSeriesTypeSelect">
              <option value="line">折线图</option>
              <option value="bar">柱状图</option>
              <option value="rank_bar">排名条形图</option>
            </select>
          </div>
          <div class="field" id="seriesGrainField">
            <label>时间粒度</label>
            <select id="widgetTimeGrainSelect">
              <option value="raw">原始时间</option>
              <option value="day">按天</option>
              <option value="week">按周</option>
              <option value="month">按月</option>
            </select>
          </div>
          <div class="field" id="seriesSortField">
            <label>排序方式</label>
            <select id="widgetSortOrderSelect">
              <option value="asc">字段升序</option>
              <option value="desc">字段降序</option>
              <option value="metric_desc">按指标降序</option>
              <option value="metric_asc">按指标升序</option>
            </select>
          </div>
          <div class="field" id="seriesLimitField">
            <label>展示点数</label>
            <input id="widgetLimitInput" type="number" min="1" max="200" placeholder="例如 12" />
          </div>
          <div class="field" id="columnBindingField">
            <label>明细字段</label>
            <select id="widgetColumnsSelect" multiple size="6"></select>
          </div>
          <div class="bind-note" id="widgetBindHint">选中组件后在这里绑定数据集字段。</div>
          <div class="actions">
            <button class="topbar-btn" type="button" id="removeWidgetBtn">删除组件</button>
          </div>
          <div class="status" id="editorStatus"></div>
        </div>
      </aside>
    </div>
  </div>
  <script>
    const editorState = { dashboard: null, dataset: null, preview: null, activeWidgetId: '' };
    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `请求失败(${resp.status})`);
      return data;
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>\"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
    }
    function metricKey(metric) {
      const agg = String(metric?.agg || 'sum').trim().toLowerCase() || 'sum';
      const field = String(metric?.field || '').trim();
      return field ? `${agg}:${field}` : agg;
    }
    function metricLabel(metric) {
      return metric?.label || metricKey(metric);
    }
    function datasetMetrics() { return Array.isArray(editorState.dataset?.metrics) ? editorState.dataset.metrics : []; }
    function datasetDimensions() { return Array.isArray(editorState.dataset?.dimensions) ? editorState.dataset.dimensions : []; }
    function datasetColumns() {
      const dims = datasetDimensions().map((item) => item.field).filter(Boolean);
      const time = editorState.dataset?.time_field ? [editorState.dataset.time_field] : [];
      const metricFields = datasetMetrics().map((item) => item.field).filter(Boolean);
      return Array.from(new Set([...time, ...dims, ...metricFields]));
    }
    function findWidgetPreview(widgetId) {
      return (editorState.preview?.widgets || []).find((item) => item.id === widgetId);
    }
    function formatChartNumber(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return String(value ?? '-');
      if (Math.abs(num) >= 100000000) return `${(num / 100000000).toFixed(1)}亿`;
      if (Math.abs(num) >= 10000) return `${(num / 10000).toFixed(1)}万`;
      if (Math.abs(num) >= 1000) return num.toLocaleString('zh-CN');
      return `${Number(num.toFixed(2))}`;
    }
    function shortBucketLabel(value) {
      const text = String(value ?? '');
      if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
      return text.length > 14 ? text.slice(0, 14) : text;
    }
    function renderLineChart(rows) {
      if (!rows.length) return '<div class="chart-empty">暂无趋势数据</div>';
      const width = 720;
      const height = 220;
      const padding = { top: 16, right: 18, bottom: 28, left: 18 };
      const values = rows.map((row) => Number(row.metric_value) || 0);
      const maxValue = Math.max(...values, 1);
      const minValue = Math.min(...values, 0);
      const range = Math.max(maxValue - minValue, 1);
      const stepX = rows.length > 1 ? (width - padding.left - padding.right) / (rows.length - 1) : 0;
      const points = rows.map((row, index) => {
        const x = padding.left + stepX * index;
        const y = height - padding.bottom - ((Number(row.metric_value) || 0) - minValue) / range * (height - padding.top - padding.bottom);
        return { x, y, row };
      });
      const linePath = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(' ');
      const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} L ${points[0].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} Z`;
      const grid = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
        const y = padding.top + (height - padding.top - padding.bottom) * ratio;
        return `<line class="chart-grid-line" x1="${padding.left}" y1="${y.toFixed(2)}" x2="${width - padding.right}" y2="${y.toFixed(2)}"></line>`;
      }).join('');
      const labels = points.filter((_, index) => rows.length <= 6 || index === 0 || index === rows.length - 1 || index % Math.ceil(rows.length / 4) === 0).map((point) => `<text class="chart-axis-label" x="${point.x.toFixed(2)}" y="${height - 8}" text-anchor="middle">${escapeHtml(shortBucketLabel(point.row.bucket))}</text>`).join('');
      const pointsSvg = points.map((point) => `<circle class="chart-point" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="4"></circle>`).join('');
      return `
        <div class="chart-shell">
          <div class="chart-meta">
            <span>共 ${rows.length} 个时间点</span>
            <span>峰值 ${escapeHtml(formatChartNumber(maxValue))}</span>
          </div>
          <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="趋势图">
            <defs>
              <linearGradient id="chartAreaFill" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#1a73e8"></stop>
                <stop offset="100%" stop-color="#1a73e8" stop-opacity="0"></stop>
              </linearGradient>
            </defs>
            ${grid}
            <line class="chart-axis-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
            <path class="chart-area" d="${areaPath}"></path>
            <path class="chart-line" d="${linePath}"></path>
            ${pointsSvg}
            ${labels}
          </svg>
        </div>
      `;
    }
    function renderBarChart(rows, rankMode = false) {
      if (!rows.length) return `<div class="chart-empty">${rankMode ? '暂无排名数据' : '暂无分组数据'}</div>`;
      const topRows = rows.slice(0, 8);
      const maxValue = Math.max(...topRows.map((row) => Number(row.metric_value) || 0), 1);
      return `
        <div class="chart-shell ${rankMode ? 'rank' : ''}">
          <div class="chart-meta">
            <span>共 ${rows.length} 个分组</span>
            <span>峰值 ${escapeHtml(formatChartNumber(maxValue))}</span>
          </div>
          <div class="chart-bars">
            ${topRows.map((row, index) => {
              const value = Number(row.metric_value) || 0;
              const width = Math.max(6, value / maxValue * 100);
              return `
                <div class="chart-bar-row">
                  <div class="chart-bar-label">${escapeHtml(rankMode ? `${index + 1}. ${shortBucketLabel(row.bucket)}` : shortBucketLabel(row.bucket))}</div>
                  <div class="chart-bar-track"><div class="chart-bar-fill" style="width:${width}%"></div></div>
                  <div class="chart-bar-value">${escapeHtml(formatChartNumber(value))}</div>
                </div>
              `;
            }).join('')}
          </div>
        </div>
      `;
    }
    function widgetTemplate(widget) {
      const preview = findWidgetPreview(widget.id);
      const pos = widget.position || {};
      const colStyle = `grid-column: span ${pos.w || 3}; min-height:${(pos.h || 2) * 56}px;`;
      const active = widget.id === editorState.activeWidgetId ? ' active' : '';
      if (preview?.error) {
        return `<section class="widget${active}" style="${colStyle}" data-widget-id="${escapeHtml(widget.id)}"><div class="title">${escapeHtml(widget.title || widget.id)}</div><div class="sub">${escapeHtml(preview.error)}</div></section>`;
      }
      if (widget.type === 'kpi') {
        const value = preview?.data?.value;
        return `<section class="widget kpi${active}" style="${colStyle}" data-widget-id="${escapeHtml(widget.id)}"><div class="title">${escapeHtml(widget.title || widget.id)}</div><div class="value">${escapeHtml(value == null ? '-' : value)}</div><div class="sub">${escapeHtml(metricLabel(preview?.data?.metric || widget.binding?.metric || {}))}</div></section>`;
      }
      if (['line', 'bar', 'rank_bar'].includes(widget.type)) {
        const rows = preview?.data?.rows || [];
        const emptyText = widget.type === 'line' ? '暂无趋势数据' : '暂无分组数据';
        const grainText = preview?.data?.time_grain && preview.data.time_grain !== 'raw' ? ` · ${preview.data.time_grain}` : '';
        const limitText = preview?.data?.limit ? ` · Top ${preview.data.limit}` : '';
        const chartHtml = widget.type === 'line'
          ? renderLineChart(rows)
          : renderBarChart(rows, widget.type === 'rank_bar');
        return `<section class="widget${active}" style="${colStyle}" data-widget-id="${escapeHtml(widget.id)}"><div class="title">${escapeHtml(widget.title || widget.id)}</div><div class="sub">${escapeHtml((preview?.data?.bucket_field || '') + grainText + ' / ' + metricLabel(preview?.data?.metric || widget.binding?.metric || {} ) + limitText)}</div>${rows.length ? chartHtml : `<div class="empty">${emptyText}</div>`}</section>`;
      }
      if (widget.type === 'table') {
        const cols = preview?.data?.columns || [];
        const rows = preview?.data?.rows || [];
        return `<section class="widget${active}" style="${colStyle}" data-widget-id="${escapeHtml(widget.id)}"><div class="title">${escapeHtml(widget.title || widget.id)}</div><div class="widget-table">${cols.length ? `<div class="widget-row head">${cols.slice(0,3).map((col)=>`<span>${escapeHtml(col)}</span>`).join('')}</div>${rows.slice(0,5).map((row)=>`<div class="widget-row">${cols.slice(0,3).map((col)=>`<span>${escapeHtml(row[col])}</span>`).join('')}</div>`).join('')}` : '<div class="empty">暂无明细数据</div>'}</div></section>`;
      }
      return `<section class="widget empty${active}" style="${colStyle}" data-widget-id="${escapeHtml(widget.id)}">未支持的组件类型</section>`;
    }
    function bindCanvasEvents() {
      document.querySelectorAll('[data-widget-id]').forEach((node) => {
        node.addEventListener('click', () => {
          editorState.activeWidgetId = node.dataset.widgetId || '';
          renderEditor();
        });
      });
    }
    function activeWidget() {
      return (editorState.dashboard?.widgets || []).find((item) => item.id === editorState.activeWidgetId) || null;
    }
    function ensureActiveWidget() {
      const widgets = editorState.dashboard?.widgets || [];
      if (!widgets.length) {
        editorState.activeWidgetId = '';
        return;
      }
      if (!widgets.some((item) => item.id === editorState.activeWidgetId)) {
        editorState.activeWidgetId = widgets[0].id;
      }
    }
    function renderWidgetBinding() {
      const widget = activeWidget();
      const widgetSelect = document.getElementById('widgetSelect');
      const titleInput = document.getElementById('widgetTitleInput');
      const metricSelect = document.getElementById('widgetMetricSelect');
      const timeSelect = document.getElementById('widgetTimeFieldSelect');
      const seriesTypeSelect = document.getElementById('widgetSeriesTypeSelect');
      const timeGrainSelect = document.getElementById('widgetTimeGrainSelect');
      const sortOrderSelect = document.getElementById('widgetSortOrderSelect');
      const limitInput = document.getElementById('widgetLimitInput');
      const columnsSelect = document.getElementById('widgetColumnsSelect');
      const hint = document.getElementById('widgetBindHint');
      const widgets = editorState.dashboard?.widgets || [];
      widgetSelect.innerHTML = widgets.map((item) => `<option value="${escapeHtml(item.id)}" ${item.id === editorState.activeWidgetId ? 'selected' : ''}>${escapeHtml(item.title || item.id)}</option>`).join('') || '<option value="">暂无组件</option>';
      if (!widget) {
        titleInput.value = '';
        metricSelect.innerHTML = '<option value="">暂无指标</option>';
        timeSelect.innerHTML = '<option value="">暂无时间字段</option>';
        seriesTypeSelect.value = 'line';
        timeGrainSelect.value = 'raw';
        sortOrderSelect.value = 'asc';
        limitInput.value = '';
        columnsSelect.innerHTML = '';
        hint.textContent = '先新增组件，再绑定数据集字段。';
        return;
      }
      titleInput.value = widget.title || '';
      const metrics = datasetMetrics();
      metricSelect.innerHTML = metrics.map((metric) => {
        const key = metricKey(metric);
        const selected = key === (widget.binding?.metric_key || '');
        return `<option value="${escapeHtml(key)}" ${selected ? 'selected' : ''}>${escapeHtml(metricLabel(metric))}</option>`;
      }).join('') || '<option value="">暂无指标</option>';
      const timeField = editorState.dataset?.time_field || '';
      const dimensionOptions = datasetDimensions().map((item) => item.field).filter(Boolean);
      timeSelect.innerHTML = ['<option value="">不设置</option>', ...[timeField, ...dimensionOptions].filter((item, index, arr) => item && arr.indexOf(item) === index).map((field) => `<option value="${escapeHtml(field)}" ${((widget.binding?.time_field || widget.binding?.dimension_field || '') === field) ? 'selected' : ''}>${escapeHtml(field)}</option>`)].join('');
      seriesTypeSelect.value = ['line', 'bar', 'rank_bar'].includes(widget.type) ? widget.type : 'line';
      timeGrainSelect.value = widget.binding?.time_grain || 'raw';
      sortOrderSelect.value = widget.binding?.sort_order || (widget.type === 'line' ? 'asc' : 'metric_desc');
      limitInput.value = widget.binding?.limit || '';
      const selectedColumns = new Set(Array.isArray(widget.binding?.columns) ? widget.binding.columns : []);
      columnsSelect.innerHTML = datasetColumns().map((column) => `<option value="${escapeHtml(column)}" ${selectedColumns.has(column) ? 'selected' : ''}>${escapeHtml(column)}</option>`).join('');
      document.getElementById('metricBindingField').style.display = ['kpi','line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('timeBindingField').style.display = ['line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('seriesTypeField').style.display = ['line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('seriesGrainField').style.display = ['line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('seriesSortField').style.display = ['line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('seriesLimitField').style.display = ['line','bar','rank_bar'].includes(widget.type) ? 'grid' : 'none';
      document.getElementById('columnBindingField').style.display = widget.type === 'table' ? 'grid' : 'none';
      hint.textContent = widget.type === 'kpi' ? 'KPI 绑定 1 个指标。' : (widget.type === 'line' ? '趋势图绑定时间字段 + 指标。' : (['bar','rank_bar'].includes(widget.type) ? '分组图绑定维度字段 + 指标。' : '明细表绑定要展示的字段。'));
    }
    function renderEditor() {
      const dashboard = editorState.dashboard || {};
      document.getElementById('dashboardTitle').textContent = dashboard.name || dashboard.id || '看板';
      document.getElementById('dashboardDatasetHint').textContent = `数据集：${dashboard.dataset_id || '-'} · 主表：${editorState.dataset?.linked_table || '-'}`;
      document.getElementById('dashboardNameInput').value = dashboard.name || '';
      document.getElementById('dashboardDescInput').value = dashboard.description || '';
      document.getElementById('dashboardDatasetInput').value = dashboard.dataset_id || '';
      document.getElementById('dashboardLayoutMode').value = dashboard.layout_mode || 'overview';
      document.getElementById('dashboardSummary').innerHTML = [
        `<span class="chip">维度 ${escapeHtml(datasetDimensions().length)}</span>`,
        `<span class="chip">指标 ${escapeHtml(datasetMetrics().length)}</span>`,
        `<span class="chip">组件 ${escapeHtml((dashboard.widgets || []).length)}</span>`
      ].join('');
      ensureActiveWidget();
      document.getElementById('dashboardCanvas').innerHTML = (dashboard.widgets || []).map(widgetTemplate).join('') || '<div class="empty">当前看板还没有组件</div>';
      bindCanvasEvents();
      renderWidgetBinding();
    }
    async function reloadPreview() {
      const parts = window.location.pathname.split('/').filter(Boolean);
      const dashboardId = parts[1];
      editorState.preview = await fetchJson(`/api/dashboard-preview/${encodeURIComponent(dashboardId)}`);
      renderEditor();
    }
    function buildWidgetBindingForType(type) {
      const metrics = datasetMetrics();
      const metric = metrics[0] || null;
      const timeField = editorState.dataset?.time_field || '';
      const dimensionField = datasetDimensions()[0]?.field || '';
      if (type === 'kpi') return { metric_key: metric ? metricKey(metric) : '', metric };
      if (type === 'line') return { metric_key: metric ? metricKey(metric) : '', metric, time_field: timeField, time_grain: 'day', sort_order: 'asc', limit: 24 };
      if (['bar', 'rank_bar'].includes(type)) return { metric_key: metric ? metricKey(metric) : '', metric, dimension_field: dimensionField || timeField, time_grain: 'raw', sort_order: 'metric_desc', limit: 12 };
      if (type === 'table') return { columns: datasetColumns().slice(0, 3) };
      return {};
    }
    function addWidget(type) {
      const widgets = editorState.dashboard.widgets || [];
      const index = widgets.length + 1;
      const base = type === 'kpi' ? { w: 3, h: 2 } : (type === 'line' ? { w: 8, h: 4 } : { w: 12, h: 5 });
      const widget = { id: `${type}_${Date.now()}`, type, title: type === 'kpi' ? `KPI ${index}` : (type === 'line' ? `趋势图 ${index}` : `明细表 ${index}`), position: { x: 0, y: index * 2, ...base }, binding: buildWidgetBindingForType(type) };
      editorState.dashboard.widgets = [...widgets, widget];
      editorState.activeWidgetId = widget.id;
      renderEditor();
    }
    function applyWidgetForm() {
      const widget = activeWidget();
      if (!widget) return;
      const nextType = document.getElementById('widgetSeriesTypeSelect').value || widget.type;
      if (['line', 'bar', 'rank_bar'].includes(widget.type) && ['line', 'bar', 'rank_bar'].includes(nextType)) {
        widget.type = nextType;
      }
      widget.title = document.getElementById('widgetTitleInput').value.trim() || widget.title;
      if (widget.type === 'kpi') {
        widget.binding = { metric_key: document.getElementById('widgetMetricSelect').value, metric: datasetMetrics().find((item) => metricKey(item) === document.getElementById('widgetMetricSelect').value) || null };
      } else if (widget.type === 'line') {
        widget.binding = {
          metric_key: document.getElementById('widgetMetricSelect').value,
          metric: datasetMetrics().find((item) => metricKey(item) === document.getElementById('widgetMetricSelect').value) || null,
          time_field: document.getElementById('widgetTimeFieldSelect').value,
          time_grain: document.getElementById('widgetTimeGrainSelect').value,
          sort_order: document.getElementById('widgetSortOrderSelect').value,
          limit: Number(document.getElementById('widgetLimitInput').value || 24),
        };
      } else if (['bar', 'rank_bar'].includes(widget.type)) {
        widget.binding = {
          metric_key: document.getElementById('widgetMetricSelect').value,
          metric: datasetMetrics().find((item) => metricKey(item) === document.getElementById('widgetMetricSelect').value) || null,
          dimension_field: document.getElementById('widgetTimeFieldSelect').value,
          time_grain: document.getElementById('widgetTimeGrainSelect').value,
          sort_order: document.getElementById('widgetSortOrderSelect').value,
          limit: Number(document.getElementById('widgetLimitInput').value || 12),
        };
      } else if (widget.type === 'table') {
        widget.binding = { columns: Array.from(document.getElementById('widgetColumnsSelect').selectedOptions).map((item) => item.value) };
      }
    }
    async function saveDashboard() {
      applyWidgetForm();
      const payload = editorState.dashboard;
      const parts = window.location.pathname.split('/').filter(Boolean);
      const dashboardId = parts[1];
      const updated = await fetchJson(`/api/dashboards/${encodeURIComponent(dashboardId)}/update`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({
          name: document.getElementById('dashboardNameInput').value.trim() || payload.name,
          description: document.getElementById('dashboardDescInput').value.trim(),
          dataset_id: payload.dataset_id,
          layout_mode: document.getElementById('dashboardLayoutMode').value,
          widgets: payload.widgets || [],
          status: payload.status || 'draft',
        }),
      });
      editorState.dashboard = updated;
      document.getElementById('editorStatus').textContent = '已保存，正在刷新预览数据…';
      document.getElementById('editorStatus').className = 'status ok';
      await reloadPreview();
      document.getElementById('editorStatus').textContent = '已保存';
      document.getElementById('editorStatus').className = 'status ok';
    }
    async function loadDashboard() {
      const parts = window.location.pathname.split('/').filter(Boolean);
      const dashboardId = parts[1];
      const dashboard = await fetchJson(`/api/dashboards/${encodeURIComponent(dashboardId)}`);
      const dataset = dashboard.dataset_id ? await fetchJson(`/api/datasets/${encodeURIComponent(dashboard.dataset_id)}`) : null;
      editorState.dashboard = dashboard;
      editorState.dataset = dataset;
      await reloadPreview();
      document.getElementById('editorStatus').textContent = `已加载看板 ${dashboard.id}`;
    }
    document.getElementById('backDashboardsBtn').addEventListener('click', () => { window.location.href = '/dashboards'; });
    document.getElementById('openDatasetsBtn').addEventListener('click', () => { window.location.href = '/datasets'; });
    document.getElementById('previewDashboardBtn').addEventListener('click', () => {
      const parts = window.location.pathname.split('/').filter(Boolean);
      window.location.href = `/dashboards/${encodeURIComponent(parts[1])}`;
    });
    document.getElementById('saveDashboardBtn').addEventListener('click', () => {
      saveDashboard().catch((error) => {
        document.getElementById('editorStatus').textContent = error.message || '保存失败';
        document.getElementById('editorStatus').className = 'status';
      });
    });
    document.querySelectorAll('[data-add-widget]').forEach((btn) => btn.addEventListener('click', () => addWidget(btn.dataset.addWidget || 'kpi')));
    document.getElementById('widgetSelect').addEventListener('change', (event) => { editorState.activeWidgetId = event.target.value; renderEditor(); });
    document.getElementById('widgetTitleInput').addEventListener('input', applyWidgetForm);
    document.getElementById('widgetMetricSelect').addEventListener('change', applyWidgetForm);
    document.getElementById('widgetTimeFieldSelect').addEventListener('change', applyWidgetForm);
    document.getElementById('widgetSeriesTypeSelect').addEventListener('change', () => { applyWidgetForm(); renderEditor(); });
    document.getElementById('widgetTimeGrainSelect').addEventListener('change', applyWidgetForm);
    document.getElementById('widgetSortOrderSelect').addEventListener('change', applyWidgetForm);
    document.getElementById('widgetLimitInput').addEventListener('input', applyWidgetForm);
    document.getElementById('widgetColumnsSelect').addEventListener('change', applyWidgetForm);
    document.getElementById('removeWidgetBtn').addEventListener('click', () => {
      if (!editorState.activeWidgetId) return;
      editorState.dashboard.widgets = (editorState.dashboard.widgets || []).filter((item) => item.id !== editorState.activeWidgetId);
      editorState.activeWidgetId = '';
      renderEditor();
    });
    loadDashboard().catch((error) => {
      document.getElementById('editorStatus').textContent = error.message || '看板加载失败';
    });
  </script>
</body>
</html>
"""

DASHBOARD_VIEW_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>看板预览</title>
  <style>
    :root { --bg:#eef1f5; --surface:#fff; --surface-soft:#f8fafc; --ink:#111827; --muted:#6b7280; --line:#dbe2ea; --brand:#1a73e8; }
    * { box-sizing:border-box; margin:0; padding:0; }
    body { font-family:-apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Helvetica Neue",sans-serif; font-size:13px; color:var(--ink); background:var(--bg); min-height:100vh; }
    button { font:inherit; }
    .page { min-height:100vh; display:grid; grid-template-rows:48px auto minmax(0,1fr); }
    .topbar { display:flex; align-items:center; justify-content:space-between; gap:12px; background:#111827; color:#fff; padding:0 18px; }
    .brand { display:flex; align-items:center; gap:8px; font-weight:700; }
    .dot { width:8px; height:8px; border-radius:999px; background:#1a73e8; }
    .topbar-actions { display:flex; align-items:center; gap:8px; }
    .topbar-btn { height:30px; padding:0 12px; border:1px solid rgba(255,255,255,0.14); border-radius:8px; background:rgba(255,255,255,0.08); color:rgba(255,255,255,0.92); cursor:pointer; }
    .hero { padding:18px 22px 10px; display:grid; gap:8px; }
    .title { font-size:28px; font-weight:800; }
    .sub { color:var(--muted); line-height:1.7; }
    .content { padding:0 22px 22px; }
    .grid { display:grid; grid-template-columns:repeat(12,minmax(0,1fr)); gap:12px; }
    .widget { background:#fff; border:1px solid var(--line); border-radius:16px; box-shadow:0 10px 30px rgba(15,23,42,0.05); padding:14px; display:grid; gap:8px; min-height:120px; }
    .widget .title-sm { font-size:13px; font-weight:700; }
    .widget .sub-sm { color:var(--muted); font-size:12px; line-height:1.6; }
    .widget.kpi .value { font-size:30px; font-weight:800; color:var(--brand); }
    .widget-table { border:1px solid var(--line); border-radius:10px; overflow:hidden; }
    .widget-row { display:grid; grid-template-columns:1fr 1fr 1fr; }
    .widget-row span { padding:8px 10px; border-bottom:1px solid var(--line); font-size:12px; }
    .widget-row.head span { background:var(--surface-soft); color:var(--muted); font-weight:700; }
    .chart-shell { border:1px solid rgba(148,163,184,0.18); border-radius:14px; background:linear-gradient(180deg,#ffffff 0%, #f8fbff 100%); padding:12px 12px 8px; display:grid; gap:10px; min-height:260px; }
    .chart-shell.rank { min-height:240px; }
    .chart-meta { display:flex; justify-content:space-between; gap:12px; align-items:center; flex-wrap:wrap; color:var(--muted); font-size:11px; }
    .chart-svg { width:100%; height:220px; display:block; }
    .chart-bars { display:grid; gap:8px; }
    .chart-bar-row { display:grid; grid-template-columns:minmax(72px, 120px) minmax(0,1fr) 64px; gap:10px; align-items:center; }
    .chart-bar-label,.chart-bar-value { font-size:11px; color:var(--muted); white-space:nowrap; overflow:hidden; text-overflow:ellipsis; }
    .chart-bar-track { height:10px; border-radius:999px; background:rgba(148,163,184,0.16); overflow:hidden; position:relative; }
    .chart-bar-fill { height:100%; border-radius:999px; background:linear-gradient(90deg,#1a73e8 0%, #3b82f6 100%); }
    .chart-empty { min-height:220px; display:flex; align-items:center; justify-content:center; border:1px dashed rgba(148,163,184,0.35); border-radius:12px; color:var(--muted); background:rgba(255,255,255,0.75); }
    .chart-axis-label { fill:#94a3b8; font-size:10px; }
    .chart-axis-line { stroke:rgba(148,163,184,0.35); stroke-width:1; }
    .chart-grid-line { stroke:rgba(148,163,184,0.18); stroke-width:1; }
    .chart-line { fill:none; stroke:#1a73e8; stroke-width:3; stroke-linecap:round; stroke-linejoin:round; }
    .chart-area { fill:url(#chartAreaFillPreview); opacity:0.22; }
    .chart-point { fill:#1a73e8; stroke:#fff; stroke-width:2; }
    .chart-column { fill:url(#chartBarFillPreview); }
    .empty { padding:18px; border:1px dashed var(--line); border-radius:12px; background:var(--surface-soft); color:var(--muted); text-align:center; }
  </style>
</head>
<body>
  <div class="page">
    <header class="topbar">
      <div class="brand"><span class="dot"></span>看板预览</div>
      <div class="topbar-actions">
        <button class="topbar-btn" type="button" id="backDashboardsBtn">返回列表</button>
        <button class="topbar-btn" type="button" id="editDashboardBtn">编辑</button>
      </div>
    </header>
    <section class="hero">
      <div class="title" id="dashboardTitle">看板</div>
      <div class="sub" id="dashboardSub">预览模式直接展示绑定数据集后的真实结果。</div>
    </section>
    <main class="content">
      <div class="grid" id="dashboardGrid"></div>
    </main>
  </div>
  <script>
    async function fetchJson(url, options = {}) {
      const resp = await fetch(url, options);
      const data = await resp.json().catch(() => ({}));
      if (!resp.ok) throw new Error(data.error || `请求失败(${resp.status})`);
      return data;
    }
    function escapeHtml(value) {
      return String(value ?? '').replace(/[&<>\"']/g, (c) => ({'&':'&amp;','<':'&lt;','>':'&gt;','\"':'&quot;',"'":'&#39;'}[c]));
    }
    function metricLabel(metric) {
      if (!metric) return '-';
      const agg = String(metric.agg || '').toUpperCase();
      return metric.label || (metric.field ? `${agg}_${metric.field}` : agg || 'COUNT');
    }
    function formatChartNumber(value) {
      const num = Number(value);
      if (!Number.isFinite(num)) return String(value ?? '-');
      if (Math.abs(num) >= 100000000) return `${(num / 100000000).toFixed(1)}亿`;
      if (Math.abs(num) >= 10000) return `${(num / 10000).toFixed(1)}万`;
      if (Math.abs(num) >= 1000) return num.toLocaleString('zh-CN');
      return `${Number(num.toFixed(2))}`;
    }
    function shortBucketLabel(value) {
      const text = String(value ?? '');
      if (/^\d{4}-\d{2}-\d{2}/.test(text)) return text.slice(0, 10);
      return text.length > 14 ? text.slice(0, 14) : text;
    }
    function renderLineChart(rows) {
      if (!rows.length) return '<div class="chart-empty">暂无趋势数据</div>';
      const width = 720;
      const height = 220;
      const padding = { top: 16, right: 18, bottom: 28, left: 18 };
      const values = rows.map((row) => Number(row.metric_value) || 0);
      const maxValue = Math.max(...values, 1);
      const minValue = Math.min(...values, 0);
      const range = Math.max(maxValue - minValue, 1);
      const stepX = rows.length > 1 ? (width - padding.left - padding.right) / (rows.length - 1) : 0;
      const points = rows.map((row, index) => {
        const x = padding.left + stepX * index;
        const y = height - padding.bottom - ((Number(row.metric_value) || 0) - minValue) / range * (height - padding.top - padding.bottom);
        return { x, y, row };
      });
      const linePath = points.map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`).join(' ');
      const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} L ${points[0].x.toFixed(2)} ${(height - padding.bottom).toFixed(2)} Z`;
      const grid = [0, 0.25, 0.5, 0.75, 1].map((ratio) => {
        const y = padding.top + (height - padding.top - padding.bottom) * ratio;
        return `<line class="chart-grid-line" x1="${padding.left}" y1="${y.toFixed(2)}" x2="${width - padding.right}" y2="${y.toFixed(2)}"></line>`;
      }).join('');
      const labels = points.filter((_, index) => rows.length <= 6 || index === 0 || index === rows.length - 1 || index % Math.ceil(rows.length / 4) === 0).map((point) => `<text class="chart-axis-label" x="${point.x.toFixed(2)}" y="${height - 8}" text-anchor="middle">${escapeHtml(shortBucketLabel(point.row.bucket))}</text>`).join('');
      const pointsSvg = points.map((point) => `<circle class="chart-point" cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="4"></circle>`).join('');
      return `
        <div class="chart-shell">
          <div class="chart-meta">
            <span>共 ${rows.length} 个时间点</span>
            <span>峰值 ${escapeHtml(formatChartNumber(maxValue))}</span>
          </div>
          <svg class="chart-svg" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none" aria-label="趋势图">
            <defs>
              <linearGradient id="chartAreaFillPreview" x1="0" y1="0" x2="0" y2="1">
                <stop offset="0%" stop-color="#1a73e8"></stop>
                <stop offset="100%" stop-color="#1a73e8" stop-opacity="0"></stop>
              </linearGradient>
            </defs>
            ${grid}
            <line class="chart-axis-line" x1="${padding.left}" y1="${height - padding.bottom}" x2="${width - padding.right}" y2="${height - padding.bottom}"></line>
            <path class="chart-area" d="${areaPath}"></path>
            <path class="chart-line" d="${linePath}"></path>
            ${pointsSvg}
            ${labels}
          </svg>
        </div>
      `;
    }
    function renderBarChart(rows, rankMode = false) {
      if (!rows.length) return `<div class="chart-empty">${rankMode ? '暂无排名数据' : '暂无分组数据'}</div>`;
      const topRows = rows.slice(0, 8);
      const maxValue = Math.max(...topRows.map((row) => Number(row.metric_value) || 0), 1);
      return `
        <div class="chart-shell ${rankMode ? 'rank' : ''}">
          <div class="chart-meta">
            <span>共 ${rows.length} 个分组</span>
            <span>峰值 ${escapeHtml(formatChartNumber(maxValue))}</span>
          </div>
          <div class="chart-bars">
            ${topRows.map((row, index) => {
              const value = Number(row.metric_value) || 0;
              const width = Math.max(6, value / maxValue * 100);
              return `
                <div class="chart-bar-row">
                  <div class="chart-bar-label">${escapeHtml(rankMode ? `${index + 1}. ${shortBucketLabel(row.bucket)}` : shortBucketLabel(row.bucket))}</div>
                  <div class="chart-bar-track"><div class="chart-bar-fill" style="width:${width}%"></div></div>
                  <div class="chart-bar-value">${escapeHtml(formatChartNumber(value))}</div>
                </div>
              `;
            }).join('')}
          </div>
        </div>
      `;
    }
    function widgetTemplate(widget) {
      const pos = widget.position || {};
      const colStyle = `grid-column: span ${pos.w || 3}; min-height:${(pos.h || 2) * 56}px;`;
      if (widget.error) {
        return `<section class="widget" style="${colStyle}"><div class="title-sm">${escapeHtml(widget.title || widget.id)}</div><div class="sub-sm">${escapeHtml(widget.error)}</div></section>`;
      }
      if (widget.type === 'kpi') {
        return `<section class="widget kpi" style="${colStyle}"><div class="title-sm">${escapeHtml(widget.title || widget.id)}</div><div class="value">${escapeHtml(widget.data?.value == null ? '-' : widget.data.value)}</div><div class="sub-sm">${escapeHtml(metricLabel(widget.data?.metric))}</div></section>`;
      }
      if (['line', 'bar', 'rank_bar'].includes(widget.type)) {
        const rows = widget.data?.rows || [];
        const emptyText = widget.type === 'line' ? '暂无趋势数据' : '暂无分组数据';
        const grainText = widget.data?.time_grain && widget.data.time_grain !== 'raw' ? ` · ${widget.data.time_grain}` : '';
        const limitText = widget.data?.limit ? ` · Top ${widget.data.limit}` : '';
        const chartHtml = widget.type === 'line'
          ? renderLineChart(rows)
          : renderBarChart(rows, widget.type === 'rank_bar');
        return `<section class="widget" style="${colStyle}"><div class="title-sm">${escapeHtml(widget.title || widget.id)}</div><div class="sub-sm">${escapeHtml((widget.data?.bucket_field || '') + grainText + ' / ' + metricLabel(widget.data?.metric) + limitText)}</div>${rows.length ? chartHtml : `<div class="empty">${emptyText}</div>`}</section>`;
      }
      if (widget.type === 'table') {
        const cols = widget.data?.columns || [];
        const rows = widget.data?.rows || [];
        return `<section class="widget" style="${colStyle}"><div class="title-sm">${escapeHtml(widget.title || widget.id)}</div><div class="widget-table">${cols.length ? `<div class="widget-row head">${cols.slice(0,3).map((col)=>`<span>${escapeHtml(col)}</span>`).join('')}</div>${rows.slice(0,8).map((row)=>`<div class="widget-row">${cols.slice(0,3).map((col)=>`<span>${escapeHtml(row[col])}</span>`).join('')}</div>`).join('')}` : '<div class="empty">暂无明细数据</div>'}</div></section>`;
      }
      return `<section class="widget" style="${colStyle}"><div class="title-sm">${escapeHtml(widget.title || widget.id)}</div><div class="sub-sm">暂不支持的组件类型</div></section>`;
    }
    async function loadDashboard() {
      const parts = window.location.pathname.split('/').filter(Boolean);
      const dashboardId = parts[1];
      const dashboard = await fetchJson(`/api/dashboards/${encodeURIComponent(dashboardId)}`);
      const preview = await fetchJson(`/api/dashboard-preview/${encodeURIComponent(dashboardId)}`);
      document.getElementById('dashboardTitle').textContent = dashboard.name || dashboard.id || '看板';
      document.getElementById('dashboardSub').textContent = `数据集：${dashboard.dataset_id || '-'} · 组件数：${(dashboard.widgets || []).length}`;
      document.getElementById('dashboardGrid').innerHTML = (preview.widgets || []).map(widgetTemplate).join('') || '<div class="empty">当前还没有组件</div>';
    }
    document.getElementById('backDashboardsBtn').addEventListener('click', () => { window.location.href = '/dashboards'; });
    document.getElementById('editDashboardBtn').addEventListener('click', () => {
      const parts = window.location.pathname.split('/').filter(Boolean);
      window.location.href = `/dashboards/${encodeURIComponent(parts[1])}/edit`;
    });
    loadDashboard();
  </script>
</body>
</html>
"""



class DataMapHandler(BaseHTTPRequestHandler):
    service = DataMapService(refresh_seconds=600)
    lineage_service = LineageService()
    _activity_cache: dict = {}
    _activity_cache_ttl: int = 300  # 5 minutes


    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/map"}:
            self._send_html(_load_repo_html_template("map.html", HTML_TEMPLATE))
            return
        if parsed.path == "/sql-workbench":
            self._send_html(SQL_WORKBENCH_TEMPLATE)
            return
        if parsed.path == "/datasets":
            self._send_html(DATASETS_TEMPLATE)
            return
        if re.fullmatch(r"/datasets/[^/]+", parsed.path):
            self._send_html(DATASET_DETAIL_TEMPLATE)
            return
        if parsed.path == "/dashboards":
            self._send_html(DASHBOARDS_TEMPLATE)
            return
        if re.fullmatch(r"/dashboards/[^/]+/edit", parsed.path):
            self._send_html(DASHBOARD_EDITOR_TEMPLATE)
            return
        if re.fullmatch(r"/dashboards/[^/]+", parsed.path):
            self._send_html(DASHBOARD_VIEW_TEMPLATE)
            return
        if parsed.path == "/api/sql-workspace":
            payload = self.service.get_sql_workspace()
            self._send_json(payload, status=200)
            return
        if parsed.path == "/api/datasets":
            payload = self.service.list_datasets()
            self._send_json(payload, status=200)
            return
        if parsed.path.startswith("/api/datasets/"):
            dataset_id = parsed.path.rsplit("/", 1)[-1]
            try:
                payload = self.service.read_dataset(dataset_id)
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/dashboards":
            payload = self.service.list_dashboards()
            self._send_json(payload, status=200)
            return
        if parsed.path.startswith("/api/dashboards/"):
            dashboard_id = parsed.path.rsplit("/", 1)[-1]
            try:
                payload = self.service.read_dashboard(dashboard_id)
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if re.fullmatch(r"/api/dashboard-preview/[^/]+", parsed.path):
            dashboard_id = parsed.path.rsplit("/", 1)[-1]
            try:
                payload = self.service.preview_dashboard(dashboard_id)
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/sql-workspace/file":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.read_sql_file(params.get("path") or "")
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/dashboard":
            payload = self.service.get_dashboard(limit=8)
            self._send_json(payload, status=200)
            return
        if parsed.path == "/api/search":
            params = self._parse_params(parsed.query)
            payload = self.service.search_tables(
                keyword=params.get("keyword", ""),
                exact=(params.get("exact") == "true"),
                source_type=params.get("source_type") or None,
                business_domain=params.get("business_domain") or None,
                project=params.get("project") or None,
                storage_type=params.get("storage_type") or None,
                owner=params.get("owner") or None,
                favorites_only=(params.get("favorites_only") == "true"),
                limit=int(params.get("limit") or "50"),
            )
            self._send_json(payload, status=200)
            return
        if parsed.path == "/api/table-profile":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_table_profile(
                    schema=params.get("schema") or "public",
                    table=params.get("table") or "",
                    sample_limit=int(params.get("sample_limit") or "20"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/table-preview":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_table_preview(
                    schema=params.get("schema") or "public",
                    table=params.get("table") or "",
                    sample_limit=int(params.get("sample_limit") or "20"),
                    preview_column=params.get("preview_column") or "",
                    preview_keyword=params.get("preview_keyword") or "",
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/table-sql":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.run_table_sql(
                    schema=params.get("schema") or "public",
                    table=params.get("table") or "",
                    sql=params.get("sql") or "",
                    row_limit=int(params.get("row_limit") or "200"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/refresh":
            payload = self.service.refresh_catalog()
            self._send_json(payload, status=200)
            return
        if parsed.path == "/healthz":
            self._send_json({"ok": True}, status=200)
            return
        if parsed.path == "/api/table-schedule":
            params = self._parse_params(parsed.query)
            try:
                payload = self.lineage_service.get_table_schedule_info(
                    connection_name=params.get("connection_name") or None,
                    schema=params.get("schema") or None,
                    table=params.get("table") or None,
                    table_comment=params.get("table_comment") or None,
                    run_limit=int(params.get("run_limit") or "10"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/lineage-graph":
            params = self._parse_params(parsed.query)
            try:
                kind = params.get("lineage_kind") or "target"
                if kind == "graph":
                    # MySQL/源库表：按图节点(connection+table)取全血缘
                    payload = self.lineage_service.get_table_node_full_lineage_graph(
                        connection_name=params.get("connection_name") or None,
                        table=params.get("table") or None,
                        direction="both",
                        depth=int(params.get("upstream_depth") or "12"),
                    )
                else:
                    payload = self.lineage_service.get_target_table_full_upstream_graph(
                        connection_name=params.get("connection_name") or None,
                        schema=params.get("schema") or None,
                        table=params.get("table") or None,
                        upstream_depth=int(params.get("upstream_depth") or "12"),
                    )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/lineage-node-activity":
            params = self._parse_params(parsed.query)
            node_id = params.get("node_id") or ""
            run_limit = int(params.get("run_limit") or "10")
            conn = params.get("connection_name")
            schema = params.get("schema")
            table = params.get("table")
            target_table_id = params.get("target_table_id")
            try:
                import time as _t
                cache_key = f"act|{node_id}|{target_table_id}|{conn}|{schema}|{table}|{run_limit}"
                now = _t.time()
                cached = self._activity_cache.get(cache_key)
                if cached and now - cached["ts"] < self._activity_cache_ttl:
                    runs = cached["data"]
                else:
                    if node_id.startswith("target::"):
                        raw = self.lineage_service.get_target_table_activity(
                            target_table_id=target_table_id or None,
                            connection_name=conn,
                            table=table,
                            schema=schema,
                            run_limit=run_limit,
                        )
                    elif node_id.startswith("graph::"):
                        try:
                            raw = self.lineage_service.get_graph_task_activity(
                                node_id=node_id[len("graph::"):],
                                run_limit=run_limit,
                            )
                        except ValueError as exc:
                            if "not a task node" not in str(exc):
                                raise
                            raw = self.lineage_service.get_table_node_activity(
                                node_id=node_id[len("graph::"):],
                                run_limit=run_limit,
                            )
                    elif conn and table:
                        raw = self.lineage_service.get_target_table_activity(
                            target_table_id=target_table_id or None,
                            connection_name=conn,
                            table=table,
                            schema=schema,
                            run_limit=run_limit,
                        )
                    else:
                        raw = {}
                    runs = _normalize_activity_payload(raw)
                    self._activity_cache[cache_key] = {"data": runs, "ts": now}
                self._send_json(runs, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        self._send_json({"error": "not found"}, status=404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        body = self._read_json_body()
        try:
            if parsed.path == "/api/toggle-favorite":
                payload = self.service.toggle_favorite(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/update-table-metadata":
                payload = self.service.update_table_metadata(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                    patch=body,
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/update-column-dict":
                payload = self.service.update_column_dict(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                    column=body.get("column") or "",
                    patch=body,
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/folder":
                payload = self.service.create_sql_folder(body.get("path") or "")
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/rename":
                payload = self.service.rename_sql_workspace_entry(
                    entry_path=body.get("path") or "",
                    new_name=body.get("new_name") or "",
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/move":
                payload = self.service.move_sql_workspace_entry(
                    entry_path=body.get("path") or "",
                    target_folder_path=body.get("target_folder_path") or "",
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/delete":
                payload = self.service.delete_sql_workspace_entry(
                    entry_path=body.get("path") or "",
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/file":
                if body.get("create"):
                    payload = self.service.create_sql_file(
                        folder_path=body.get("folder_path") or "",
                        file_name=body.get("file_name") or "",
                        initial_sql=body.get("content") or "",
                        linked_table=body.get("linked_table") or "",
                    )
                else:
                    payload = self.service.save_sql_file(
                        file_path=body.get("path") or "",
                        content=body.get("content") or "",
                        linked_table=body.get("linked_table") or "",
                    )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/run":
                payload = self.service.run_workspace_sql(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                    sql=body.get("sql") or "",
                    row_limit=int(body.get("row_limit") or "200"),
                    page=int(body.get("page") or "1"),
                    page_size=int(body.get("page_size") or body.get("row_limit") or "200"),
                    filter_column=body.get("filter_column") or "",
                    filter_keyword=body.get("filter_keyword") or "",
                    column_filters=body.get("column_filters") or {},
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/assistant":
                payload = self._run_sql_workbench_ai_assistant(body)
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/datasets":
                payload = self.service.create_dataset(body)
                self._send_json(payload, status=200)
                return
            if re.fullmatch(r"/api/datasets/[^/]+/update", parsed.path):
                dataset_id = parsed.path.split("/")[3]
                payload = self.service.update_dataset(dataset_id, body)
                self._send_json(payload, status=200)
                return
            if re.fullmatch(r"/api/datasets/[^/]+/delete", parsed.path):
                dataset_id = parsed.path.split("/")[3]
                payload = self.service.delete_dataset(dataset_id)
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/dashboards":
                payload = self.service.create_dashboard(body)
                self._send_json(payload, status=200)
                return
            if re.fullmatch(r"/api/dashboards/[^/]+/update", parsed.path):
                dashboard_id = parsed.path.split("/")[3]
                payload = self.service.update_dashboard(dashboard_id, body)
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/sql-workspace/filter-options":
                payload = self.service.get_workspace_filter_options(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                    sql=body.get("sql") or "",
                    column=body.get("column") or "",
                    column_filters=body.get("column_filters") or {},
                    keyword=body.get("keyword") or "",
                    limit=int(body.get("limit") or "200"),
                )
                self._send_json(payload, status=200)
                return
            if parsed.path == "/api/table-sql":
                payload = self.service.run_table_sql(
                    schema=body.get("schema") or "public",
                    table=body.get("table") or "",
                    sql=body.get("sql") or "",
                    row_limit=int(body.get("row_limit") or "200"),
                )
                self._send_json(payload, status=200)
                return
            self._send_json({"error": "not found"}, status=404)
        except Exception as exc:
            self._send_json({"error": str(exc)}, status=400)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _read_json_body(self) -> Dict[str, Any]:
        length = int(self.headers.get("Content-Length") or "0")
        raw = self.rfile.read(length) if length > 0 else b"{}"
        if not raw:
            return {}
        return json.loads(raw.decode("utf-8"))

    def _parse_params(self, query: str) -> Dict[str, str]:
        raw = parse_qs(query, keep_blank_values=True)
        return {key: values[0] for key, values in raw.items()}

    def _send_html(self, body: str, status: int = 200) -> None:
        encoded = body.encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _send_json(self, payload: Dict[str, Any], status: int = 200) -> None:
        encoded = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)

    def _run_sql_workbench_ai_assistant(self, body: Dict[str, Any]) -> Dict[str, Any]:
        schema = str(body.get("schema") or "public").strip() or "public"
        table = str(body.get("table") or "").strip()
        prompt = str(body.get("prompt") or "").strip()
        if not prompt:
            raise ValueError("prompt 不能为空")
        requested_mode = str(body.get("mode") or "").strip().lower()
        refine_prompt = _is_ai_refine_prompt(prompt)
        extra_context_tables = _normalize_ai_context_table_inputs(body.get("extra_context_tables"), max_tables=6)
        if not table and extra_context_tables:
            first = _parse_qualified_table_name(extra_context_tables[0])
            if first:
                schema, table = first
                body["linked_table"] = body.get("linked_table") or extra_context_tables[0]
                extra_context_tables = extra_context_tables[1:]
        if not table:
            return _build_ai_table_search_response(self.service, prompt)

        linked_table = str(body.get("linked_table") or "").strip() or f"{schema}.{table}"
        if requested_mode in {"relation_confirm", "table_search", "auto", ""} and refine_prompt:
            refreshed_candidates = _search_ai_candidate_tables(self.service, prompt, limit=6)
            if refreshed_candidates:
                best_table = str((refreshed_candidates[0] or {}).get("qualified_name") or "").strip()
                parsed_best = _parse_qualified_table_name(best_table)
                if parsed_best and best_table != linked_table:
                    schema, table = parsed_best
                    linked_table = best_table
                merged_context = [
                    name for name in [*(extra_context_tables or []), *[str(item.get("qualified_name") or "").strip() for item in refreshed_candidates]]
                    if _parse_qualified_table_name(name) and name != linked_table
                ]
                deduped_context: List[str] = []
                seen_context = set()
                for name in merged_context:
                    if name in seen_context:
                        continue
                    seen_context.add(name)
                    deduped_context.append(name)
                    if len(deduped_context) >= 4:
                        break
                extra_context_tables = deduped_context
        current_sql = _clip_text(body.get("sql") or "", 5000)
        context_tables = _resolve_ai_context_tables(
            self.service,
            prompt,
            linked_table,
            current_sql=current_sql,
            extra_tables=extra_context_tables,
            max_tables=6,
        )
        cross_datasource_blocked = _has_cross_datasource_tables(self.service, context_tables)
        same_source_context_tables = _same_datasource_tables(self.service, context_tables, linked_table)
        if linked_table and linked_table not in same_source_context_tables:
            same_source_context_tables = [linked_table, *same_source_context_tables]
        deduped_same_source: List[str] = []
        seen_context = set()
        for qualified_name in same_source_context_tables:
            if qualified_name in seen_context:
                continue
            seen_context.add(qualified_name)
            deduped_same_source.append(qualified_name)
        context_tables = deduped_same_source or [linked_table]
        profiles: List[Tuple[str, Dict[str, Any]]] = []
        for qualified_name in context_tables:
            parsed = _parse_qualified_table_name(qualified_name)
            if not parsed:
                continue
            context_schema, context_table = parsed
            try:
                profiles.append((
                    qualified_name,
                    self.service.get_table_profile(schema=context_schema, table=context_table, sample_limit=20),
                ))
            except Exception:
                continue
        profile = profiles[0][1] if profiles else self.service.get_table_profile(schema=schema, table=table, sample_limit=20)
        candidates = _search_ai_candidate_tables(self.service, prompt, limit=6)
        guessed_relations = _guess_ai_relation_hints(self.service, profiles or [(linked_table, profile)], prompt=prompt)
        should_confirm_first = (
            requested_mode not in {"sql_generate", "generate_sql"}
            and not _is_ai_confirmation_prompt(prompt)
            and len(context_tables) >= 1
        )
        if should_confirm_first and (len(context_tables) > 1 or not _should_generate_sql_directly(prompt)):
            return _build_ai_relation_confirm_response(
                prompt=prompt,
                linked_table=linked_table,
                context_tables=context_tables,
                candidates=candidates,
                guessed_relations=guessed_relations,
                cross_datasource_blocked=cross_datasource_blocked,
            )

        api_key = (os.environ.get("OPENAI_API_KEY") or "").strip()
        if not api_key:
            return _build_sql_assistant_fallback_response(
                schema=schema,
                table=table,
                linked_table=linked_table,
                context_tables=context_tables,
                guessed_relations=guessed_relations,
                prompt=prompt,
                current_sql=current_sql,
                profile=profile,
            )

        conversation = _normalize_ai_history(body.get("conversation"))
        result_preview = body.get("result_preview") if isinstance(body.get("result_preview"), dict) else {}

        history_lines: List[str] = []
        for item in conversation:
            role = "用户" if item["role"] == "user" else "AI"
            history_lines.append(f"{role}：{item['content']}")

        result_lines: List[str] = []
        preview_cols = result_preview.get("columns") if isinstance(result_preview.get("columns"), list) else []
        preview_rows_data = result_preview.get("rows") if isinstance(result_preview.get("rows"), list) else []
        if preview_cols and preview_rows_data:
            result_lines.append("最近一次执行结果字段：" + ", ".join(str(col) for col in preview_cols[:40]))
            for row in preview_rows_data[:8]:
                if isinstance(row, dict):
                    result_lines.append(json.dumps(row, ensure_ascii=False, default=str))

        context_sections = _build_ai_table_context_sections(profiles or [(linked_table, profile)])
        context_table_note = "、".join(name for name, _ in profiles) if profiles else linked_table
        relation_lines = [
            f"- {item.get('expression') or ''} | {item.get('join_type') or '待确认'} | {item.get('confidence') or '低'} | {item.get('reason') or ''}"
            for item in guessed_relations[:6]
            if isinstance(item, dict) and (item.get("expression") or "")
        ]

        prompt_text = "\n".join([
            AI_SQL_REPLY_PROMPT,
            f"本轮主表：{linked_table}",
            f"本轮已加载上下文表：{context_table_note}",
            f"跨库限制：{'当前上下文已按同数据源收窄，不能直接跨库 JOIN' if cross_datasource_blocked else '当前上下文均在同一数据源内'}",
            f"建议关联关系：\n{_clip_text(chr(10).join(relation_lines) if relation_lines else '（暂无已验证关系）', 3000)}",
            f"当前文件：{body.get('file_path') or '未提供'}",
            f"当前 SQL：\n{current_sql or '（空）'}",
            *context_sections,
            f"最近一次执行结果：\n{_clip_text(chr(10).join(result_lines) if result_lines else '（无）', 4000)}",
            f"历史对话：\n{_clip_text(chr(10).join(history_lines) if history_lines else '（无）', 4000)}",
            f"本轮问题：{prompt}",
            "请只输出 JSON 对象。",
        ])

        model = (os.environ.get("OPENAI_MODEL") or OPENAI_DEFAULT_MODEL).strip() or OPENAI_DEFAULT_MODEL
        request_body = json.dumps(
            {
                "model": model,
                "input": prompt_text,
                "max_output_tokens": 1600,
            },
            ensure_ascii=False,
        ).encode("utf-8")
        req = urllib.request.Request(
            f"{OPENAI_BASE_URL}/responses",
            data=request_body,
            method="POST",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=OPENAI_TIMEOUT_SECONDS) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except urllib.error.HTTPError as exc:
            error_text = exc.read().decode("utf-8", errors="replace")
            try:
                error_payload = json.loads(error_text)
                message = error_payload.get("error", {}).get("message") or error_text
            except Exception:
                message = error_text or str(exc)
            raise ValueError(f"AI 请求失败：{message}") from exc
        except urllib.error.URLError as exc:
            raise ValueError(f"AI 请求失败：{exc}") from exc

        raw_text = _extract_openai_output_text(payload)
        parsed = _extract_first_json_object(raw_text) or {}
        reply = str(parsed.get("reply") or raw_text or "").strip()
        sql_text = str(parsed.get("sql") or "").strip()
        if not sql_text:
            sql_text = _extract_sql_from_text(raw_text)
        title = str(parsed.get("title") or "").strip() or "AI 生成 SQL"
        follow_ups = parsed.get("follow_ups")
        if not isinstance(follow_ups, list):
            follow_ups = []
        follow_ups = [str(item).strip() for item in follow_ups if str(item or "").strip()][:5]
        return {
            "reply": reply or "AI 已返回结果，但没有补充说明。",
            "sql": sql_text,
            "title": title,
            "follow_ups": follow_ups,
            "model": model,
            "linked_table": linked_table,
            "mode": "sql_generate",
            "context_tables": [name for name, _ in profiles] or [linked_table],
            "confirmed_tables": [name for name, _ in profiles] or [linked_table],
            "table_candidates": candidates,
            "guessed_relations": guessed_relations,
            "need_confirmation": False,
            "next_actions": ["write_sql", "run_sql", "refine_prompt"],
        }


def build_map_url(host: str, port: int, schema: str = "public", table: str = "") -> str:
    query = {}
    if schema:
        query["schema"] = schema
    if table:
        query["table"] = table
    suffix = f"?{urlencode(query)}" if query else ""
    return f"http://{host}:{port}/map{suffix}"


def main() -> None:
    parser = argparse.ArgumentParser(description="数据地图本地查看器")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8780)
    parser.add_argument("--schema", default="public")
    parser.add_argument("--table", default="")
    parser.add_argument("--open", action="store_true", dest="open_browser")
    args = parser.parse_args()

    DataMapHandler.service.refresh_catalog()
    server = ThreadingHTTPServer((args.host, args.port), DataMapHandler)
    url = build_map_url(host=args.host, port=args.port, schema=args.schema, table=args.table)
    print(url, flush=True)

    if args.open_browser:
      threading.Timer(0.6, lambda: webbrowser.open(url)).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
