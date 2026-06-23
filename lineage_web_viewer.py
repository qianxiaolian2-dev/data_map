from __future__ import annotations

import argparse
import json
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Dict, Optional
from urllib.parse import parse_qs, urlencode, urlparse

from lineage_service import LineageService


HTML_TEMPLATE = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate" />
  <meta http-equiv="Pragma" content="no-cache" />
  <meta http-equiv="Expires" content="0" />
  <title>FDL Lineage — 数据血缘与调度监控</title>
  <style>
    /* ===== Design Tokens (Data Platform Style) ===== */
    :root {
      --sidebar-bg: #1a1d23;
      --sidebar-text: #c8ccd4;
      --sidebar-width: 280px;
      --nav-height: 52px;
      --brand: #1a73e8;
      --brand-light: #4a90e2;
      --bg-content: #f0f1f3;
      --bg-canvas: #ffffff;
      --bg-panel: #ffffff;
      --bg-hover: #e8eaed;
      --ink: #1a1d23;
      --ink-secondary: #5f6368;
      --ink-muted: #9aa0a6;
      --line: #dadce0;
      --line-light: #e8eaed;
      --shadow-sm: 0 1px 2px rgba(0,0,0,0.06), 0 1px 3px rgba(0,0,0,0.1);
      --shadow-md: 0 2px 6px rgba(0,0,0,0.08), 0 4px 12px rgba(0,0,0,0.06);
      --radius-sm: 6px;
      --radius-md: 8px;
      --radius-lg: 12px;
      /* Node colors */
      --node-table: #1a73e8;
      --node-target: #0d9488;
      --node-task: #7c3aed;
      --node-pipeline: #ea580c;
      --node-source: #475569;
      /* Status colors */
      --status-success: #16a34a;
      --status-running: #2563eb;
      --status-failed: #dc2626;
      --status-waiting: #d97706;
      --status-skip: #9ca3af;
      --status-bg-success: #dcfce7;
      --status-bg-running: #dbeafe;
      --status-bg-failed: #fee2e2;
      --status-bg-waiting: #fef3c7;
      --status-bg-skip: #f3f4f6;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Helvetica Neue", sans-serif;
      font-size: 13px;
      color: var(--ink);
      background: var(--bg-content);
      overflow: hidden;
      height: 100vh;
      display: flex;
      flex-direction: column;
    }
    input, select, button { font-family: inherit; font-size: inherit; }

    /* ===== Top Navigation ===== */
    .topnav {
      height: var(--nav-height);
      background: var(--sidebar-bg);
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 0 16px;
      flex-shrink: 0;
      z-index: 100;
    }
    .topnav-left {
      display: flex;
      align-items: center;
      gap: 12px;
    }
    .topnav-brand {
      display: flex;
      align-items: center;
      gap: 8px;
      color: #fff;
      font-weight: 700;
      font-size: 15px;
      letter-spacing: -0.01em;
    }
    .topnav-brand svg { flex-shrink: 0; }
    .topnav-breadcrumb {
      display: flex;
      align-items: center;
      gap: 6px;
      color: rgba(255,255,255,0.5);
      font-size: 12px;
      padding-left: 12px;
      border-left: 1px solid rgba(255,255,255,0.12);
    }
    .topnav-breadcrumb span { color: rgba(255,255,255,0.85); }
    .topnav-right {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .topnav-btn {
      background: rgba(255,255,255,0.08);
      border: 1px solid rgba(255,255,255,0.12);
      color: rgba(255,255,255,0.85);
      padding: 6px 12px;
      border-radius: var(--radius-sm);
      cursor: pointer;
      font-size: 12px;
      display: flex;
      align-items: center;
      gap: 5px;
      white-space: nowrap;
      transition: background 0.15s;
    }
    .topnav-btn:hover { background: rgba(255,255,255,0.14); }
    .topnav-btn.primary {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    .topnav-btn.primary:hover { background: #1557b0; }

    /* ===== Mode Toggle Switch ===== */
    .mode-switch {
      display: flex;
      background: rgba(255,255,255,0.06);
      border-radius: var(--radius-sm);
      padding: 2px;
      gap: 1px;
    }
    .mode-switch button {
      background: transparent;
      border: none;
      color: rgba(255,255,255,0.5);
      padding: 5px 10px;
      border-radius: 5px;
      cursor: pointer;
      font-size: 12px;
      white-space: nowrap;
      transition: all 0.15s;
    }
    .mode-switch button.active {
      background: rgba(255,255,255,0.12);
      color: #fff;
    }
    .mode-switch button:hover:not(.active) { color: rgba(255,255,255,0.75); }

    /* ===== Body Layout ===== */
    .app-body {
      display: flex;
      flex: 1;
      min-height: 0;
    }

    /* ===== Left Sidebar ===== */
    .sidebar {
      width: var(--sidebar-width);
      background: var(--bg-panel);
      border-right: 1px solid var(--line);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      overflow: hidden;
    }
    .sidebar-section {
      padding: 12px 14px;
      border-bottom: 1px solid var(--line-light);
    }
    .sidebar-section:last-child { border-bottom: none; flex: 1; overflow: auto; }
    .sidebar-label {
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--ink-muted);
      margin-bottom: 8px;
    }
    .sidebar-search {
      width: 100%;
      padding: 8px 10px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: #f8f9fa;
      font-size: 13px;
      outline: none;
      transition: border-color 0.15s;
    }
    .sidebar-search:focus { border-color: var(--brand); background: #fff; }

    .form-group { margin-bottom: 8px; }
    .form-group:last-child { margin-bottom: 0; }
    .form-group label {
      display: block;
      font-size: 11px;
      font-weight: 600;
      color: var(--ink-secondary);
      margin-bottom: 3px;
    }
    .form-group input,
    .form-group select {
      width: 100%;
      padding: 7px 9px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: #f8f9fa;
      font-size: 13px;
      outline: none;
    }
    .form-group input::placeholder,
    .form-group select::placeholder { color: #bbb; font-style: italic; }
    .field-required { color: #e34935; font-size: 11px; }
    .form-group input.field-error { border-color: #e34935; background: #fff5f5; }

    .form-hint {
      font-size: 11px;
      color: var(--ink-muted);
      margin-top: 4px;
      padding: 5px 8px;
      background: #f0f4ff;
      border-radius: var(--radius-sm);
      border-left: 3px solid var(--brand);
    }
    .form-row {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .form-actions {
      display: flex;
      gap: 6px;
      margin-top: 10px;
    }
    .form-actions button {
      flex: 1;
      padding: 7px 12px;
      border-radius: var(--radius-sm);
      border: 1px solid var(--line);
      background: #fff;
      cursor: pointer;
      font-size: 12px;
      font-weight: 600;
      color: var(--ink);
      transition: all 0.15s;
    }
    .form-actions button:hover { background: var(--bg-hover); }
    .form-actions button.primary {
      background: var(--brand);
      border-color: var(--brand);
      color: #fff;
    }
    .form-actions button.primary:hover { background: #1557b0; }

    /* ===== Legend ===== */
    .legend-grid {
      display: grid;
      gap: 5px;
    }
    .legend-item {
      display: flex;
      align-items: center;
      gap: 8px;
      font-size: 12px;
      color: var(--ink-secondary);
    }
    .legend-dot {
      width: 10px;
      height: 10px;
      border-radius: 50%;
      flex-shrink: 0;
    }
    .legend-status-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 4px;
    }

    /* ===== Summary ===== */
    .summary-grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
    }
    .summary-item {
      padding: 6px 8px;
      background: #f8f9fa;
      border-radius: var(--radius-sm);
    }
    .summary-item-label {
      font-size: 10px;
      color: var(--ink-muted);
      text-transform: uppercase;
      letter-spacing: 0.03em;
    }
    .summary-item-value {
      font-size: 14px;
      font-weight: 700;
      color: var(--ink);
      margin-top: 1px;
    }
    .summary-item-value.highlight { color: var(--brand); }

    /* ===== Main Content Area ===== */
    .main-content {
      flex: 1;
      display: flex;
      flex-direction: column;
      min-width: 0;
      overflow: hidden;
    }

    /* ===== Canvas Toolbar ===== */
    .canvas-toolbar {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 8px 14px;
      background: var(--bg-panel);
      border-bottom: 1px solid var(--line-light);
      flex-shrink: 0;
    }
    .canvas-toolbar-left {
      display: flex;
      align-items: center;
      gap: 8px;
    }
    .canvas-toolbar-title {
      font-size: 14px;
      font-weight: 600;
    }
    .canvas-toolbar-subtitle {
      font-size: 12px;
      color: var(--ink-muted);
    }
    .canvas-toolbar-right {
      display: flex;
      gap: 4px;
    }
    .canvas-btn {
      padding: 5px 8px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: #fff;
      cursor: pointer;
      font-size: 12px;
      color: var(--ink-secondary);
      display: flex;
      align-items: center;
      gap: 4px;
      transition: all 0.15s;
    }
    .canvas-btn:hover { background: var(--bg-hover); color: var(--ink); }
    .canvas-btn svg { flex-shrink: 0; }

    /* ===== Canvas + Right Panel Container ===== */
    .canvas-row {
      flex: 1;
      display: flex;
      min-height: 0;
      overflow: hidden;
    }

    /* ===== SVG Canvas ===== */
    .canvas-stage {
      flex: 1;
      position: relative;
      overflow: auto;
      background:
        linear-gradient(90deg, rgba(0,0,0,0.03) 1px, transparent 1px),
        linear-gradient(180deg, rgba(0,0,0,0.03) 1px, transparent 1px);
      background-size: 20px 20px;
      background-color: var(--bg-canvas);
      min-width: 0;
    }
    #canvas svg {
      display: block;
    }
    #canvas.empty {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100%;
      color: var(--ink-muted);
      font-size: 14px;
    }
    .edge { stroke: #9aa0a6; stroke-width: 1.5; fill: none; }
    .edge-highlighted { stroke: var(--brand); stroke-width: 2; }
    .edge-label {
      font-size: 10px;
      fill: var(--ink-muted);
      pointer-events: none;
    }
    .node-group { cursor: pointer; }
    .node-rect {
      rx: 6;
      ry: 6;
      stroke: rgba(255,255,255,0.9);
      stroke-width: 2;
      filter: drop-shadow(0 2px 4px rgba(0,0,0,0.1));
      transition: filter 0.15s;
    }
    .node-group:hover .node-rect {
      filter: drop-shadow(0 4px 8px rgba(0,0,0,0.18));
    }
    .node-selected .node-rect {
      stroke: var(--brand);
      stroke-width: 3;
      filter: drop-shadow(0 0 0 3px rgba(26,115,232,0.2));
    }
    .node-group:hover .node-label { fill: #fff; }
    .node-label {
      fill: #fff;
      font-weight: 600;
      font-size: 13px;
      pointer-events: none;
    }
    .node-meta {
      fill: rgba(255,255,255,0.8);
      font-size: 10px;
      pointer-events: none;
    }
    .node-status-dot {
      pointer-events: none;
    }

    /* ===== Right Panel ===== */
    .right-panel {
      width: 340px;
      background: var(--bg-panel);
      border-left: 1px solid var(--line);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      overflow: hidden;
    }
    .right-panel-section {
      border-bottom: 1px solid var(--line-light);
      flex-shrink: 0;
    }
    .right-panel-section:last-child {
      border-bottom: none;
      flex: 1;
      overflow: auto;
      flex-shrink: 1;
    }
    .right-panel-header {
      display: flex;
      align-items: center;
      justify-content: space-between;
      padding: 10px 14px;
      cursor: pointer;
      user-select: none;
    }
    .right-panel-header:hover { background: var(--bg-hover); }
    .right-panel-title {
      font-size: 12px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--ink-muted);
      display: flex;
      align-items: center;
      gap: 6px;
    }
    .right-panel-body {
      padding: 0 14px 12px;
    }
    .right-panel-body.empty {
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 60px;
      color: var(--ink-muted);
      font-size: 12px;
    }

    /* ===== Detail Meta ===== */
    .detail-head {
      display: flex;
      align-items: center;
      gap: 8px;
      margin-bottom: 10px;
    }
    .detail-name {
      font-size: 15px;
      font-weight: 700;
      word-break: break-all;
    }
    .type-badge {
      display: inline-flex;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
    }
    .type-badge.table { background: #dbeafe; color: #1d4ed8; }
    .type-badge.target { background: #ccfbf1; color: #0f766e; }
    .type-badge.task { background: #f3e8ff; color: #7c3aed; }
    .type-badge.pipeline { background: #ffedd5; color: #c2410c; }
    .type-badge.source { background: #f1f5f9; color: #475569; }

    .detail-grid {
      display: grid;
      gap: 5px;
    }
    .detail-row {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      padding: 4px 0;
      border-bottom: 1px solid var(--line-light);
      gap: 8px;
    }
    .detail-row:last-child { border-bottom: none; }
    .detail-row-label {
      color: var(--ink-muted);
      font-size: 12px;
      flex-shrink: 0;
      min-width: 60px;
    }
    .detail-row-value {
      color: var(--ink);
      font-size: 12px;
      text-align: right;
      word-break: break-all;
    }

    .detail-actions {
      display: flex;
      gap: 6px;
      margin-top: 10px;
    }
    .detail-actions button {
      padding: 5px 10px;
      border: 1px solid var(--line);
      border-radius: var(--radius-sm);
      background: #fff;
      cursor: pointer;
      font-size: 12px;
      color: var(--ink-secondary);
      transition: all 0.15s;
    }
    .detail-actions button:hover { background: var(--bg-hover); color: var(--ink); }
    .detail-actions button.primary {
      background: var(--brand);
      color: #fff;
      border-color: var(--brand);
    }
    .detail-actions button.primary:hover { background: var(--brand-light); border-color: var(--brand-light); color: #fff; }

    /* ===== Impact Analysis ===== */
    .impact-item {
      display: flex;
      align-items: center;
      gap: 8px;
      padding: 6px 8px;
      border-radius: var(--radius-sm);
      margin-bottom: 4px;
      background: #f8f9fa;
      font-size: 12px;
    }
    .impact-item:last-child { margin-bottom: 0; }
    .impact-item .impact-arrow { color: var(--ink-muted); flex-shrink: 0; }
    .impact-item .impact-name { color: var(--ink); word-break: break-all; flex: 1; }

    /* ===== Bottom Panel ===== */
    .bottom-panel {
      height: 240px;
      background: var(--bg-panel);
      border-top: 1px solid var(--line);
      display: flex;
      flex-direction: column;
      flex-shrink: 0;
      overflow: hidden;
    }
    .bottom-tabs {
      display: flex;
      align-items: center;
      padding: 0 14px;
      border-bottom: 1px solid var(--line-light);
      flex-shrink: 0;
      gap: 0;
    }
    .bottom-tab {
      padding: 9px 14px;
      font-size: 12px;
      font-weight: 500;
      color: var(--ink-muted);
      cursor: pointer;
      border-bottom: 2px solid transparent;
      transition: all 0.15s;
      white-space: nowrap;
    }
    .bottom-tab:hover { color: var(--ink); }
    .bottom-tab.active {
      color: var(--brand);
      border-bottom-color: var(--brand);
    }
    .bottom-tab .count-badge {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      min-width: 18px;
      height: 18px;
      padding: 0 5px;
      border-radius: 9px;
      background: var(--line-light);
      font-size: 11px;
      font-weight: 600;
      color: var(--ink-muted);
      margin-left: 4px;
    }
    .bottom-tab.active .count-badge {
      background: #e8f0fe;
      color: var(--brand);
    }
    .bottom-content {
      flex: 1;
      overflow: auto;
      padding: 10px 14px;
    }
    .bottom-content.empty {
      display: flex;
      align-items: center;
      justify-content: center;
      color: var(--ink-muted);
      font-size: 12px;
    }

    /* ===== Task Run Cards ===== */
    .run-list {
      display: flex;
      flex-direction: column;
      gap: 6px;
    }
    .run-card {
      display: flex;
      align-items: center;
      gap: 12px;
      padding: 8px 12px;
      border-radius: var(--radius-sm);
      background: #f8f9fa;
      border: 1px solid var(--line-light);
    }
    .run-card .run-status {
      width: 32px;
      display: flex;
      justify-content: center;
      flex-shrink: 0;
    }
    .run-card .run-info {
      flex: 1;
      min-width: 0;
    }
    .run-card .run-name {
      font-size: 13px;
      font-weight: 600;
      color: var(--ink);
    }
    .run-card .run-meta {
      font-size: 11px;
      color: var(--ink-muted);
      margin-top: 2px;
      display: flex;
      gap: 12px;
      flex-wrap: wrap;
    }
    .run-card .run-duration {
      flex-shrink: 0;
      font-size: 12px;
      color: var(--ink-secondary);
    }

    /* ===== Status Badges ===== */
    .status-badge {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 2px 8px;
      border-radius: 4px;
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
    }
    .status-badge.success { background: var(--status-bg-success); color: var(--status-success); }
    .status-badge.running { background: var(--status-bg-running); color: var(--status-running); }
    .status-badge.failed,
    .status-badge.error { background: var(--status-bg-failed); color: var(--status-failed); }
    .status-badge.waiting { background: var(--status-bg-waiting); color: var(--status-waiting); }
    .status-badge.skip,
    .status-badge.muted { background: var(--status-bg-skip); color: var(--status-skip); }
    .status-badge.neutral { background: #e8f0fe; color: var(--brand); }

    /* ===== Status Dots ===== */
    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      display: inline-block;
    }
    .status-dot.success { background: var(--status-success); }
    .status-dot.running { background: var(--status-running); animation: pulse 1.5s infinite; }
    .status-dot.failed,
    .status-dot.error { background: var(--status-failed); }
    .status-dot.waiting { background: var(--status-waiting); }
    .status-dot.skip,
    .status-dot.muted { background: var(--status-skip); }
    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50% { opacity: 0.4; }
    }

    /* ===== Task Cards (for sidebar detail) ===== */
    .task-card {
      padding: 8px 10px;
      border-radius: var(--radius-sm);
      background: #f8f9fa;
      border: 1px solid var(--line-light);
      margin-bottom: 6px;
    }
    .task-card:last-child { margin-bottom: 0; }
    .task-card-head {
      display: flex;
      justify-content: space-between;
      align-items: center;
      gap: 8px;
      margin-bottom: 4px;
    }
    .task-card-name {
      font-size: 12px;
      font-weight: 600;
    }
    .task-card-sub {
      font-size: 11px;
      color: var(--ink-muted);
    }
    .task-card-meta {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 3px 8px;
      font-size: 11px;
      color: var(--ink-secondary);
      margin-top: 4px;
    }
    .task-card-note {
      font-size: 11px;
      color: var(--ink-muted);
      margin-top: 4px;
    }

    /* ===== Error ===== */
    .error-box {
      padding: 8px 10px;
      border-radius: var(--radius-sm);
      background: var(--status-bg-failed);
      color: var(--status-failed);
      font-size: 12px;
      margin-top: 8px;
      display: none;
    }
    .error-box:not(:empty) { display: block; }

    /* ===== Empty State ===== */
    .empty-state {
      padding: 20px;
      text-align: center;
      color: var(--ink-muted);
      font-size: 13px;
    }
    .empty-state-inline {
      padding: 10px;
      text-align: center;
      color: var(--ink-muted);
      font-size: 12px;
    }

    /* ===== Meta Cells Grid ===== */
    .meta-cells {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 4px;
    }
    .meta-cell {
      padding: 5px 8px;
      background: #f8f9fa;
      border-radius: var(--radius-sm);
    }
    .meta-cell-label {
      font-size: 10px;
      color: var(--ink-muted);
      text-transform: uppercase;
    }
    .meta-cell-value {
      font-size: 12px;
      font-weight: 600;
      color: var(--ink);
      margin-top: 1px;
    }

    /* ===== Overview Card ===== */
    .overview-card {
      padding: 10px 12px;
      background: linear-gradient(135deg, #eef2ff 0%, #f0fdf4 100%);
      border: 1px solid #dbeafe;
      border-radius: var(--radius-md);
      margin-bottom: 8px;
    }
    .overview-card .oc-head {
      display: flex;
      justify-content: space-between;
      align-items: flex-start;
      gap: 8px;
    }
    .overview-card .oc-title {
      font-size: 13px;
      font-weight: 700;
    }
    .overview-card .oc-copy {
      font-size: 11px;
      color: var(--ink-secondary);
      margin-top: 2px;
    }
    .overview-card .oc-metrics {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 6px;
      margin-top: 8px;
    }
    .overview-card .oc-metric {
      padding: 6px 8px;
      background: rgba(255,255,255,0.7);
      border-radius: var(--radius-sm);
    }
    .overview-card .oc-metric-label {
      font-size: 10px;
      color: var(--ink-muted);
      text-transform: uppercase;
    }
    .overview-card .oc-metric-value {
      font-size: 13px;
      font-weight: 700;
    }
    .overview-card .oc-spotlight {
      margin-top: 6px;
      padding: 6px 8px;
      border-radius: var(--radius-sm);
      background: rgba(255,255,255,0.8);
      border: 1px solid #e0e7ff;
      font-size: 11px;
    }
    .overview-card .oc-spotlight strong { color: var(--brand); }

    /* ===== Insight Card ===== */
    .insight-card {
      margin-bottom: 8px;
    }
    .insight-card .ic-head {
      margin-bottom: 6px;
    }
    .insight-card .ic-kicker {
      font-size: 10px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--ink-muted);
    }
    .insight-card .ic-title {
      font-size: 13px;
      font-weight: 700;
    }
    .insight-card .ic-copy {
      font-size: 11px;
      color: var(--ink-secondary);
      margin-top: 2px;
    }

    /* ===== Responsive ===== */
    @media (max-width: 1400px) {
      .right-panel { width: 280px; }
    }
    @media (max-width: 1100px) {
      .sidebar { width: 240px; }
      .right-panel { display: none; }
    }
    @media (max-width: 800px) {
      .sidebar { display: none; }
      .bottom-panel { height: 180px; }
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: var(--line); border-radius: 3px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--ink-muted); }

    /* hidden helper */
    [hidden] { display: none !important; }
  </style>
</head>
<body>

  <!-- ===== Top Navigation ===== -->
  <nav class="topnav">
    <div class="topnav-left">
      <div class="topnav-brand">
        <svg width="20" height="20" viewBox="0 0 20 20" fill="none">
          <circle cx="10" cy="10" r="9" fill="none" stroke="rgba(255,255,255,0.3)" stroke-width="1.5"/>
          <circle cx="10" cy="10" r="4" fill="var(--brand)" stroke="#fff" stroke-width="1.5"/>
          <line x1="10" y1="4" x2="10" y2="0" stroke="var(--brand-light)" stroke-width="1.5" stroke-dasharray="2 1"/>
          <line x1="10" y1="16" x2="10" y2="20" stroke="var(--brand-light)" stroke-width="1.5" stroke-dasharray="2 1"/>
          <line x1="4" y1="10" x2="0" y2="10" stroke="var(--brand-light)" stroke-width="1.5" stroke-dasharray="2 1"/>
          <line x1="16" y1="10" x2="20" y2="10" stroke="var(--brand-light)" stroke-width="1.5" stroke-dasharray="2 1"/>
        </svg>
        FDL Lineage
      </div>
      <div class="topnav-breadcrumb">
        <span id="breadcrumbPath">输入参数 → 查询 → 查看血缘</span>
      </div>
    </div>
    <div class="topnav-right">
      <div class="mode-switch" id="modeSwitch">
        <button type="button" value="target_table">目标表</button>
        <button type="button" value="table_node" class="active">表节点</button>
      </div>
      <button class="topnav-btn" id="refreshBtn" title="刷新图谱">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M1 7a6 6 0 0 1 10.2-4.2M13 7a6 6 0 0 1-10.2 4.2"/><path d="M13 1v4h-4M1 13V9h4"/></svg>
        刷新
      </button>
      <button class="topnav-btn" id="copyLinkBtn" title="复制链接">
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M6 8.5a3 3 0 0 0 4.2.3l2-2a3 3 0 0 0-4.2-4.2"/><path d="M8 5.5a3 3 0 0 0-4.2-.3l-2 2a3 3 0 0 0 4.2 4.2"/></svg>
        复制链接
      </button>
    </div>
  </nav>

  <!-- ===== App Body ===== -->
  <div class="app-body">

    <!-- ===== Left Sidebar ===== -->
    <aside class="sidebar" id="sidebar">
      <!-- Search -->
      <div class="sidebar-section">
        <div style="font-size:12px;color:var(--ink-secondary);line-height:1.5;">
          在下方的查询参数中填写表信息，点击<strong>"查询"</strong>查看血缘关系与调度监控。
        </div>
      </div>

      <!-- Query Form -->
      <div class="sidebar-section">
        <div class="sidebar-label">查询参数</div>
        <form id="queryForm">
          <input id="target_table_id" name="target_table_id" type="hidden" />
          <input id="node_id" name="node_id" type="hidden" />
          <div id="targetModeFields">
            <div class="form-row">
              <div class="form-group">
                <label for="target_connection_name">目标连接 <span class="field-required">*</span></label>
                <input id="target_connection_name" name="target_connection_name" placeholder="如: PG" />
              </div>
              <div class="form-group">
                <label for="target_schema">Schema</label>
                <input id="target_schema" name="target_schema" placeholder="如: public" />
              </div>
            </div>
            <div class="form-group">
              <label for="target_table">目标表 <span class="field-required">*</span></label>
              <input id="target_table" name="target_table" placeholder="如: sales_order" />
            </div>
            <div class="form-group">
              <label for="upstream_depth">上溯层数</label>
              <input id="upstream_depth" name="upstream_depth" type="number" min="1" max="20" value="12" />
            </div>
          </div>
          <div id="tableNodeFields" hidden>
            <div class="form-row">
              <div class="form-group">
                <label for="node_connection_name">节点连接 <span class="field-required">*</span></label>
                <input id="node_connection_name" name="node_connection_name" placeholder="如: dwd_mdbase" />
              </div>
              <div class="form-group">
                <label for="database_name">数据库 <span class="field-required">*</span></label>
                <input id="database_name" name="database_name" placeholder="如: dwd_mdbase" />
              </div>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label for="node_schema_name">Schema</label>
                <input id="node_schema_name" name="node_schema_name" placeholder="留空即可" />
              </div>
              <div class="form-group">
                <label for="node_table">表名 <span class="field-required">*</span></label>
                <input id="node_table" name="node_table" placeholder="如: sales_order" />
              </div>
            </div>
            <div class="form-row">
              <div class="form-group">
                <label for="direction">方向</label>
                <select id="direction" name="direction">
                  <option value="both">上下游</option>
                  <option value="upstream">仅上游</option>
                  <option value="downstream">仅下游</option>
                </select>
              </div>
              <div class="form-group">
                <label for="lineage_depth">展示层数</label>
                <input id="lineage_depth" name="lineage_depth" type="number" min="1" max="20" value="3" />
              </div>
            </div>
          </div>
          <div class="form-actions">
            <button type="button" class="primary" id="queryBtn">查询</button>
            <button type="button" id="copyLinkBtn2">复制链接</button>
          </div>
        </form>
        <div id="errorBox" class="error-box"></div>
      </div>

      <!-- Legend -->
      <div class="sidebar-section">
        <div class="sidebar-label">节点图例</div>
        <div class="legend-grid">
          <div class="legend-item"><span class="legend-dot" style="background:var(--node-table)"></span> 普通表</div>
          <div class="legend-item"><span class="legend-dot" style="background:var(--node-target)"></span> 目标表</div>
          <div class="legend-item"><span class="legend-dot" style="background:var(--node-task)"></span> 任务节点</div>
          <div class="legend-item"><span class="legend-dot" style="background:var(--node-pipeline)"></span> Pipeline</div>
          <div class="legend-item"><span class="legend-dot" style="background:var(--node-source)"></span> 仅配置源表</div>
        </div>
        <div class="sidebar-label" style="margin-top:10px;">调度状态</div>
        <div class="legend-status-grid">
          <div class="legend-item"><span class="status-dot success"></span> 成功</div>
          <div class="legend-item"><span class="status-dot running"></span> 运行中</div>
          <div class="legend-item"><span class="status-dot failed"></span> 失败</div>
          <div class="legend-item"><span class="status-dot waiting"></span> 等待</div>
        </div>
      </div>

      <!-- Summary -->
      <div class="sidebar-section">
        <div class="sidebar-label">视图摘要</div>
        <div id="summary" class="summary-grid">
          <div class="summary-item">
            <div class="summary-item-label">节点数</div>
            <div class="summary-item-value" id="summaryNodes">-</div>
          </div>
          <div class="summary-item">
            <div class="summary-item-label">边数</div>
            <div class="summary-item-value" id="summaryEdges">-</div>
          </div>
          <div class="summary-item">
            <div class="summary-item-label">标题</div>
            <div class="summary-item-value highlight" id="summaryTitle" style="font-size:11px;font-weight:500;word-break:break-all;">等待加载</div>
          </div>
          <div class="summary-item">
            <div class="summary-item-label">目标</div>
            <div class="summary-item-value" id="summaryTargets" style="font-size:11px;font-weight:500;">-</div>
          </div>
        </div>
      </div>
    </aside>

    <!-- ===== Main Content ===== -->
    <main class="main-content">

      <!-- Canvas Toolbar -->
      <div class="canvas-toolbar">
        <div class="canvas-toolbar-left">
          <div class="canvas-toolbar-title">血缘拓扑图</div>
          <div class="canvas-toolbar-subtitle" id="canvasSubtitle">点击节点查看详情与调度</div>
        </div>
        <div class="canvas-toolbar-right">
          <button class="canvas-btn" id="fitBtn" title="缩放到适应">
            <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M2 5V2h3M9 2h3v3M12 9v3H9M5 12H2V9"/></svg>
            适应
          </button>
          <button class="canvas-btn" id="zoomInBtn" title="放大">+</button>
          <button class="canvas-btn" id="zoomOutBtn" title="缩小">−</button>
        </div>
      </div>

      <!-- Canvas + Right Panel Row -->
      <div class="canvas-row">

        <!-- SVG Canvas -->
        <div class="canvas-stage" id="canvasStage">
          <div id="canvas" class="empty">请在左侧填写参数，点击「查询」加载血缘图</div>
        </div>

        <!-- Right Detail Panel -->
        <aside class="right-panel" id="rightPanel">

          <!-- Node Detail -->
          <div class="right-panel-section">
            <div class="right-panel-header">
              <div class="right-panel-title">
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5"><circle cx="6" cy="6" r="4.5"/><path d="M6 3.5v5M3.5 6h5"/></svg>
                节点详情
              </div>
            </div>
            <div class="right-panel-body" id="detailBody">
              <div class="empty-state-inline">左侧填写参数 → 点击「查询」→ 点击画布中的节点查看详情</div>
            </div>
          </div>

          <!-- Impact Analysis -->
          <div class="right-panel-section" style="flex:1;overflow:auto;">
            <div class="right-panel-header">
              <div class="right-panel-title">
                <svg width="12" height="12" viewBox="0 0 12 12" fill="none" stroke="currentColor" stroke-width="1.5"><path d="M1 11L6 1l5 10H1z"/></svg>
                影响分析
              </div>
            </div>
            <div class="right-panel-body" id="impactBody">
              <div class="empty-state-inline">查询血缘图后，选中节点展示下游影响链</div>
            </div>
          </div>

        </aside>
      </div>

      <!-- ===== Bottom Panel ===== -->
      <div class="bottom-panel">
        <div class="bottom-tabs" id="bottomTabs">
          <div class="bottom-tab active" data-tab="overview">调度概览</div>
          <div class="bottom-tab" data-tab="producers">生产者</div>
          <div class="bottom-tab" data-tab="runs">运行记录 <span class="count-badge" id="runsCount">0</span></div>
          <div class="bottom-tab" data-tab="consumers">消费者</div>
        </div>
        <div class="bottom-content" id="bottomContent">
          <div class="empty-state-inline">查询血缘图 → 选中节点 → 查看调度监控</div>
        </div>
      </div>

    </main>
  </div>

  <script>
    // ===== Color Palette =====
    const palette = {
      target_table: '#0d9488',
      pipeline: '#ea580c',
      graph_table: '#1a73e8',
      graph_task: '#7c3aed',
      source_table_config: '#475569'
    };

    // ===== DOM References =====
    const params = new URLSearchParams(window.location.search);
    const form = document.getElementById('queryForm');
    const errorBox = document.getElementById('errorBox');
    const canvas = document.getElementById('canvas');
    const canvasStage = document.getElementById('canvasStage');
    const detailBody = document.getElementById('detailBody');
    const impactBody = document.getElementById('impactBody');
    const bottomContent = document.getElementById('bottomContent');
    const bottomTabs = document.getElementById('bottomTabs');
    const runsCount = document.getElementById('runsCount');
    const summaryNodes = document.getElementById('summaryNodes');
    const summaryEdges = document.getElementById('summaryEdges');
    const summaryTitle = document.getElementById('summaryTitle');
    const summaryTargets = document.getElementById('summaryTargets');
    const breadcrumbPath = document.getElementById('breadcrumbPath');
    const modeInput = document.getElementById('target_connection_name') ? null : null;
    const modeSwitch = document.getElementById('modeSwitch');

    let currentGraph = null;
    let selectedNodeId = null;
    let currentPositions = null;
    let currentActivity = null;
    let currentTab = 'overview';
    let loadedOnce = false;

    function getCurrentMode() {
      const active = modeSwitch.querySelector('.active');
      return active ? active.value : 'table_node';
    }

    function setIfPresent(id, value) {
      if (value === null || value === undefined) return;
      const el = document.getElementById(id);
      if (el) el.value = value;
    }

    function applyModeUI() {
      const mode = getCurrentMode();
      document.getElementById('targetModeFields').hidden = mode !== 'target_table';
      document.getElementById('tableNodeFields').hidden = mode !== 'table_node';
      modeSwitch.querySelectorAll('button').forEach(btn => {
        btn.classList.toggle('active', btn.value === mode);
      });
    }

    function updateBreadcrumb(text) {
      breadcrumbPath.textContent = text || '数据血缘关系';
    }

    function initializeFormFromParams() {
      const mode = params.get('mode') || 'table_node';
      modeSwitch.querySelectorAll('button').forEach(btn => {
        btn.classList.toggle('active', btn.value === mode);
      });
      applyModeUI();

      if (mode === 'table_node') {
        setIfPresent('node_connection_name', params.get('connection_name'));
        setIfPresent('database_name', params.get('database_name'));
        // schema is optional, leave blank unless user explicitly fills it
        setIfPresent('node_table', params.get('table'));
        setIfPresent('direction', params.get('direction') || 'both');
        setIfPresent('lineage_depth', params.get('depth') || '3');
        setIfPresent('node_id', params.get('node_id'));
      } else {
        setIfPresent('target_connection_name', params.get('connection_name') || 'PG');
        setIfPresent('target_schema', params.get('schema') || 'public');
        setIfPresent('target_table', params.get('table') || 'sales_order');
        setIfPresent('upstream_depth', params.get('upstream_depth') || '12');
        setIfPresent('target_table_id', params.get('target_table_id'));
      }
      if (!document.getElementById('direction').value) {
        document.getElementById('direction').value = 'both';
      }
      if (!document.getElementById('lineage_depth').value) {
        document.getElementById('lineage_depth').value = '3';
      }
    }

    function setQueryParam(search, key, value) {
      if (value) search.set(key, value);
    }

    function clearTargetResolution() {
      document.getElementById('target_table_id').value = '';
    }
    function clearNodeResolution() {
      document.getElementById('node_id').value = '';
    }

    // ===== Mode Switching =====
    modeSwitch.addEventListener('click', (e) => {
      const btn = e.target.closest('button');
      if (!btn) return;
      modeSwitch.querySelectorAll('button').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      applyModeUI();
      errorBox.textContent = '';
      errorBox.style.display = 'none';
    });

    ['target_connection_name','target_schema','target_table'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('input', clearTargetResolution);
    });
    ['node_connection_name','database_name','node_schema_name','node_table'].forEach(id => {
      const el = document.getElementById(id);
      if (el) el.addEventListener('input', clearNodeResolution);
    });

    // ===== Form Submit (no page reload) =====
    function doQuery() {
      const btn = document.getElementById('queryBtn');
      if (btn) btn.textContent = '查询中...';

      // Build new URL
      const next = new URLSearchParams();
      const mode = getCurrentMode();
      next.set('mode', mode);
      if (mode === 'table_node') {
        setQueryParam(next, 'connection_name', document.getElementById('node_connection_name').value.trim());
        setQueryParam(next, 'database_name', document.getElementById('database_name').value.trim());
        setQueryParam(next, 'schema_name', document.getElementById('node_schema_name').value.trim());
        setQueryParam(next, 'table', document.getElementById('node_table').value.trim());
        setQueryParam(next, 'node_id', document.getElementById('node_id').value.trim());
        next.set('direction', document.getElementById('direction').value || 'both');
        next.set('depth', document.getElementById('lineage_depth').value || '3');
      } else {
        setQueryParam(next, 'connection_name', document.getElementById('target_connection_name').value.trim());
        setQueryParam(next, 'schema', document.getElementById('target_schema').value.trim());
        setQueryParam(next, 'table', document.getElementById('target_table').value.trim());
        setQueryParam(next, 'target_table_id', document.getElementById('target_table_id').value.trim());
        next.set('upstream_depth', document.getElementById('upstream_depth').value || '12');
      }

      // Update URL without page reload
      const newUrl = window.location.pathname + '?' + next.toString();
      history.pushState({}, '', newUrl);

      // Reload graph data directly
      canvas.className = 'empty';
      canvas.textContent = '查询中...';
      selectedNodeId = null;
      currentGraph = null;
      loadGraph();
    }

    document.getElementById('queryBtn').addEventListener('click', doQuery);
    form.addEventListener('submit', (e) => { e.preventDefault(); doQuery(); });

    // Listen for popstate (browser back/forward)
    window.addEventListener('popstate', () => {
      window.location.reload();
    });

    // ===== Copy Link =====
    document.getElementById('copyLinkBtn').addEventListener('click', async () => {
      await navigator.clipboard.writeText(window.location.href);
    });
    document.getElementById('copyLinkBtn2').addEventListener('click', async () => {
      await navigator.clipboard.writeText(window.location.href);
    });

    // ===== Refresh =====
    document.getElementById('refreshBtn').addEventListener('click', () => {
      window.location.reload();
    });

    // ===== Utility =====
    function escapeXml(text) {
      return String(text).replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&apos;');
    }

    function truncateMiddle(text, maxLength = 42) {
      const value = String(text || '');
      if (!value || value.length <= maxLength) return value;
      const head = Math.max(12, Math.floor((maxLength - 1) * 0.6));
      const tail = Math.max(8, maxLength - head - 1);
      return value.slice(0, head) + '…' + value.slice(-tail);
    }

    function getNodeMetaLine(node) {
      const meta = node.meta || {};
      const parts = [meta.connection_name, meta.database_name || meta.fdl_database, meta.schema_name || meta.schema, meta.table_name || meta.fdl_table].filter(Boolean);
      const fallback = meta.resource_id || meta.full_name || [meta.work_name, meta.node_name].filter(Boolean).join(' / ') || '';
      return truncateMiddle(parts.join('.') || fallback, 42);
    }

    function getTypeLabel(type) {
      const map = { target_table: '目标表', pipeline: 'Pipeline', graph_table: '表', graph_task: '任务', source_table_config: '源配置' };
      return map[type] || type;
    }

    function getNodeTypeClass(type) {
      const map = { target_table: 'target', pipeline: 'pipeline', graph_table: 'table', graph_task: 'task', source_table_config: 'source' };
      return map[type] || 'table';
    }

    // ===== Status Helpers =====
    function judgementStatusClass(judgement) {
      if (['running'].includes(judgement)) return 'running';
      if (['recent_success','idle'].includes(judgement)) return 'success';
      if (['recent_error','error'].includes(judgement)) return 'error';
      if (['paused','draft','no_task','no_runs','no_state'].includes(judgement)) return 'muted';
      return 'neutral';
    }

    function statusDotClass(judgement) {
      const map = { running: 'running', recent_success: 'success', idle: 'success', recent_error: 'failed', error: 'failed', paused: 'skip', draft: 'skip', no_task: 'skip', no_runs: 'skip', no_state: 'skip' };
      return map[judgement] || 'skip';
    }

    function renderStatusBadge(summary) {
      const label = (summary && summary.status_label) || '状态未知';
      const judgement = (summary && summary.judgement) || '';
      const cls = judgementStatusClass(judgement);
      return `<span class="status-badge ${cls}"><span class="status-dot ${statusDotClass(judgement)}"></span>${escapeXml(label)}</span>`;
    }

    function renderRunStatusBadge(taskStatus, isRunning) {
      if (isRunning) return '<span class="status-badge running"><span class="status-dot running"></span>运行中</span>';
      const s = (taskStatus || '').toUpperCase();
      if (s === 'SUCCESS') return '<span class="status-badge success"><span class="status-dot success"></span>成功</span>';
      if (s === 'ERROR' || s === 'FAILED') return '<span class="status-badge failed"><span class="status-dot failed"></span>失败</span>';
      if (s === 'WAITING') return '<span class="status-badge waiting"><span class="status-dot waiting"></span>等待</span>';
      if (s === 'RUNNING') return '<span class="status-badge running"><span class="status-dot running"></span>运行中</span>';
      return `<span class="status-badge neutral">${escapeXml(taskStatus || '未知')}</span>`;
    }

    function renderMetaCells(items) {
      const valid = (items || []).filter(item => item && item.value !== undefined && item.value !== null && String(item.value).trim() !== '');
      if (!valid.length) return '';
      return `<div class="meta-cells">${valid.map(item => `
        <div class="meta-cell">
          <div class="meta-cell-label">${escapeXml(item.label)}</div>
          <div class="meta-cell-value">${escapeXml(String(item.value))}</div>
        </div>
      `).join('')}</div>`;
    }

    function renderEmptyState(text) {
      return `<div class="empty-state">${escapeXml(text)}</div>`;
    }

    function renderEmptyInline(text) {
      return `<div class="empty-state-inline">${escapeXml(text)}</div>`;
    }

    function withoutPipelineTasks(tasks) {
      return Array.isArray(tasks) ? tasks.filter(task => task?.resource_type !== 'PIPELINE') : [];
    }

    function renderTaskCards(tasks, emptyText, includeSchedule) {
      if (!tasks || !tasks.length) return renderEmptyInline(emptyText);
      return tasks.map(task => {
        const summary = task.schedule_summary || {};
        const taskTitle = task.task_name || task.node_name || task.task_resource_id || '未命名任务';
        const isPipeline = task.resource_type === 'PIPELINE';
        const typeLabel = isPipeline ? '<span class="type-badge pipeline" style="font-size:10px;padding:1px 6px;margin-right:6px;">Pipeline</span>' : '';
        let subParts = [];
        if (isPipeline) {
          if (task.source_table_names?.length) subParts.push(`来源: ${task.source_table_names.join(', ')}`);
          if (task.pipeline_target) subParts.push(`写入: ${task.pipeline_target}`);
        } else {
          if (task.node_name) subParts.push(`节点: ${task.node_name}`);
        }
        if (task.task_resource_id && !isPipeline) subParts.push(`任务ID: ${task.task_resource_id}`);
        const taskSub = subParts.join(' / ');
        return `
          <div class="task-card">
            <div class="task-card-head">
              <div>
                <div class="task-card-name">${typeLabel}${escapeXml(taskTitle)}</div>
                ${taskSub ? `<div class="task-card-sub">${escapeXml(taskSub)}</div>` : ''}
              </div>
              ${includeSchedule ? renderStatusBadge(summary) : ''}
            </div>
            ${includeSchedule && !isPipeline ? renderMetaCells([
              { label: '调度计划', value: task.schedule_plan_name },
              { label: '调度周期', value: task.schedule_cycle_text },
              { label: '调度类型', value: task.schedule_type },
              { label: '开始时间', value: task.schedule_start_time_text },
              { label: '最近记录', value: summary.run_count },
              { label: '最近时间', value: summary.latest_time_text }
            ]) : ''}
            ${(summary.latest_time_text || task.schedule_cycle_text) ? `<div class="task-card-note">${[
              summary.latest_time_text ? `最近活跃: ${summary.latest_time_text}` : '',
              task.schedule_cycle_text ? `周期: ${task.schedule_cycle_text}` : '',
              task.schedule_start_time_text ? `起始: ${task.schedule_start_time_text}` : ''
            ].filter(Boolean).join(' · ')}</div>` : ''}
          </div>
        `;
      }).join('');
    }

    function renderRuns(runs, emptyText) {
      if (!runs || !runs.length) return renderEmptyInline(emptyText);
      const firstRunTaskName = runs[0]?.task_name || runs[0]?.node_name || '';
      const allRunsSameTask = !!firstRunTaskName && runs.every(run => (run.task_name || run.node_name || '') === firstRunTaskName);
      return `${allRunsSameTask ? `<div style="margin:0 0 8px 0;font-size:12px;font-weight:600;">${escapeXml(firstRunTaskName)} · 最近运行记录</div>` : ''}<div class="run-list">${runs.map(run => {
        const triggerLabel = [run.trigger_method, run.trigger_plan_name].filter(Boolean).join(' / ');
        return `
          <div class="run-card">
            <div class="run-status">${renderRunStatusBadge(run.task_status, run.is_running)}</div>
            <div class="run-info">
              <div class="run-name">${escapeXml(allRunsSameTask ? (run.start_time_text || run.finish_time_text || run.task_id || '未命名') : (run.task_name || run.node_name || run.task_id || '未命名'))}</div>
              <div class="run-meta">
                ${run.start_time_text ? `<span>${escapeXml(run.start_time_text)}</span>` : ''}
                ${triggerLabel ? `<span>${escapeXml(triggerLabel)}</span>` : ''}
                ${run.node_name ? `<span>节点: ${escapeXml(run.node_name)}</span>` : ''}
              </div>
            </div>
            ${run.duration_text ? `<div class="run-duration">${escapeXml(run.duration_text)}</div>` : ''}
          </div>
        `;
      }).join('')}</div>`;
    }

    function renderTableCards(tables, emptyText) {
      if (!tables || !tables.length) return renderEmptyInline(emptyText);
      return tables.map(t => `
        <div class="task-card">
          <div class="task-card-name">${escapeXml(t.resource_id || t.display_name || t.table_name || '')}</div>
          ${renderMetaCells([
            { label: '连接', value: t.connection_name },
            { label: '数据库', value: t.database_name },
            { label: 'Schema', value: t.schema_name },
            { label: '表名', value: t.table_name || t.display_name }
          ])}
        </div>
      `).join('');
    }

    // ===== Impact Analysis =====
    function renderImpactAnalysis(node, graph) {
      if (!node || !graph || !graph.edges) {
        return '<div class="empty-state-inline">查询血缘图后，选中节点展示下游影响链</div>';
      }
      const mode = getCurrentMode();
      const outgoing = new Map();
      graph.nodes.forEach(n => outgoing.set(n.id, []));
      graph.edges.forEach(e => {
        if (outgoing.has(e.source)) outgoing.get(e.source).push(e);
      });

      const downstream = [];
      const visited = new Set();
      const queue = [node.id];
      visited.add(node.id);

      while (queue.length) {
        const current = queue.shift();
        for (const edge of (outgoing.get(current) || [])) {
          if (!visited.has(edge.target)) {
            visited.add(edge.target);
            const targetNode = graph.nodes.find(n => n.id === edge.target);
            if (targetNode) {
              downstream.push({ node: targetNode, via: edge.relation || 'lineage' });
            }
            queue.push(edge.target);
          }
        }
      }

      if (!downstream.length) {
        return '<div class="empty-state-inline">该节点没有下游影响</div>';
      }

      return `<div style="margin-bottom:6px;font-size:11px;color:var(--ink-muted);">影响 ${downstream.length} 个下游节点</div>
        ${downstream.map(d => `
          <div class="impact-item">
            <span class="impact-arrow">→</span>
            <span class="impact-name">${escapeXml(d.node.label)}</span>
            <span style="font-size:10px;color:var(--ink-muted);flex-shrink:0;">${escapeXml(d.via)}</span>
          </div>
        `).join('')}`;
    }

    // ===== Node Detail =====
    function renderNodeDetail(node) {
      if (!node) {
        detailBody.innerHTML = '<div class="empty-state-inline">左侧填写参数 → 点击「查询」→ 点击画布中的节点查看详情</div>';
        impactBody.innerHTML = '<div class="empty-state-inline">查询血缘图后，选中节点展示下游影响链</div>';
        return;
      }
      const meta = node.meta || {};
      const rows = [
        ['类型', node.type],
        ['标签', node.label],
        ['任务名', meta.work_name],
        ['节点名', meta.node_name],
        ['表名', meta.table_name || meta.fdl_table],
        ['标识', meta.resource_id || meta.full_name || meta.task_id],
        ['图ID/任务ID', meta.graph_id || meta.task_id],
        ['连接', meta.connection_name],
        ['数据库/Schema', [meta.database_name || meta.fdl_database, meta.schema_name || meta.schema].filter(Boolean).join(' / ')],
        ['资源类型', meta.resource_type],
        ['分组', meta.group_id]
      ].filter(([, v]) => v);

      detailBody.innerHTML = `
        <div class="detail-head">
          <span class="type-badge ${getNodeTypeClass(node.type)}">${escapeXml(getTypeLabel(node.type))}</span>
          <div class="detail-name">${escapeXml(node.label)}</div>
        </div>
        <div class="detail-grid">
          ${rows.map(([k, v]) => `
            <div class="detail-row">
              <span class="detail-row-label">${escapeXml(k)}</span>
              <span class="detail-row-value">${escapeXml(v)}</span>
            </div>
          `).join('')}
        </div>
        <div class="detail-actions">
          <button class="primary" onclick="queryFromNode('${escapeXml(node.id)}')">以此节点查询血缘</button>
          <button onclick="copyTableName()">复制表名</button>
          <button onclick="copyIdentifier()">复制标识</button>
        </div>
      `;

      // Impact analysis
      impactBody.innerHTML = renderImpactAnalysis(node, currentGraph);
    }

    function getSelectedNode() {
      if (!currentGraph || !selectedNodeId) return null;
      return currentGraph.nodes.find(n => n.id === selectedNodeId) || null;
    }

    function getCopyTableNameValue() {
      const node = getSelectedNode();
      if (!node) return '';
      const meta = node.meta || {};
      return meta.table_name || meta.fdl_table || (meta.full_name ? meta.full_name.split('.').slice(-1)[0] : node.label);
    }

    function getCopyIdentifierValue() {
      const node = getSelectedNode();
      if (!node) return '';
      const meta = node.meta || {};
      return meta.resource_id || meta.full_name || meta.task_id || node.label;
    }

    window.copyTableName = async function() {
      const val = getCopyTableNameValue();
      if (val) await navigator.clipboard.writeText(val);
    };
    window.copyIdentifier = async function() {
      const val = getCopyIdentifierValue();
      if (val) await navigator.clipboard.writeText(val);
    };

    window.queryFromNode = function(nodeId) {
      if (!currentGraph) return;
      const node = currentGraph.nodes.find(n => n.id === nodeId);
      if (!node) return;
      const meta = node.meta || {};

      if (node.type === 'target_table') {
        // Switch to target_table mode and fill fields
        modeSwitch.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        modeSwitch.querySelector('[value="target_table"]').classList.add('active');
        applyModeUI();
        setIfPresent('target_connection_name', meta.connection_name);
        setIfPresent('target_schema', meta.schema || meta.schema_name);
        setIfPresent('target_table', meta.table || meta.table_name);
        document.getElementById('target_table_id').value = meta.target_table_id || '';
      } else {
        // Switch to table_node mode and fill fields
        modeSwitch.querySelectorAll('button').forEach(b => b.classList.remove('active'));
        modeSwitch.querySelector('[value="table_node"]').classList.add('active');
        applyModeUI();
        setIfPresent('node_connection_name', meta.connection_name || meta.group_id);
        setIfPresent('database_name', meta.database_name || meta.fdl_database);
        setIfPresent('node_schema_name', meta.schema_name || meta.schema || '');
        setIfPresent('node_table', meta.table_name || meta.fdl_table || meta.table);
        // Use node_id for precise resolution
        const rawId = nodeId.includes('::') ? nodeId.split('::').slice(1).join('::') : nodeId;
        document.getElementById('node_id').value = rawId;
      }
      doQuery();
    };

    // ===== Bottom Tabs =====
    bottomTabs.addEventListener('click', (e) => {
      const tab = e.target.closest('.bottom-tab');
      if (!tab) return;
      bottomTabs.querySelectorAll('.bottom-tab').forEach(t => t.classList.remove('active'));
      tab.classList.add('active');
      currentTab = tab.dataset.tab;
      renderBottomContent();
    });

    function renderBottomContent() {
      if (!selectedNodeId || !currentActivity) {
        bottomContent.innerHTML = '<div class="empty-state-inline">查询血缘图 → 选中节点 → 查看调度监控</div>';
        return;
      }

      const node = getSelectedNode();
      if (!node) return;

      switch (currentTab) {
        case 'overview':
          renderOverviewTab(node);
          break;
        case 'producers':
          renderProducersTab(node);
          break;
        case 'runs':
          renderRunsTab(node);
          break;
        case 'consumers':
          renderConsumersTab(node);
          break;
      }
    }

    function renderOverviewTab(node) {
      const payload = currentActivity;
      const summary = payload.schedule_summary || {};
      const primaryProducer = (payload.producers || [])[0] || {};
      let producerCount = payload.producer_count || (payload.producers ? payload.producers.length : 0);
      let consumerCount = payload.consumer_count || (payload.consumers ? payload.consumers.length : 0);
      const isTarget = node.type === 'target_table' || node.type === 'pipeline';
      const isSourceFallback = isTarget && payload.lineage_resolution === 'source_fallback';
      const spotlightHTML = (summary.latest_task_name || primaryProducer.task_name)
        ? (() => {
            const taskDisplay = summary.latest_task_name || primaryProducer.task_name;
            const cycleDisplay = primaryProducer.schedule_cycle_text || summary.schedule_cycle_text || '';
            return `<div class="oc-spotlight">
              <strong>当前产出任务:</strong> ${escapeXml(taskDisplay)}${cycleDisplay ? ` · 周期 ${escapeXml(cycleDisplay)}` : ''}
              ${primaryProducer.node_name ? `<br/><span style="color:var(--ink-muted);">节点: ${escapeXml(primaryProducer.node_name)}</span>` : ''}
              ${isSourceFallback ? `<br/><span style="color:var(--ink-muted);">当前仅识别到任务级归属，未定位到目标侧具体产出节点</span>` : ''}
            </div>`;
          })()
        : (isTarget && isSourceFallback
          ? `<div class="oc-spotlight">
              <span style="color:var(--ink-muted);">未识别到目标侧产出节点，当前仅展示任务级归属信息</span>
            </div>`
          : '');

      bottomContent.innerHTML = `
        <div class="overview-card">
          <div class="oc-head">
            <div>
              <div class="oc-title">${escapeXml(node.label)} 调度概览</div>
              <div class="oc-copy">运行状态与上下游产出消费关系</div>
            </div>
            ${renderStatusBadge(summary)}
          </div>
          <div class="oc-metrics">
            <div class="oc-metric">
              <div class="oc-metric-label">产出任务</div>
              <div class="oc-metric-value">${producerCount}</div>
            </div>
            <div class="oc-metric">
              <div class="oc-metric-label">消费任务</div>
              <div class="oc-metric-value">${consumerCount}</div>
            </div>
            <div class="oc-metric">
              <div class="oc-metric-label">最近记录</div>
              <div class="oc-metric-value">${summary.run_count ?? 0}</div>
            </div>
            <div class="oc-metric">
              <div class="oc-metric-label">最近时间</div>
              <div class="oc-metric-value" style="font-size:12px;">${summary.latest_time_text || '未发现'}</div>
            </div>
          </div>
          ${spotlightHTML}
        </div>
        ${(payload.producers && payload.producers.length) ? `
          <div style="margin-top:4px;font-size:11px;color:var(--ink-muted);">下方切换标签页可查看详细任务和运行记录</div>
        ` : ''}
      `;
    }

    function renderProducersTab(node) {
      const payload = currentActivity;
      const isTarget = node.type === 'target_table' || node.type === 'pipeline';
      const isSourceFallback = isTarget && payload.lineage_resolution === 'source_fallback';
      const fallbackNote = isSourceFallback
        ? `<div style="margin:-2px 0 8px 0;font-size:11px;color:var(--ink-muted);">以下展示任务级归属；目标侧具体产出节点暂未识别</div>`
        : '';
      bottomContent.innerHTML = `
        <div style="margin-bottom:6px;font-size:12px;font-weight:600;">产出任务 (${payload.producers ? payload.producers.length : 0})</div>
        ${fallbackNote}
        ${renderTaskCards(payload.producers || [], '未识别到直接产出任务', true)}
      `;
    }

    function renderConsumersTab(node) {
      const payload = currentActivity;
      bottomContent.innerHTML = `
        <div style="margin-bottom:6px;font-size:12px;font-weight:600;">消费任务 (${payload.consumers ? payload.consumers.length : 0})</div>
        ${renderTaskCards(payload.consumers || [], '未识别到直接下游消费任务', false)}
      `;
    }

    function renderRunsTab(node) {
      const payload = currentActivity;
      const isTarget = node.type === 'target_table' || node.type === 'pipeline';
      const isSourceFallback = isTarget && payload.lineage_resolution === 'source_fallback';
      const sourceNote = isTarget && payload.source_table_names?.length
        ? `<div style="font-size:11px;color:var(--ink-muted);margin:-4px 0 8px 0;">${isSourceFallback ? `未识别到目标侧产出节点；以下展示任务级归属对应的最近运行记录，来源表为 ${payload.source_table_names.join(', ')}。` : `以下优先展示目标表真实血缘任务的运行记录；来源表 (${payload.source_table_names.join(', ')}) 的产出记录作为补充参考。`}</div>`
        : '';
      const sourceRunsSection = isTarget && !isSourceFallback && payload.source_reference_runs?.length
        ? `
          <div style="margin:10px 0 6px 0;font-size:12px;font-weight:600;">来源表产出记录（参考）</div>
          ${renderRuns(payload.source_reference_runs || [], '未发现来源表最近执行记录')}
        `
        : '';
      bottomContent.innerHTML = `
        <div style="margin-bottom:6px;font-size:12px;font-weight:600;">最近 10 次运行记录</div>
        ${sourceNote}
        ${renderRuns(payload.recent_runs || [], '未发现最近执行记录')}
        ${sourceRunsSection}
      `;
    }

    // ===== Update Bottom Content after load =====
    function updateBottomPanel(node, payload) {
      currentActivity = payload;
      const summary = payload.schedule_summary || {};
      runsCount.textContent = (payload.recent_runs || []).length;
      renderBottomContent();
    }

    // ===== Build Layout (DAG) =====
    function buildLayout(graph) {
      const incoming = new Map();
      const outgoing = new Map();
      graph.nodes.forEach(n => { incoming.set(n.id, []); outgoing.set(n.id, []); });
      graph.edges.forEach(e => {
        if (incoming.has(e.target)) incoming.get(e.target).push(e.source);
        if (outgoing.has(e.source)) outgoing.get(e.source).push(e.target);
      });

      const upDist = new Map([[graph.root_node_id, 0]]);
      const upQ = [graph.root_node_id];
      while (upQ.length) {
        const c = upQ.shift();
        for (const p of incoming.get(c) || []) {
          if (!upDist.has(p)) { upDist.set(p, upDist.get(c) + 1); upQ.push(p); }
        }
      }
      const downDist = new Map([[graph.root_node_id, 0]]);
      const downQ = [graph.root_node_id];
      while (downQ.length) {
        const c = downQ.shift();
        for (const n of outgoing.get(c) || []) {
          if (!downDist.has(n)) { downDist.set(n, downDist.get(c) + 1); downQ.push(n); }
        }
      }

      const level = new Map();
      let minL = 0, maxL = 0;
      graph.nodes.forEach(n => {
        let lv = 0;
        if (n.id !== graph.root_node_id && downDist.has(n.id)) lv = downDist.get(n.id);
        else if (n.id !== graph.root_node_id && upDist.has(n.id)) lv = -upDist.get(n.id);
        level.set(n.id, lv);
        minL = Math.min(minL, lv);
        maxL = Math.max(maxL, lv);
      });

      const layers = new Map();
      graph.nodes.forEach(n => {
        const l = level.get(n.id) - minL;
        if (!layers.has(l)) layers.set(l, []);
        layers.get(l).push(n);
      });

      const layerKeys = Array.from(layers.keys()).sort((a, b) => a - b);
      const colGap = 120, rowGap = 28, topPad = 40, leftPad = 30;
      const positions = new Map();
      const metrics = new Map();

      layerKeys.forEach(layer => {
        const nodes = layers.get(layer).sort((a, b) => a.label.localeCompare(b.label, 'zh-CN'));
        let cw = 0, ch = 0;
        const mn = nodes.map(n => {
          const ml = getNodeMetaLine(n);
          const w = Math.min(300, Math.max(190, n.label.length * 12 + 56, ml.length * 6 + 40));
          const h = 64;
          cw = Math.max(cw, w);
          ch += h;
          return { node: n, width: w, height: h };
        });
        ch += Math.max(0, mn.length - 1) * rowGap;
        metrics.set(layer, { nodes: mn, width: cw, height: ch });
      });

      const totalW = layerKeys.reduce((s, lk, i) => {
        const m = metrics.get(lk);
        return s + m.width + (i > 0 ? colGap : 0);
      }, 0);
      const maxH = Math.max(...Array.from(metrics.values()).map(m => m.height), 0);

      let cx = leftPad;
      layerKeys.forEach(layer => {
        const m = metrics.get(layer);
        const sy = topPad + Math.max(0, (maxH - m.height) / 2);
        let cy = sy;
        m.nodes.forEach(({ node, width, height }) => {
          positions.set(node.id, { x: cx, y: cy, width, height });
          cy += height + rowGap;
        });
        cx += m.width + colGap;
      });

      return {
        positions,
        width: Math.max(totalW + leftPad * 2, 600),
        height: Math.max(maxH + topPad * 2, 260)
      };
    }

    // ===== Scroll Node Into View =====
    function scrollNodeIntoView(nodeId, positions) {
      if (!canvasStage || !nodeId || !positions || !positions.has(nodeId)) return;
      const box = positions.get(nodeId);
      const targetLeft = Math.max(0, box.x - (canvasStage.clientWidth - box.width) / 2);
      const targetTop = Math.max(0, box.y - (canvasStage.clientHeight - box.height) / 2);
      canvasStage.scrollTo({ left: targetLeft, top: targetTop, behavior: 'smooth' });
    }

    // ===== Fit To Screen =====
    function fitToScreen() {
      const svg = canvas.querySelector('svg');
      if (!svg || !currentPositions) return;
      let minX = Infinity, minY = Infinity, maxX = 0, maxY = 0;
      currentPositions.forEach(b => {
        minX = Math.min(minX, b.x); minY = Math.min(minY, b.y);
        maxX = Math.max(maxX, b.x + b.width); maxY = Math.max(maxY, b.y + b.height);
      });
      const pad = 30;
      const contentW = maxX - minX + pad * 2;
      const contentH = maxY - minY + pad * 2;
      const stageW = canvasStage.clientWidth;
      const stageH = canvasStage.clientHeight;
      zoomLevel = Math.min(stageW / contentW, stageH / contentH, 1);
      zoomLevel = Math.max(0.2, zoomLevel);
      applyZoom();
      canvasStage.scrollTo({ left: Math.max(0, minX - pad), top: Math.max(0, minY - pad) });
    }

    // ===== Zoom (simple SVG scale) =====
    let zoomLevel = 1;
    function applyZoom() {
      const svg = canvas.querySelector('svg');
      if (!svg) return;
      svg.style.transform = `scale(${zoomLevel})`;
      svg.style.transformOrigin = 'top left';
    }
    document.getElementById('zoomInBtn').addEventListener('click', () => {
      zoomLevel = Math.min(3, zoomLevel + 0.2);
      applyZoom();
    });
    document.getElementById('zoomOutBtn').addEventListener('click', () => {
      zoomLevel = Math.max(0.3, zoomLevel - 0.2);
      applyZoom();
    });
    document.getElementById('fitBtn').addEventListener('click', fitToScreen);

    // ===== Render Graph =====
    function renderGraph(graph) {
      if (!graph.nodes.length) {
        canvas.className = 'empty';
        canvas.textContent = '该表未找到血缘关系，请检查表名或切换为「目标表」模式';
        return;
      }
      const { positions, width, height } = buildLayout(graph);
      currentPositions = positions;
      zoomLevel = 1;

      const defs = `<defs>
        <marker id="arrow" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
          <path d="M0,0 L0,6 L7,3 z" fill="#9aa0a6"/>
        </marker>
        <marker id="arrow-hl" markerWidth="8" markerHeight="8" refX="7" refY="3" orient="auto">
          <path d="M0,0 L0,6 L7,3 z" fill="#1a73e8"/>
        </marker>
      </defs>`;

      const edges = graph.edges.map(edge => {
        const from = positions.get(edge.source);
        const to = positions.get(edge.target);
        if (!from || !to) return '';
        const x1 = from.x + from.width, y1 = from.y + from.height / 2;
        const x2 = to.x, y2 = to.y + to.height / 2;
        const dx = Math.max(30, (x2 - x1) / 2);
        const path = `M ${x1} ${y1} C ${x1 + dx} ${y1}, ${x2 - dx} ${y2}, ${x2} ${y2}`;
        const lx = (x1 + x2) / 2, ly = (y1 + y2) / 2 - 6;
        return `
          <path class="edge" d="${path}" marker-end="url(#arrow)"/>
          <text class="edge-label" x="${lx}" y="${ly}" text-anchor="middle">${escapeXml(edge.relation)}</text>
        `;
      }).join('');

      const nodes = graph.nodes.map(node => {
        const box = positions.get(node.id);
        const fill = palette[node.type] || '#334155';
        const ml = getNodeMetaLine(node);
        return `
          <g class="node-group" data-node-id="${escapeXml(node.id)}">
            <rect class="node-rect" x="${box.x}" y="${box.y}" width="${box.width}" height="${box.height}" fill="${fill}" rx="6" ry="6"/>
            <text class="node-label" x="${box.x + 14}" y="${box.y + 24}">${escapeXml(node.label)}</text>
            <text class="node-meta" x="${box.x + 14}" y="${box.y + 44}">${escapeXml(ml)}</text>
          </g>
        `;
      }).join('');

      canvas.className = '';
      canvas.innerHTML = `<svg width="${width}" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMinYMin meet">${defs}${edges}${nodes}</svg>`;
      currentGraph = graph;

      canvas.querySelectorAll('.node-group').forEach(el => {
        el.addEventListener('click', () => { setSelectedNode(el.dataset.nodeId); });
      });
      requestAnimationFrame(() => { scrollNodeIntoView(graph.root_node_id, positions); });
      setSelectedNode(graph.root_node_id);
    }

    // ===== Set Selected Node =====
    async function setSelectedNode(nodeId) {
      selectedNodeId = nodeId;
      canvas.querySelectorAll('.node-group').forEach(el => {
        el.classList.toggle('node-selected', el.dataset.nodeId === nodeId);
      });
      const node = getSelectedNode();
      renderNodeDetail(node);
      if (node) {
        await loadActivity(node);
      }
    }

    // ===== Load Activity =====
    async function loadActivity(node) {
      if (!node) {
        bottomContent.innerHTML = '<div class="empty-state-inline">查询血缘图后，选中节点查看产出任务与调度</div>';
        return;
      }
      try {
        if (node.type === 'graph_table' && node.id.startsWith('graph::')) {
          bottomContent.innerHTML = '<div class="empty-state-inline">加载中...</div>';
          const rawId = node.id.slice('graph::'.length);
          const resp = await fetch(`/api/table-node-activity?node_id=${encodeURIComponent(rawId)}&run_limit=10`);
          const payload = await resp.json();
          if (!resp.ok) throw new Error(payload.error || '活动查询失败');
          const producers = withoutPipelineTasks(payload.producers);
          const consumers = withoutPipelineTasks(payload.consumers);
          currentActivity = {
            ...payload,
            producers,
            consumers,
            producer_count: producers.length,
            consumer_count: consumers.length,
          };
          runsCount.textContent = (payload.recent_runs || []).length;
          // Reset to overview tab
          bottomTabs.querySelectorAll('.bottom-tab').forEach(t => t.classList.remove('active'));
          bottomTabs.querySelector('[data-tab="overview"]').classList.add('active');
          currentTab = 'overview';
          renderBottomContent();
          return;
        }
        if (node.type === 'graph_task' && node.id.startsWith('graph::')) {
          bottomContent.innerHTML = '<div class="empty-state-inline">加载中...</div>';
          const rawId = node.id.slice('graph::'.length);
          const resp = await fetch(`/api/graph-task-activity?node_id=${encodeURIComponent(rawId)}&run_limit=10`);
          const payload = await resp.json();
          if (!resp.ok) throw new Error(payload.error || '任务活动查询失败');
          currentActivity = payload;
          runsCount.textContent = (payload.recent_runs || []).length;
          bottomTabs.querySelectorAll('.bottom-tab').forEach(t => t.classList.remove('active'));
          bottomTabs.querySelector('[data-tab="overview"]').classList.add('active');
          currentTab = 'overview';
          renderBottomContent();
          return;
        }
        if (node.type === 'pipeline' || node.type === 'target_table') {
          bottomContent.innerHTML = '<div class="empty-state-inline">加载中...</div>';
          const meta = node.meta || {};
          const query = new URLSearchParams({ run_limit: '10' });
          if (meta.target_table_id) {
            query.set('target_table_id', meta.target_table_id);
          } else {
            if (meta.connection_name) query.set('connection_name', meta.connection_name);
            if (meta.schema) query.set('schema', meta.schema);
            if (meta.table) query.set('table', meta.table);
          }
          const resp = await fetch(`/api/target-table-activity?${query.toString()}`);
          const payload = await resp.json();
          if (!resp.ok) throw new Error(payload.error || '活动查询失败');
          // Normalize target-table-activity response to match the structure
          // expected by renderOverviewTab / renderProducersTab / renderRunsTab
          const pipelines = payload.pipelines || [];
          const directTargetProducers = withoutPipelineTasks(payload.target_lineage_producers);
          const directTargetConsumers = withoutPipelineTasks(payload.target_lineage_consumers);
          const directTargetSummary = payload.target_lineage_schedule_summary || {};
          const directTargetRuns = Array.isArray(payload.target_recent_lineage_runs)
            ? payload.target_recent_lineage_runs
            : [];
          const sourceReferenceProducers = withoutPipelineTasks(payload.source_lineage_producers);
          const sourceReferenceRuns = Array.isArray(payload.source_recent_lineage_runs)
            ? payload.source_recent_lineage_runs
            : (Array.isArray(payload.recent_source_runs) ? payload.recent_source_runs : []);
          const sourceSummary = payload.source_schedule_summary || payload.source_lineage_schedule_summary || {};
          const sourceRuns = Array.isArray(payload.recent_source_runs) ? payload.recent_source_runs : [];
          const isSourceFallback = payload.lineage_resolution === 'source_fallback';
          const fallbackProducerCandidates = sourceReferenceProducers.map(item => ({
            ...item,
            node_name: '',
          }));
          const preferredFallbackTaskName = sourceSummary.latest_task_name || '';
          const matchedFallbackProducer = preferredFallbackTaskName
            ? fallbackProducerCandidates.find(item => item.task_name === preferredFallbackTaskName)
            : null;
          const selectedFallbackProducer = matchedFallbackProducer || fallbackProducerCandidates[0] || null;
          const fallbackProducers = selectedFallbackProducer
            ? [selectedFallbackProducer]
            : fallbackProducerCandidates.slice(0, 1);
          const fallbackRuns = (selectedFallbackProducer?.recent_runs || sourceReferenceRuns).map(run => ({
            ...run,
            node_name: '',
          }));
          const fallbackSummary = selectedFallbackProducer?.schedule_summary || sourceSummary;
          const displayProducers = isSourceFallback ? fallbackProducers : directTargetProducers;
          const displayRuns = isSourceFallback ? fallbackRuns : directTargetRuns;
          const displaySummary = isSourceFallback ? fallbackSummary : directTargetSummary;

          // Producers for a target table = the pipelines that write to it.
          // Pipeline runs (task_id=fc934a04) may not exist in fdl_work_last_record;
          // the runs shown are actually the SOURCE table's scheduling runs.
          // Display pipeline_name as the producer, and the source table name
          // as additional context.
          const firstTarget = (payload.targets || [])[0] || {};
          const sourceTableNames = [...new Set(
            (pipelines || []).flatMap(p =>
              (p.source_tables || []).map(st => st.full_name)
            )
          )].filter(Boolean);

          currentActivity = {
            ...payload,
            producers: displayProducers,
            producer_count: isSourceFallback
              ? fallbackProducers.length
              : directTargetProducers.length,
            consumers: directTargetConsumers,
            consumer_count: directTargetConsumers.length,
            schedule_summary: displaySummary,
            recent_runs: displayRuns,
            source_schedule_summary: sourceSummary,
            recent_source_runs: sourceRuns,
            source_reference_producers: isSourceFallback ? [] : sourceReferenceProducers,
            source_reference_runs: isSourceFallback ? [] : sourceReferenceRuns,
            pipeline_producers: pipelines.map(p => ({
              task_name: p.pipeline_name || '未命名Pipeline',
              node_name: '',
              resource_type: 'PIPELINE',
              task_resource_id: p.task_id || '',
              pipeline_target: firstTarget.full_name || '',
              source_table_names: sourceTableNames,
              schedule_plan_name: '',
              schedule_cycle_text: '',
              schedule_summary: p.pipeline_schedule_summary || {},
            })),
            source_table_names: sourceTableNames,
          };
          runsCount.textContent = displayRuns.length;
          bottomTabs.querySelectorAll('.bottom-tab').forEach(t => t.classList.remove('active'));
          bottomTabs.querySelector('[data-tab="overview"]').classList.add('active');
          currentTab = 'overview';
          renderBottomContent();
          return;
        }
        bottomContent.innerHTML = '<div class="empty-state-inline">当前节点暂不支持调度展示</div>';
      } catch (err) {
        bottomContent.innerHTML = `<div class="empty-state-inline">${escapeXml(err.message || '活动查询失败')}</div>`;
      }
    }

    // ===== Load Graph =====
    async function loadGraph() {
      try {
        const mode = getCurrentMode();
        const apiParams = new URLSearchParams();
        let endpoint = '/api/target-graph';

        if (mode === 'table_node') {
          endpoint = '/api/table-node-graph';
          setQueryParam(apiParams, 'connection_name', document.getElementById('node_connection_name').value.trim());
          setQueryParam(apiParams, 'database_name', document.getElementById('database_name').value.trim());
          setQueryParam(apiParams, 'schema_name', document.getElementById('node_schema_name').value.trim());
          setQueryParam(apiParams, 'table', document.getElementById('node_table').value.trim());
          setQueryParam(apiParams, 'node_id', document.getElementById('node_id').value.trim());
          apiParams.set('direction', document.getElementById('direction').value || 'both');
          apiParams.set('depth', document.getElementById('lineage_depth').value || '3');

          if (!apiParams.get('table') && !apiParams.get('node_id')) {
            canvas.className = 'empty';
            canvas.textContent = '缺少表名：请至少填写「表名」后点击查询';
            renderNodeDetail(null);
            if (document.getElementById('queryBtn')) document.getElementById('queryBtn').textContent = '查询';
            return;
          }
        } else {
          setQueryParam(apiParams, 'connection_name', document.getElementById('target_connection_name').value.trim());
          setQueryParam(apiParams, 'schema', document.getElementById('target_schema').value.trim());
          setQueryParam(apiParams, 'table', document.getElementById('target_table').value.trim());
          setQueryParam(apiParams, 'target_table_id', document.getElementById('target_table_id').value.trim());
          apiParams.set('upstream_depth', document.getElementById('upstream_depth').value || '12');
        }

        let response = await fetch(`${endpoint}?${apiParams.toString()}`);
        let payload = await response.json();

        // Auto-fallback: if table_node query fails with "not found", try target-graph
        if (!response.ok && mode === 'table_node' && (payload.error || '').toLowerCase().includes('not found')) {
          const fallbackParams = new URLSearchParams();
          const connVal = document.getElementById('node_connection_name').value.trim();
          const dbVal = document.getElementById('database_name').value.trim();
          const schemaVal = document.getElementById('node_schema_name').value.trim();
          const tableVal = document.getElementById('node_table').value.trim();
          setQueryParam(fallbackParams, 'connection_name', connVal);
          // User may have put schema in the database field — try both
          setQueryParam(fallbackParams, 'schema', schemaVal || dbVal);
          setQueryParam(fallbackParams, 'table', tableVal);
          fallbackParams.set('upstream_depth', '12');
          const fallbackResp = await fetch(`/api/target-graph?${fallbackParams.toString()}`);
          if (fallbackResp.ok) {
            // Switch to target_table mode silently
            modeSwitch.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            modeSwitch.querySelector('[value="target_table"]').classList.add('active');
            applyModeUI();
            setIfPresent('target_connection_name', connVal);
            setIfPresent('target_schema', schemaVal || dbVal);
            setIfPresent('target_table', tableVal);
            response = fallbackResp;
            payload = await fallbackResp.json();
          }
        }

        // Reverse auto-fallback: if target_table query fails with "not found", try table-node-graph
        if (!response.ok && mode === 'target_table' && (payload.error || '').toLowerCase().includes('not found')) {
          const fallbackParams = new URLSearchParams();
          const connVal = document.getElementById('target_connection_name').value.trim();
          const schemaVal = document.getElementById('target_schema').value.trim();
          const tableVal = document.getElementById('target_table').value.trim();
          setQueryParam(fallbackParams, 'connection_name', connVal);
          // User may have put database_name in schema field
          setQueryParam(fallbackParams, 'database_name', schemaVal);
          setQueryParam(fallbackParams, 'table', tableVal);
          fallbackParams.set('direction', 'both');
          fallbackParams.set('depth', '3');
          const fallbackResp = await fetch(`/api/table-node-graph?${fallbackParams.toString()}`);
          if (fallbackResp.ok) {
            // Switch to table_node mode silently
            modeSwitch.querySelectorAll('button').forEach(b => b.classList.remove('active'));
            modeSwitch.querySelector('[value="table_node"]').classList.add('active');
            applyModeUI();
            setIfPresent('node_connection_name', connVal);
            setIfPresent('database_name', schemaVal);
            setIfPresent('node_table', tableVal);
            response = fallbackResp;
            payload = await fallbackResp.json();
          }
        }

        if (!response.ok) throw new Error(payload.error || '请求失败');

        errorBox.textContent = '';
        errorBox.style.display = 'none';

        // Re-read mode in case auto-fallback switched it
        const activeMode = getCurrentMode();

        // Update summary
        summaryNodes.textContent = payload.nodes.length;
        summaryEdges.textContent = payload.edges.length;
        summaryTitle.textContent = payload.title || '-';
        summaryTargets.textContent = activeMode === 'table_node'
          ? `${payload.related_target_count || 0} 个目标`
          : `${payload.target_count || 0} 个目标`;

        // Update canvas toolbar subtitle with data
        const subtitle = document.getElementById('canvasSubtitle');
        if (subtitle) {
          subtitle.textContent = `已显示 ${payload.nodes.length} 个节点，${payload.edges.length} 条血缘关系`;
        }

        // Update breadcrumb
        if (activeMode === 'table_node' && payload.center) {
          const parts = [payload.center.connection_name, payload.center.database_name, payload.center.schema_name, payload.center.table_name].filter(Boolean);
          updateBreadcrumb(parts.join(' > ') || payload.title);
          document.getElementById('node_id').value = payload.center.id || '';
        } else {
          const firstTarget = (payload.targets || [])[0];
          if (firstTarget) {
            document.getElementById('target_table_id').value = firstTarget.target_table_id || '';
          }
          updateBreadcrumb(payload.title || '血缘关系');
        }

        renderGraph(payload);

        // Auto-fit if graph is wider than the canvas
        const stage = document.querySelector('.canvas-stage');
        if (stage && payload.nodes && payload.nodes.length) {
          const svg = stage.querySelector('svg');
          if (svg && parseInt(svg.getAttribute('width') || '0') > stage.clientWidth) {
            fitToScreen();
          } else {
            stage.scrollTo({ left: 0, top: 0 });
          }
        }
      } catch (err) {
        summaryTitle.textContent = '加载失败';
        errorBox.textContent = err.message;
        errorBox.style.display = 'block';
        canvas.className = 'empty';
        canvas.textContent = '无法展示血缘图';
        renderNodeDetail(null);
      } finally {
        const btn = document.getElementById('queryBtn');
        if (btn) btn.textContent = '查询';
      }
    }

    // ===== Init =====
    initializeFormFromParams();
    loadGraph();
  </script>
</body>
</html>
"""


class LineageViewerHandler(BaseHTTPRequestHandler):
    service = LineageService(refresh_seconds=3600)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path in {"/", "/viewer"}:
            self._send_html(HTML_TEMPLATE)
            return
        if parsed.path == "/api/target-graph":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_target_table_full_upstream_graph(
                    connection_name=params.get("connection_name") or None,
                    schema=params.get("schema") or None,
                    table=params.get("table") or None,
                    target_table_id=params.get("target_table_id") or None,
                    upstream_depth=int(params.get("upstream_depth") or "12"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/table-node-graph":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_table_node_full_lineage_graph(
                    connection_name=params.get("connection_name") or None,
                    database_name=params.get("database_name") or None,
                    schema_name=params.get("schema_name") or None,
                    table=params.get("table") or None,
                    node_id=params.get("node_id") or None,
                    direction=params.get("direction") or "both",
                    depth=int(params.get("depth") or "3"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/table-node-activity":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_table_node_activity(
                    connection_name=params.get("connection_name") or None,
                    database_name=params.get("database_name") or None,
                    schema_name=params.get("schema_name") or None,
                    table=params.get("table") or None,
                    node_id=params.get("node_id") or None,
                    run_limit=int(params.get("run_limit") or "10"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/graph-task-activity":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_graph_task_activity(
                    node_id=params.get("node_id") or "",
                    run_limit=int(params.get("run_limit") or "10"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/api/target-table-activity":
            params = self._parse_params(parsed.query)
            try:
                payload = self.service.get_target_table_activity(
                    connection_name=params.get("connection_name") or None,
                    schema=params.get("schema") or None,
                    table=params.get("table") or None,
                    target_table_id=params.get("target_table_id") or None,
                    run_limit=int(params.get("run_limit") or "10"),
                )
                self._send_json(payload, status=200)
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)
            return
        if parsed.path == "/healthz":
            self._send_json({"ok": True}, status=200)
            return
        self._send_json({"error": "not found"}, status=404)

    def log_message(self, format: str, *args: object) -> None:
        return

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

    def _send_json(self, payload: Dict[str, object], status: int = 200) -> None:
        encoded = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(encoded)))
        self.end_headers()
        self.wfile.write(encoded)


def build_target_table_viewer_url(
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


def build_table_node_viewer_url(
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


def main() -> None:
    parser = argparse.ArgumentParser(description="本地血缘图页面查看器")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--mode", choices=["target_table", "table_node"], default="target_table")
    parser.add_argument("--connection", default="PG")
    parser.add_argument("--database", default="")
    parser.add_argument("--schema", default="public")
    parser.add_argument("--table", default="sales_order")
    parser.add_argument("--node-id", default="")
    parser.add_argument("--upstream-depth", type=int, default=12)
    parser.add_argument("--direction", choices=["upstream", "downstream", "both"], default="both")
    parser.add_argument("--depth", type=int, default=3)
    parser.add_argument("--open", action="store_true", dest="open_browser")
    args = parser.parse_args()

    LineageViewerHandler.service.refresh_cache()
    server = ThreadingHTTPServer((args.host, args.port), LineageViewerHandler)
    if args.mode == "table_node":
        url = build_table_node_viewer_url(
            host=args.host,
            port=args.port,
            connection_name=args.connection,
            database_name=args.database,
            schema_name=args.schema,
            table=args.table,
            node_id=args.node_id,
            direction=args.direction,
            depth=args.depth,
        )
    else:
        url = build_target_table_viewer_url(
            host=args.host,
            port=args.port,
            connection_name=args.connection,
            schema=args.schema,
            table=args.table,
            upstream_depth=args.upstream_depth,
        )
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
