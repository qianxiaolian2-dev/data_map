# 图数据库血缘项目

## frontend-design

当用户说出"设计前端"、"改样式"、"调 UI"、"搭个页面"等与前端/UI 设计相关的请求时，使用此技能。

### 设计规范

**配色系统 (CSS 变量)：**
```
--bg-page: #f2efe7;         // 页面背景
--bg-panel: rgba(255, 251, 243, 0.9);  // 面板背景
--ink: #172033;              // 正文色
--muted: #667085;            // 辅助文字
--line: #ded4c4;            // 边框
--target: #0f766e;          // 目标表
--pipeline: #c2410c;        // 管道
--table: #1d4ed8;           // 表
--task: #8b5cf6;            // 任务
--sourcecfg: #475569;       // 源配置
```

**排版：**
- 字体: `"Avenir Next", "PingFang SC", "Helvetica Neue", sans-serif`
- 标题衬线: `"Iowan Old Style", "Palatino Linotype", "PingFang SC", serif`
- 圆角: panel 26px, card 18px, pill 999px

**风格：**
- 暖色调米白背景 + 渐变光晕
- 玻璃态毛玻璃面板 (backdrop-filter: blur)
- 柔雾阴影，避免硬边
- 极简主义，信息密度适中

### 技术栈

- **后端渲染**: Python `http.server` 原生模板 (HTML_TEMPLATE 字符串)
- **图可视化**: HTML5 Canvas + 自绘引擎 (节点/边渲染)
- **交互**: 鼠标拖拽平移、滚轮缩放、悬浮高亮、点击详情
- **数据**: `/api/target-graph` JSON 端点返回 `{nodes, edges}`

### 开发原则

1. 单 HTML 文件原则 — 所有 CSS/JS 内嵌在 `HTML_TEMPLATE` 中，无需构建工具
2. Canvas 渲染性能优先 — 大数据量图不引入 DOM 节点方案
3. 增量迭代 — 先跑通交互逻辑，再打磨视觉细节
4. 保持现有设计语言一致 — 新组件复用上述 CSS 变量体系
5. 所有新样式/JS 修改在 `lineage_web_viewer.py` 的 `HTML_TEMPLATE` 字符串中完成

### 常用任务

- 修改节点样式（颜色、大小、标签）
- 添加图例 / 筛选器
- 优化缩放平移交互
- 增加边标签（关系描述）
- 布局算法调优（力导向 / 层次布局）
- 详情侧面板
