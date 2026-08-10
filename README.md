# 📊 CISP 血缘质量溯源系统

> 面向证券公司监管报送场景的数据血缘自动解析与可视化溯源系统

---

## 一、系统概述

本系统旨在解决 CISP（中国证券市场投资者保护基金）监管报送场景下的数据溯源痛点：

- **SQL 自动解析**：从 Hive/Spark SQL 脚本中自动提取字段级血缘关系
- **多层血缘追溯**：支持指定表/字段的上下游多层级 BFS 溯源
- **可视化图谱**：力导向图 + 分层布局图展示表级/字段级血缘
- **筛选条件追踪**：记录 GROUP BY / WHERE / HAVING 等分组筛选逻辑
- **批量任务分析**：从生产数据库批量拉取历史 SQL 并解析血缘
- **AI 文字解释**：调用 LLM 将技术血缘关系转化为业务可读的文字说明

## 二、技术架构

```
┌──────────────────────────────────────────────────────────┐
│                      前端展示层                           │
│        Vue 3 + Element Plus + ECharts (CDN)              │
│  SQL输入 │ 统计卡片 │ 双区域血缘展示 │ 节点详情弹窗          │
└────────────────────────┬─────────────────────────────────┘
                         │ HTTP / REST API
┌────────────────────────▼─────────────────────────────────┐
│                      后端服务层                           │
│                  Node.js + Express                       │
│  API路由 │ BFS多层查询 │ 批量分析 │ 进度轮询 │ AI调用       │
└──────────────┬───────────────────┬───────────────────────┘
               │                   │
┌──────────────▼──────────┐ ┌──────▼───────────────────────┐
│     SQL 解析引擎         │ │        数据存储层             │
│  Python + sqlglot       │ │  SQLite (血缘库, 本地)        │
│  词法/语法分析           │ │  PostgreSQL (任务元数据,      │
│  字段级血缘提取          │ │             生产环境)         │
│  子查询递归展开          │ │                              │
└─────────────────────────┘ └──────────────────────────────┘
```

## 三、技术栈

| 层级      | 技术              | 说明                             |
| --------- | ----------------- | -------------------------------- |
| 前端框架  | Vue 3 (CDN)       | 轻量响应式，无需构建工具         |
| UI 组件库 | Element Plus      | 企业级组件，中文文档完善         |
| 可视化    | ECharts           | 力导向图/分层布局图              |
| 后端框架  | Node.js + Express | REST API 服务                    |
| SQL 解析  | Python + sqlglot  | 支持 Hive/Spark/MySQL/PostgreSQL |
| 本地存储  | SQLite            | 血缘数据持久化                   |
| 远程存储  | PostgreSQL        | 生产任务元数据                   |
| AI 接入   | DeepSeek API      | 业务化文字解释生成               |

## 四、项目结构

```
cisp_demo/
├── server.js              # Node.js 后端服务（API路由、BFS查询、批量分析）
├── init-db.js             # 数据库初始化 & 迁移脚本
├── parse_sql_glot.py      # SQL 解析引擎（Python + sqlglot）
├── parse_sql.py           # 旧版解析器（保留，可忽略）
├── ai_client.js           # AI 客户端封装（DeepSeek API 调用）
├── cisp.db                # SQLite 数据库文件（自动生成）
├── testSQL.txt            # 测试 SQL 脚本
├── test_complex.sql       # 综合测试 SQL（覆盖所有子句场景）
├── field_lineage.json     # 解析结果导出（调试用）
├── 0_output.json          # 批量分析结果
├── frontend/
│   ├── index.html         # 前端单页应用（Vue 3 + Element Plus CDN）
│   ├── package.json       # 前端依赖
│   └── package-lock.json
└── package.json           # 后端依赖
```

## 五、快速开始

### 5.1 环境要求

- **Python 3.8+** + `sqlglot`
- **Node.js 18+** + `npm`
- 可选：**PostgreSQL**（用于批量分析生产任务）

### 5.2 安装依赖

```bash
# Python 依赖
pip install sqlglot

# Node.js 依赖
npm install

# 前端依赖（CDN 方式引入，无需安装）
# 如需要本地构建：
cd frontend && npm install
```

### 5.3 初始化数据库

```bash
# 创建 SQLite 数据库及表结构
npm run init-db
# 或
node init-db.js
```

### 5.4 启动服务

```bash
# 生产模式
npm start
# 或
node server.js

# 开发模式（需安装 nodemon）
npm run dev
```

服务默认运行在 **http://localhost:3000**，前端页面直接访问根路径即可。

### 5.5 快速验证

1. 打开 `http://localhost:3000`
2. 在 SQL 输入区粘贴SQL语句并点击「🚀 解析并存储」，或点击「🚀 从数据库批量分析」（`public.y4_trace_tasks`表中的所有纯sql任务）
3. 在下方图谱区域查看血缘关系

## 六、功能说明

### 6.1 SQL 解析

- 支持方言：**Hive / Spark SQL / MySQL / PostgreSQL**
- 支持语法：INSERT OVERWRITE、SELECT、CTE (WITH)、UNION ALL、子查询嵌套
- 解析粒度：**字段级**（source_field → target_field + 完整表达式）
- 特殊处理：GROUP BY / WHERE / HAVING 子句提取、子查询别名穿透、多层 COALESCE 扁平化

### 6.2 血缘查询

| 查询模式   | 说明                                           |
| ---------- | ---------------------------------------------- |
| 总览模式   | 展示所有血缘关系，支持按层/角色/最少字段数筛选 |
| 表级搜索   | 指定表名，展示其上下游所有关联表               |
| 字段级搜索 | 指定表+字段，展示该字段的多层上下游血缘        |

### 6.3 可视化

- **图形视图**：ECharts 力导向图 / 分层布局图，点击节点查看字段级详情
- **表格视图**：结构化展示血缘关系，包含筛选条件列

## 七、API 接口

| 方法 | 路径                     | 说明                 |
| ---- | ------------------------ | -------------------- |
| POST | `/api/parse-sql`       | 解析 SQL 并存储血缘  |
| GET  | `/api/lineage/stored`  | 查询已存储的血缘数据 |
| GET  | `/api/lineage/field`   | 查询单条字段级血缘   |
| GET  | `/api/lineage/multi`   | 多层 BFS 血缘查询    |
| POST | `/api/lineage/explain` | AI 业务化解释生成    |
| GET  | `/api/lineage/stats`   | 血缘统计概览         |
| GET  | `/api/table/fields`    | 查询表的字段列表     |
| POST | `/api/batch/analyze`   | 批量分析生产任务     |
| GET  | `/api/batch/progress`  | 查询批量分析进度     |
| POST | `/api/batch/cancel`    | 取消批量分析         |

## 八、数据库设计

### field_lineage（字段血缘表）

| 字段            | 类型     | 说明                                               |
| --------------- | -------- | -------------------------------------------------- |
| id              | INTEGER  | 主键自增                                           |
| target_field    | TEXT     | 目标字段名                                         |
| target_table    | TEXT     | 目标表名                                           |
| source_table    | TEXT     | 来源表名                                           |
| source_field    | TEXT     | 来源字段名                                         |
| expression      | TEXT     | 简化表达式                                         |
| full_expression | TEXT     | 完整替换后表达式                                   |
| expression_type | TEXT     | direct / agg_sum / agg_count / computed / constant |
| source_role     | TEXT     | direct / data_source / filter / table_level        |
| filter_cond     | TEXT     | 分组筛选条件（JSON，含 subquery_filters）          |
| job_id          | TEXT     | 任务ID                                             |
| report_code     | TEXT     | 报送代码                                           |
| layer           | TEXT     | 数据层                                             |
| created_at      | DATETIME | 创建时间                                           |

### TODO：snapshot（分层快照表）、quality_rule（质量规则表）、rule_result（规则结果表）

详见 `init-db.js`。

## 九、后续规划

- [ ] 字段级 AI 解释（点击单字段生成解释）
- [ ] 真实数据追溯与自动校验（表达式 → SQL → 采样对账）
- [ ] 质量规则引擎（自动检测空值、异常值、血缘断裂）
- [ ] 血缘变更告警（DDL 变更触发血缘重算）
