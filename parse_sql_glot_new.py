#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SQL血缘解析器 - 完整优化版
功能：
1. 解析SQL提取结构化Block（SELECT块）
2. 直接写入SQLite数据库（绕过JSON序列化）
3. 支持流式处理，内存可控
4. 兼容旧版column_lineage输出

输出格式：
- blocks: 结构化块列表（用于调试/迁移）
- 同时直接写入数据库（生产模式）
"""

import io
import json
import sys
import re
import hashlib
import sqlite3
from datetime import datetime
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.errors import ParseError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')


class SQLBloodlineParser:
    """完整SQL血缘解析器，输出结构化Block"""

    def __init__(self, dialect='hive', db_path=None, batch_size=50):
        self.dialect = dialect
        self.db_path = db_path
        self.batch_size = batch_size

        # 解析状态
        self.source_tables = set()
        self.target_tables = set()
        self.blocks = []
        self.column_lineage = []
        self.case_mappings = []
        self.alias_map = {}
        self.column_alias_map = {}
        self.block_id_counter = 0
        self._current_job_id = None
        self._current_period = None

        # 批量写入缓存
        self._block_buffer = []
        self._col_buffer = []
        self._join_buffer = []
        self._where_buffer = []
        self._group_buffer = []
        self._having_buffer = []
        self._union_buffer = []

        # 数据库连接（延迟初始化）
        self._conn = None
        self._cursor = None

    # ==================== 主入口 ====================

    def parse_sql_from_file(self, file_path, job_id=None, period=None, store_to_db=False):
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        return self.parse_sql_text(sql_text, job_id, period, store_to_db)

    def parse_sql_text(self, sql_text, job_id=None, period=None, store_to_db=False):
        """主解析入口"""
        self._current_job_id = job_id or 'manual'
        self._current_period = period or 'unknown'
        self._reset_state()

        if store_to_db and self.db_path:
            self._init_db_connection()

        try:
            sql_text = self._preprocess_sql(sql_text)
            statements = self._split_statements(sql_text)

            for stmt_sql in statements:
                if not stmt_sql.strip():
                    continue

                stripped = stmt_sql.strip().upper()
                if stripped.startswith('SELECT') and 'FROM' not in stripped:
                    continue

                stmt_clean = self._strip_comments(stmt_sql)
                if not stmt_clean.strip():
                    continue

                try:
                    parsed = parse_one(stmt_clean, dialect=self.dialect)
                    self._parse_statement(parsed, stmt_clean)
                except Exception as e:
                    print(f"⚠️ 解析失败: {e}", file=sys.stderr)
                    self._parse_with_fallback(stmt_clean)

            # 批量写入剩余数据
            if store_to_db and self.db_path:
                self._flush_buffer()

            # 返回结果
            return self._build_result()

        except Exception as e:
            return {"error": str(e)}
        finally:
            if self._conn:
                self._conn.close()

    # ==================== 核心解析 ====================

    def _parse_statement(self, parsed, original_sql=None):
        self._collect_alias_map(parsed)
        self._extract_target_tables(parsed)
        self._extract_source_tables(parsed)

        tree = parsed
        try:
            from sqlglot.optimizer.qualify import qualify
            tree = qualify(parsed, dialect=self.dialect)
        except Exception as e:
            print(f"⚠️ qualify失败: {e}", file=sys.stderr)

        self._build_column_alias_map(tree)

        for insert in tree.find_all(exp.Insert):
            select = insert.args.get('expression')
            target_table = None
            if insert.this and isinstance(insert.this, exp.Table):
                target_table = self._get_full_name(insert.this)
            if select and isinstance(select, (exp.Select, exp.Union)):
                self._extract_blocks(select, target_table, 'INSERT')

        for create in tree.find_all(exp.Create):
            if create.this and isinstance(create.this, exp.Table):
                target_table = self._get_full_name(create.this)
                select = create.args.get('expression')
                if select and isinstance(select, (exp.Select, exp.Union)):
                    self._extract_blocks(select, target_table, 'CREATE')

        self._extract_column_lineage_compat(tree)
        self._extract_case_mappings(tree)

    def _extract_blocks(self, node, target_table, statement_type):
        if node is None:
            return None

        if isinstance(node, exp.Union):
            left_block = self._extract_blocks(node.left, target_table, statement_type)
            right_block = self._extract_blocks(node.right, target_table, statement_type)
            return self._create_union_block(left_block, right_block, target_table, statement_type)

        if isinstance(node, exp.Subquery):
            return self._extract_blocks(node.this, target_table, statement_type)

        if isinstance(node, exp.Select):
            return self._extract_select_block(node, target_table, statement_type)

        return None

    def _extract_select_block(self, select, target_table, statement_type):
        block_id = self._next_block_id()

        from_info = self._extract_from_info(select)
        joins = self._extract_joins(select)
        where_conditions = self._extract_where(select)
        group_by = self._extract_group_by(select)
        having = self._extract_having(select)

        columns = []
        for expr in select.expressions:
            if isinstance(expr, exp.Alias):
                columns.append(self._extract_column_info(expr.this, expr.alias_or_name, select))
            elif isinstance(expr, exp.Column):
                columns.append(self._extract_column_info(expr, expr.name, select))
            elif isinstance(expr, exp.Star):
                columns.append({
                    "target": "*",
                    "source": "*",
                    "source_table": None,
                    "agg_func": None,
                    "has_distinct": False,
                    "expression": "*",
                    "expression_type": "star"
                })

        block = {
            "block_id": block_id,
            "block_type": "SELECT",
            "target_table": target_table,
            "statement_type": statement_type,
            "from_table": from_info.get('table') if from_info else None,
            "from_alias": from_info.get('alias') if from_info else None,
            "from_subquery": from_info.get('subquery') if from_info else None,
            "joins": joins,
            "columns": columns,
            "where_conditions": where_conditions,
            "group_by": group_by,
            "having": having,
            "sql_hash": self._compute_sql_hash(str(select)),
            "job_id": self._current_job_id,
            "period": self._current_period,
            "created_at": datetime.now().isoformat()
        }

        self.blocks.append(block)

        # 如果开启了数据库存储，立即写入
        if self.db_path:
            self._write_block_to_db(block)

        return block

    # ==================== 数据库写入（核心新增） ====================

    def _init_db_connection(self):
        """初始化数据库连接"""
        if not self.db_path:
            return
        self._conn = sqlite3.connect(self.db_path)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._cursor = self._conn.cursor()

    def _write_block_to_db(self, block):
        """将单个Block写入数据库（使用批量缓存）"""
        # 收集到缓存
        self._block_buffer.append((
            block["block_id"],
            None,  # parent_block_id
            block["block_type"],
            block["target_table"],
            block["statement_type"],
            block["from_table"],
            block["from_alias"],
            block["sql_hash"],
            block["job_id"],
            block["period"],
            block["created_at"]
        ))

        # 字段列
        for col in block.get("columns", []):
            self._col_buffer.append((
                block["block_id"],
                col.get("target"),
                col.get("source"),
                col.get("source_table"),
                col.get("agg_func"),
                1 if col.get("has_distinct") else 0,
                col.get("expression"),
                col.get("expression_type", "direct")
            ))

        # JOIN
        for join in block.get("joins", []):
            self._join_buffer.append((
                block["block_id"],
                join.get("type"),
                join.get("table"),
                join.get("alias"),
                join.get("on")
            ))

        # WHERE
        for w in block.get("where_conditions", []):
            self._where_buffer.append((
                block["block_id"],
                w.get("expression")
            ))

        # GROUP BY
        for g in block.get("group_by", []):
            self._group_buffer.append((
                block["block_id"],
                g
            ))

        # HAVING
        if block.get("having"):
            self._having_buffer.append((
                block["block_id"],
                block["having"]
            ))

        # 达到批量大小则刷新
        if len(self._block_buffer) >= self.batch_size:
            self._flush_buffer()

    def _flush_buffer(self):
        """批量写入所有缓存数据"""
        if not self._cursor:
            return

        try:
            # 写入 Block
            if self._block_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block
                    (block_id, parent_block_id, block_type, target_table,
                     statement_type, from_table, from_alias, sql_hash,
                     job_id, period, created_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, self._block_buffer)
                self._block_buffer.clear()

            # 写入 Column
            if self._col_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block_column
                    (block_id, target_column, source_column, source_table,
                     agg_func, has_distinct, expression, expression_type)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, self._col_buffer)
                self._col_buffer.clear()

            # 写入 JOIN
            if self._join_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block_join
                    (block_id, join_type, join_table, join_alias, on_condition)
                    VALUES (?, ?, ?, ?, ?)
                """, self._join_buffer)
                self._join_buffer.clear()

            # 写入 WHERE
            if self._where_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block_where
                    (block_id, condition_expr)
                    VALUES (?, ?)
                """, self._where_buffer)
                self._where_buffer.clear()

            # 写入 GROUP BY
            if self._group_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block_group_by
                    (block_id, group_column)
                    VALUES (?, ?)
                """, self._group_buffer)
                self._group_buffer.clear()

            # 写入 HAVING
            if self._having_buffer:
                self._cursor.executemany("""
                    INSERT OR REPLACE INTO lineage_block_having
                    (block_id, having_expr)
                    VALUES (?, ?)
                """, self._having_buffer)
                self._having_buffer.clear()

            # 提交事务
            self._conn.commit()

        except Exception as e:
            print(f"⚠️ 批量写入失败: {e}", file=sys.stderr)
            if self._conn:
                self._conn.rollback()

    def _write_compat_lineage_to_db(self):
        """将兼容的column_lineage写入field_lineage表"""
        if not self._cursor or not self.column_lineage:
            return

        batch = []
        for cl in self.column_lineage:
            batch.append((
                cl.get("source_table"),
                cl.get("target_table"),
                cl.get("source_column"),
                cl.get("target_column"),
                cl.get("expression"),
                cl.get("full_expression", cl.get("expression")),
                cl.get("expression_type", "direct"),
                cl.get("source_role", "direct"),
                cl.get("job_id", self._current_job_id),
                "Y4",
                cl.get("layer", "UNKNOWN"),
                cl.get("agg_func"),
                1 if cl.get("has_distinct") else 0,
                json.dumps(cl.get("group_by", [])) if cl.get("group_by") else None,
                cl.get("having"),
                cl.get("where_condition"),
                1 if cl.get("is_grouped") else 0
            ))

        self._cursor.executemany("""
            INSERT OR REPLACE INTO field_lineage
            (source_table, target_table, source_field, target_field,
             expression, full_expression, expression_type, source_role,
             job_id, report_code, layer, agg_func, has_distinct,
             group_by, having, where_condition, is_grouped)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, batch)
        self._conn.commit()

    # ==================== 兼容旧接口 ====================

    def _extract_column_lineage_compat(self, tree):
        for insert in tree.find_all(exp.Insert):
            select = insert.args.get('expression')
            target_table = None
            if insert.this and isinstance(insert.this, exp.Table):
                target_table = self._get_full_name(insert.this)
            if select and isinstance(select, exp.Select):
                self._extract_columns_from_select_compat(select, target_table)

    def _extract_columns_from_select_compat(self, select, target_table):
        for expr in select.expressions:
            if isinstance(expr, exp.Alias):
                alias = expr.alias_or_name
                source_cols = self._find_source_columns(expr.this, role='direct')
                expr_type = self._classify_expression_type(
                    self._clean_expression(expr.this.sql()), expr.this
                )
                for col in source_cols:
                    self.column_lineage.append({
                        "source_table": col.get('table'),
                        "source_column": col.get('column'),
                        "target_table": target_table,
                        "target_column": alias,
                        "expression": self._get_simplified_expression(expr.this),
                        "full_expression": self._clean_expression(expr.this.sql()),
                        "expression_type": expr_type,
                        "source_role": col.get('role', 'direct'),
                        "agg_func": expr.this.__class__.__name__.lower() if isinstance(expr.this, exp.AggFunc) else None,
                        "has_distinct": self._has_distinct(expr.this),
                        "job_id": self._current_job_id,
                        "layer": self._infer_layer(target_table)
                    })

    def _has_distinct(self, expr):
        if not isinstance(expr, exp.AggFunc):
            return False
        for arg in expr.args.get('expressions', []):
            if isinstance(arg, exp.Distinct):
                return True
        return False

    def _infer_layer(self, table_name):
        if not table_name:
            return 'UNKNOWN'
        name = table_name.lower()
        if 'ods' in name or 'stg' in name:
            return 'COLLECT'
        if 'dwd' in name or 'dwm' in name:
            return 'BASE'
        if 'dws' in name or 'ads' in name:
            return 'RESULT'
        return 'UNKNOWN'

    def _extract_case_mappings(self, tree):
        for case in tree.find_all(exp.Case):
            for when in case.args.get('ifs', []):
                if isinstance(when, exp.If):
                    condition = when.args.get('this')
                    true_value = when.args.get('true')
                    if condition and true_value:
                        self.case_mappings.append({
                            "condition": self._clean_expression(condition.sql()),
                            "result": self._clean_expression(true_value.sql())
                        })

    def _find_source_columns(self, expr, scoped_aliases=None, role='direct'):
        results = []
        if isinstance(expr, exp.Column):
            table_name = self._extract_table_from_column(expr)
            if table_name:
                table_lower = table_name.lower()
                if table_lower in self.column_alias_map:
                    sq_cols = self.column_alias_map[table_lower]
                    col_lower = expr.name.lower()
                    if col_lower in sq_cols:
                        for src in sq_cols[col_lower]['source_columns']:
                            results.append({
                                "table": src.get('table'),
                                "column": src.get('column'),
                                "expression": self._clean_expression(expr.sql()),
                                "role": src.get('role', role)
                            })
                        return results
                resolved_table = self._resolve_table_name(table_name)
                table_name = resolved_table
            results.append({
                "table": table_name,
                "column": expr.name,
                "expression": self._clean_expression(expr.sql()),
                "role": role
            })
        elif isinstance(expr, exp.AggFunc):
            for arg in expr.args.get('expressions', []):
                results.extend(self._find_source_columns(arg, scoped_aliases, 'data_source'))
        elif isinstance(expr, exp.Case):
            for when in expr.args.get('ifs', []):
                if isinstance(when, exp.If):
                    results.extend(self._find_source_columns(when.args.get('this'), scoped_aliases, 'filter'))
                    results.extend(self._find_source_columns(when.args.get('true'), scoped_aliases, 'data_source'))
            default_val = expr.args.get('default')
            if default_val:
                results.extend(self._find_source_columns(default_val, scoped_aliases, 'data_source'))
        elif isinstance(expr, exp.Binary):
            results.extend(self._find_source_columns(expr.this, scoped_aliases, role))
            if expr.args.get('expression'):
                results.extend(self._find_source_columns(expr.args.get('expression'), scoped_aliases, role))
        elif isinstance(expr, exp.Func):
            for arg in expr.args.get('expressions', []):
                results.extend(self._find_source_columns(arg, scoped_aliases, role))
        return results

    def _extract_table_from_column(self, col):
        if col.args.get('table'):
            t = col.args.get('table')
            if isinstance(t, exp.Identifier):
                return t.name
            if isinstance(t, str):
                return t
        if hasattr(col, 'this') and col.this and hasattr(col.this, 'table') and col.this.table:
            t = col.this.table
            return t.name if isinstance(t, exp.Identifier) else t
        return None

    def _classify_expression_type(self, full_expr_sql, original_expr):
        if isinstance(original_expr, exp.Literal):
            return 'constant'
        if full_expr_sql:
            expr_upper = full_expr_sql.upper()
            if 'SUM(' in expr_upper:
                return 'agg_sum'
            if 'COUNT(' in expr_upper:
                return 'agg_count'
            if 'AVG(' in expr_upper:
                return 'agg_avg'
            if 'MAX(' in expr_upper:
                return 'agg_max'
            if 'MIN(' in expr_upper:
                return 'agg_min'
            if 'CASE WHEN' in expr_upper or 'CASE ' in expr_upper:
                return 'computed'
            if '+' in full_expr_sql or '-' in full_expr_sql or '*' in full_expr_sql or '/' in full_expr_sql:
                return 'computed'
        if isinstance(original_expr, exp.Column):
            return 'direct'
        return 'other'

    def _get_simplified_expression(self, expr):
        if isinstance(expr, exp.Column):
            return expr.name
        if isinstance(expr, exp.AggFunc):
            func_name = expr.__class__.__name__.lower()
            args = []
            for arg in expr.args.get('expressions', []):
                if isinstance(arg, exp.Column):
                    args.append(arg.name)
                elif isinstance(arg, exp.Distinct) and arg.this and isinstance(arg.this, exp.Column):
                    args.append(f"DISTINCT {arg.this.name}")
            if not args and hasattr(expr, 'this') and expr.this:
                if isinstance(expr.this, exp.Column):
                    args.append(expr.this.name)
            return f"{func_name.upper()}({', '.join(args)})" if args else f"{func_name.upper()}(*)"
        if isinstance(expr, exp.Case):
            return "CASE_WHEN(...)"
        if isinstance(expr, exp.Func):
            return f"{expr.__class__.__name__.lower()}(...)"
        return self._clean_expression(expr.sql())[:60]

    def _resolve_table_name(self, name):
        if not name:
            return name
        visited = set()
        current = name.lower() if isinstance(name, str) else name
        while current in self.alias_map and current not in visited:
            visited.add(current)
            current = self.alias_map[current]
            if isinstance(current, str):
                current = current.lower()
        return self.alias_map.get(current, name)

    def _clean_expression(self, sql_str):
        return re.sub(r'"([^"]*)"', r'\1', sql_str) if sql_str else sql_str

    # ==================== 辅助提取方法 ====================

    def _extract_from_info(self, select):
        from_expr = select.args.get('from_')
        if not from_expr:
            return None
        if isinstance(from_expr, exp.From):
            this = from_expr.args.get('this')
            if isinstance(this, exp.Table):
                return {"table": self._get_full_name(this), "alias": this.alias if hasattr(this, 'alias') and this.alias else None}
            if isinstance(this, exp.Subquery):
                return {"subquery": self._extract_blocks(this.this, None, 'SUBQUERY'), "alias": this.alias if hasattr(this, 'alias') and this.alias else None}
        return None

    def _extract_joins(self, select):
        joins = []
        join_exprs = select.args.get('joins', [])
        for join in join_exprs:
            if not isinstance(join, exp.Join):
                continue
            join_type = 'INNER'
            if isinstance(join, exp.LeftJoin):
                join_type = 'LEFT'
            elif isinstance(join, exp.RightJoin):
                join_type = 'RIGHT'
            elif isinstance(join, exp.FullJoin):
                join_type = 'FULL'
            this = join.args.get('this')
            join_info = {"type": join_type}
            if isinstance(this, exp.Table):
                join_info["table"] = self._get_full_name(this)
                join_info["alias"] = this.alias if hasattr(this, 'alias') and this.alias else None
            elif isinstance(this, exp.Subquery):
                join_info["subquery"] = self._extract_blocks(this.this, None, 'SUBQUERY')
                join_info["alias"] = this.alias if hasattr(this, 'alias') and this.alias else None
            else:
                continue
            on_expr = join.args.get('on')
            if on_expr:
                join_info["on"] = self._clean_expression(on_expr.sql())
            joins.append(join_info)
        return joins

    def _extract_where(self, select):
        where_expr = select.args.get('where')
        if not where_expr or not where_expr.this:
            return []
        return [{"expression": self._clean_expression(where_expr.this.sql())}]

    def _extract_group_by(self, select):
        group_expr = select.args.get('group')
        if not group_expr:
            return []
        return [self._clean_expression(g.sql()) for g in group_expr.expressions]

    def _extract_having(self, select):
        having_expr = select.args.get('having')
        if not having_expr or not having_expr.this:
            return None
        return self._clean_expression(having_expr.this.sql())

    def _extract_column_info(self, expr, target_name, select):
        result = {
            "target": target_name,
            "source": None,
            "source_table": None,
            "agg_func": None,
            "has_distinct": False,
            "expression": self._clean_expression(expr.sql()),
            "expression_type": 'direct'
        }
        if isinstance(expr, exp.AggFunc):
            func_name = expr.__class__.__name__.lower()
            result["agg_func"] = func_name
            result["expression_type"] = f'agg_{func_name}'
            for arg in expr.args.get('expressions', []):
                if isinstance(arg, exp.Distinct):
                    result["has_distinct"] = True
                    for de in arg.args.get('expressions', []):
                        if isinstance(de, exp.Column):
                            result["source"] = de.name
                            result["source_table"] = self._extract_table_from_column(de)
                elif isinstance(arg, exp.Column):
                    result["source"] = arg.name
                    result["source_table"] = self._extract_table_from_column(arg)
        elif isinstance(expr, exp.Case):
            result["expression_type"] = 'case'
            result["source"] = 'CASE_WHEN'
        elif isinstance(expr, exp.Column):
            result["source"] = expr.name
            result["source_table"] = self._extract_table_from_column(expr)
            result["expression_type"] = 'direct'
        elif isinstance(expr, exp.Binary):
            result["expression_type"] = 'binary'
            cols = []
            for col in expr.find_all(exp.Column):
                cols.append(col.name)
            result["source"] = ', '.join(cols) if cols else None
        elif isinstance(expr, exp.Func):
            result["expression_type"] = 'function'
            cols = []
            for col in expr.find_all(exp.Column):
                cols.append(col.name)
            result["source"] = ', '.join(cols) if cols else None
        return result

    def _create_union_block(self, left_block, right_block, target_table, statement_type):
        block = {
            "block_id": self._next_block_id(),
            "block_type": "UNION",
            "target_table": target_table,
            "statement_type": statement_type,
            "union_children": [left_block, right_block] if left_block and right_block else [],
            "job_id": self._current_job_id,
            "period": self._current_period,
            "created_at": datetime.now().isoformat()
        }
        self.blocks.append(block)
        if self.db_path:
            self._write_block_to_db(block)
        return block

    def _get_full_name(self, table_node):
        name = table_node.name if hasattr(table_node, 'name') else str(table_node)
        db = table_node.db if hasattr(table_node, 'db') and table_node.db else None
        return f"{db}.{name}" if db else name

    def _next_block_id(self):
        self.block_id_counter += 1
        return f"BLK_{self.block_id_counter:04d}"

    def _compute_sql_hash(self, sql):
        return hashlib.md5(sql.encode('utf-8')).hexdigest()[:16]

    def _reset_state(self):
        self.source_tables.clear()
        self.target_tables.clear()
        self.blocks.clear()
        self.column_lineage.clear()
        self.case_mappings.clear()
        self.alias_map.clear()
        self.column_alias_map.clear()
        self.block_id_counter = 0

    def _build_result(self):
        target_table_set = set(self.target_tables)
        filtered_sources = {t for t in self.source_tables if t not in target_table_set}
        for cl in self.column_lineage:
            if cl.get('source_table') and cl['source_table'] not in target_table_set:
                filtered_sources.add(cl['source_table'])

        seen = set()
        deduped = []
        for cl in self.column_lineage:
            key = (cl.get('source_table'), cl.get('source_column'),
                   cl.get('target_table'), cl.get('target_column'),
                   cl.get('source_role', 'direct'))
            if key not in seen:
                seen.add(key)
                deduped.append(cl)

        return {
            "source_tables": sorted(list(filtered_sources)),
            "target_tables": list(self.target_tables),
            "column_lineage": deduped,
            "case_mappings": self.case_mappings,
            "blocks": self.blocks,
            "block_count": len(self.blocks)
        }

    # ==================== 预处理器 ====================

    def _strip_comments(self, sql):
        result = []
        i, in_string, string_char = 0, False, None
        while i < len(sql):
            char = sql[i]
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string, string_char = True, char
                elif char == string_char:
                    in_string = False
                result.append(char)
                i += 1
                continue
            if not in_string:
                if char == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
                    while i < len(sql) and sql[i] != '\n':
                        i += 1
                    continue
                if char == '/' and i + 1 < len(sql) and sql[i + 1] == '*':
                    i += 2
                    while i < len(sql):
                        if sql[i] == '*' and i + 1 < len(sql) and sql[i + 1] == '/':
                            i += 2
                            break
                        i += 1
                    continue
            result.append(char)
            i += 1
        return ''.join(result)

    def _preprocess_sql(self, sql):
        sql = re.sub(r'`([^`]+)`', r'\1', sql)
        sql = sql.replace("'''", "")
        sql = re.sub(r"\$\{bdp\.system\.bizdate\}", "20240105", sql)
        sql = re.sub(r"\$\{last_month_last_trading_day\}", "2025-12-31", sql)
        sql = re.sub(r"\$\{last_month_first_day\}", "2025-12-01", sql)
        sql = re.sub(r"\$\{last_month\}", "202512", sql)

        var_map = {
            'last_trading_day': '20240105',
            'last_one_year': '20230101',
            'last_three_month': '20231001',
            'last_six_month': '20230701',
            'last_one_month': '20231201',
            'last_serven_day': '20231229',
            'year_first_day': '20240101',
            'last_year_last_trading_day': '20231229',
            'last_month': '202512',
            'last_month_last_day': '20251231',
            'last_2_month': '202411',
        }
        for var, val in var_map.items():
            sql = sql.replace("'{" + var + "}'", "'" + val + "'")
            sql = sql.replace("{" + var + "}", val)
        sql = re.sub(r"'\{[a-zA-Z_][a-zA-Z0-9_]*\}'", "'20240101'", sql)
        sql = re.sub(r'\{[a-zA-Z_][a-zA-Z0-9_]*\}', "20240101", sql)
        sql = re.sub(r'get_last_n_txdate\(\s*([^)]+)\s*,\s*-1\s*\)', r"\1", sql)
        sql = re.sub(r'\bnvl\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)
        return sql

    def _split_statements(self, sql):
        statements = []
        current, in_string, string_char = [], False, None
        i = 0
        while i < len(sql):
            char = sql[i]
            if not in_string and char == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
                while i < len(sql) and sql[i] != '\n':
                    i += 1
                continue
            if not in_string and char == '/' and i + 1 < len(sql) and sql[i + 1] == '*':
                i += 2
                while i < len(sql):
                    if sql[i] == '*' and i + 1 < len(sql) and sql[i + 1] == '/':
                        i += 2
                        break
                    i += 1
                continue
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string, string_char = True, char
                elif char == string_char:
                    in_string = False
            if char == ';' and not in_string:
                stmt = ''.join(current).strip()
                if stmt:
                    statements.append(stmt)
                current = []
            else:
                current.append(char)
            i += 1
        if current:
            stmt = ''.join(current).strip()
            if stmt:
                statements.append(stmt)
        return statements

    def _extract_target_tables(self, parsed):
        for create in parsed.find_all(exp.Create):
            if create.this and isinstance(create.this, exp.Table):
                self._add_table(self.target_tables, create.this)
        for insert in parsed.find_all(exp.Insert):
            if insert.this and isinstance(insert.this, exp.Table):
                self._add_table(self.target_tables, insert.this)

    def _add_table(self, table_set, table_node):
        if table_node.name:
            full_name = table_node.db + '.' + table_node.name if table_node.db else table_node.name
            table_set.add(full_name)

    def _extract_source_tables(self, parsed):
        try:
            for scope in traverse_scope(parsed):
                for table in scope.tables:
                    if table.name:
                        self.source_tables.add(table.db + '.' + table.name if table.db else table.name)
        except Exception:
            pass
        for table in parsed.find_all(exp.Table):
            if table.name:
                self.source_tables.add(table.db + '.' + table.name if table.db else table.name)

    def _collect_alias_map(self, parsed):
        self.alias_map = {}
        for t in parsed.find_all(exp.Table):
            if t.name:
                full_name = t.db + '.' + t.name if t.db else t.name
                if hasattr(t, 'alias') and t.alias:
                    key = t.alias.lower()
                    if key not in self.alias_map:
                        self.alias_map[key] = full_name
                key = t.name.lower()
                if key not in self.alias_map:
                    self.alias_map[key] = full_name

    def _collect_table_aliases_for_select(self, select, table_aliases=None):
        if table_aliases is None:
            table_aliases = {}
        from_expr = select.args.get('from_')
        if from_expr:
            if isinstance(from_expr, exp.From):
                this = from_expr.args.get('this')
                if this:
                    self._collect_table_aliases_from_expr(this, table_aliases)
                for e in from_expr.args.get('expressions', []):
                    self._collect_table_aliases_from_expr(e, table_aliases)
        joins = select.args.get('joins', [])
        for join in joins:
            if isinstance(join, exp.Join):
                this = join.args.get('this')
                if this:
                    self._collect_table_aliases_from_expr(this, table_aliases)
        return table_aliases

    def _collect_table_aliases_from_expr(self, expr, table_aliases):
        if isinstance(expr, exp.Table):
            if expr.alias:
                key = expr.alias.lower()
                full_name = expr.db + '.' + expr.name if expr.db else expr.name
                table_aliases[key] = full_name
            if expr.name:
                full_name = expr.db + '.' + expr.name if expr.db else expr.name
                table_aliases[expr.name.lower()] = full_name
        elif isinstance(expr, exp.Subquery):
            select = expr.args.get('this')
            if isinstance(select, exp.Select):
                self._collect_table_aliases_for_select(select, table_aliases)

    def _build_column_alias_map(self, qualified):
        self.column_alias_map = {}
        for subquery in qualified.find_all(exp.Subquery):
            if not subquery.alias:
                continue
            sq_alias = subquery.alias.lower()
            inner_select = subquery.args.get('this')
            if not isinstance(inner_select, exp.Select):
                continue
            scoped_table_aliases = self._collect_table_aliases_for_select(inner_select)
            alias_columns = {}
            for sel_expr in inner_select.expressions:
                if isinstance(sel_expr, exp.Alias):
                    col_alias = sel_expr.alias_or_name.lower()
                    source_cols = self._find_source_columns(sel_expr.this, scoped_table_aliases)
                    raw_expr = self._clean_expression(sel_expr.this.sql())
                    alias_columns[col_alias] = {'source_columns': source_cols, 'expression': raw_expr}
            if alias_columns:
                if sq_alias in self.column_alias_map:
                    existing = self.column_alias_map[sq_alias]
                    for col_key, col_val in alias_columns.items():
                        if col_key not in existing:
                            existing[col_key] = col_val
                else:
                    alias_columns['__table_aliases__'] = scoped_table_aliases
                    self.column_alias_map[sq_alias] = alias_columns

    def _get_from_list(self, select):
        from_expr = select.args.get('from_')
        if not from_expr:
            return []
        if isinstance(from_expr, exp.From):
            result = []
            this = from_expr.args.get('this')
            if this:
                result.append(this)
            for e in from_expr.args.get('expressions', []):
                result.append(e)
            return result
        return [from_expr]

    def _collect_select_aliases(self, select, alias_dict):
        from_expr = select.args.get('from_')
        if from_expr:
            if isinstance(from_expr, exp.From):
                this = from_expr.args.get('this')
                if this:
                    self._collect_from_aliases(this, alias_dict)
                for e in from_expr.args.get('expressions', []):
                    self._collect_from_aliases(e, alias_dict)
            else:
                self._collect_from_aliases(from_expr, alias_dict)
        joins = select.args.get('joins', [])
        for join in joins:
            if isinstance(join, exp.Join):
                this = join.args.get('this')
                if this:
                    self._collect_from_aliases(this, alias_dict)

    def _collect_from_aliases(self, from_expr, alias_dict):
        if isinstance(from_expr, exp.Subquery):
            if from_expr.alias:
                key = from_expr.alias.lower()
                if key not in alias_dict and key not in self.alias_map:
                    inner_tables = self._find_real_tables_in_subquery(from_expr)
                    if inner_tables:
                        alias_dict[key] = inner_tables[0]
        elif isinstance(from_expr, exp.Join):
            for key_name in ('this', 'expression'):
                if from_expr.args.get(key_name):
                    self._collect_from_aliases(from_expr.args[key_name], alias_dict)
        elif isinstance(from_expr, exp.Select):
            for inner_from in self._get_from_list(from_expr):
                self._collect_from_aliases(inner_from, alias_dict)
        else:
            for sq in from_expr.find_all(exp.Subquery):
                if sq.alias:
                    key = sq.alias.lower()
                    if key not in alias_dict and key not in self.alias_map:
                        inner_tables = self._find_real_tables_in_subquery(sq)
                        if inner_tables:
                            alias_dict[key] = inner_tables[0]

    def _find_real_tables_in_subquery(self, subquery):
        real_tables = []
        for t in subquery.find_all(exp.Table):
            if t.name:
                real_tables.append(t.db + '.' + t.name if t.db else t.name)
        if not real_tables:
            for sq in subquery.find_all(exp.Subquery):
                if sq is not subquery:
                    real_tables.extend(self._find_real_tables_in_subquery(sq))
        return real_tables

    def _parse_with_fallback(self, sql_text):
        table_pattern = r'(?:FROM|JOIN)\s+(`?[a-zA-Z_][a-zA-Z0-9_]*`?\.?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        for t in re.findall(table_pattern, sql_text, re.IGNORECASE):
            t = t.strip('`')
            if '.' in t:
                parts = t.split('.')
                if len(parts) >= 2 and parts[1]:
                    self.source_tables.add(t)
            elif t:
                self.source_tables.add(t)

        insert_pattern = r'INSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?(`?[a-zA-Z_][a-zA-Z0-9_]*`?\.?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        for t in re.findall(insert_pattern, sql_text, re.IGNORECASE):
            t = t.strip('`')
            if '.' in t:
                parts = t.split('.')
                if len(parts) >= 2 and parts[1]:
                    self.target_tables.add(t)
            elif t:
                self.target_tables.add(t)


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"error": "缺少文件路径参数"}), file=sys.stderr)
        sys.exit(1)

    file_path = sys.argv[1]
    dialect = sys.argv[2] if len(sys.argv) > 2 else 'hive'
    db_path = sys.argv[3] if len(sys.argv) > 3 else None

    parser = SQLBloodlineParser(dialect=dialect, db_path=db_path)
    result = parser.parse_sql_from_file(file_path, store_to_db=bool(db_path))

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()