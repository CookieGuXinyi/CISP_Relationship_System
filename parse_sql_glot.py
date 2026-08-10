#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import json
import sys
import re
from sqlglot import parse_one, exp
from sqlglot.optimizer.scope import traverse_scope
from sqlglot.errors import ParseError

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

class SQLBloodlineParser:
    def __init__(self, dialect='hive'):
        self.dialect = dialect
        self.source_tables = set()
        self.target_tables = set()
        self.column_lineage = []
        self.case_mappings = []
        self.alias_map = {}  # 别名 -> 实际表名
        self.column_alias_map = {}  # 子查询别名 -> {列别名 -> {source_columns, expression}}
        
    def parse_sql_from_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        return self.parse_sql_text(sql_text)

    def parse_sql_text(self, sql_text):
        try:
            sql_text = self._preprocess_sql(sql_text)
            statements = self._split_statements(sql_text)
            
            for stmt_sql in statements:
                if not stmt_sql.strip():
                    continue

                # 跳过无FROM的独立SELECT语句（如select 1, 2, 3）
                stripped = stmt_sql.strip().upper()
                if stripped.startswith('SELECT') and 'FROM' not in stripped:
                    print(f"⏭️  跳过无FROM的SELECT语句", file=sys.stderr)
                    continue

                # 去除注释（SQLGlot tokenizer可能无法处理中文注释）
                stmt_clean = self._strip_comments(stmt_sql)

                if not stmt_clean.strip():
                    continue

                try:
                    parsed = parse_one(stmt_clean, dialect=self.dialect)
                    self._parse_statement(parsed)
                except Exception as e:
                    print(f"⚠️ 解析单条SQL失败: {e}", file=sys.stderr)
                    self._parse_with_fallback(stmt_clean)
            
            # 过滤source_tables：排除target_tables中的表，以及在column_lineage中仅作为target的表
            filtered_sources = set()
            target_table_set = set(self.target_tables)
            for t in self.source_tables:
                if t not in target_table_set:
                    filtered_sources.add(t)
            
            # 通过column_lineage补充缺失的源表（从lineage的source_table字段）
            for cl in self.column_lineage:
                if cl.get('source_table') and cl['source_table'] not in target_table_set:
                    filtered_sources.add(cl['source_table'])
            
            # 去重：同一 source_table + source_column + target_column + source_role 只保留一条
            seen = set()
            deduped = []
            for cl in self.column_lineage:
                key = (
                    cl.get('source_table'),
                    cl.get('source_column'),
                    cl.get('target_table'),
                    cl.get('target_column'),
                    cl.get('source_role', 'direct')
                )
                if key not in seen:
                    seen.add(key)
                    deduped.append(cl)
            self.column_lineage = deduped

            return {
                "source_tables": sorted(list(filtered_sources)),
                "target_tables": list(self.target_tables),
                "column_lineage": self.column_lineage,
                "case_mappings": self.case_mappings
            }
        except Exception as e:
            return {"error": str(e)}
    
    def _strip_comments(self, sql):
        """去除SQL中的行注释和块注释，跳过字符串内的内容"""
        result = []
        i = 0
        in_string = False
        string_char = None

        while i < len(sql):
            char = sql[i]

            # 字符串状态处理
            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
                elif char == string_char:
                    in_string = False
                result.append(char)
                i += 1
                continue

            if not in_string:
                # 行注释 --
                if char == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
                    while i < len(sql) and sql[i] != '\n':
                        i += 1
                    continue

                # 块注释 /* */
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
        """预处理：去掉反引号、替换参数"""
        # 去掉表名中的反引号
        sql = re.sub(r'`([^`]+)`', r'\1', sql)

        # 去掉 ''' 三引号分隔符（部分SQL脚本用作task分隔）
        sql = sql.replace("'''", "")

        # 替换 ${...} 格式的日期变量（不消费周围引号，避免破坏字符串）
        sql = re.sub(r"\$\{bdp\.system\.bizdate\}", "20240105", sql)
        sql = re.sub(r"\$\{last_month_last_trading_day\}", "2025-12-31", sql)
        sql = re.sub(r"\$\{last_month_first_day\}", "2025-12-01", sql)
        sql = re.sub(r"\$\{last_month\}", "202512", sql)

        # 替换 {...} 格式的日期变量（不带$前缀），处理引号包裹的情况
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
            # '{var}' → 'val'（引号包裹）
            sql = sql.replace("'{" + var + "}'", "'" + val + "'")
            # {var} → val（无引号，如在算术表达式中）
            sql = sql.replace("{" + var + "}", val)
        # 兜底：替换剩余的 {xxx} 格式变量
        # 先处理 '{xxx}'（引号包裹），避免变成 ''val'' 双引号
        sql = re.sub(r"'\{[a-zA-Z_][a-zA-Z0-9_]*\}'", "'20240101'", sql)
        sql = re.sub(r'\{[a-zA-Z_][a-zA-Z0-9_]*\}', "20240101", sql)

        # 处理 get_last_n_txdate
        sql = re.sub(
            r'get_last_n_txdate\(\s*([^)]+)\s*,\s*-1\s*\)',
            r"\1",
            sql
        )

        # 将 Hive 的 nvl 函数替换为标准 COALESCE（SQLGlot 默认不识别 nvl）
        # 使用正则确保不误伤列名中包含 nvl 的情况（如 nvl_xxx）
        sql = re.sub(r'\bnvl\s*\(', 'COALESCE(', sql, flags=re.IGNORECASE)

        return sql
    
    def _split_statements(self, sql):
        """将SQL脚本分割为单独的语句，支持分号和关键字分割"""
        # 先按分号分割（跳过注释和字符串中的分号）
        statements = []
        current = []
        in_string = False
        string_char = None
        i = 0

        while i < len(sql):
            char = sql[i]

            # 检测行注释 --
            if not in_string and char == '-' and i + 1 < len(sql) and sql[i + 1] == '-':
                # 跳到行尾
                while i < len(sql) and sql[i] != '\n':
                    current.append(sql[i])
                    i += 1
                continue

            # 检测块注释 /* */
            if not in_string and char == '/' and i + 1 < len(sql) and sql[i + 1] == '*':
                current.append(sql[i])
                current.append(sql[i + 1])
                i += 2
                while i < len(sql):
                    if sql[i] == '*' and i + 1 < len(sql) and sql[i + 1] == '/':
                        current.append(sql[i])
                        current.append(sql[i + 1])
                        i += 2
                        break
                    current.append(sql[i])
                    i += 1
                continue

            if char in ("'", '"') and (i == 0 or sql[i-1] != '\\'):
                if not in_string:
                    in_string = True
                    string_char = char
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
        
        # 对没有分号的语句，按关键字进一步分割
        final_statements = []
        for stmt in statements:
            # 检查是否包含多个INSERT/CREATE/DROP语句
            sub_stmts = self._split_by_keywords(stmt)
            final_statements.extend(sub_stmts)

        # 过滤掉纯注释语句（去掉注释后无实际SQL内容）
        result = []
        for stmt in final_statements:
            # 去掉所有行注释和块注释后的内容
            cleaned = re.sub(r'--[^\n]*', '', stmt)
            cleaned = re.sub(r'/\*.*?\*/', '', cleaned, flags=re.DOTALL)
            cleaned = cleaned.strip()
            if cleaned:
                result.append(stmt)

        return result
    
    def _split_by_keywords(self, sql):
        """按INSERT/CREATE/DROP等关键字分割无分号的语句"""
        lines = sql.split('\n')
        segments = []
        current_lines = []
        # 只在行首的INSERT/CREATE/DROP关键字处分割（不在子查询的SELECT处分割）
        sql_keywords = re.compile(
            r'^\s*(INSERT\s+(?:OVERWRITE|INTO)|CREATE\s+TABLE|DROP\s+TABLE)\b',
            re.IGNORECASE
        )
        
        for line in lines:
            stripped = line.strip()
            
            # 检查是否是新的SQL语句开始
            if sql_keywords.match(line) and current_lines:
                stmt = '\n'.join(current_lines).strip()
                if stmt:
                    segments.append(stmt)
                current_lines = [line]
            else:
                current_lines.append(line)
        
        if current_lines:
            stmt = '\n'.join(current_lines).strip()
            if stmt:
                segments.append(stmt)
        
        return segments if segments else [sql]
    
    def _parse_statement(self, parsed):
        self._collect_alias_map(parsed)
        self._extract_target_tables(parsed)
        self._extract_source_tables(parsed)
        self._extract_column_lineage(parsed)
        self._extract_case_mappings(parsed)
    
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
                        full_name = table.db + '.' + table.name if table.db else table.name
                        resolved = self._resolve_table_name(full_name)
                        self.source_tables.add(resolved)
        except Exception as e:
            print(f"⚠️ scope遍历失败: {e}", file=sys.stderr)
        for table in parsed.find_all(exp.Table):
            if table.name:
                full_name = table.db + '.' + table.name if table.db else table.name
                resolved = self._resolve_table_name(full_name)
                self.source_tables.add(resolved)
    
    def _collect_alias_map(self, parsed):
        """收集别名到实际表名的映射，支持嵌套子查询（大小写无关，顶层优先）"""
        self.alias_map = {}
        
        # 第一层：直接表别名（总是优先，用小写key）
        for t in parsed.find_all(exp.Table):
            if t.name:
                full_name = t.db + '.' + t.name if t.db else t.name
                full_name_lower = full_name.lower()
                if hasattr(t, 'alias') and t.alias:
                    key = t.alias.lower()
                    if key not in self.alias_map:
                        self.alias_map[key] = full_name
                # 表名本身也映射
                key = t.name.lower()
                if key not in self.alias_map:
                    self.alias_map[key] = full_name
        
        # 第二层：子查询别名 -> 内部真实表名（只处理顶层子查询，避免嵌套冲突）
        # 通过INSERT和CREATE的FROM子句及JOINS找到顶层子查询
        top_subquery_aliases = {}
        for insert in parsed.find_all(exp.Insert):
            select = insert.args.get('expression')
            if isinstance(select, exp.Select):
                self._collect_select_aliases(select, top_subquery_aliases)
        
        for create in parsed.find_all(exp.Create):
            if create.this and isinstance(create.this, exp.Table):
                select = create.args.get('expression')
                if isinstance(select, exp.Select):
                    self._collect_select_aliases(select, top_subquery_aliases)
        
        # 如果没有找到上述结构，回退到处理所有子查询（去重，顶层优先）
        if not top_subquery_aliases:
            for subq in parsed.find_all(exp.Subquery):
                if subq.alias:
                    key = subq.alias.lower()
                    if key not in self.alias_map:
                        inner_tables = self._find_real_tables_in_subquery(subq)
                        if inner_tables:
                            top_subquery_aliases[key] = inner_tables[0]
        
        # 合并子查询别名映射（不覆盖已有）
        for alias_key, real_table in top_subquery_aliases.items():
            if alias_key not in self.alias_map:
                self.alias_map[alias_key] = real_table
    
    def _get_from_list(self, select):
        """获取SELECT的FROM表达式列表（兼容from_键名，支持From节点的this和expressions）"""
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
        """从SELECT语句中收集所有子查询别名（包括FROM和JOINS）"""
        # 处理FROM子句（键名为from_，因为from是Python保留字）
        from_expr = select.args.get('from_')
        if from_expr:
            if isinstance(from_expr, exp.From):
                # From节点的子查询可能在this或expressions中
                this = from_expr.args.get('this')
                if this:
                    self._collect_from_aliases(this, alias_dict)
                for expr in from_expr.args.get('expressions', []):
                    self._collect_from_aliases(expr, alias_dict)
            else:
                self._collect_from_aliases(from_expr, alias_dict)
        
        # 处理JOINS列表
        joins = select.args.get('joins', [])
        if joins:
            for join in joins:
                if isinstance(join, exp.Join):
                    # JOIN的this是子查询或表
                    this = join.args.get('this')
                    if this:
                        self._collect_from_aliases(this, alias_dict)
    
    def _collect_from_aliases(self, from_expr, alias_dict):
        """递归收集FROM子句中的子查询别名（顶层优先，避免嵌套冲突）"""
        # 第一优先级：直接处理当前层的Subquery
        if isinstance(from_expr, exp.Subquery):
            if from_expr.alias:
                key = from_expr.alias.lower()
                if key not in alias_dict and key not in self.alias_map:
                    inner_tables = self._find_real_tables_in_subquery(from_expr)
                    if inner_tables:
                        alias_dict[key] = inner_tables[0]
            # 递归深入子查询内部（处理其子查询）
            select = from_expr.args.get('this')
            if isinstance(select, exp.Select):
                for inner_from in self._get_from_list(select):
                    self._collect_from_aliases(inner_from, alias_dict)
        elif isinstance(from_expr, exp.Join):
            # JOIN的两侧可能有子查询
            for key_name in ('this', 'expression'):
                if from_expr.args.get(key_name):
                    self._collect_from_aliases(from_expr.args[key_name], alias_dict)
        elif isinstance(from_expr, exp.Select):
            # 直接是SELECT（无包裹的Subquery）
            for inner_from in self._get_from_list(from_expr):
                self._collect_from_aliases(inner_from, alias_dict)
        else:
            # 其他情况：用find_all找子查询，但只处理当前层的
            # 不递归处理嵌套，以避免内层覆盖外层
            for sq in from_expr.find_all(exp.Subquery):
                if sq.alias:
                    key = sq.alias.lower()
                    if key not in alias_dict and key not in self.alias_map:
                        inner_tables = self._find_real_tables_in_subquery(sq)
                        if inner_tables:
                            alias_dict[key] = inner_tables[0]
    
    def _find_real_tables_in_subquery(self, subquery):
        """在子查询中递归查找真实表名（跳过子查询别名，找到物理表）"""
        real_tables = []
        # 直接表引用
        for t in subquery.find_all(exp.Table):
            if t.name:
                full_name = t.db + '.' + t.name if t.db else t.name
                real_tables.append(full_name)
        # 如果没找到直接表，递归进入嵌套子查询
        if not real_tables:
            for sq in subquery.find_all(exp.Subquery):
                if sq is not subquery:
                    nested = self._find_real_tables_in_subquery(sq)
                    real_tables.extend(nested)
        return real_tables

    def _collect_table_aliases_from_expr(self, expr, table_aliases):
        """从表达式中收集表别名→真实表名的映射"""
        if isinstance(expr, exp.Table):
            if expr.alias:
                key = expr.alias.lower()
                full_name = expr.db + '.' + expr.name if expr.db else expr.name
                table_aliases[key] = full_name
            if expr.name:
                full_name = expr.db + '.' + expr.name if expr.db else expr.name
                table_aliases[expr.name.lower()] = full_name
        elif isinstance(expr, exp.From):
            this = expr.args.get('this')
            if this:
                self._collect_table_aliases_from_expr(this, table_aliases)
            for e in expr.args.get('expressions', []):
                self._collect_table_aliases_from_expr(e, table_aliases)
        elif isinstance(expr, exp.Join):
            this = expr.args.get('this')
            if this:
                self._collect_table_aliases_from_expr(this, table_aliases)
        elif isinstance(expr, exp.Subquery):
            select = expr.args.get('this')
            if isinstance(select, exp.Select):
                self._collect_table_aliases_for_select(select, table_aliases)

    def _collect_table_aliases_for_select(self, select, table_aliases=None):
        """从SELECT的FROM/JOINS中收集表别名→真实表名的映射（scoped）"""
        if table_aliases is None:
            table_aliases = {}
        from_expr = select.args.get('from_')
        if from_expr:
            self._collect_table_aliases_from_expr(from_expr, table_aliases)
        joins = select.args.get('joins', [])
        for join in joins:
            if isinstance(join, exp.Join):
                this = join.args.get('this')
                if this:
                    self._collect_table_aliases_from_expr(this, table_aliases)
        return table_aliases

    def _resolve_source_cols_scoped(self, source_cols, scoped_table_aliases):
        """用 scoped table_aliases 解析 source columns 中的表名"""
        if not scoped_table_aliases or not source_cols:
            return source_cols
        resolved = []
        for col in source_cols:
            col_copy = dict(col)
            table = col_copy.get('table')
            if table:
                table_lower = table.lower()
                if table_lower in scoped_table_aliases:
                    col_copy['table'] = scoped_table_aliases[table_lower]
                elif table_lower in self.column_alias_map:
                    real_table = self._resolve_table_name(table_lower)
                    if real_table:
                        col_copy['table'] = real_table
            resolved.append(col_copy)
        return resolved

    def _replace_table_aliases_in_expression(self, expr_str, scope_alias):
        """替换表达式中的表别名为真实表名（使用 scoped aliases）"""
        if not expr_str or not scope_alias:
            return expr_str
        if scope_alias not in self.column_alias_map:
            return expr_str
        entry = self.column_alias_map[scope_alias]
        table_aliases = entry.get('__table_aliases__', {})
        if not table_aliases:
            return expr_str
        
        def replace_alias(match):
            alias = match.group(1)
            col = match.group(2)
            alias_lower = alias.lower()
            if alias_lower in table_aliases:
                return f"{table_aliases[alias_lower]}.{col}"
            return match.group(0)
        
        return re.sub(r'"?(\w+)"?\."?(\w+)"?', replace_alias, expr_str)

    def _build_column_alias_map(self, qualified):
        """构建子查询列别名 → 真实来源列的映射表，支持多层嵌套"""
        self.column_alias_map = {}
        
        # 先处理内层子查询（深度优先），确保外层引用时内层已解析
        subqueries = list(qualified.find_all(exp.Subquery))
        # 按嵌套深度排序（内层先处理）
        subqueries.sort(key=lambda sq: self._get_subquery_depth(sq), reverse=True)
        
        for subquery in subqueries:
            if not subquery.alias:
                continue
            sq_alias = subquery.alias.lower()
            
            inner_select = subquery.args.get('this')
            if not isinstance(inner_select, exp.Select):
                continue
            
            # 收集当前子查询的 scoped table_aliases
            scoped_table_aliases = self._collect_table_aliases_for_select(inner_select)
            
            alias_columns = {}
            for sel_expr in inner_select.expressions:
                if isinstance(sel_expr, exp.Alias):
                    col_alias = sel_expr.alias_or_name.lower()
                    source_cols = self._find_source_columns(sel_expr.this, scoped_table_aliases)
                    raw_expr = self._clean_expression(sel_expr.this.sql())
                    alias_columns[col_alias] = {
                        'source_columns': source_cols,
                        'expression': raw_expr
                    }
                elif isinstance(sel_expr, exp.Column):
                    col_name = sel_expr.name.lower()
                    source_cols = self._find_source_columns(sel_expr, scoped_table_aliases)
                    
                    # 如果无表限定符，尝试通过FROM子查询别名穿透
                    if source_cols and source_cols[0].get('table') is None:
                        from_aliases = self._get_from_aliases_for_select(inner_select)
                        for from_alias in from_aliases:
                            if from_alias in self.column_alias_map:
                                if col_name in self.column_alias_map[from_alias]:
                                    entry = self.column_alias_map[from_alias][col_name]
                                    source_cols = list(entry['source_columns'])
                                    raw_expr = self._clean_expression(f"{from_alias}.{col_name}")
                                    alias_columns[col_name] = {
                                        'source_columns': source_cols,
                                        'expression': raw_expr
                                    }
                                    break
                        else:
                            raw_expr = self._clean_expression(sel_expr.sql())
                            alias_columns[col_name] = {
                                'source_columns': source_cols,
                                'expression': raw_expr
                            }
                    else:
                        raw_expr = self._clean_expression(sel_expr.sql())
                        alias_columns[col_name] = {
                            'source_columns': source_cols,
                            'expression': raw_expr
                        }
            
            if alias_columns:
                # 同名子查询别名（如外层 y 与内层 y）合并而非整体覆盖，
                # 否则外层子查询的列（如 zgyjl/zdyjl）会被内层同名子查询覆盖丢失。
                if sq_alias in self.column_alias_map:
                    existing = self.column_alias_map[sq_alias]
                    # 合并 scoped table_aliases（已有 key 优先，避免外层别名被内层同名覆盖）
                    merged_ta = dict(scoped_table_aliases)
                    merged_ta.update(existing.get('__table_aliases__', {}))
                    existing['__table_aliases__'] = merged_ta
                    # 仅补充不存在的列，不覆盖已有列（保留先解析的来源）
                    for col_key, col_val in alias_columns.items():
                        if col_key not in existing:
                            existing[col_key] = col_val
                else:
                    alias_columns['__table_aliases__'] = scoped_table_aliases
                    self.column_alias_map[sq_alias] = alias_columns
    
    def _get_from_aliases_for_select(self, select):
        """获取SELECT的FROM子句中所有子查询别名（包括FROM和JOINS）"""
        aliases = []
        # 处理FROM子句
        from_expr = select.args.get('from_')
        if from_expr:
            if isinstance(from_expr, exp.From):
                # From节点的子查询可能存储在this或expressions中
                this = from_expr.args.get('this')
                if isinstance(this, exp.Subquery) and this.alias:
                    aliases.append(this.alias.lower())
                elif isinstance(this, exp.Table):
                    pass  # 物理表无需处理
                for expr in from_expr.args.get('expressions', []):
                    if isinstance(expr, exp.Subquery) and expr.alias:
                        aliases.append(expr.alias.lower())
                    elif isinstance(expr, exp.Join):
                        join_this = expr.args.get('this')
                        if isinstance(join_this, exp.Subquery) and join_this.alias:
                            aliases.append(join_this.alias.lower())
            elif isinstance(from_expr, exp.Subquery) and from_expr.alias:
                aliases.append(from_expr.alias.lower())
            elif isinstance(from_expr, exp.Join):
                this = from_expr.args.get('this')
                if isinstance(this, exp.Subquery) and this.alias:
                    aliases.append(this.alias.lower())
        
        # 处理JOINS
        joins = select.args.get('joins', [])
        for join in joins:
            if isinstance(join, exp.Join):
                this = join.args.get('this')
                if isinstance(this, exp.Subquery) and this.alias:
                    aliases.append(this.alias.lower())
        
        return aliases
    
    def _get_subquery_depth(self, subquery):
        """计算子查询的嵌套深度"""
        depth = 0
        current = subquery
        while True:
            parent = current.parent if hasattr(current, 'parent') else None
            if parent is None:
                break
            if isinstance(parent, exp.Subquery):
                depth += 1
                current = parent
            else:
                break
        return depth
    
    def _resolve_column_lineage(self, table, column, depth=0):
        """递归解析列引用，穿透子查询边界到物理列"""
        if depth > 5:
            resolved_table = self._resolve_table_name(table) if table else table
            return [{"table": resolved_table, "column": column}]
        
        table_lower = table.lower() if table else None
        
        if table_lower and table_lower in self.column_alias_map:
            sq_cols = self.column_alias_map[table_lower]
            col_lower = column.lower() if column else None
            
            if col_lower and col_lower in sq_cols:
                entry = sq_cols[col_lower]
                resolved = []
                for src in entry['source_columns']:
                    resolved.extend(
                        self._resolve_column_lineage(
                            src.get('table'),
                            src.get('column'),
                            depth + 1
                        )
                    )
                return resolved
        
        resolved_table = self._resolve_table_name(table) if table else table
        return [{"table": resolved_table, "column": column}]
    
    def _substitute_expression(self, expr_str, depth=0):
        """替换表达式中的子查询列引用为原始计算表达式（递归多层），并替换表别名"""
        if depth > 5 or not expr_str:
            return expr_str
        
        def replace_match(match):
            alias = match.group(1).lower()
            col = match.group(2).lower()
            # 优先处理子查询别名
            if alias in self.column_alias_map:
                sq_cols = self.column_alias_map[alias]
                if col in sq_cols:
                    sub_expr = sq_cols[col]['expression']
                    # 先替换子表达式中的表别名为真实表名（scoped，避免被全局alias_map误替换）
                    sub_expr = self._replace_table_aliases_in_expression(sub_expr, alias)
                    # 再递归替换子表达式中的子查询引用
                    sub_expr = self._substitute_expression(sub_expr, depth + 1)
                    return sub_expr
            # 普通表别名替换（使用 alias_map）
            if alias in self.alias_map:
                real_table = self.alias_map[alias]
                return f"{real_table}.{col}"
            return match.group(0)
        
        return re.sub(r'"?(\w+)"?\."?(\w+)"?', replace_match, expr_str)

    def _simplify_coalesce(self, expr_str):
        """扁平化嵌套 COALESCE 并去重相同参数，消除冗余。
        例如 COALESCE(COALESCE(a, d), d) -> COALESCE(a, d)；
        COALESCE(a, COALESCE(b, d), d) -> COALESCE(a, b, d)。"""
        if not expr_str or 'COALESCE' not in expr_str.upper():
            return expr_str
        try:
            tree = parse_one(expr_str, dialect=self.dialect)
        except Exception:
            return expr_str

        changed = True
        while changed:
            changed = False
            for co in list(tree.find_all(exp.Coalesce)):
                args = []
                if co.this is not None:
                    args.append(co.this)
                args.extend(co.args.get('expressions', []))
                if not args:
                    continue
                # 扁平化：把作为参数的内层 Coalesce 的参数提升上来
                flat = []
                did_flatten = False
                for a in args:
                    if isinstance(a, exp.Coalesce):
                        if a.this is not None:
                            flat.append(a.this)
                        flat.extend(a.args.get('expressions', []))
                        did_flatten = True
                    else:
                        flat.append(a)
                # 去重（按 SQL 文本，保留顺序）
                seen = set()
                deduped = []
                for a in flat:
                    key = a.sql()
                    if key not in seen:
                        seen.add(key)
                        deduped.append(a)
                if did_flatten or len(deduped) != len(args):
                    co.set('this', deduped[0])
                    co.set('expressions', deduped[1:])
                    changed = True
        return tree.sql()

    def _extract_column_lineage(self, parsed):
        self._collect_alias_map(parsed)

        # 尝试 qualify（限定列名到表），失败则用未限定的 AST
        tree = parsed
        try:
            from sqlglot.optimizer.qualify import qualify
            tree = qualify(parsed, dialect=self.dialect)
        except Exception as e:
            print(f"⚠️ qualify失败，使用未限定AST: {e}", file=sys.stderr)

        self._build_column_alias_map(tree)

        for insert in tree.find_all(exp.Insert):
            select = insert.args.get('expression')
            target_table = None
            if insert.this and isinstance(insert.this, exp.Table):
                target_table = insert.this.db + '.' + insert.this.name if insert.this.db else insert.this.name
            self._extract_from_select_or_union(select, target_table)

        for create in tree.find_all(exp.Create):
            if create.this and isinstance(create.this, exp.Table):
                target_table = create.this.db + '.' + create.this.name if create.this.db else create.this.name
                select = create.args.get('expression')
                self._extract_from_select_or_union(select, target_table)

    def _extract_from_select_or_union(self, node, target_table):
        """处理 Select 或 Union（含嵌套 UNION ALL），对每个 Select 分支提取字段血缘"""
        if node is None:
            return
        if isinstance(node, exp.Union):
            self._extract_from_select_or_union(node.left, target_table)
            self._extract_from_select_or_union(node.right, target_table)
        elif isinstance(node, exp.Subquery):
            self._extract_from_select_or_union(node.this, target_table)
        elif isinstance(node, exp.Select):
            self._extract_columns_from_select(node, target_table)
    
    def _extract_columns_from_select(self, select, target_table):
        for expr in select.expressions:
            if isinstance(expr, exp.Alias):
                alias = expr.alias_or_name
                source_cols = self._find_source_columns(expr.this, role='direct')
                
                raw_expr_sql = self._clean_expression(expr.this.sql())
                full_expr_sql = self._substitute_expression(raw_expr_sql)
                full_expr_sql = self._simplify_coalesce(full_expr_sql)
                simplified = self._get_simplified_expression(expr.this)
                expr_type = self._classify_expression_type(full_expr_sql, expr.this)
                
                for col in source_cols:
                    self.column_lineage.append({
                        "source_table": col.get('table'),
                        "source_column": col.get('column'),
                        "target_table": target_table,
                        "target_column": alias,
                        "expression": simplified,
                        "full_expression": full_expr_sql,
                        "expression_type": expr_type,
                        "source_role": col.get('role', 'direct')
                    })
            elif isinstance(expr, exp.Column):
                col_name = expr.name
                source_cols = self._find_source_columns(expr)
                
                raw_expr_sql = self._clean_expression(expr.sql())
                full_expr_sql = self._substitute_expression(raw_expr_sql)
                full_expr_sql = self._simplify_coalesce(full_expr_sql)
                expr_type = self._classify_expression_type(full_expr_sql, expr)
                
                for col in source_cols:
                    self.column_lineage.append({
                        "source_table": col.get('table'),
                        "source_column": col.get('column'),
                        "target_table": target_table,
                        "target_column": col_name,
                        "expression": col_name,
                        "full_expression": full_expr_sql,
                        "expression_type": expr_type,
                        "source_role": col.get('role', 'direct')
                    })
    
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
            if '(' in full_expr_sql and 'SUM' not in expr_upper and 'COUNT' not in expr_upper and 'AVG' not in expr_upper and 'MAX' not in expr_upper and 'MIN' not in expr_upper:
                if not isinstance(original_expr, exp.Column):
                    return 'computed'
        
        if isinstance(original_expr, exp.Column):
            return 'direct'
        
        return 'other'

    def _get_simplified_expression(self, expr):
        if isinstance(expr, exp.Column):
            return expr.name
        elif isinstance(expr, exp.AggFunc):
            func_name = expr.__class__.__name__.lower()
            args = []
            for arg in expr.args.get('expressions', []):
                if isinstance(arg, exp.Column):
                    args.append(arg.name)
                elif isinstance(arg, exp.Distinct):
                    if arg.this and isinstance(arg.this, exp.Column):
                        args.append(f"DISTINCT {arg.this.name}")
                else:
                    args.append(self._clean_expression(arg.sql())[:20])
            if not args and hasattr(expr, 'this') and expr.this:
                if isinstance(expr.this, exp.Column):
                    args.append(expr.this.name)
                elif isinstance(expr.this, exp.Distinct):
                    for de in expr.this.args.get('expressions', []):
                        if isinstance(de, exp.Column):
                            args.append(f"DISTINCT {de.name}")
                        else:
                            args.append(f"DISTINCT {self._clean_expression(de.sql())[:20]}")
                else:
                    args.append(self._clean_expression(expr.this.sql())[:20])
            return f"{func_name.upper()}({', '.join(args)})"
        elif isinstance(expr, exp.Case):
            return "CASE_WHEN(...)"
        elif isinstance(expr, exp.Func):
            func_name = expr.__class__.__name__.lower()
            return f"{func_name}(...)"
        elif isinstance(expr, exp.Binary):
            return self._clean_expression(expr.sql())[:60]
        else:
            return self._clean_expression(expr.sql())[:60]

    def _resolve_table_name(self, name):
        """将别名解析为实际表名（大小写无关，支持多层递归）"""
        if not name:
            return name
        visited = set()
        current = name.lower() if isinstance(name, str) else name
        while current in self.alias_map and current not in visited:
            visited.add(current)
            current = self.alias_map[current]
            if isinstance(current, str):
                current = current.lower()
        # 返回原始大小写的实际表名
        if current in self.alias_map:
            return self.alias_map[current]
        # 尝试直接返回（可能已经是实际表名）
        return name if not isinstance(current, str) else current

    def _clean_expression(self, sql_str):
        """清理表达式中的双引号"""
        return re.sub(r'"([^"]*)"', r'\1', sql_str)

    def _find_source_columns(self, expr, scoped_aliases=None, role='direct'):
        results = []
        if isinstance(expr, exp.Column):
            table_name = None
            
            # 方法1：从args中获取table（限定化后最可靠）
            table_arg = expr.args.get('table')
            if table_arg:
                if isinstance(table_arg, exp.Identifier):
                    table_name = table_arg.name
                elif isinstance(table_arg, str):
                    table_name = table_arg
            
            # 方法2：从this.table获取
            if not table_name and hasattr(expr, 'this') and expr.this:
                if hasattr(expr.this, 'table') and expr.this.table:
                    t = expr.this.table
                    if isinstance(t, exp.Identifier):
                        table_name = t.name
                    elif isinstance(t, str):
                        table_name = t
            
            # 方法3：正则回退
            if not table_name:
                sql_str = expr.sql()
                table_match = re.match(r'"?([^".]+)"?\."?([^"]+)"?', sql_str)
                if table_match:
                    table_name = table_match.group(1)
            
            # 使用别名映射解析实际表名
            if table_name:
                # 先检查是否是子查询列别名，若是则穿透到子查询内部
                table_lower = table_name.lower()
                if table_lower in self.column_alias_map:
                    sq_cols = self.column_alias_map[table_lower]
                    col_lower = expr.name.lower()
                    if col_lower in sq_cols:
                        entry = sq_cols[col_lower]
                        fallback_table = self._resolve_table_name(table_lower)
                        for src in entry['source_columns']:
                            src_table = src.get('table')
                            src_role = src.get('role', role)
                            if not src_table:
                                src_table = fallback_table
                            resolved_list = self._resolve_column_lineage(
                                src_table, src.get('column')
                            )
                            for r in resolved_list:
                                results.append({
                                    "table": r['table'],
                                    "column": r['column'],
                                    "expression": self._clean_expression(expr.sql()),
                                    "role": src_role
                                })
                        return results
                
                # 非子查询别名，先用 scoped aliases 解析，再回退到全局
                resolved_table = None
                if scoped_aliases and table_lower in scoped_aliases:
                    resolved_table = scoped_aliases[table_lower]
                else:
                    resolved_table = self._resolve_table_name(table_name)
                table_name = resolved_table
            
            results.append({
                "table": table_name,
                "column": expr.name,
                "expression": self._clean_expression(expr.sql()),
                "role": role
            })
        elif isinstance(expr, exp.AggFunc):
            agg_role = 'data_source'
            for arg in expr.args.get('expressions', []):
                results.extend(self._find_source_columns(arg, scoped_aliases, agg_role))
            if hasattr(expr, 'this') and expr.this:
                if isinstance(expr.this, exp.Distinct):
                    for distinct_expr in expr.this.args.get('expressions', []):
                        results.extend(self._find_source_columns(distinct_expr, scoped_aliases, agg_role))
                else:
                    results.extend(self._find_source_columns(expr.this, scoped_aliases, agg_role))
        elif isinstance(expr, exp.Case):
            for when in expr.args.get('ifs', []):
                if isinstance(when, exp.If):
                    results.extend(self._find_source_columns(when.args.get('this'), scoped_aliases, 'filter'))
                    results.extend(self._find_source_columns(when.args.get('true'), scoped_aliases, 'data_source'))
            default_val = expr.args.get('default')
            if default_val:
                results.extend(self._find_source_columns(default_val, scoped_aliases, 'data_source'))
        elif isinstance(expr, exp.Between):
            results.extend(self._find_source_columns(expr.this, scoped_aliases, 'filter'))
            results.extend(self._find_source_columns(expr.args.get('low'), scoped_aliases, 'filter'))
            results.extend(self._find_source_columns(expr.args.get('high'), scoped_aliases, 'filter'))
        elif isinstance(expr, exp.In):
            results.extend(self._find_source_columns(expr.this, scoped_aliases, 'filter'))
            for arg in expr.args.get('expressions', []):
                results.extend(self._find_source_columns(arg, scoped_aliases, 'filter'))
        elif isinstance(expr, exp.Contains):
            results.extend(self._find_source_columns(expr.this, scoped_aliases, 'filter'))
            if expr.args.get('expression'):
                results.extend(self._find_source_columns(expr.args.get('expression'), scoped_aliases, 'filter'))
        elif isinstance(expr, exp.Binary):
            bin_role = 'filter' if role == 'filter' else 'data_source'
            results.extend(self._find_source_columns(expr.this, scoped_aliases, bin_role))
            if expr.args.get('expression'):
                results.extend(self._find_source_columns(expr.args.get('expression'), scoped_aliases, bin_role))
        elif isinstance(expr, exp.Func):
            if hasattr(expr, 'this') and expr.this:
                results.extend(self._find_source_columns(expr.this, scoped_aliases, role))
            for arg in expr.args.get('expressions', []):
                results.extend(self._find_source_columns(arg, scoped_aliases, role))
        elif isinstance(expr, exp.Subquery):
            pass
        return results
    
    def _extract_column_lineage_basic(self, parsed):
        """基础模式：用正则提取字段映射"""
        # 从 SELECT 语句中提取 target_column = source_column 的映射
        select_matches = re.findall(
            r'SELECT\s+([^,]+?)\s+AS\s+([^\s,]+)',
            parsed.sql(),
            re.IGNORECASE
        )
        for expr, alias in select_matches:
            # 提取列名
            col_match = re.search(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*$', expr.strip())
            if col_match:
                self.column_lineage.append({
                    "source_table": None,
                    "source_column": col_match.group(1),
                    "target_table": None,
                    "target_column": alias,
                    "expression": expr.strip()
                })
    
    def _extract_case_mappings(self, parsed):
        for case in parsed.find_all(exp.Case):
            for when in case.args.get('ifs', []):
                if isinstance(when, exp.If):
                    condition = when.args.get('this')
                    true_value = when.args.get('true')
                    if condition and true_value:
                        self.case_mappings.append({
                            "condition": self._clean_expression(condition.sql()),
                            "result": self._clean_expression(true_value.sql())
                        })
    
    def _parse_with_fallback(self, sql_text):
        # 匹配表名
        table_pattern = r'(?:FROM|JOIN)\s+(`?[a-zA-Z_][a-zA-Z0-9_]*`?\.?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        tables = re.findall(table_pattern, sql_text, re.IGNORECASE)
        for t in tables:
            t = t.strip('`')
            if '.' in t:
                parts = t.split('.')
                if len(parts) >= 2 and parts[1]:
                    self.source_tables.add(t)
            elif t:
                self.source_tables.add(t)
        
        insert_pattern = r'INSERT\s+(?:OVERWRITE|INTO)\s+(?:TABLE\s+)?(`?[a-zA-Z_][a-zA-Z0-9_]*`?\.?`?[a-zA-Z_][a-zA-Z0-9_]*`?)'
        inserts = re.findall(insert_pattern, sql_text, re.IGNORECASE)
        for t in inserts:
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
    
    parser = SQLBloodlineParser(dialect=dialect)
    result = parser.parse_sql_from_file(file_path)
    print(json.dumps(result, ensure_ascii=False))

if __name__ == "__main__":
    main()