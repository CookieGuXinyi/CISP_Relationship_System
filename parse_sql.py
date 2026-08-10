#!/usr/bin/env python
# -*- coding: utf-8 -*-

import io
import json
import sys
import re

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

class SQLBloodlineParser:
    def __init__(self, dialect='hive'):
        self.dialect = dialect
        self.source_tables = set()
        self.target_tables = set()
        self.column_lineage = []
        self.case_mappings = []
        
    def parse_sql_from_file(self, file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            sql_text = f.read()
        return self.parse_sql_text(sql_text)

    def parse_sql_text(self, sql_text):
        try:
            from sqllineage.runner import LineageRunner
            from sqllineage.exceptions import SQLLineageException
            
            sql_text = self._preprocess_sql(sql_text)
            
            runner = LineageRunner(sql_text, dialect=self.dialect)
            
            self.source_tables = {str(t).replace('<default>.', '') for t in runner.source_tables}
            self.target_tables = {str(t).replace('<default>.', '') for t in runner.target_tables}
            
            column_lineage_list = list(runner.get_column_lineage())
            for item in column_lineage_list:
                if isinstance(item, tuple) and len(item) == 2:
                    src, tgt = item
                else:
                    print(f"⚠️ 跳过非标准格式: {type(item)} = {item}", file=sys.stderr)
                    continue
                
                src_str = str(src)
                tgt_str = str(tgt)
                
                src_match = re.match(r'[^.]+\.([^.]+)\.([^.]+)$', src_str)
                tgt_match = re.match(r'[^.]+\.([^.]+)\.([^.]+)$', tgt_str)
                
                source_table = src_match.group(1) if src_match else None
                source_column = src_match.group(2) if src_match else getattr(src, 'raw_name', None)
                target_table = tgt_match.group(1) if tgt_match else None
                target_column = tgt_match.group(2) if tgt_match else getattr(tgt, 'raw_name', None)
                
                self.column_lineage.append({
                    "source_table": source_table,
                    "source_column": source_column,
                    "target_table": target_table,
                    "target_column": target_column,
                    "expression": f"{source_table}.{source_column}" if source_table and source_column else str(source_column or '')
                })
            
            self._extract_case_mappings(sql_text)
            
            return {
                "source_tables": list(self.source_tables),
                "target_tables": list(self.target_tables),
                "column_lineage": self.column_lineage,
                "case_mappings": self.case_mappings
            }
        except SQLLineageException as e:
            print(f"⚠️ sqllineage解析失败: {e}", file=sys.stderr)
            return self._parse_with_fallback(sql_text)
        except ImportError as e:
            print(f"⚠️ sqllineage库未安装: {e}", file=sys.stderr)
            return {"error": "sqllineage库未安装"}
        except Exception as e:
            print(f"⚠️ 解析异常: {type(e).__name__} - {e}", file=sys.stderr)
            return self._parse_with_fallback(sql_text)
    
    def _preprocess_sql(self, sql):
        sql = re.sub(r'`([^`]+)`', r'\1', sql)
        sql = re.sub(r"'?\$\{bdp\.system\.bizdate\}'?", "'20240105'", sql)
        sql = re.sub(r"'?\$\{last_month_last_trading_day\}'?", "'2025-12-31'", sql)
        sql = re.sub(r"'?\$\{last_month_first_day\}'?", "'2025-12-01'", sql)
        sql = re.sub(r"'?\$\{last_month\}'?", "'202512'", sql)
        sql = re.sub(
            r'get_last_n_txdate\(\s*([^)]+)\s*,\s*-1\s*\)',
            r"\1",
            sql
        )
        return sql
    
    def _extract_case_mappings(self, sql_text):
        case_pattern = r'CASE\s+WHEN\s+([^\n]+?)\s+THEN\s+([^\n,]+?)'
        matches = re.findall(case_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        for condition, result in matches:
            self.case_mappings.append({
                "condition": condition.strip(),
                "result": result.strip()
            })
    
    def _parse_with_fallback(self, sql_text):
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
        
        return {
            "source_tables": list(self.source_tables),
            "target_tables": list(self.target_tables),
            "column_lineage": self.column_lineage,
            "case_mappings": self.case_mappings
        }

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
