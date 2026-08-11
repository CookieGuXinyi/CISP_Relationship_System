const express = require('express');
const cors = require('cors');
const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const { exec } = require('child_process');
const fs = require('fs');
const os = require('os');
const { Pool } = require('pg');

const app = express();
const PORT = 3000;

app.use(cors());
app.use(express.static(path.join(__dirname, 'frontend'), {index: false}));
app.use(express.json({ limit: '10mb' }));

// 添加这个路由，让根路径访问 index_new.html
app.get('/', (req, res) => {
    res.sendFile(path.join(__dirname, 'frontend', 'index_new.html'));
});

// PostgreSQL连接配置
const pgPool = new Pool({
    host: '10.142.22.66',
    port: 7300,
    database: 'dwzq_warehouse',
    user: 'seabox',
    password: 'seabox',
    max: 5,
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 5000,
});

// 连接SQLite
const db = new sqlite3.Database(path.join(__dirname, 'cisp_new.db'));

// 批量处理进度（内存中存储，供前端轮询）
let batchProgress = { status: 'idle', total: 0, processed: 0, succeeded: 0, failed: 0, current_task: '', errors: [] };

// ==================== 核心逻辑：解析SQL并存储血缘 ====================
function parseAndStore(sqlText, jobName, reportCode, dialect) {
    return new Promise((resolve, reject) => {
        const tmpFile = path.join(os.tmpdir(), `sql_${Date.now()}_${Math.random().toString(36).slice(2)}.sql`);
        fs.writeFileSync(tmpFile, sqlText, 'utf-8');

        const pythonScript = path.join(__dirname, 'parse_sql_glot.py');

        exec(
            `python "${pythonScript}" "${tmpFile}" "${dialect || 'hive'}"`,
            {
                maxBuffer: 10 * 1024 * 1024,
                env: {
                    ...process.env,
                    PYTHONIOENCODING: 'utf-8',
                    PYTHONUTF8: '1'
                }
            },
            (error, stdout, stderr) => {
                try { fs.unlinkSync(tmpFile); } catch (e) {}

                if (error) {
                    return reject({ error: 'SQL解析失败', detail: stderr || error.message });
                }

                try {
                    const result = JSON.parse(stdout);
                    if (result.error) {
                        return reject({ error: '解析失败', detail: result.error });
                    }

                    // ===== 新存储方式：存储Block + 兼容ColumnLineage =====
                    const lineageData = extractLineage(result, jobName || 'manual', reportCode || 'Y4');

                    // 保存到数据库（Block + ColumnLineage）
                    saveAllToDatabase(result, lineageData, jobName || 'manual', reportCode || 'Y4', (err) => {
                        if (err) return reject({ error: '保存失败', detail: err.message });
                        resolve({
                            success: true,
                            summary: {
                                source_tables: result.source_tables || [],
                                target_tables: result.target_tables || [],
                                column_lineage_count: lineageData.length,
                                block_count: result.blocks?.length || 0,
                                total_tables: (result.source_tables?.length || 0) + (result.target_tables?.length || 0)
                            },
                            lineage: lineageData,
                            blocks: result.blocks || [],
                            raw: result
                        });
                    });
                } catch (e) {
                    reject({ success: false, raw: stdout, error: '解析结果格式异常' });
                }
            }
        );
    });
}

// ==================== 提取血缘 ====================
function extractLineage(parsed, jobName, reportCode) {
    const results = [];
    const tableEdgeSet = new Set();
    
    // 1. 提取字段级血缘
    const columnLineage = parsed.column_lineage || [];
    columnLineage.forEach(item => {
        if (!item.target_column && !item.target_table) {
            return;
        }
        
        const sourceTable = item.source_table || item.source || null;
        const targetTable = item.target_table || item.target || 'UNKNOWN_TABLE';
        
        results.push({
            source_table: sourceTable,
            target_table: targetTable,
            source_field: item.source_column || item.source_field || null,
            target_field: item.target_column || item.target_field || 'UNKNOWN_FIELD',
            expression: item.expression || '直接映射',
            full_expression: item.full_expression || item.expression || '直接映射',
            expression_type: item.expression_type || 'direct',
            source_role: item.source_role || 'direct',
            filter_cond: item.filter_cond || null,
            agg_func: item.agg_func || null,
            has_distinct: item.has_distinct || false,
            group_by: item.group_by || null,
            having_cond: item.having_cond || null,
            where_condition: item.where_condition || null,
            is_grouped: item.is_grouped || false,
            job_id: jobName || 'manual',
            report_code: reportCode || 'Y4',
            layer: inferLayer(targetTable)
        });
        
        if (sourceTable && sourceTable !== targetTable) {
            const edgeKey = `${sourceTable}->${targetTable}`;
            tableEdgeSet.add(edgeKey);
        }
    });
    
    // 2. 表级依赖
    tableEdgeSet.forEach(edgeKey => {
        const [source, target] = edgeKey.split('->');
        results.push({
            source_table: source,
            target_table: target,
            source_field: 'TABLE_LEVEL',
            target_field: 'TABLE_LEVEL',
            expression: 'TABLE_DEPENDENCY',
            full_expression: 'TABLE_DEPENDENCY',
            expression_type: 'TABLE_DEPENDENCY',
            source_role: 'table_level',
            filter_cond: null,
            agg_func: null,
            has_distinct: false,
            group_by: null,
            having_cond: null,
            where_condition: null,
            is_grouped: false,
            job_id: jobName || 'manual',
            report_code: reportCode || 'Y4',
            layer: inferLayer(target)
        });
    });
    
    return results;
}

// ==================== 推断数据层级 ====================
function inferLayer(tableName) {
    const name = (tableName || '').toLowerCase();
    if (name.includes('ods') || name.includes('stg') || name.includes('采集')) return 'COLLECT';
    if (name.includes('dwd') || name.includes('dwm') || name.includes('基础')) return 'BASE';
    if (name.includes('dws') || name.includes('ads') || name.includes('结果')) return 'RESULT';
    return 'UNKNOWN';
}

// ==================== 保存所有数据（Block + ColumnLineage）====================
function saveAllToDatabase(parsed, lineageData, jobId, reportCode, callback) {
    db.serialize(() => {
        // 开启事务
        db.run('BEGIN TRANSACTION');

        // 1. 删除该job的旧数据
        db.run('DELETE FROM field_lineage WHERE job_id = ?', [jobId], (err) => {
            if (err) { db.run('ROLLBACK'); return callback(err); }
        });

        db.run('DELETE FROM lineage_block WHERE job_id = ?', [jobId], (err) => {
            if (err) { db.run('ROLLBACK'); return callback(err); }
        });

        // 2. 保存ColumnLineage（兼容旧表）
        if (lineageData.length > 0) {
            const stmt = db.prepare(`
                INSERT INTO field_lineage 
                (source_table, target_table, source_field, target_field, expression, 
                 full_expression, expression_type, source_role, filter_cond,
                 agg_func, has_distinct, group_by, having_cond, where_condition, is_grouped,
                 job_id, report_code, layer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            lineageData.forEach(row => {
                stmt.run(
                    row.source_table || null,
                    row.target_table || null,
                    row.source_field || null,
                    row.target_field || null,
                    row.expression || '直接映射',
                    row.full_expression || row.expression || '直接映射',
                    row.expression_type || 'direct',
                    row.source_role || 'direct',
                    row.filter_cond || null,
                    row.agg_func || null,
                    row.has_distinct ? 1 : 0,
                    row.group_by || null,
                    row.having_cond || null,
                    row.where_condition || null,
                    row.is_grouped ? 1 : 0,
                    row.job_id || jobId || 'manual',
                    row.report_code || reportCode || 'Y4',
                    row.layer || 'UNKNOWN'
                );
            });
            stmt.finalize();
        }

        // 3. 保存Blocks（结构化存储）
        const blocks = parsed.blocks || [];
        if (blocks.length > 0) {
            // 3.1 保存Block主表
            const blockStmt = db.prepare(`
                INSERT INTO lineage_block 
                (block_id, parent_block_id, block_type, target_table, statement_type,
                 from_table, from_alias, sql_hash, job_id, period)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            `);

            blocks.forEach(block => {
                blockStmt.run(
                    block.block_id || null,
                    block.parent_block_id || null,
                    block.block_type || 'SELECT',
                    block.target_table || null,
                    block.statement_type || null,
                    block.from_table || null,
                    block.from_alias || null,
                    block.sql_hash || null,
                    block.job_id || jobId || 'manual',
                    block.period || null
                );
            });
            blockStmt.finalize();

            // 3.2 保存Columns
            const colStmt = db.prepare(`
                INSERT INTO lineage_block_column 
                (block_id, target_column, source_column, source_table, agg_func, 
                 has_distinct, expression, expression_type)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            `);

            blocks.forEach(block => {
                const cols = block.columns || [];
                cols.forEach(col => {
                    colStmt.run(
                        block.block_id,
                        col.target || null,
                        col.source || null,
                        col.source_table || null,
                        col.agg_func || null,
                        col.has_distinct ? 1 : 0,
                        col.expression || null,
                        col.expression_type || 'direct'
                    );
                });
            });
            colStmt.finalize();

            // 3.3 保存JOINs
            const joinStmt = db.prepare(`
                INSERT INTO lineage_block_join 
                (block_id, join_type, join_table, join_alias, on_condition)
                VALUES (?, ?, ?, ?, ?)
            `);

            blocks.forEach(block => {
                const joins = block.joins || [];
                joins.forEach(join => {
                    joinStmt.run(
                        block.block_id,
                        join.type || 'INNER',
                        join.table || null,
                        join.alias || null,
                        join.on || null
                    );
                });
            });
            joinStmt.finalize();

            // 3.4 保存WHERE
            const whereStmt = db.prepare(`
                INSERT INTO lineage_block_where 
                (block_id, condition_expr)
                VALUES (?, ?)
            `);

            blocks.forEach(block => {
                const wheres = block.where_conditions || [];
                wheres.forEach(w => {
                    whereStmt.run(
                        block.block_id,
                        w.expression || null
                    );
                });
            });
            whereStmt.finalize();

            // 3.5 保存GROUP BY
            const groupStmt = db.prepare(`
                INSERT INTO lineage_block_group_by 
                (block_id, group_column)
                VALUES (?, ?)
            `);

            blocks.forEach(block => {
                const groups = block.group_by || [];
                groups.forEach(g => {
                    groupStmt.run(
                        block.block_id,
                        g
                    );
                });
            });
            groupStmt.finalize();

            // 3.6 保存HAVING
            const havingStmt = db.prepare(`
                INSERT INTO lineage_block_having 
                (block_id, having_expr)
                VALUES (?, ?)
            `);

            blocks.forEach(block => {
                if (block.having_cond) {
                    havingStmt.run(block.block_id, block.having_cond);
                }
            });
            havingStmt.finalize();

            // 3.7 保存UNION关系
            const unionStmt = db.prepare(`
                INSERT INTO lineage_block_union 
                (parent_block_id, child_block_id)
                VALUES (?, ?)
            `);

            blocks.forEach(block => {
                const unionChildren = block.union_children || [];
                unionChildren.forEach(child => {
                    if (child && child.block_id) {
                        unionStmt.run(block.block_id, child.block_id);
                    }
                });
            });
            unionStmt.finalize();
        }

        // 提交事务
        db.run('COMMIT', (err) => {
            if (err) {
                db.run('ROLLBACK');
                return callback(err);
            }
            console.log(`✅ 已保存 ${lineageData.length} 条血缘关系 + ${blocks.length} 个Block (job_id=${jobId})`);
            callback(null);
        });
    });
}

// ==================== API: 解析SQL并存储血缘 ====================
app.post('/api/parse-sql', async (req, res) => {
    const { sql, dialect, job_id } = req.body;
    
    if (!sql) {
        return res.status(400).json({ error: '缺少sql参数' });
    }

    try {
        const result = await parseAndStore(sql, job_id, 'Y4', dialect);
        res.json(result);
    } catch (e) {
        res.status(500).json(e);
    }
});

// ==================== 查询已存储的血缘（兼容旧接口）====================
app.get('/api/lineage/stored', (req, res) => {
    const reportCode = req.query.report_code || 'Y4';
    
    db.all(
        `SELECT * FROM field_lineage WHERE report_code = ?`,
        [reportCode],
        (err, rows) => {
            if (err) {
                return res.status(500).json({ error: err.message });
            }
            res.json({
                report_code: reportCode,
                total: rows.length,
                lineage: rows
            });
        }
    );
});

// ==================== 获取表的字段列表 ====================
app.get('/api/table/fields', (req, res) => {
    const tableName = req.query.table;
    const reportCode = req.query.report_code || 'Y4';

    if (!tableName) {
        return res.status(400).json({ error: '缺少table参数' });
    }

    db.all(
        `SELECT DISTINCT source_field AS field FROM field_lineage
         WHERE report_code = ? AND source_table = ? AND source_field IS NOT NULL AND source_field != 'TABLE_LEVEL'
         UNION
         SELECT DISTINCT target_field AS field FROM field_lineage
         WHERE report_code = ? AND target_table = ? AND target_field IS NOT NULL AND target_field != 'TABLE_LEVEL'
         ORDER BY field`,
        [reportCode, tableName, reportCode, tableName],
        (err, rows) => {
            if (err) return res.status(500).json({ error: err.message });
            res.json({ fields: rows.map(r => r.field) });
        }
    );
});

// ==================== 按字段查询血缘（兼容旧接口）====================
app.get('/api/lineage/field', (req, res) => {
    const fieldName = req.query.field_name;
    const reportCode = req.query.report_code || 'Y4';
    
    if (!fieldName) {
        return res.status(400).json({ error: '缺少field_name参数' });
    }
    
    db.all(
        `SELECT * FROM field_lineage 
         WHERE report_code = ? 
         AND (target_field = ? OR source_field = ?)`,
        [reportCode, fieldName, fieldName],
        (err, rows) => {
            if (err) {
                return res.status(500).json({ error: err.message });
            }
            res.json({
                field_name: fieldName,
                upstream: rows.filter(r => r.source_field === fieldName || r.source_field === null),
                downstream: rows.filter(r => r.target_field === fieldName || r.target_field === null),
                all: rows
            });
        }
    );
});

// ==================== 多层血缘查询（基于field_lineage，兼容旧接口）====================
app.get('/api/lineage/multi', async (req, res) => {
    const reportCode = req.query.report_code || 'Y4';
    const targetTable = req.query.table;
    const targetField = req.query.field;
    const direction = req.query.direction || 'both';
    const maxLevel = parseInt(req.query.max_level) || 5;
    const maxRows = parseInt(req.query.max_rows) || 2000;

    if (!targetTable && !targetField) {
        return res.status(400).json({ error: '需要提供 table 或 field 参数' });
    }

    try {
        let seedRows;
        if (targetTable && targetField) {
            seedRows = await new Promise((resolve, reject) => {
                db.all(
                    `SELECT * FROM field_lineage WHERE report_code = ? AND (
                        (source_table = ? AND source_field = ?) OR
                        (target_table = ? AND target_field = ?)
                    )`,
                    [reportCode, targetTable, targetField, targetTable, targetField],
                    (err, rows) => err ? reject(err) : resolve(rows)
                );
            });
        } else if (targetTable) {
            seedRows = await new Promise((resolve, reject) => {
                db.all(
                    `SELECT * FROM field_lineage WHERE report_code = ? AND (source_table = ? OR target_table = ?)`,
                    [reportCode, targetTable, targetTable],
                    (err, rows) => err ? reject(err) : resolve(rows)
                );
            });
        } else {
            seedRows = await new Promise((resolve, reject) => {
                db.all(
                    `SELECT * FROM field_lineage WHERE report_code = ? AND (source_field = ? OR target_field = ?)`,
                    [reportCode, targetField, targetField],
                    (err, rows) => err ? reject(err) : resolve(rows)
                );
            });
        }

        if (seedRows.length === 0) {
            return res.json({ query: { table: targetTable, field: targetField, direction }, total: 0, tables: [], lineage: [] });
        }

        // 使用与之前相同的BFS逻辑
        if (targetTable && targetField) {
            const visitedRowIds = new Set();
            const resultRows = [];
            const resultTables = new Set();

            for (const row of seedRows) {
                if (!visitedRowIds.has(row.id)) {
                    visitedRowIds.add(row.id);
                    resultRows.push({ ...row, level: 0 });
                }
                if (row.source_table) resultTables.add(row.source_table);
                if (row.target_table) resultTables.add(row.target_table);
            }

            const needDownstream = direction === 'downstream' || direction === 'both';
            const needUpstream = direction === 'upstream' || direction === 'both';

            if (needUpstream) {
                const visitedPairs = new Set([`${targetTable}|${targetField}`]);
                let currentPairs = [[targetTable, targetField]];

                for (let lv = 0; lv < maxLevel && resultRows.length < maxRows; lv++) {
                    if (currentPairs.length === 0) break;

                    const sqlParts = [];
                    const sqlParams = [];
                    for (const [tbl, fld] of currentPairs) {
                        sqlParts.push(`SELECT * FROM field_lineage WHERE report_code = ? AND target_table = ? AND target_field = ?`);
                        sqlParams.push(reportCode, tbl, fld);
                    }

                    const layerRows = await new Promise((resolve, reject) => {
                        db.all(sqlParts.join(' UNION '), sqlParams, (err, rows) => err ? reject(err) : resolve(rows));
                    });

                    const nextPairs = [];
                    for (const row of layerRows) {
                        if (resultRows.length >= maxRows) break;
                        if (!visitedRowIds.has(row.id)) {
                            visitedRowIds.add(row.id);
                            resultRows.push({ ...row, level: lv + 1 });
                        }
                        if (row.source_table) resultTables.add(row.source_table);
                        if (row.target_table) resultTables.add(row.target_table);

                        const key = `${row.source_table}|${row.source_field}`;
                        if (!visitedPairs.has(key)) {
                            visitedPairs.add(key);
                            nextPairs.push([row.source_table, row.source_field]);
                        }
                    }
                    currentPairs = nextPairs;
                }
            }

            if (needDownstream) {
                const visitedPairs = new Set([`${targetTable}|${targetField}`]);
                let currentPairs = [[targetTable, targetField]];

                for (let lv = 0; lv < maxLevel && resultRows.length < maxRows; lv++) {
                    if (currentPairs.length === 0) break;

                    const sqlParts = [];
                    const sqlParams = [];
                    for (const [tbl, fld] of currentPairs) {
                        sqlParts.push(`SELECT * FROM field_lineage WHERE report_code = ? AND source_table = ? AND source_field = ?`);
                        sqlParams.push(reportCode, tbl, fld);
                    }

                    const layerRows = await new Promise((resolve, reject) => {
                        db.all(sqlParts.join(' UNION '), sqlParams, (err, rows) => err ? reject(err) : resolve(rows));
                    });

                    const nextPairs = [];
                    for (const row of layerRows) {
                        if (resultRows.length >= maxRows) break;
                        if (!visitedRowIds.has(row.id)) {
                            visitedRowIds.add(row.id);
                            resultRows.push({ ...row, level: lv + 1 });
                        }
                        if (row.source_table) resultTables.add(row.source_table);
                        if (row.target_table) resultTables.add(row.target_table);

                        const key = `${row.target_table}|${row.target_field}`;
                        if (!visitedPairs.has(key)) {
                            visitedPairs.add(key);
                            nextPairs.push([row.target_table, row.target_field]);
                        }
                    }
                    currentPairs = nextPairs;
                }
            }

            return res.json({
                query: { table: targetTable, field: targetField, direction },
                total: resultRows.length,
                tables: Array.from(resultTables),
                lineage: resultRows
            });
        }

        // 表级BFS（逻辑与之前相同，略）
        const seedTables = new Set();
        seedRows.forEach(r => {
            if (r.source_table) seedTables.add(r.source_table);
            if (r.target_table) seedTables.add(r.target_table);
        });

        const allRows = [...seedRows];
        const visitedTables = new Set(seedTables);
        const visitedRowIds = new Set(seedRows.map(r => r.id));
        let currentTables = Array.from(seedTables);

        for (let lv = 0; lv < maxLevel && allRows.length < maxRows; lv++) {
            if (currentTables.length === 0) break;

            const placeholders = currentTables.map(() => '?').join(',');
            const needDownstream = direction === 'downstream' || direction === 'both';
            const needUpstream = direction === 'upstream' || direction === 'both';

            let sqlParts = [];
            let sqlParams = [];
            if (needDownstream) {
                sqlParts.push(`SELECT * FROM field_lineage WHERE report_code = ? AND source_table IN (${placeholders})`);
                sqlParams.push(reportCode, ...currentTables);
            }
            if (needUpstream) {
                sqlParts.push(`SELECT * FROM field_lineage WHERE report_code = ? AND target_table IN (${placeholders})`);
                sqlParams.push(reportCode, ...currentTables);
            }

            const layerRows = await new Promise((resolve, reject) => {
                db.all(sqlParts.join(' UNION '), sqlParams, (err, rows) => err ? reject(err) : resolve(rows));
            });

            const nextTables = [];
            for (const row of layerRows) {
                if (allRows.length >= maxRows) break;
                if (!visitedRowIds.has(row.id)) {
                    visitedRowIds.add(row.id);
                    allRows.push(row);
                }
                const next = needDownstream ? row.target_table : row.source_table;
                if (next && !visitedTables.has(next)) {
                    visitedTables.add(next);
                    nextTables.push(next);
                }
                if (direction === 'both') {
                    const nextUp = row.source_table;
                    if (nextUp && !visitedTables.has(nextUp)) {
                        visitedTables.add(nextUp);
                        nextTables.push(nextUp);
                    }
                }
            }
            currentTables = [...new Set(nextTables)];
        }

        // 构建结果（与之前相同）
        const upstreamMap = new Map();
        const downstreamMap = new Map();

        for (const row of allRows) {
            const st = row.source_table;
            const tt = row.target_table;
            if (!upstreamMap.has(st)) upstreamMap.set(st, []);
            upstreamMap.get(st).push(row);
            if (!downstreamMap.has(tt)) downstreamMap.set(tt, []);
            downstreamMap.get(tt).push(row);
        }

        let resultRows = [];
        const visitedRows = new Set();

        const bfs = (startTable, directionFn, maxLv, levelOffset) => {
            const queue = [{ table: startTable, level: levelOffset }];
            const localVisited = new Set([startTable]);

            while (queue.length > 0 && resultRows.length < maxRows) {
                const { table, level } = queue.shift();
                if (level > maxLv + levelOffset) continue;

                const edges = directionFn(table) || [];
                for (const row of edges) {
                    if (resultRows.length >= maxRows) break;
                    const key = row.id || `${row.source_table}.${row.source_field}->${row.target_table}.${row.target_field}`;
                    if (!visitedRows.has(key)) {
                        visitedRows.add(key);
                        resultRows.push({ ...row, level: level + 1 });
                    }
                    const nextTable = directionFn === 'downstream'
                        ? row.target_table
                        : row.source_table;
                    if (!localVisited.has(nextTable)) {
                        localVisited.add(nextTable);
                        queue.push({ table: nextTable, level: level + 1 });
                    }
                }
            }
        };

        const downstreamEdges = (t) => upstreamMap.get(t) || [];
        const upstreamEdges = (t) => downstreamMap.get(t) || [];

        if (targetTable && targetField) {
            const directMatches = allRows.filter(r => {
                const matchSource = (r.source_table === targetTable && r.source_field === targetField);
                const matchTarget = (r.target_table === targetTable && r.target_field === targetField);
                return matchSource || matchTarget;
            });

            for (const row of directMatches) {
                const key = row.id || `${row.source_table}.${row.source_field}->${row.target_table}.${row.target_field}`;
                if (!visitedRows.has(key)) {
                    visitedRows.add(key);
                    resultRows.push({ ...row, level: 0 });
                }
            }

            const startTables = new Set();
            for (const row of directMatches) {
                startTables.add(row.source_table);
                startTables.add(row.target_table);
            }

            for (const t of startTables) {
                if (direction === 'downstream') {
                    bfs(t, downstreamEdges, maxLevel, 1);
                } else if (direction === 'upstream') {
                    bfs(t, upstreamEdges, maxLevel, 1);
                } else {
                    bfs(t, downstreamEdges, maxLevel, 1);
                    bfs(t, upstreamEdges, maxLevel, 1);
                }
            }
        } else if (targetTable) {
            if (direction === 'downstream') {
                bfs(targetTable, downstreamEdges, maxLevel, 0);
            } else if (direction === 'upstream') {
                bfs(targetTable, upstreamEdges, maxLevel, 0);
            } else {
                bfs(targetTable, downstreamEdges, maxLevel, 0);
                bfs(targetTable, upstreamEdges, maxLevel, 0);
            }
        } else if (targetField) {
            const directMatches = allRows.filter(r =>
                (r.source_field === targetField || r.target_field === targetField)
            );

            for (const row of directMatches) {
                const key = row.id || `${row.source_table}.${row.source_field}->${row.target_table}.${row.target_field}`;
                if (!visitedRows.has(key)) {
                    visitedRows.add(key);
                    resultRows.push({ ...row, level: 0 });
                }
            }

            const startTables = new Set();
            for (const row of directMatches) {
                startTables.add(row.source_table);
                startTables.add(row.target_table);
            }

            for (const t of startTables) {
                if (direction === 'downstream') {
                    bfs(t, downstreamEdges, maxLevel, 1);
                } else if (direction === 'upstream') {
                    bfs(t, upstreamEdges, maxLevel, 1);
                } else {
                    bfs(t, downstreamEdges, maxLevel, 1);
                    bfs(t, upstreamEdges, maxLevel, 1);
                }
            }
        }

        const resultTables = new Set();
        resultRows.forEach(r => {
            if (r.source_table) resultTables.add(r.source_table);
            if (r.target_table) resultTables.add(r.target_table);
        });

        res.json({
            query: { table: targetTable, field: targetField, direction },
            total: resultRows.length,
            tables: Array.from(resultTables),
            lineage: resultRows
        });
    } catch (err) {
        console.error('多层血缘查询错误:', err);
        res.status(500).json({ error: err.message });
    }
});

// ==================== 获取解析统计 ====================
app.get('/api/lineage/stats', (req, res) => {
    const reportCode = req.query.report_code || 'Y4';
    
    db.all(
        `SELECT 
            COUNT(*) as total,
            COUNT(DISTINCT source_table) as source_tables,
            COUNT(DISTINCT target_table) as target_tables,
            COUNT(DISTINCT job_id) as jobs,
            layer
         FROM field_lineage 
         WHERE report_code = ? 
         GROUP BY layer`,
        [reportCode],
        (err, rows) => {
            if (err) {
                return res.status(500).json({ error: err.message });
            }
            if (!rows || rows.length === 0) {
                db.get(
                    `SELECT 
                        COUNT(*) as total,
                        COUNT(DISTINCT source_table) as source_tables,
                        COUNT(DISTINCT target_table) as target_tables,
                        COUNT(DISTINCT job_id) as jobs
                     FROM field_lineage 
                     WHERE report_code = ?`,
                    [reportCode],
                    (err2, summary) => {
                        if (err2) return res.status(500).json({ error: err2.message });
                        res.json({ layers: [], summary: summary || { total: 0, source_tables: 0, target_tables: 0, jobs: 0 } });
                    }
                );
            } else {
                const summary = {
                    total: rows.reduce((sum, r) => sum + r.total, 0),
                    source_tables: rows.reduce((sum, r) => sum + r.source_tables, 0),
                    target_tables: rows.reduce((sum, r) => sum + r.target_tables, 0),
                    jobs: rows.reduce((sum, r) => sum + r.jobs, 0)
                };
                res.json({ layers: rows, summary });
            }
        }
    );
});

// ==================== 新增：获取Block结构查询 ====================
app.get('/api/lineage/blocks', (req, res) => {
    const jobId = req.query.job_id;
    const blockId = req.query.block_id;
    
    let sql = `
        SELECT * FROM lineage_block 
        WHERE 1=1
    `;
    const params = [];
    
    if (jobId) {
        sql += ' AND job_id = ?';
        params.push(jobId);
    }
    if (blockId) {
        sql += ' AND block_id = ?';
        params.push(blockId);
    }
    sql += ' ORDER BY created_at DESC';
    
    db.all(sql, params, (err, blocks) => {
        if (err) {
            return res.status(500).json({ error: err.message });
        }
        res.json({ blocks });
    });
});

// ==================== 新增：获取Block的完整详情 ====================
app.get('/api/lineage/block/detail/:blockId', (req, res) => {
    const blockId = req.params.blockId;
    
    db.get('SELECT * FROM lineage_block WHERE block_id = ?', [blockId], (err, block) => {
        if (err) return res.status(500).json({ error: err.message });
        if (!block) return res.status(404).json({ error: 'Block not found' });
        
        // 获取关联数据
        const detail = { ...block };
        
        db.all('SELECT * FROM lineage_block_column WHERE block_id = ?', [blockId], (err, cols) => {
            if (err) return res.status(500).json({ error: err.message });
            detail.columns = cols;
            
            db.all('SELECT * FROM lineage_block_join WHERE block_id = ?', [blockId], (err, joins) => {
                if (err) return res.status(500).json({ error: err.message });
                detail.joins = joins;
                
                db.all('SELECT * FROM lineage_block_where WHERE block_id = ?', [blockId], (err, wheres) => {
                    if (err) return res.status(500).json({ error: err.message });
                    detail.where_conditions = wheres;
                    
                    db.all('SELECT * FROM lineage_block_group_by WHERE block_id = ?', [blockId], (err, groups) => {
                        if (err) return res.status(500).json({ error: err.message });
                        detail.group_by = groups.map(g => g.group_column);
                        
                        db.get('SELECT * FROM lineage_block_having WHERE block_id = ?', [blockId], (err, having) => {
                            if (err) return res.status(500).json({ error: err.message });
                            detail.having_cond = having?.having_expr || null;
                            
                            db.all('SELECT * FROM lineage_block_union WHERE parent_block_id = ?', [blockId], (err, unions) => {
                                if (err) return res.status(500).json({ error: err.message });
                                detail.union_children = unions.map(u => u.child_block_id);
                                
                                res.json(detail);
                            });
                        });
                    });
                });
            });
        });
    });
});

// ==================== 批量分析：从PostgreSQL读取SQL任务 ====================
app.post('/api/batch/analyze', async (req, res) => {
    if (batchProgress.status === 'running') {
        return res.status(409).json({ error: '已有批量任务正在运行，请稍候再试' });
    }

    batchProgress = { status: 'running', total: 0, processed: 0, succeeded: 0, failed: 0, current_task: '正在从数据库读取任务...', errors: [] };

    try {
        const client = await pgPool.connect();
        const result = await client.query(
            'SELECT task_id, task_name, task_type, sql_text FROM public.y4_trace_tasks WHERE sql_text IS NOT NULL AND sql_text != \'\' AND task_type IN (\'TiDB SQL\', \'Oracle SQL\', \'HiveSQL\', \'SPARK SQL\', \'PostgreSQL\')'
        );
        client.release();

        const tasks = result.rows;
        batchProgress.total = tasks.length;
        batchProgress.current_task = '';

        if (tasks.length === 0) {
            batchProgress.status = 'completed';
            batchProgress.current_task = '无待处理任务';
            return res.json({ success: true, message: '无待处理任务', total: 0 });
        }

        res.json({ success: true, message: `开始处理 ${tasks.length} 个任务`, total: tasks.length });

        const zeroOutputTasks = [];
        const outputJsonPath = path.join(__dirname, '0_output.json');

        for (const task of tasks) {
            if (batchProgress.status === 'cancelled') break;

            batchProgress.current_task = `${task.task_id} (${task.task_name || '未命名'})`;
            
            try {
                const result = await parseAndStore(task.sql_text, task.task_id, 'Y4', 'hive');
                batchProgress.succeeded++;
                
                if (result.summary && result.summary.column_lineage_count === 0) {
                    zeroOutputTasks.push({
                        task_id: task.task_id,
                        task_name: task.task_name,
                        task_type: task.task_type,
                        source_tables: result.summary.source_tables || [],
                        target_tables: result.summary.target_tables || [],
                        sql_text: task.sql_text,
                        reason: '字段级血缘为0（可能仅用常量/count聚合，无字段级映射）'
                    });
                }
            } catch (err) {
                batchProgress.failed++;
                batchProgress.errors.push({
                    task_id: task.task_id,
                    task_name: task.task_name,
                    error: err.error || err.message || JSON.stringify(err)
                });
                console.error(`❌ 任务 ${task.task_id} 失败:`, err.error || err.message);
            }
            
            batchProgress.processed++;
        }

        batchProgress.status = 'completed';
        batchProgress.current_task = `完成: 成功 ${batchProgress.succeeded}, 失败 ${batchProgress.failed}, 0输出 ${zeroOutputTasks.length}`;

        fs.writeFileSync(outputJsonPath, JSON.stringify({
            generated_at: new Date().toISOString(),
            total_zero_output: zeroOutputTasks.length,
            tasks: zeroOutputTasks
        }, null, 2), 'utf-8');
        console.log(`📄 0输出任务已写入: ${outputJsonPath} (共 ${zeroOutputTasks.length} 条)`);

    } catch (e) {
        batchProgress.status = 'failed';
        batchProgress.current_task = `错误: ${e.message}`;
        console.error('批量分析失败:', e);
        if (!res.headersSent) {
            res.status(500).json({ error: '批量分析失败', detail: e.message });
        }
    }
});

// ==================== 获取批量分析进度 ====================
app.get('/api/batch/progress', (req, res) => {
    res.json(batchProgress);
});

// ==================== 取消批量分析 ====================
app.post('/api/batch/cancel', (req, res) => {
    if (batchProgress.status === 'running') {
        batchProgress.status = 'cancelled';
        res.json({ success: true, message: '已发送取消请求' });
    } else {
        res.json({ success: false, message: '当前没有正在运行的批量任务' });
    }
});

// ==================== 启动服务 ====================
app.listen(PORT, () => {
    console.log(`🚀 CISP Demo 服务已启动: http://localhost:${PORT}`);
    console.log(`📊 API接口:`);
    console.log(`   POST /api/parse-sql        - 解析SQL并存储血缘（含Block结构）`);
    console.log(`   GET  /api/lineage/stored   - 查询已存储的血缘`);
    console.log(`   GET  /api/lineage/field    - 按字段查询血缘`);
    console.log(`   GET  /api/lineage/stats    - 解析统计`);
    console.log(`   GET  /api/lineage/blocks   - 查询Block结构`);
    console.log(`   GET  /api/lineage/block/detail/:blockId - 获取Block完整详情`);
    console.log(`   POST /api/batch/analyze    - 从PG批量读取SQL任务`);
    console.log(`   GET  /api/batch/progress   - 批量任务进度`);
    console.log(`   POST /api/batch/cancel     - 取消批量任务`);
});