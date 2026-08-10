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
app.use(express.static(path.join(__dirname, 'frontend')));
app.use(express.json({ limit: '10mb' }));

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
const db = new sqlite3.Database(path.join(__dirname, 'cisp.db'));

// 创建索引（如果不存在）以加速按表查询
db.serialize(() => {
    db.run('CREATE INDEX IF NOT EXISTS idx_source_table ON field_lineage(source_table)');
    db.run('CREATE INDEX IF NOT EXISTS idx_target_table ON field_lineage(target_table)');
    db.run('CREATE INDEX IF NOT EXISTS idx_report_code ON field_lineage(report_code)');
});

// 批量处理进度（内存中存储，供前端轮询）
let batchProgress = { status: 'idle', total: 0, processed: 0, succeeded: 0, failed: 0, current_task: '', errors: [] };

// ==================== 核心逻辑：解析SQL并存储血缘（可复用） ====================
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

                    const lineageData = extractLineage(result, jobName || 'manual', reportCode || 'Y4');

                    saveLineage(lineageData, (err) => {
                        if (err) return reject({ error: '保存失败', detail: err.message });
                        resolve({
                            success: true,
                            summary: {
                                source_tables: result.source_tables || [],
                                target_tables: result.target_tables || [],
                                column_lineage_count: lineageData.length,
                                total_tables: (result.source_tables?.length || 0) + (result.target_tables?.length || 0)
                            },
                            lineage: lineageData,
                            raw: result
                        });
                    }, jobName);
                } catch (e) {
                    reject({ success: false, raw: stdout, error: '解析结果格式异常' });
                }
            }
        );
    });
}

// ==================== 核心API：解析SQL并存储血缘 ====================
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

// ==================== 提取血缘 ====================
function extractLineage(parsed, jobName, reportCode) {
    const results = [];
    const tableEdgeSet = new Set();
    
    // 1. 提取字段级血缘（优先处理，用于反推表级依赖）
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
            job_id: jobName || 'manual',
            report_code: reportCode || 'Y4',
            layer: inferLayer(targetTable)
        });
        
        if (sourceTable && sourceTable !== targetTable) {
            const edgeKey = `${sourceTable}->${targetTable}`;
            tableEdgeSet.add(edgeKey);
        }
    });
    
    // 2. 从字段级血缘反推表级依赖（仅创建有实际字段关联的表对）
    tableEdgeSet.forEach(edgeKey => {
        const [source, target] = edgeKey.split('->');
        results.push({
            source_table: source,
            target_table: target,
            source_field: 'TABLE_LEVEL',
            target_field: 'TABLE_LEVEL',
            expression: 'TABLE_DEPENDENCY',
            expression_type: 'TABLE_DEPENDENCY',
            source_role: 'table_level',
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

// ==================== 保存到数据库 ====================
function saveLineage(lineageData, callback, fallbackJobId) {
    const jobId = lineageData[0]?.job_id || fallbackJobId || 'manual';
    // 先按 job_id 清空旧数据（重分析时替换同一任务的旧结果）
    db.run('DELETE FROM field_lineage WHERE job_id = ?',
        [jobId],
        (err) => {
            if (err) return callback(err);
            
            const stmt = db.prepare(`
                INSERT INTO field_lineage 
                (source_table, target_table, source_field, target_field, expression, full_expression, expression_type, source_role, job_id, report_code, layer)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                    row.job_id || fallbackJobId || 'manual',
                    row.report_code || 'Y4',
                    row.layer || 'UNKNOWN'
                );
            });
            
            stmt.finalize((err) => {
                if (err) return callback(err);
                console.log(`✅ 已保存 ${lineageData.length} 条血缘关系 (job_id=${jobId})`);
                callback(null);
            });
        }
    );
}

// ==================== 查询已存储的血缘 ====================
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

// ==================== 按字段查询血缘 ====================
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

// ==================== 多层血缘查询（BFS 遍历上下游所有层级）====================
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
        // 优化：用 SQL 按表预过滤，避免全表加载到内存
        // 1. 先找到种子表（直接匹配的行涉及的表）
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

        // ========== 字段级 BFS：追踪该字段的数据流链路 ==========
        if (targetTable && targetField) {
            const visitedRowIds = new Set();
            const resultRows = [];
            const resultTables = new Set();

            // Level 0: 种子行（直接匹配）
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

            // 上游 BFS：查谁流向当前字段，展开到 (source_table, source_field)
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

            // 下游 BFS：查当前字段流向谁，展开到 (target_table, target_field)
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

        // ========== 表级 BFS：按表展开所有字段血缘 ==========
        const seedTables = new Set();
        seedRows.forEach(r => {
            if (r.source_table) seedTables.add(r.source_table);
            if (r.target_table) seedTables.add(r.target_table);
        });

        // 用迭代方式逐层查询，避免全表加载
        const allRows = [...seedRows];
        const visitedTables = new Set(seedTables);
        const visitedRowIds = new Set(seedRows.map(r => r.id));
        let currentTables = Array.from(seedTables);

        for (let lv = 0; lv < maxLevel && allRows.length < maxRows; lv++) {
            if (currentTables.length === 0) break;

            // 查询当前层表的相关行
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
                // both 模式下也要追踪上游方向的新表
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

        // 3. 在预过滤后的数据上构建图结构并分配层级
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

        // BFS 辅助函数
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
            // 如果没有分层数据，返回汇总
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

        // 异步执行（不阻塞响应）
        res.json({ success: true, message: `开始处理 ${tasks.length} 个任务`, total: tasks.length });

        const zeroOutputTasks = [];
        const outputJsonPath = path.join(__dirname, '0_output.json');

        let idx = 0;
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

        // 将0输出任务写入0_output.json
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
    console.log(`   POST /api/parse-sql        - 解析SQL并存储血缘`);
    console.log(`   GET  /api/lineage/stored   - 查询已存储的血缘`);
    console.log(`   GET  /api/lineage/field    - 按字段查询血缘`);
    console.log(`   GET  /api/lineage/stats    - 解析统计`);
    console.log(`   POST /api/batch/analyze    - 从PG批量读取SQL任务`);
    console.log(`   GET  /api/batch/progress   - 批量任务进度`);
    console.log(`   POST /api/batch/cancel     - 取消批量任务`);
});