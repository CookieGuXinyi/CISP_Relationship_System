const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const dbPath = path.join(__dirname, 'cisp.db');

// 删除旧数据库
if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
    console.log('🗑️  已删除旧数据库');
}

const db = new sqlite3.Database(dbPath);

// 创建3张核心表
db.serialize(() => {
    // 1. 字段血缘表（核心）
    db.run(`
        CREATE TABLE field_lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            target_field TEXT NOT NULL,
            target_table TEXT NOT NULL,
            source_table TEXT,
            source_field TEXT,
            expression TEXT,
            full_expression TEXT,
            expression_type TEXT,
            source_role TEXT DEFAULT 'direct',
            job_id TEXT,
            report_code TEXT DEFAULT 'Y4',
            layer TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    // 2. 分层快照表
    db.run(`
        CREATE TABLE snapshot (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            period TEXT NOT NULL,
            table_name TEXT NOT NULL,
            row_count INTEGER,
            total_amount DECIMAL(20,2),
            status TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    // 3. 质量规则表
    db.run(`
        CREATE TABLE quality_rule (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            rule_id TEXT NOT NULL UNIQUE,
            rule_name TEXT NOT NULL,
            layer TEXT NOT NULL,
            target_field TEXT,
            rule_sql TEXT,
            threshold TEXT,
            owner TEXT,
            status TEXT DEFAULT 'PENDING'
        )
    `);

    // 4. 规则结果表
    db.run(`
        CREATE TABLE rule_result (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id TEXT NOT NULL,
            rule_id TEXT NOT NULL,
            result TEXT,
            bad_count INTEGER,
            sample TEXT,
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP
        )
    `);

    console.log('✅ 数据库表创建完成');
});

// // 导入预置数据
// const lineageData = require('./data/lineage.json');
// const snapshotData = require('./data/snapshot.json');
// const rulesData = require('./data/rules.json');

// // 插入血缘数据
// const insertLineage = db.prepare(`
//     INSERT INTO field_lineage (field_name, target_table, source_table, source_field, expression, job_name, layer)
//     VALUES (?, ?, ?, ?, ?, ?, ?)
// `);

// lineageData.forEach(row => {
//     insertLineage.run(row.field_name, row.target_table, row.source_table, row.source_field, row.expression, row.job_name, row.layer);
// });
// insertLineage.finalize();

// // 插入快照数据
// const insertSnapshot = db.prepare(`
//     INSERT INTO snapshot (period, table_name, row_count, total_amount, status)
//     VALUES (?, ?, ?, ?, ?)
// `);

// snapshotData.forEach(row => {
//     insertSnapshot.run(row.period, row.table_name, row.row_count, row.total_amount, row.status);
// });
// insertSnapshot.finalize();

// // 插入规则数据
// const insertRule = db.prepare(`
//     INSERT INTO quality_rule (rule_id, rule_name, layer, target_field, rule_sql, threshold, owner)
//     VALUES (?, ?, ?, ?, ?, ?, ?)
// `);

// rulesData.forEach(row => {
//     insertRule.run(row.rule_id, row.rule_name, row.layer, row.target_field, row.rule_sql, row.threshold, row.owner);
// });
// insertRule.finalize();

db.close(() => {
    console.log('✅ 数据库初始化完成');
    // console.log('✅ 数据导入完成');
    // console.log(`📊 导入血缘 ${lineageData.length} 条`);
    // console.log(`📊 导入快照 ${snapshotData.length} 条`);
    // console.log(`📊 导入规则 ${rulesData.length} 条`);
});