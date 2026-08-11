const sqlite3 = require('sqlite3').verbose();
const path = require('path');
const fs = require('fs');

const dbPath = path.join(__dirname, 'cisp_new.db');

// 删除旧数据库
if (fs.existsSync(dbPath)) {
    fs.unlinkSync(dbPath);
    console.log('🗑️  已删除旧数据库');
}

const db = new sqlite3.Database(dbPath);

// 创建表
db.serialize(() => {
    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT UNIQUE NOT NULL,
            parent_block_id TEXT,
            block_type TEXT DEFAULT 'SELECT',
            target_table TEXT,
            statement_type TEXT,
            from_table TEXT,
            from_alias TEXT,
            sql_hash TEXT,
            job_id TEXT,
            period TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_block_job_id ON lineage_block(job_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_block_target_table ON lineage_block(target_table)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_block_from_table ON lineage_block(from_table)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_column (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            target_column TEXT NOT NULL,
            source_column TEXT,
            source_table TEXT,
            agg_func TEXT,
            has_distinct BOOLEAN DEFAULT 0,
            expression TEXT,
            expression_type TEXT,
            FOREIGN KEY (block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_col_block_id ON lineage_block_column(block_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_col_target ON lineage_block_column(target_column)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_join (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            join_type TEXT,
            join_table TEXT,
            join_alias TEXT,
            on_condition TEXT,
            FOREIGN KEY (block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_join_block_id ON lineage_block_join(block_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_join_table ON lineage_block_join(join_table)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_where (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            condition_expr TEXT,
            FOREIGN KEY (block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_where_block_id ON lineage_block_where(block_id)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_group_by (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            group_column TEXT,
            FOREIGN KEY (block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_group_block_id ON lineage_block_group_by(block_id)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_having (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            block_id TEXT NOT NULL,
            having_expr TEXT,
            FOREIGN KEY (block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_having_block_id ON lineage_block_having(block_id)`);

    db.run(`
        CREATE TABLE IF NOT EXISTS lineage_block_union (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            parent_block_id TEXT NOT NULL,
            child_block_id TEXT NOT NULL,
            FOREIGN KEY (parent_block_id) REFERENCES lineage_block(block_id),
            FOREIGN KEY (child_block_id) REFERENCES lineage_block(block_id)
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_union_parent ON lineage_block_union(parent_block_id)`);

    // ===== 关键修复：将 having 改为 having_cond =====
    db.run(`
        CREATE TABLE IF NOT EXISTS field_lineage (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_table TEXT,
            target_table TEXT,
            source_field TEXT,
            target_field TEXT NOT NULL,
            expression TEXT,
            full_expression TEXT,
            expression_type TEXT,
            source_role TEXT DEFAULT 'direct',
            job_id TEXT,
            report_code TEXT DEFAULT 'Y4',
            layer TEXT,
            agg_func TEXT,
            has_distinct BOOLEAN DEFAULT 0,
            group_by TEXT,
            having_cond TEXT,
            where_condition TEXT,
            is_grouped BOOLEAN DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    `);
    db.run(`CREATE INDEX IF NOT EXISTS idx_fl_job_id ON field_lineage(job_id)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_fl_target_field ON field_lineage(target_field)`);
    db.run(`CREATE INDEX IF NOT EXISTS idx_fl_target_table ON field_lineage(target_table)`);

    console.log('✅ 数据库表创建完成');
});

db.close(() => {
    console.log('✅ 数据库创建完成');
});