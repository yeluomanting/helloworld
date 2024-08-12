/**
 *
 * @author liuyu
 * @date 2024/8/6
 */

const mysql = require('mysql');
const { Client } = require('pg');

// MySQL数据库配置
const mysqlConfig = {
    host: '192.168.100.100',
    user: 'root',
    password: 'b0Xtw5yE#3Du11',
    database: 't1'
};

// PostgreSQL数据库配置
const pgConfig = {
    host: '192.168.100.100',
    user: 'postgres',
    password: 'uqw87171h#9',
    database: 't2'
};

// 连接到MySQL数据库
const mysqlConnection = mysql.createConnection(mysqlConfig);
mysqlConnection.connect();

// 连接到PostgreSQL数据库
const pgClient = new Client(pgConfig);
pgClient.connect();

// 查询MySQL中的表结构
mysqlConnection.query('SHOW TABLES', async (error, tables) => {
    if (error) throw error;

    for (let table of tables) {
        const tableName = table[`Tables_in_${mysqlConfig.database}`];

        // 获取表结构
        const [createTable] = await mysqlConnection.promise().query(`SHOW CREATE TABLE ${tableName}`);
        const createTableSQL = createTable[0]['Create Table'];

        // 将CREATE TABLE语句转换为PostgreSQL兼容的语句
        const pgCreateTableSQL = createTableSQL
            .replace(/`/g, '"') // 替换反引号为双引号
            .replace(/AUTO_INCREMENT/g, 'SERIAL') // 替换AUTO_INCREMENT为SERIAL
            .replace(/DEFAULT CHARSET=\w+/g, ''); // 移除默认字符集

        // 在PostgreSQL中创建表
        await pgClient.query(pgCreateTableSQL);

        // 从MySQL中选择数据并插入PostgreSQL中
        const [rows] = await mysqlConnection.promise().query(`SELECT * FROM ${tableName}`);
        for (let row of rows) {
            const columns = Object.keys(row).map(col => `"${col}"`).join(', ');
            const values = Object.values(row).map(val => `'${val}'`).join(', ');
            await pgClient.query(`INSERT INTO "${tableName}" (${columns}) VALUES (${values})`);
        }
    }

    // 关闭连接
    mysqlConnection.end();
    pgClient.end();
});

