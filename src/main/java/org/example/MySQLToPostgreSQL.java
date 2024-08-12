package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class MySQLToPostgreSQL {

    // MySQL 数据库连接信息
    private static final String MYSQL_URL = "jdbc:mysql://192.168.100.100:3306/t1";
    private static final String MYSQL_USER = "root";
    private static final String MYSQL_PASSWORD = "b0Xtw5yE#3Du11";

    // PostgreSQL 数据库连接信息
    private static final String POSTGRES_URL = "jdbc:postgresql://192.168.100.100:5432/t2";
    private static final String POSTGRES_USER = "postgres";
    private static final String POSTGRES_PASSWORD = "uqw87171h#9";

    public static void main(String[] args) {
        try (Connection mysqlConnection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
             Connection postgresConnection = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD)) {

            // 只从MySQL的t1数据库中获取数据
            String databaseName = "t1";
            ResultSet tables = mysqlConnection.getMetaData().getTables(databaseName, null, "%", new String[]{"TABLE"});

            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                // 跳过不需要迁移的特定系统表
                if ("columns_priv".equalsIgnoreCase(tableName) ||
                        "some_other_system_table".equalsIgnoreCase(tableName)) {
                    System.out.println("跳过表: " + tableName);
                    continue;
                }

                System.out.println("正在迁移表: " + tableName);
                createTableInPostgres(mysqlConnection, postgresConnection, tableName);
                migrateData(mysqlConnection, postgresConnection, tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTableInPostgres(Connection mysqlConnection, Connection postgresConnection, String tableName) throws SQLException {
        // 创建语句以从MySQL获取表创建SQL
        Statement mysqlStatement = mysqlConnection.createStatement();
        ResultSet tables = mysqlStatement.executeQuery("SHOW CREATE TABLE `" + tableName + "`");

        if (tables.next()) {
            // 修改MySQL的CREATE TABLE SQL以适应PostgreSQL
            String createTableSQL = tables.getString(2)
                    .replaceAll("(?i)ENGINE\\s*=\\s*\\w+\\s*", "") // 移除 ENGINE 声明及其参数
                    .replaceAll("(?i)DEFAULT\\s+CHARSET\\s*=\\s*\\w+\\s*", "") // 移除 DEFAULT CHARSET 声明
                    .replaceAll("(?i)CHARACTER\\s+SET\\s*=\\s*\\w+", "") // 移除 CHARACTER SET 声明
                    .replaceAll("(?i)COLLATE\\s*=\\s*\\w+", "") // 移除 COLLATE 声明
                    .replaceAll("COLLATE utf8_bin", "")  // 移除COLLATE声明
                    .replaceAll("(?i)ROW_FORMAT\\s*=\\s*\\w+\\s*", "") // 移除 ROW_FORMAT 声明
                    .replaceAll("CHARACTER SET \\w+", "")  // 移除CHARACTER SET声明
                    .replaceAll("ON UPDATE CURRENT_TIMESTAMP\\(\\d*\\)", "")  // 移除ON UPDATE CURRENT_TIMESTAMP
                    .replaceAll("`", "\"") // 将反引号替换为双引号
                    .replaceAll("(?i)AUTO_INCREMENT", "") // 移除 AUTO_INCREMENT
                    .replaceAll("(?i)TINYINT\\(1\\)", "SMALLINT") // 替换 TINYINT(1) 为 SMALLINT
                    .replaceAll("(?i)MEDIUMINT", "INTEGER") // 替换 MEDIUMINT 为 INTEGER
                    .replaceAll("(?i)INT", "INTEGER") // 替换 INT 为 INTEGER
                    .replaceAll("bigint", "BIGINT")  // 修正数据类型
                    .replaceAll("(?i)BIGINTEGER", "BIGINT") // 替换 BIGINT 为 BIGINT
                    .replaceAll("(?i)FLOAT", "REAL") // 替换 FLOAT 为 REAL
                    .replaceAll("(?i)DOUBLE", "DOUBLE PRECISION") // 替换 DOUBLE 为 DOUBLE PRECISION
                    .replaceAll("(?i)DECIMAL", "DECIMAL") // 替换 DECIMAL 为 DECIMAL
                    .replaceAll("(?i)NUMERIC", "NUMERIC") // 替换 NUMERIC 为 NUMERIC
                    .replaceAll("(?i)CHAR", "CHAR") // 替换 CHAR 为 CHAR
                    .replaceAll("(?i)VARCHAR", "VARCHAR") // 替换 VARCHAR 为 VARCHAR
                    .replaceAll("(?i)TINYTEXT", "TEXT") // 替换 TINYTEXT 为 TEXT
                    .replaceAll("(?i)TEXT", "TEXT") // 替换 TEXT 为 TEXT
                    .replaceAll("(?i)MEDIUMTEXT", "TEXT") // 替换 MEDIUMTEXT 为 TEXT
                    .replaceAll("(?i)LONGTEXT", "TEXT") // 替换 LONGTEXT 为 TEXT
                    .replaceAll("(?i)DATE", "DATE") // 替换 DATE 为 DATE
                    .replaceAll("(?i)DATETIME", "TIMESTAMP") // 替换 DATETIME 为 TIMESTAMP
                    .replaceAll("(?i)TIME", "TIME") // 替换 TIME 为 TIME
                    .replaceAll("(?i)YEAR", "INTEGER") // 替换 YEAR 为 INTEGER
                    .replaceAll("(?i)ENUM", "VARCHAR") // 替换 ENUM 为 VARCHAR
                    .replaceAll("(?i)SET", "VARCHAR") // 替换 SET 为 VARCHAR
                    .replaceAll("(?i)BLOB", "BYTEA") // 替换 BLOB 为 BYTEA
                    .replaceAll("(?i)TINYBLOB", "BYTEA") // 替换 TINYBLOB 为 BYTEA
                    .replaceAll("tinyINTEGER", "SMALLINT")  // 替换 TINYINTEGER 为 BYTEA
                    .replaceAll("(?i)MEDIUMBLOB", "BYTEA") // 替换 MEDIUMBLOB 为 BYTEA
                    .replaceAll("(?i)LONGBLOB", "BYTEA") // 替换 LONGBLOB 为 BYTEA
                    .replaceAll("longBYTEA", "BYTEA")  // 替换LONGBYTEA 为 BYTEA
                    .replaceAll("(?i)UNSIGNED", "") // 移除 UNSIGNED
                    .replaceAll("(?i)DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP") // 替换 DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP 为 DEFAULT CURRENT_TIMESTAMP
                    .replaceAll("(?i)DEFAULT '0000-00-00 00:00:00'", "DEFAULT '1970-01-01 00:00:00'") // 替换 DEFAULT '0000-00-00 00:00:00' 为 DEFAULT '1970-01-01 00:00:00'
                    .replaceAll("(?i)DEFAULT '0000-00-00'", "DEFAULT '1970-01-01'") // 替换 DEFAULT '0000-00-00' 为 DEFAULT '1970-01-01'
                    .replaceAll("(?i)DEFAULT '0000-00'", "DEFAULT '1970-01'") // 替换 DEFAULT '0000-00' 为 DEFAULT '1970-01'
                    .replaceAll("(?i)DEFAULT 0", "DEFAULT 0") // 替换 DEFAULT 0 为 DEFAULT 0
                    .replaceAll("(?i)USING BTREE", ""); // 移除 USING BTREE

            createTableSQL = createTableSQL.replaceFirst("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

// 分离外键约束
            StringBuilder foreignKeys = new StringBuilder();
            StringBuilder cleanedSQL = new StringBuilder();
            String[] lines = createTableSQL.split("\n");
            for (String line : lines) {
                if (line.trim().startsWith("CONSTRAINT") && line.contains("FOREIGN KEY")) {
                    foreignKeys.append(line.trim()).append("\n");
                } else if (line.trim().startsWith("KEY")) {
                    // 在PostgreSQL中，应该使用 CREATE INDEX 替换 MySQL 中的 KEY 语句
                    String indexName = line.split(" ")[1];
                    String columns = line.substring(line.indexOf("("));
                    foreignKeys.append("CREATE INDEX ").append(indexName).append(" ON ").append(tableName).append(" ").append(columns).append(";\n");
                } else {
                    cleanedSQL.append(line).append("\n");
                }
            }

            try (Statement postgresStatement = postgresConnection.createStatement()) {
                postgresStatement.execute(cleanedSQL.toString());
            }

            // 应用外键约束
            if (foreignKeys.length() > 0) {
                try (Statement postgresStatement = postgresConnection.createStatement()) {
                    String[] foreignKeyStatements = foreignKeys.toString().split("\n");
                    for (String fk : foreignKeyStatements) {
                        if (!fk.trim().isEmpty()) {
                            postgresStatement.execute(fk.trim());
                        }
                    }
                }
            }
        }
    }


    private static void migrateData(Connection mysqlConnection, Connection postgresConnection, String tableName) throws SQLException {
        // 从MySQL表中获取所有数据
        Statement mysqlStatement = mysqlConnection.createStatement();
        ResultSet data = mysqlStatement.executeQuery("SELECT * FROM `t1`.`" + tableName + "`");

        int columnCount = data.getMetaData().getColumnCount();
        StringBuilder insertSQL = new StringBuilder("INSERT INTO \"" + tableName + "\" VALUES (");

        // 构造PostgreSQL的参数化SQL查询
        for (int i = 0; i < columnCount; i++) {
            insertSQL.append("?");
            if (i < columnCount - 1) {
                insertSQL.append(", ");
            }
        }
        insertSQL.append(")");

        // 准备数据插入语句
        try (PreparedStatement postgresStatement = postgresConnection.prepareStatement(insertSQL.toString())) {
            while (data.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    postgresStatement.setObject(i, data.getObject(i)); // 使用 setObject 支持所有数据类型
                }
                postgresStatement.addBatch(); // 批量插入
            }
            postgresStatement.executeBatch(); // 执行批量插入
        }
    }
}
