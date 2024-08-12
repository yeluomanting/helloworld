package org.example;

import java.sql.*;


public class MigrateMySQLToPostgreSQL {

    // MySQL 数据库连接信息
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

            String databaseName = "t1";
            ResultSet tables = mysqlConnection.getMetaData().getTables(databaseName, null, "%", new String[]{"TABLE"});

            // 第一阶段：创建所有表结构
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                if ("columns_priv".equalsIgnoreCase(tableName) ||
                        "some_other_system_table".equalsIgnoreCase(tableName)) {
                    System.out.println("跳过表: " + tableName);
                    continue;
                }

                System.out.println("正在创建表结构: " + tableName);
                createTableInPostgres(mysqlConnection, postgresConnection, tableName, false);
            }

            // 第二阶段：添加外键约束
            tables.beforeFirst();
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                if ("columns_priv".equalsIgnoreCase(tableName) ||
                        "some_other_system_table".equalsIgnoreCase(tableName)) {
                    continue;
                }

                System.out.println("正在添加外键约束: " + tableName);
                createTableInPostgres(mysqlConnection, postgresConnection, tableName, true);
            }

            // 创建索引
            createIndexes(postgresConnection);

            // 数据迁移
            tables.beforeFirst();
            while (tables.next()) {
                String tableName = tables.getString("TABLE_NAME");

                if ("columns_priv".equalsIgnoreCase(tableName) ||
                        "some_other_system_table".equalsIgnoreCase(tableName)) {
                    continue;
                }

                System.out.println("正在迁移数据: " + tableName);
                migrateData(mysqlConnection, postgresConnection, tableName);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void createTableInPostgres(Connection mysqlConnection, Connection postgresConnection, String tableName, boolean addConstraints) throws SQLException {
        Statement mysqlStatement = mysqlConnection.createStatement();
        ResultSet tableStructure = mysqlStatement.executeQuery("SHOW CREATE TABLE `" + tableName + "`");

        if (tableStructure.next()) {
            String createTableSQL = tableStructure.getString(2)
                    .replaceAll("(?i)ENGINE\\s*=\\s*\\w+\\s*", "")
                    .replaceAll("(?i)DEFAULT\\s+CHARSET\\s*=\\s*\\w+\\s*", "")
                    .replaceAll("(?i)CHARACTER\\s+SET\\s*=\\s*\\w+", "")
                    .replaceAll("(?i)COLLATE\\s*=\\s*\\w+", "")
                    .replaceAll("COLLATE utf8_bin", "")
                    .replaceAll("(?i)ROW_FORMAT\\s*=\\s*\\w+\\s*", "")
                    .replaceAll("CHARACTER SET \\w+", "")
                    .replaceAll("ON UPDATE CURRENT_TIMESTAMP\\(\\d*\\)", "")
                    .replaceAll("`", "\"")
                    .replaceAll("(?i)AUTO_INCREMENT", "")
                    .replaceAll("(?i)TINYINT\\(1\\)", "SMALLINT")
                    .replaceAll("(?i)MEDIUMINT", "INTEGER")
                    .replaceAll("(?i)INT", "INTEGER")
                    .replaceAll("bigint", "BIGINT")
                    .replaceAll("(?i)BIGINTEGER", "BIGINT")
                    .replaceAll("(?i)FLOAT", "REAL")
                    .replaceAll("(?i)DOUBLE", "DOUBLE PRECISION")
                    .replaceAll("(?i)DECIMAL", "DECIMAL")
                    .replaceAll("(?i)NUMERIC", "NUMERIC")
                    .replaceAll("(?i)CHAR", "CHAR")
                    .replaceAll("(?i)VARCHAR", "VARCHAR")
                    .replaceAll("(?i)TINYTEXT", "TEXT")
                    .replaceAll("(?i)TEXT", "TEXT")
                    .replaceAll("(?i)MEDIUMTEXT", "TEXT")
                    .replaceAll("(?i)LONGTEXT", "TEXT")
                    .replaceAll("(?i)DATE", "DATE")
                    .replaceAll("(?i)DATETIME", "TIMESTAMP")
                    .replaceAll("(?i)TIME", "TIME")
                    .replaceAll("(?i)YEAR", "INTEGER")
                    .replaceAll("(?i)ENUM", "VARCHAR")
                    .replaceAll("(?i)SET", "VARCHAR")
                    .replaceAll("(?i)BLOB", "BYTEA")
                    .replaceAll("(?i)TINYBLOB", "BYTEA")
                    .replaceAll("tinyINTEGER", "SMALLINT")
                    .replaceAll("(?i)MEDIUMBLOB", "BYTEA")
                    .replaceAll("(?i)LONGBLOB", "BYTEA")
                    .replaceAll("longBYTEA", "BYTEA")
                    .replaceAll("(?i)UNSIGNED", "")
                    .replaceAll("(?i)DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP", "DEFAULT CURRENT_TIMESTAMP")
                    .replaceAll("(?i)DEFAULT '0000-00-00 00:00:00'", "DEFAULT '1970-01-01 00:00:00'")
                    .replaceAll("(?i)DEFAULT '0000-00-00'", "DEFAULT '1970-01-01'")
                    .replaceAll("(?i)DEFAULT '0000-00'", "DEFAULT '1970-01'")
                    .replaceAll("(?i)DEFAULT 0", "DEFAULT 0")
                    .replaceAll("(?i)USING BTREE", "")
                    .replaceAll("CONSTRAINT", "CONSTRAINT");

            createTableSQL = createTableSQL.replaceFirst("CREATE TABLE", "CREATE TABLE IF NOT EXISTS");

            StringBuilder foreignKeys = new StringBuilder();
            StringBuilder cleanedSQL = new StringBuilder();
            String[] lines = createTableSQL.split("\n");
            for (String line : lines) {
                if (line.trim().startsWith("CONSTRAINT") && line.contains("FOREIGN KEY")) {
                    if (addConstraints) {
                        foreignKeys.append(line.trim()).append("\n");
                    }
                } else {
                    cleanedSQL.append(line).append("\n");
                }
            }

            String cleanedSQLString = cleanedSQL.toString().replaceAll(",\n\\s*\\)", "\n)");

            try (Statement postgresStatement = postgresConnection.createStatement()) {
                postgresStatement.executeUpdate(cleanedSQLString);
            }

            if (addConstraints && foreignKeys.length() > 0) {
                try (Statement postgresStatement = postgresConnection.createStatement()) {
                    String[] foreignKeyStatements = foreignKeys.toString().split("\n");
                    for (String fk : foreignKeyStatements) {
                        if (!fk.trim().isEmpty()) {
                            postgresStatement.executeUpdate("ALTER TABLE \"" + tableName + "\" ADD " + fk.trim());
                        }
                    }
                }
            }

            System.out.println((addConstraints ? "已添加外键约束到表" : "已创建表结构") + ": " + tableName);
        }
    }

    private static void createIndexes(Connection postgresConnection) throws SQLException {
        String[] createIndexesSQL = {
                "CREATE INDEX IF NOT EXISTS \"ACT_IDX_START\" ON \"act_hi_actinst\" (\"START_TIME_\");",
                "CREATE INDEX IF NOT EXISTS \"ACT_IDX_END\" ON \"act_hi_actinst\" (\"END_TIME_\");",
                "CREATE INDEX IF NOT EXISTS \"ACT_IDX_PROCINST\" ON \"act_hi_actinst\" (\"PROC_INST_ID_\", \"ACT_ID_\");",
                "CREATE INDEX IF NOT EXISTS \"ACT_IDX_EXEC\" ON \"act_hi_actinst\" (\"EXECUTION_ID_\", \"ACT_ID_\");"
        };

        try (Statement postgresStatement = postgresConnection.createStatement()) {
            for (String sql : createIndexesSQL) {
                postgresStatement.executeUpdate(sql);
            }
            System.out.println("索引已创建");
        }
    }





    private static void migrateData(Connection mysqlConnection, Connection postgresConnection, String tableName) throws SQLException {
        String selectQuery = "SELECT * FROM " + tableName;
        try (Statement mysqlStatement = mysqlConnection.createStatement();
             ResultSet resultSet = mysqlStatement.executeQuery(selectQuery);
             PreparedStatement preparedStatement = postgresConnection.prepareStatement(buildInsertQuery(resultSet, tableName))) {

            int batchSize = 1000;
            int count = 0;

            while (resultSet.next()) {
                for (int i = 1; i <= resultSet.getMetaData().getColumnCount(); i++) {
                    preparedStatement.setObject(i, resultSet.getObject(i));
                }

                preparedStatement.addBatch();

                if (++count % batchSize == 0) {
                    preparedStatement.executeBatch();
                }
            }

            preparedStatement.executeBatch(); // 执行批量插入剩余的数据
            System.out.println("已迁移数据: " + tableName);
        }
    }

    private static String buildInsertQuery(ResultSet resultSet, String tableName) throws SQLException {
        ResultSetMetaData metaData = resultSet.getMetaData();
        StringBuilder query = new StringBuilder("INSERT INTO \"" + tableName + "\" (");
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            query.append("\"").append(metaData.getColumnName(i)).append("\"");
            if (i < metaData.getColumnCount()) {
                query.append(", ");
            }
        }
        query.append(") VALUES (");
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            query.append("?");
            if (i < metaData.getColumnCount()) {
                query.append(", ");
            }
        }
        query.append(")");
        return query.toString();
    }
}
