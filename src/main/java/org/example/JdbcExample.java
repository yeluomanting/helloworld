package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
public class JdbcExample {

    // 数据库URL、用户名和密码
    private static final String DB_URL = "jdbc:mysql://192.168.100.100:3306/t1"; // 修改为你的数据库URL
    private static final String USER = "root"; // 修改为你的数据库用户名
    private static final String PASSWORD = "b0Xtw5yE#3Du11"; // 修改为你的数据库密码

    public static void main(String[] args) {
        Connection connection = null;

        try {
            // 1. 加载JDBC驱动
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 2. 创建连接
            connection = DriverManager.getConnection(DB_URL, USER, PASSWORD);
            System.out.println("成功连接到数据库!");

            // 3. 创建表
            createTable(connection);

            // 4. 插入数据
            insertData(connection, "Alice", 25);
            insertData(connection, "Bob", 30);

            // 5. 查询数据
            queryData(connection);

        } catch (ClassNotFoundException e) {
            System.out.println("JDBC驱动加载失败: " + e.getMessage());
        } catch (SQLException e) {
            System.out.println("数据库操作失败: " + e.getMessage());
        } finally {
            // 6. 关闭连接
            try {
                if (connection != null) {
                    connection.close();
                    System.out.println("数据库连接已关闭.");
                }
            } catch (SQLException e) {
                System.out.println("关闭连接时发生错误: " + e.getMessage());
            }
        }
    }

    private static void createTable(Connection connection) throws SQLException {
        String createTableSQL = "CREATE TABLE IF NOT EXISTS users (" +
                "id INT AUTO_INCREMENT PRIMARY KEY, " +
                "name VARCHAR(255), " +
                "age INT)";
        Statement statement = connection.createStatement();
        statement.execute(createTableSQL);
        System.out.println("表创建成功!");
    }

    private static void insertData(Connection connection, String name, int age) throws SQLException {
        String insertSQL = "INSERT INTO users (name, age) VALUES (?, ?)";
        PreparedStatement preparedStatement = connection.prepareStatement(insertSQL);
        preparedStatement.setString(1, name);
        preparedStatement.setInt(2, age);
        preparedStatement.executeUpdate();
        System.out.println("插入数据成功: " + name + ", " + age);
    }

    private static void queryData(Connection connection) throws SQLException {
        String querySQL = "SELECT * FROM users";
        Statement statement = connection.createStatement();
        ResultSet resultSet = statement.executeQuery(querySQL);

        System.out.println("查询结果:");
        while (resultSet.next()) {
            int id = resultSet.getInt("id");
            String name = resultSet.getString("name");
            int age = resultSet.getInt("age");
            System.out.println("ID: " + id + ", Name: " + name + ", Age: " + age);
        }
    }
}
