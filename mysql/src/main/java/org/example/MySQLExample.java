package org.example;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MySQLExample {
    public static void main(String[] args) {
        String url = "jdbc:mysql://localhost:3306/student";
        String username = "root";
        String password = "CHe471130385@";

        try {
            // 加载驱动程序
            Class.forName("com.mysql.cj.jdbc.Driver");

            // 建立连接
            Connection connection = DriverManager.getConnection(url, username, password);

            // 向 Student 表中添加记录
            addRecord(connection, "scofield", 45, 89, 100);

            // 获取 scofield 的 English 成绩信息
            int englishScore = getEnglishScore(connection, "scofield");
            System.out.println("Scofield's English Score: " + englishScore);

            // 关闭连接
            connection.close();
        } catch (ClassNotFoundException | SQLException e) {
            e.printStackTrace();
        }
    }

    // 添加记录到 Student 表
    private static void addRecord(Connection connection, String studentName,
                                  int englishScore, int mathScore, int computerScore) throws SQLException {
        String sql = "INSERT INTO Student (Name, English, Math, Computer) VALUES (?, ?, ?, ?)";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, studentName);
            statement.setInt(2, englishScore);
            statement.setInt(3, mathScore);
            statement.setInt(4, computerScore);
            statement.executeUpdate();
            System.out.println("Record added successfully.");
        }
    }

    // 获取指定学生的 English 成绩信息
    private static int getEnglishScore(Connection connection, String studentName) throws SQLException {
        String sql = "SELECT English FROM Student WHERE Name = ?";
        try (PreparedStatement statement = connection.prepareStatement(sql)) {
            statement.setString(1, studentName);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (resultSet.next()) {
                    return resultSet.getInt("English");
                } else {
                    System.out.println("Student not found.");
                    return -1;
                }
            }
        }
    }
}
