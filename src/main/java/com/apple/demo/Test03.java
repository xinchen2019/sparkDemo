package com.apple.demo;

import com.apple.streaming.ConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @Program: spark-java
 * @ClassName: Test03
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-29 18:35
 * @Version 1.1.0
 **/
public class Test03 {
    public static void main(String[] args) throws SQLException {
        Connection conn = ConnectionPool.getConnection();
        for (int i = 4; i < 1000000000; i++) {
            String username = "小花" + i;
            String sql = "insert into user (name) "
                    + "values('" + username + "')";
            System.out.println("sql: " + sql);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate(sql);
        }
        ConnectionPool.returnConnection(conn);
    }
}
