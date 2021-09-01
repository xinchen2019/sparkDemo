package com.apple.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Program: spark-java
 * @ClassName: Test02
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-29 13:07
 * @Version 1.1.0
 **/
public class Test02 {
    public static void main(String[] args) {
        String sql = "select name from users where id=?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager.getConnection("jdbc:hive2://master:10000/apple",
                    "ubuntu",
                    "123");
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, 1);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                String name = rs.getString(1);
                System.out.println(name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
