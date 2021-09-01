package com.apple.streaming;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedList;

/**
 * @Program: spark-java
 * @ClassName: ConnectionPool
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-24 16:01
 * @Version 1.1.0
 **/
public class ConnectionPool {

    /**
     * 静态的Connection队列
     */
    private static LinkedList<Connection> connectionQueue;

    /**
     * 加载驱动
     */
    static {
        try {
            Class.forName("com.mysql.jdbc.Driver");
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取连接，多线程访问控制
     *
     * @return
     */
    synchronized public static Connection getConnection() {
        try {
            if (connectionQueue == null) {
                connectionQueue = new LinkedList<Connection>();
                for (int i = 0; i < 10; i++) {
                    Connection conn = DriverManager.getConnection(
                            "jdbc:mysql://master:3306/apple?useSSL=false&createDatabaseIfNotExist=true&useUnicode=true&characterEncoding=UTF-8",
                            "ubuntu",
                            "123456"
                    );
                    connectionQueue.push(conn);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return connectionQueue.poll();
    }

    /**
     * 还回去一个连接
     *
     * @param conn
     */
    public static void returnConnection(Connection conn) {
        connectionQueue.push(conn);
    }
}
