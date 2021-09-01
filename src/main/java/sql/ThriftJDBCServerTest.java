package sql;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @Program: spark-java
 * @ClassName: ThriftJDBCServerTest
 * @Description: TODO
 * @Author Mr.Apple
 * @Create: 2021-08-29 10:40
 * @Version 1.1.0
 **/
public class ThriftJDBCServerTest {
    public static void main(String[] args) {
        String sql = "select id,name from users where id=?";
        Connection conn = null;
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            Class.forName("org.apache.hive.jdbc.HiveDriver");
            conn = DriverManager
                    .getConnection("jdbc:hive2://master:10001/apple?hive.server2.transport.mode=http;hive.server2.thrift.http.path=cliservice"
                            , "ubuntu"
                            , "123");
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, 1);
            rs = pstmt.executeQuery();
            while (rs.next()) {
                int id = rs.getInt(1);
                String name = rs.getString(2);
                System.out.println("id: " + id);
                System.out.println("name: " + name);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
