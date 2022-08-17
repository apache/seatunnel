package org.apache.seatunnel.connectors.seatunnel.doris.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCUtils {

    private static final String connectionURL = "jdbc:mysql://192.168.88.120:9030/lyh?useUnicode=true&characterEncoding=UTF8&useSSL=false";
    private static final String username = "root";
    private static final String password = "";
    
    //创建数据库的连接
    public static Connection getConnection() {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            return   DriverManager.getConnection(connectionURL,username,password);
        } catch (Exception e) {
            
            e.printStackTrace();
        }
        return null;
    }
    
    //关闭数据库的连接
    public static void close(ResultSet rs,Statement stmt,Connection con) throws SQLException {
        if(rs!=null) {
            rs.close();
        }
        if(stmt!=null) {
            stmt.close();
        }
        if(con!=null) {
            con.close();
        }
    }
}

