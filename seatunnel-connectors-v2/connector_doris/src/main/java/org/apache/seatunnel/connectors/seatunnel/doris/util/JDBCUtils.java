package org.apache.seatunnel.connectors.seatunnel.doris.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCUtils {



    //创建数据库的连接
    public static Connection getConnection(String dorisbeaddress, String username, String password, String database) {
        try {
            Class.forName("com.mysql.jdbc.Driver");
            String connectionURL = "jdbc:mysql://"+dorisbeaddress+"/"+database+"?useUnicode=true&characterEncoding=UTF8&useSSL=false";
            return DriverManager.getConnection(connectionURL, username, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    //关闭数据库的连接
    public static void close( Statement stmt, Connection con) throws SQLException {

        if (stmt != null) {
            stmt.close();
        }
        if (con != null) {
            con.close();
        }
    }

    public static void selectData(String sql) throws SQLException {
        //注册驱动    使用驱动连接数据库
        Connection con = null;
        PreparedStatement statement = null;
        ResultSet resultset = null;
        System.out.println("00000000000000000");
        statement = con.prepareStatement(sql);
        resultset = statement.executeQuery(sql);//返回查询结果
        int columnCount = resultset.getMetaData().getColumnCount();
        while (resultset.next()) {
            for (int i = 1; i <= columnCount; i++) {
                Object object = resultset.getObject(i);
                System.out.println("数据 " + object);
            }
            System.out.println("一行数据结束 :-------------------");

        }
    }

    public static ResultSetMetaData getMetaData(Connection con,String sql) throws SQLException {
        //注册驱动    使用驱动连接数据库

        PreparedStatement statement = null;
        ResultSet resultset = null;
        statement = con.prepareStatement(sql);
        resultset = statement.executeQuery(sql);//返回查询结果
        ResultSetMetaData metaData = resultset.getMetaData();

        return metaData;
    }
}

