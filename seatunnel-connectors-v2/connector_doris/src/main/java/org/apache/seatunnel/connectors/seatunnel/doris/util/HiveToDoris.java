/*
package org.apache.seatunnel.connectors.seatunnel.doris.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class HiveToDoris {
    public static void main(String[] args) throws SQLException {
        selectData();
    }
    public static void insert() throws SQLException {
        //注册驱动    使用驱动连接数据库
        Connection con = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            con = JDBCUtils.getConnection();
            System.out.println("00000000000000000");
            String sql = "LOAD LABEL lyh.label12542\n" +
                    "(\n" +
                    "    DATA INFILE(\"hdfs://192.168.88.130:9000/doris.txt\")\n" +
                    "    INTO TABLE `table1`\n" +
                    "    COLUMNS TERMINATED BY \",\"\n" +
                    ")\n" +
                    "WITH BROKER hdfs\n" +
                    "(\n" +
                    "    \"username\"=\"hdfs_user\",\n" +
                    "    \"password\"=\"hdfs_password\"\n" +
                    ")";
            stmt = con.prepareStatement(sql);

            int result =stmt.executeUpdate();// 返回值代表收到影响的行数
            System.out.println("插入成功"+result);
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }finally {
            JDBCUtils.close(rs, stmt, con);
        }
    }
    public static void selectData() throws SQLException {
        //注册驱动    使用驱动连接数据库
        Connection con = null;
        PreparedStatement statement = null;
        ResultSet resultset = null;
        con = JDBCUtils.getConnection();
        System.out.println("00000000000000000");
        String sql = "select * from information_schema.columns;";
        statement = con.prepareStatement(sql);
        resultset = statement.executeQuery(sql);//返回查询结果
        int columnCount = resultset.getMetaData().getColumnCount();
        while (resultset.next()) {
            for (int i = 1; i <=columnCount; i++) {
                Object object = resultset.getObject(i);
                System.out.println("数据 "+object);
            }
            System.out.println("一行数据结束 :-------------------");

        }
    }
}
*/
