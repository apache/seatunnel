package io.github.interestinglab.waterdrop.utils;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionPoll implements Serializable {

    private static JdbcConnectionPoll poll;

    private  DruidDataSource dataSource;

    private JdbcConnectionPoll(Properties properties) throws Exception {
        dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
    }

    public static JdbcConnectionPoll getPoll(Properties properties) throws Exception {
        if (poll == null){
            synchronized (JdbcConnectionPoll.class){
                if (poll == null){
                    poll = new JdbcConnectionPoll(properties);
                }
            }
        }
        return poll;
    }

    public Connection getConnection() throws SQLException {

       return dataSource.getConnection();
    }

}
