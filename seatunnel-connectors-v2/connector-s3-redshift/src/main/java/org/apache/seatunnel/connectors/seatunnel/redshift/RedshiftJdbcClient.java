package org.apache.seatunnel.connectors.seatunnel.redshift;

import org.apache.seatunnel.connectors.seatunnel.redshift.config.S3RedshiftConfig;
import org.apache.seatunnel.connectors.seatunnel.redshift.exception.JdbcException;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class RedshiftJdbcClient {

    private static volatile RedshiftJdbcClient INSTANCE = null;

    private final Connection connection;

    public static RedshiftJdbcClient getInstance(Config config) throws JdbcException {
        if (INSTANCE == null) {
            synchronized (RedshiftJdbcClient.class) {
                if (INSTANCE == null) {

                    try {
                        INSTANCE = new RedshiftJdbcClient(config.getString(S3RedshiftConfig.JDBC_URL.key()),
                            config.getString(S3RedshiftConfig.JDBC_USER.key()),
                            config.getString(S3RedshiftConfig.JDBC_PASSWORD.key()));
                    } catch (SQLException | ClassNotFoundException e) {
                        throw new JdbcException("RedshiftJdbcClient init error", e);
                    }
                }
            }
        }
        return INSTANCE;
    }

    private RedshiftJdbcClient(String url, String user, String password) throws SQLException, ClassNotFoundException {
        Class.forName("com.amazon.redshift.jdbc42.Driver");
        this.connection = DriverManager.getConnection(url, user, password);
    }

    public boolean checkTableExists(String tableName) {
        boolean flag = false;
        try {
            DatabaseMetaData meta = connection.getMetaData();
            String[] type = {"TABLE"};
            ResultSet rs = meta.getTables(null, null, tableName, type);
            flag = rs.next();
        } catch (SQLException e) {
            throw new JdbcException(String.format("checkTableExists error, table name is %s ", tableName), e);
        }
        return flag;

    }

    public boolean execute(String sql) throws Exception {
        try (Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        }
    }

    public synchronized void close() throws SQLException {
        connection.close();

    }

}
