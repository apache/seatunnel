package org.apache.seatunnel.connectors.seatunnel.access.client;


import java.io.Serializable;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

public class AccessClient implements Serializable {
    private String driver;
    private String url;
    private String username;
    private String password;
    private String query;
    private Connection connection;

    public AccessClient(String driver, String url, String username, String password, String query) {
        this.driver = driver;
        this.url = url;
        this.username = username;
        this.password = password;
        this.query = query;
    }

    public Connection getAccessConnection(String url, String username, String password) {
        try {
            Class.forName(driver);
            this.connection = DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return this.connection;
    }

    public ResultSetMetaData selectMetaData() throws Exception {
        connection = this.getAccessConnection(url, username, password);
        Statement statement = connection.createStatement();
        ResultSet result = statement.executeQuery(query);
        ResultSetMetaData metaData = result.getMetaData();
        statement.close();
        return metaData;
    }

    public ResultSetMetaData getTableSchema(String tableName) throws Exception {
        connection = this.getAccessConnection(url, username, password);
        Statement statement = connection.createStatement();
        ResultSet result =
                statement.executeQuery(String.format("select * from %s limit 1", tableName));
        ResultSetMetaData metaData = result.getMetaData();
        statement.close();
        return metaData;
    }
}
