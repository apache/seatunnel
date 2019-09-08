package io.github.interestinglab.waterdrop.flink.source;

import com.typesafe.config.waterdrop.Config;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.stream.FlinkStreamSource;
import io.github.interestinglab.waterdrop.plugin.CheckResult;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.*;

/**
 * @author mr_xiong
 * @date 2019-09-05 21:30
 * @description
 */
public class JdbcSourceV2 extends RichParallelSourceFunction<Row> implements FlinkStreamSource<Row> {

    private Config config;

    private String splitField;
    private String tableName;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private String condition;
    private String fileds;
    private int parallelism = 1;
    private long total;

    private transient Connection connection;
    private transient PreparedStatement statement;


    @Override
    public DataStream<Row> getData(FlinkEnvironment env) {

        return null;
    }

    @Override
    public void setConfig(Config config) {
        this.config = config;
    }

    @Override
    public Config getConfig() {
        return config;
    }

    @Override
    public CheckResult checkConfig() {
        return null;
    }

    @Override
    public void prepare() {
        tableName = config.getString("table_name");
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("condition")) {
            condition = config.getString("condition");
        }
        if (config.hasPath("fields")) {
            fileds = config.getString("fields");
        }
        if (config.hasPath("split_field")) {
            splitField = config.getString("split_field");
        }
        if (config.hasPath("parallelism")) {
            parallelism = config.getInt("parallelism");
        }

        if (parallelism > 1) {
            try {
                establishConnection();
                PreparedStatement ps = connection.prepareStatement(getTotalSql());
                ResultSet resultSet = ps.executeQuery();
                total = resultSet.getLong("total");
                closeDbConnection();
            } catch (SQLException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    private String getTotalSql() {
        StringBuilder sqlBuild = new StringBuilder();
        sqlBuild.append("select count(*) total from ")
                .append(tableName);
        if (StringUtils.isNotBlank(condition)) {
            sqlBuild.append(" where ").append(condition);
        }
        return sqlBuild.toString();
    }

    private String getSql(int index) {
        StringBuilder sqlBuild = new StringBuilder();
        sqlBuild.append("select ");
        if (StringUtils.isNotBlank(fileds)) {
            sqlBuild.append(fileds);
        } else {
            sqlBuild.append("* ");
        }
        sqlBuild.append(tableName);
        if (StringUtils.isNotBlank(condition)) {
            sqlBuild.append(" where ").append(condition);
        }
        long pageCount = total / parallelism + parallelism;
        sqlBuild.append(" order by ").append(splitField)
                .append(" limit ")
                .append(index)
                .append(",")
                .append(pageCount);
        return sqlBuild.toString();
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        RuntimeContext context = getRuntimeContext();
        int index = context.getIndexOfThisSubtask();
        String sql = getSql(index);
        establishConnection();
        statement = connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        super.close();
        closeDbConnection();
    }

    @Override
    public void run(SourceContext<Row> ctx) throws Exception {
        ResultSet resultSet = statement.executeQuery();
        int columnCount = resultSet.getMetaData().getColumnCount();
        Row row = new Row(columnCount);
        while (resultSet.next()) {
            for (int i = 1; i <= columnCount; i++) {
                row.setField(i-1,resultSet.getObject(i));
            }
            ctx.collect(row);
        }
    }

    @Override
    public void cancel() {

    }

    private void establishConnection() throws SQLException, ClassNotFoundException {
        Class.forName(driverName);
        if (username == null) {
            connection = DriverManager.getConnection(dbUrl);
        } else {
            connection = DriverManager.getConnection(dbUrl, username, password);
        }
    }

    protected void closeDbConnection() throws IOException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException se) {
//                LOG.warn("JDBC connection could not be closed: " + se.getMessage());
            } finally {
                connection = null;
            }
        }
    }
}
