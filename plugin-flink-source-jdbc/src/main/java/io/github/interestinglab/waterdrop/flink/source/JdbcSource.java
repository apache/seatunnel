package io.github.interestinglab.waterdrop.flink.source;

import io.github.interestinglab.waterdrop.config.Config;
import io.github.interestinglab.waterdrop.common.config.CheckConfigUtil;
import io.github.interestinglab.waterdrop.flink.FlinkEnvironment;
import io.github.interestinglab.waterdrop.flink.batch.FlinkBatchSource;
import io.github.interestinglab.waterdrop.common.config.CheckResult;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.types.Row;

import java.sql.*;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.flink.api.common.typeinfo.BasicTypeInfo.*;
import static org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;

public class JdbcSource implements FlinkBatchSource<Row> {

    private Config config;
    private String tableName;
    private String driverName;
    private String dbUrl;
    private String username;
    private String password;
    private int fetchSize = Integer.MIN_VALUE;
    private Set<String> fields;

    private static final Pattern COMPILE = Pattern.compile("select (.+) from (.+).*");


    private HashMap<String, TypeInformation> informationMapping = new HashMap<>();

    private JDBCInputFormat jdbcInputFormat;

    {
        informationMapping.put("VARCHAR", STRING_TYPE_INFO);
        informationMapping.put("BOOLEAN", BOOLEAN_TYPE_INFO);
        informationMapping.put("TINYINT", BYTE_TYPE_INFO);
        informationMapping.put("SMALLINT", SHORT_TYPE_INFO);
        informationMapping.put("INTEGER", INT_TYPE_INFO);
        informationMapping.put("MEDIUMINT", INT_TYPE_INFO);
        informationMapping.put("INT", INT_TYPE_INFO);
        informationMapping.put("BIGINT", LONG_TYPE_INFO);
        informationMapping.put("FLOAT", FLOAT_TYPE_INFO);
        informationMapping.put("DOUBLE", DOUBLE_TYPE_INFO);
        informationMapping.put("CHAR", STRING_TYPE_INFO);
        informationMapping.put("TEXT", STRING_TYPE_INFO);
        informationMapping.put("LONGTEXT", STRING_TYPE_INFO);
        informationMapping.put("DATE", SqlTimeTypeInfo.DATE);
        informationMapping.put("TIME", SqlTimeTypeInfo.TIME);
        informationMapping.put("TIMESTAMP", SqlTimeTypeInfo.TIMESTAMP);
        informationMapping.put("DECIMAL", BIG_DEC_TYPE_INFO);
        informationMapping.put("BINARY", BYTE_PRIMITIVE_ARRAY_TYPE_INFO);

    }

    @Override
    public DataSet<Row> getData(FlinkEnvironment env) {
        return env.getBatchEnvironment().createInput(jdbcInputFormat);
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
        return CheckConfigUtil.check(config, "driver", "url", "username", "query");
    }

    @Override
    public void prepare(FlinkEnvironment env) {
        driverName = config.getString("driver");
        dbUrl = config.getString("url");
        username = config.getString("username");
        String query = config.getString("query");
        Matcher matcher = COMPILE.matcher(query);
        if (matcher.find()) {
            String var = matcher.group(1);
            tableName = matcher.group(2);
            if ("*".equals(var.trim())) {
                //do nothing
            } else {
                LinkedHashSet<String> vars = new LinkedHashSet<>();
                String[] split = var.split(",");
                for (String s : split) {
                    vars.add(s.trim());
                }
                fields = vars;
            }
        }
        if (config.hasPath("password")) {
            password = config.getString("password");
        }
        if (config.hasPath("fetch_size")) {
            fetchSize = config.getInt("fetch_size");
        }

        jdbcInputFormat = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driverName)
                .setDBUrl(dbUrl)
                .setUsername(username)
                .setPassword(password)
                .setQuery(query)
                .setFetchSize(fetchSize)
                .setRowTypeInfo(getRowTypeInfo())
                .finish();
    }

    private RowTypeInfo getRowTypeInfo() {
        HashMap<String, TypeInformation> map = new LinkedHashMap<>();

        try {
            Class.forName(driverName);
            Connection connection = DriverManager.getConnection(dbUrl, username, password);
            DatabaseMetaData metaData = connection.getMetaData();
            ResultSet columns = metaData.getColumns(connection.getCatalog(), connection.getSchema(), tableName, "%");
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String dataTypeName = columns.getString("TYPE_NAME");
                if (fields == null || fields.contains(columnName)) {
                    map.put(columnName, informationMapping.get(dataTypeName));
                }
            }
            connection.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

        int size = map.size();
        if (fields != null && fields.size() > 0) {
            size = fields.size();
        } else {
            fields = map.keySet();
        }

        TypeInformation<?>[] typeInformation = new TypeInformation<?>[size];
        String[] names = new String[size];
        int i = 0;

        for (String field : fields) {
            typeInformation[i] = map.get(field);
            names[i] = field;
            i++;
        }
        return new RowTypeInfo(typeInformation, names);
    }

}
