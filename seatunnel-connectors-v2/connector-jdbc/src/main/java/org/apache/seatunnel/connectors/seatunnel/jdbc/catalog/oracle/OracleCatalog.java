/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.AbstractJdbcCatalog;

import lombok.extern.slf4j.Slf4j;

import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_BFILE;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_BLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_CHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_CLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_LONG;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_LONG_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NCHAR;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NCLOB;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_NVARCHAR2;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_RAW;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_ROWID;
import static org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle.OracleDataTypeConvertor.ORACLE_VARCHAR2;

@Slf4j
public class OracleCatalog extends AbstractJdbcCatalog {
    private static final OracleDataTypeConvertor DATA_TYPE_CONVERTOR =
            new OracleDataTypeConvertor();
    private static final List<String> EXCLUDED_SCHEMAS =
            Collections.unmodifiableList(
                    Arrays.asList(
                            "APPQOSSYS",
                            "AUDSYS",
                            "CTXSYS",
                            "DVSYS",
                            "DBSFWUSER",
                            "DBSNMP",
                            "GSMADMIN_INTERNAL",
                            "LBACSYS",
                            "MDSYS",
                            "OJVMSYS",
                            "OLAPSYS",
                            "ORDDATA",
                            "ORDSYS",
                            "OUTLN",
                            "SYS",
                            "SYSTEM",
                            "WMSYS",
                            "XDB",
                            "EXFSYS",
                            "SYSMAN"));

    private static final String SELECT_COLUMNS_SQL =
            "SELECT\n"
                    + "    cols.COLUMN_NAME,\n"
                    + "    CASE \n"
                    + "        WHEN cols.data_type LIKE 'INTERVAL%%' THEN 'INTERVAL'\n"
                    + "        ELSE REGEXP_SUBSTR(cols.data_type, '^[^(]+')\n"
                    + "    END as TYPE_NAME,\n"
                    + "    cols.data_type || \n"
                    + "        CASE \n"
                    + "            WHEN cols.data_type IN ('VARCHAR2', 'CHAR') THEN '(' || cols.data_length || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NOT NULL THEN '(' || cols.data_precision || ', ' || cols.data_scale || ')'\n"
                    + "            WHEN cols.data_type IN ('NUMBER') AND cols.data_precision IS NOT NULL AND cols.data_scale IS NULL THEN '(' || cols.data_precision || ')'\n"
                    + "            WHEN cols.data_type IN ('RAW') THEN '(' || cols.data_length || ')'\n"
                    + "        END AS FULL_TYPE_NAME,\n"
                    + "    cols.data_length AS COLUMN_LENGTH,\n"
                    + "    cols.data_precision AS COLUMN_PRECISION,\n"
                    + "    cols.data_scale AS COLUMN_SCALE,\n"
                    + "    com.comments AS COLUMN_COMMENT,\n"
                    + "    cols.data_default AS DEFAULT_VALUE,\n"
                    + "    CASE cols.nullable WHEN 'N' THEN 'NO' ELSE 'YES' END AS IS_NULLABLE\n"
                    + "FROM\n"
                    + "    all_tab_columns cols\n"
                    + "LEFT JOIN \n"
                    + "    all_col_comments com ON cols.table_name = com.table_name AND cols.column_name = com.column_name AND cols.owner = com.owner\n"
                    + "WHERE \n"
                    + "    cols.owner = '%s'\n"
                    + "    AND cols.table_name = '%s'\n"
                    + "ORDER BY \n"
                    + "    cols.column_id \n";

    public OracleCatalog(
            String catalogName,
            String username,
            String pwd,
            JdbcUrlUtil.UrlInfo urlInfo,
            String defaultSchema) {
        super(catalogName, username, pwd, urlInfo, defaultSchema);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        try (PreparedStatement ps =
                defaultConnection.prepareStatement("SELECT name FROM v$database")) {

            List<String> databases = new ArrayList<>();
            ResultSet rs = ps.executeQuery();

            while (rs.next()) {
                String databaseName = rs.getString(1);
                databases.add(databaseName);
            }
            return databases;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", this.catalogName), e);
        }
    }

    @Override
    protected boolean createTableInternal(TablePath tablePath, CatalogTable table)
            throws CatalogException {
        String createTableSql = new OracleCreateTableSqlBuilder(table).build(tablePath);
        String[] createTableSqls = createTableSql.split(";");
        for (String sql : createTableSqls) {
            log.info("create table sql: {}", sql);
            try (PreparedStatement ps = defaultConnection.prepareStatement(sql)) {
                ps.execute();
            } catch (Exception e) {
                throw new CatalogException(
                        String.format("Failed creating table %s", tablePath.getFullName()), e);
            }
        }
        return true;
    }

    @Override
    protected boolean dropTableInternal(TablePath tablePath) throws CatalogException {
        return false;
    }

    @Override
    protected boolean createDatabaseInternal(String databaseName) {
        return false;
    }

    @Override
    protected boolean dropDatabaseInternal(String databaseName) throws CatalogException {
        return false;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        try {
            return databaseExists(tablePath.getDatabaseName())
                    && listTables(tablePath.getDatabaseName())
                            .contains(tablePath.getSchemaAndTableName().toUpperCase());
        } catch (DatabaseNotExistException e) {
            return false;
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }

        try (PreparedStatement ps =
                defaultConnection.prepareStatement(
                        "SELECT OWNER, TABLE_NAME FROM ALL_TABLES\n"
                                + "WHERE TABLE_NAME NOT LIKE 'MDRT_%'\n"
                                + "  AND TABLE_NAME NOT LIKE 'MDRS_%'\n"
                                + "  AND TABLE_NAME NOT LIKE 'MDXT_%'\n"
                                + "  AND (TABLE_NAME NOT LIKE 'SYS_IOT_OVER_%' AND IOT_NAME IS NULL)")) {

            ResultSet rs = ps.executeQuery();
            List<String> tables = new ArrayList<>();
            while (rs.next()) {
                if (EXCLUDED_SCHEMAS.contains(rs.getString(1))) {
                    continue;
                }
                tables.add(rs.getString(1) + "." + rs.getString(2));
            }

            return tables;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed listing database in catalog %s", catalogName), e);
        }
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException(catalogName, tablePath);
        }

        try {
            DatabaseMetaData metaData = defaultConnection.getMetaData();
            Optional<PrimaryKey> primaryKey =
                    getPrimaryKey(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            List<ConstraintKey> constraintKeys =
                    getConstraintKeys(
                            metaData,
                            tablePath.getDatabaseName(),
                            tablePath.getSchemaName(),
                            tablePath.getTableName());

            String sql =
                    String.format(
                            SELECT_COLUMNS_SQL,
                            tablePath.getSchemaName(),
                            tablePath.getTableName());
            try (PreparedStatement ps = defaultConnection.prepareStatement(sql);
                    ResultSet resultSet = ps.executeQuery()) {
                TableSchema.Builder builder = TableSchema.builder();
                // add column
                while (resultSet.next()) {
                    buildColumn(resultSet, builder);
                }

                // add primary key
                primaryKey.ifPresent(builder::primaryKey);
                // add constraint key
                constraintKeys.forEach(builder::constraintKey);
                TableIdentifier tableIdentifier =
                        TableIdentifier.of(
                                catalogName,
                                tablePath.getDatabaseName(),
                                tablePath.getSchemaName(),
                                tablePath.getTableName());
                return CatalogTable.of(
                        tableIdentifier,
                        builder.build(),
                        buildConnectorOptions(tablePath),
                        Collections.emptyList(),
                        "");
            }

        } catch (Exception e) {
            throw new CatalogException(
                    String.format("Failed getting table %s", tablePath.getFullName()), e);
        }
    }

    private void buildColumn(ResultSet resultSet, TableSchema.Builder builder) throws SQLException {
        String columnName = resultSet.getString("COLUMN_NAME");
        String typeName = resultSet.getString("TYPE_NAME");
        String fullTypeName = resultSet.getString("FULL_TYPE_NAME");
        long columnLength = resultSet.getLong("COLUMN_LENGTH");
        long columnPrecision = resultSet.getLong("COLUMN_PRECISION");
        long columnScale = resultSet.getLong("COLUMN_SCALE");
        String columnComment = resultSet.getString("COLUMN_COMMENT");
        Object defaultValue = resultSet.getObject("DEFAULT_VALUE");
        boolean isNullable = resultSet.getString("IS_NULLABLE").equals("YES");

        SeaTunnelDataType<?> type = fromJdbcType(typeName, columnPrecision, columnScale);
        long bitLen = 0;
        switch (typeName) {
            case ORACLE_LONG:
            case ORACLE_ROWID:
            case ORACLE_NCLOB:
            case ORACLE_CLOB:
                columnLength = -1;
                break;
            case ORACLE_RAW:
                bitLen = 2000 * 8;
                break;
            case ORACLE_BLOB:
            case ORACLE_LONG_RAW:
            case ORACLE_BFILE:
                bitLen = -1;
                break;
            case ORACLE_CHAR:
            case ORACLE_NCHAR:
            case ORACLE_NVARCHAR2:
            case ORACLE_VARCHAR2:
            default:
                break;
        }

        PhysicalColumn physicalColumn =
                PhysicalColumn.of(
                        columnName,
                        type,
                        0,
                        isNullable,
                        defaultValue,
                        columnComment,
                        fullTypeName,
                        false,
                        false,
                        bitLen,
                        null,
                        columnLength);
        builder.column(physicalColumn);
    }

    @SuppressWarnings("unchecked")
    private SeaTunnelDataType<?> fromJdbcType(ResultSetMetaData metadata, int colIndex)
            throws SQLException {
        String columnType = metadata.getColumnTypeName(colIndex);
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(OracleDataTypeConvertor.PRECISION, metadata.getPrecision(colIndex));
        dataTypeProperties.put(OracleDataTypeConvertor.SCALE, metadata.getScale(colIndex));
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(columnType, dataTypeProperties);
    }

    private SeaTunnelDataType<?> fromJdbcType(String typeName, long precision, long scale) {
        Map<String, Object> dataTypeProperties = new HashMap<>();
        dataTypeProperties.put(OracleDataTypeConvertor.PRECISION, precision);
        dataTypeProperties.put(OracleDataTypeConvertor.SCALE, scale);
        return DATA_TYPE_CONVERTOR.toSeaTunnelType(typeName, dataTypeProperties);
    }

    @SuppressWarnings("MagicNumber")
    private Map<String, String> buildConnectorOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>(8);
        options.put("connector", "jdbc");
        options.put("url", baseUrl);
        options.put("table-name", tablePath.getSchemaAndTableName());
        options.put("username", username);
        options.put("password", pwd);
        return options;
    }
}
