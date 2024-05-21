/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.config.sql;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.common.utils.ParserException;
import org.apache.seatunnel.config.sql.model.BaseConfig;
import org.apache.seatunnel.config.sql.model.Option;
import org.apache.seatunnel.config.sql.model.SeaTunnelConfig;
import org.apache.seatunnel.config.sql.model.SinkConfig;
import org.apache.seatunnel.config.sql.model.SourceConfig;
import org.apache.seatunnel.config.sql.model.TransformConfig;

import org.apache.commons.lang3.StringUtils;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.CreateTable;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_DELIMITER;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_DOUBLE_SINGLE_QUOTES;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_KV_DELIMITER;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_RESULT_TABLE_NAME_KEY;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_SINGLE_QUOTES;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_SOURCE_TABLE_NAME_KEY;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_TABLE_CONNECTOR_KEY;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_TABLE_TYPE_KEY;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_TABLE_TYPE_SINK;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_TABLE_TYPE_SOURCE;
import static org.apache.seatunnel.config.sql.utils.Constant.OPTION_TABLE_TYPE_TRANSFORM;
import static org.apache.seatunnel.config.sql.utils.Constant.SQL_ANNOTATION_PREFIX;
import static org.apache.seatunnel.config.sql.utils.Constant.SQL_ANNOTATION_PREFIX2;
import static org.apache.seatunnel.config.sql.utils.Constant.SQL_ANNOTATION_SUFFIX;
import static org.apache.seatunnel.config.sql.utils.Constant.SQL_CONFIG_ANNOTATION_PREFIX;
import static org.apache.seatunnel.config.sql.utils.Constant.SQL_DELIMITER;
import static org.apache.seatunnel.config.sql.utils.Constant.TEMP_TABLE_SUFFIX;

@Slf4j
public class SqlConfigBuilder {

    public static Config of(@NonNull Path sqlFilePath) {
        try {
            List<String> lines = Files.readAllLines(sqlFilePath);
            Map<String, BaseConfig> sqlTables = new LinkedHashMap<>();
            SeaTunnelConfig seaTunnelConfig = new SeaTunnelConfig();

            List<String> sqlLines = parseAnnoConfigAndSqlLine(lines, seaTunnelConfig);

            // Split SQL
            List<String> sqlList = split4SqlList(sqlLines);

            for (Iterator<String> it = sqlList.iterator(); it.hasNext(); ) {
                String sql = it.next();
                Statement statement = CCJSqlParserUtil.parse(sql);
                if (statement instanceof CreateTable) {
                    CreateTable createTable = (CreateTable) statement;
                    if (createTable.getTableOptionsStrings() == null) {
                        continue;
                    }
                    parseCreateTableSql(createTable, sqlTables, seaTunnelConfig);
                    it.remove();
                }
            }

            AtomicInteger tempTableIndex = new AtomicInteger(1);
            for (String sql : sqlList) {
                Statement statement = CCJSqlParserUtil.parse(sql);
                if (statement instanceof CreateTable) {
                    CreateTable createTable = (CreateTable) statement;
                    TransformConfig transformConfig = parseCreateAsSql(createTable, sqlTables);
                    seaTunnelConfig.getTransformConfigs().add(transformConfig);
                } else if (statement instanceof Insert) {
                    parseInsertSql((Insert) statement, sqlTables, seaTunnelConfig, tempTableIndex);
                } else {
                    throw new ParserException(
                            String.format("Unsupported SQL syntax: %s", statement));
                }
            }

            // filter out the sink config without 'source_table_name' option
            seaTunnelConfig.setSinkConfigs(
                    seaTunnelConfig.getSinkConfigs().stream()
                            .filter(
                                    sinkConfig -> {
                                        boolean containSourceTable = false;
                                        for (Option option : sinkConfig.getOptions()) {
                                            if (option.getKey()
                                                    .equals(OPTION_SOURCE_TABLE_NAME_KEY)) {
                                                containSourceTable = true;
                                                break;
                                            }
                                        }
                                        return containSourceTable;
                                    })
                            .collect(Collectors.toList()));
            if (seaTunnelConfig.getSourceConfigs().isEmpty()) {
                throw new ParserException("The SQL config must contain at least one source table");
            }
            if (seaTunnelConfig.getSinkConfigs().isEmpty()) {
                throw new ParserException(
                        "The SQL config must contain `INSERT INTO ... SELECT ...` syntax");
            }

            // render to hocon config
            String configContent = ConfigTemplate.generate(seaTunnelConfig);
            log.debug("Generated config: \n{}", configContent);
            return ConfigFactory.parseString(configContent);
        } catch (ParserException e) {
            throw e;
        } catch (Exception e) {
            throw new ParserException(e);
        }
    }

    private static List<String> parseAnnoConfigAndSqlLine(
            List<String> lines, SeaTunnelConfig seaTunnelConfig) {
        List<String> sqlLines = new ArrayList<>();
        List<String> annotationConfigs = new ArrayList<>();
        boolean annoConfig = false;
        boolean anno = false;
        StringJoiner annotationConfig = new StringJoiner("\n");

        for (String line : lines) {
            if (line.trim().startsWith(SQL_ANNOTATION_PREFIX2)) {
                continue;
            }
            if (line.trim().equals(SQL_CONFIG_ANNOTATION_PREFIX)) {
                annoConfig = true;
                continue;
            }
            if (line.trim().startsWith(SQL_ANNOTATION_PREFIX)) {
                anno = true;
                continue;
            }
            if (anno) {
                if (line.trim().equals(SQL_ANNOTATION_SUFFIX)) {
                    anno = false;
                }
            } else if (annoConfig) {
                if (line.trim().equals(SQL_ANNOTATION_SUFFIX)) {
                    annoConfig = false;
                    annotationConfigs.add(annotationConfig.toString());
                    annotationConfig = new StringJoiner("\n");
                } else {
                    annotationConfig.add(line);
                }
            } else {
                if (StringUtils.isNotEmpty(line.trim())) {
                    sqlLines.add(line);
                }
            }
        }
        seaTunnelConfig.getEnvConfigs().addAll(annotationConfigs);
        return sqlLines;
    }

    private static List<String> split4SqlList(List<String> sqlLines) {
        List<String> sqlList = new ArrayList<>();
        StringJoiner sqlSj = new StringJoiner(" ");
        for (String line : sqlLines) {
            line = line.trim();
            int commentIdx = line.indexOf(" " + SQL_ANNOTATION_PREFIX2);
            if (commentIdx > -1) {
                line = line.substring(0, commentIdx);
            }
            if (line.endsWith(SQL_DELIMITER)) {
                line = line.substring(0, line.length() - 1);
                sqlSj.add(line);
                sqlList.add(sqlSj.toString());
                sqlSj = new StringJoiner(" ");
            } else {
                sqlSj.add(line);
            }
        }
        return sqlList;
    }

    private static void parseCreateTableSql(
            CreateTable createTable,
            Map<String, BaseConfig> sqlTables,
            SeaTunnelConfig seaTunnelConfig) {

        Map<String, String> optionsMap = parseOptions(createTable);

        String tableName = createTable.getTable().getName();
        if (sqlTables.containsKey(tableName)) {
            throw new ParserException(String.format("Table name duplicate: %s", tableName));
        }
        String type = optionsMap.get(OPTION_TABLE_TYPE_KEY);
        if (OPTION_TABLE_TYPE_SOURCE.equalsIgnoreCase(type)) {
            SourceConfig sourceConfig = parseSourceSql(createTable, optionsMap);
            sqlTables.put(tableName, sourceConfig);
            seaTunnelConfig.getSourceConfigs().add(sourceConfig);
        } else if (OPTION_TABLE_TYPE_SINK.equalsIgnoreCase(type)) {
            SinkConfig sinkConfig = parseSinkSql(optionsMap);
            sqlTables.put(tableName, sinkConfig);
            seaTunnelConfig.getSinkConfigs().add(sinkConfig);
        }
    }

    private static Map<String, String> parseOptions(CreateTable createTable) {
        String options = createTable.getTableOptionsStrings().get(1);
        options = options.substring(0, options.length() - 1).substring(1);
        String[] optionItems = options.split(OPTION_DELIMITER);
        Map<String, String> optionsMap = new LinkedHashMap<>();
        for (String optionItem : optionItems) {
            int idx = optionItem.indexOf(OPTION_KV_DELIMITER);
            if (idx < 0) {
                continue;
            }
            String key = clean(optionItem.substring(0, idx).trim());
            String value = clean(optionItem.substring(idx + 1).trim());
            optionsMap.put(key, value);
        }
        return optionsMap;
    }

    private static SourceConfig parseSourceSql(
            CreateTable createTable, Map<String, String> options) {
        String connector = options.get(OPTION_TABLE_CONNECTOR_KEY);
        if (StringUtils.isEmpty(connector)) {
            throw new ParserException("The connector of option is none");
        }
        SourceConfig sourceConfig = new SourceConfig();
        sourceConfig.setConnector(connector);

        String resultTableName = createTable.getTable().getName();
        sourceConfig.setResultTableName(resultTableName);
        convertOptions(options, sourceConfig.getOptions());
        sourceConfig
                .getOptions()
                .add(Option.of(OPTION_RESULT_TABLE_NAME_KEY, "\"" + resultTableName + "\""));
        return sourceConfig;
    }

    private static SinkConfig parseSinkSql(Map<String, String> options) {
        String connector = options.get(OPTION_TABLE_CONNECTOR_KEY);
        if (StringUtils.isEmpty(connector)) {
            throw new ParserException("The connector of option is none");
        }
        SinkConfig sinkConfig = new SinkConfig();
        sinkConfig.setConnector(connector);
        // original sink table without source_table_name
        options.remove(OPTION_SOURCE_TABLE_NAME_KEY);
        convertOptions(options, sinkConfig.getOptions());

        return sinkConfig;
    }

    private static void convertOptions(Map<String, String> options, Collection<Option> optionList) {
        options.forEach(
                (k, v) -> {
                    if (OPTION_TABLE_CONNECTOR_KEY.equalsIgnoreCase(k)
                            || OPTION_TABLE_TYPE_KEY.equalsIgnoreCase(k)
                            || OPTION_RESULT_TABLE_NAME_KEY.equalsIgnoreCase(k)) {
                        return;
                    }
                    String trimVal = v.trim();
                    // if not sub-config
                    if (!(trimVal.startsWith("{") && trimVal.endsWith("}"))
                            && !(trimVal.startsWith("[") && trimVal.endsWith("]"))) {
                        v = "\"" + v + "\"";
                    }
                    Option option = Option.of(k, v);
                    optionList.add(option);
                });
    }

    private static TransformConfig parseCreateAsSql(
            CreateTable createTable, Map<String, BaseConfig> sqlTables) {
        Select select = createTable.getSelect();
        if (select != null) {
            TransformConfig transformConfig = new TransformConfig();
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();
            Table table = (Table) plainSelect.getFromItem();
            String sourceTableName = table.getName();
            if (!sqlTables.containsKey(sourceTableName)) {
                throw new ParserException(
                        String.format("The source table[%s] is not found", sourceTableName));
            }

            String resultTableName = createTable.getTable().getName();
            if (sqlTables.containsKey(resultTableName)) {
                throw new ParserException(
                        String.format("Table name duplicate: %s", resultTableName));
            }
            sqlTables.put(resultTableName, transformConfig);

            String query = select.toString();
            transformConfig.setSourceTableName(sourceTableName);
            transformConfig.setResultTableName(resultTableName);
            transformConfig.setQuery(query);

            return transformConfig;
        } else {
            throw new ParserException(String.format("Unsupported syntax: %s", createTable));
        }
    }

    private static void parseInsertSql(
            Insert insertSql,
            Map<String, BaseConfig> sqlTables,
            SeaTunnelConfig seaTunnelConfig,
            AtomicInteger tempTableIndex) {
        if (insertSql.getColumns() != null && !insertSql.getColumns().isEmpty()) {
            throw new ParserException("Insert sql must not have columns");
        }
        TransformConfig transformConfig = new TransformConfig();
        Select select = insertSql.getSelect();
        if (select == null
                || select.getSelectBody() == null
                || !(select.getSelectBody() instanceof PlainSelect)) {
            throw new ParserException("Insert sql must have select statement");
        }
        String targetTableName = insertSql.getTable().getName();
        if (select.getSelectBody() instanceof PlainSelect) {
            PlainSelect plainSelect = (PlainSelect) select.getSelectBody();

            String sourceTableName;
            String resultTableName;
            if (plainSelect.getFromItem() == null) {
                List<SelectItem> selectItems = plainSelect.getSelectItems();
                if (selectItems.size() != 1) {
                    throw new ParserException(
                            "Source table must be specified in SQL: " + insertSql);
                }
                SelectExpressionItem selectItem = (SelectExpressionItem) selectItems.get(0);
                Column column = (Column) selectItem.getExpression();
                sourceTableName = column.getColumnName();
                resultTableName = sourceTableName;
            } else {
                if (!(plainSelect.getFromItem() instanceof Table)) {
                    throw new ParserException("Unsupported syntax: " + insertSql);
                }
                Table table = (Table) plainSelect.getFromItem();
                sourceTableName = table.getName();
                resultTableName =
                        sourceTableName + TEMP_TABLE_SUFFIX + tempTableIndex.getAndIncrement();
                String query = select.toString();
                transformConfig.setSourceTableName(sourceTableName);
                transformConfig.setResultTableName(resultTableName);
                transformConfig.setQuery(query);
                seaTunnelConfig.getTransformConfigs().add(transformConfig);
            }

            if (!sqlTables.containsKey(sourceTableName)
                    || (!OPTION_TABLE_TYPE_SOURCE.equalsIgnoreCase(
                                    sqlTables.get(sourceTableName).getType())
                            && !OPTION_TABLE_TYPE_TRANSFORM.equalsIgnoreCase(
                                    sqlTables.get(sourceTableName).getType()))) {
                throw new ParserException(
                        String.format("The source table[%s] is not found", sourceTableName));
            }
            if (!sqlTables.containsKey(targetTableName)
                    || !OPTION_TABLE_TYPE_SINK.equalsIgnoreCase(
                            sqlTables.get(targetTableName).getType())) {
                throw new ParserException(
                        String.format("The sink table[%s] is not found", sourceTableName));
            }

            SinkConfig sinkConfig = (SinkConfig) sqlTables.get(targetTableName);
            SinkConfig sinkConfigNew = new SinkConfig();
            sinkConfigNew.setConnector(sinkConfig.getConnector());
            sinkConfigNew.setSourceTableName(resultTableName);
            sinkConfigNew.getOptions().addAll(sinkConfig.getOptions());
            sinkConfigNew
                    .getOptions()
                    .add(Option.of(OPTION_SOURCE_TABLE_NAME_KEY, "\"" + resultTableName + "\""));

            seaTunnelConfig.getSinkConfigs().add(sinkConfigNew);
        } else {
            throw new ParserException("Unsupported syntax: " + insertSql);
        }
    }

    private static String clean(String val) {
        if (val.startsWith(OPTION_SINGLE_QUOTES)) {
            val = val.substring(1);
        }
        if (val.endsWith(OPTION_SINGLE_QUOTES)) {
            val = val.substring(0, val.length() - 1);
        }
        val = val.replace(OPTION_DOUBLE_SINGLE_QUOTES, OPTION_SINGLE_QUOTES);
        return val;
    }
}
