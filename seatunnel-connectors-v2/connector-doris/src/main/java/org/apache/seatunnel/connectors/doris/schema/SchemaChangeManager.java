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
 */

package org.apache.seatunnel.connectors.doris.schema;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.schema.event.AlterTableAddColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableChangeColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableDropColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableModifyColumnEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.datatype.DorisTypeConverterV2;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisSchemaChangeException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class SchemaChangeManager implements Serializable {
    private static final long serialVersionUID = 1L;

    private static final String CHECK_COLUMN_EXISTS =
            "SELECT COLUMN_NAME FROM information_schema.`COLUMNS` WHERE TABLE_SCHEMA = '%s' AND TABLE_NAME = '%s' AND COLUMN_NAME = '%s'";
    private static final String SCHEMA_CHANGE_API = "http://%s/api/query/default_cluster/%s";
    private ObjectMapper objectMapper = new ObjectMapper();
    private DorisSinkConfig dorisSinkConfig;
    private String charsetEncoding = "UTF-8";

    public SchemaChangeManager(DorisSinkConfig dorisSinkConfig) {
        this.dorisSinkConfig = dorisSinkConfig;
    }

    public SchemaChangeManager(DorisSinkConfig dorisSinkConfig, String charsetEncoding) {
        this.dorisSinkConfig = dorisSinkConfig;
        this.charsetEncoding = charsetEncoding;
    }

    /**
     * Refresh physical table schema by schema change event
     *
     * @param event schema change event
     * @param tablePath sink table path
     */
    public void applySchemaChange(TablePath tablePath, SchemaChangeEvent event) throws IOException {
        if (event instanceof AlterTableColumnsEvent) {
            for (AlterTableColumnEvent columnEvent : ((AlterTableColumnsEvent) event).getEvents()) {
                applySchemaChange(tablePath, columnEvent);
            }
        } else {
            if (event instanceof AlterTableChangeColumnEvent) {
                AlterTableChangeColumnEvent changeColumnEvent = (AlterTableChangeColumnEvent) event;
                if (!changeColumnEvent
                        .getOldColumn()
                        .equals(changeColumnEvent.getColumn().getName())) {
                    if (!columnExists(tablePath, changeColumnEvent.getOldColumn())
                            && columnExists(tablePath, changeColumnEvent.getColumn().getName())) {
                        log.warn(
                                "Column {} already exists in table {}. Skipping change column operation. event: {}",
                                changeColumnEvent.getColumn().getName(),
                                tablePath.getFullName(),
                                event);
                        return;
                    }
                }
                applySchemaChange(tablePath, changeColumnEvent);
            } else if (event instanceof AlterTableModifyColumnEvent) {
                applySchemaChange(tablePath, (AlterTableModifyColumnEvent) event);
            } else if (event instanceof AlterTableAddColumnEvent) {
                AlterTableAddColumnEvent addColumnEvent = (AlterTableAddColumnEvent) event;
                if (columnExists(tablePath, addColumnEvent.getColumn().getName())) {
                    log.warn(
                            "Column {} already exists in table {}. Skipping add column operation. event: {}",
                            addColumnEvent.getColumn().getName(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(tablePath, addColumnEvent);
            } else if (event instanceof AlterTableDropColumnEvent) {
                AlterTableDropColumnEvent dropColumnEvent = (AlterTableDropColumnEvent) event;
                if (!columnExists(tablePath, dropColumnEvent.getColumn())) {
                    log.warn(
                            "Column {} does not exist in table {}. Skipping drop column operation. event: {}",
                            dropColumnEvent.getColumn(),
                            tablePath.getFullName(),
                            event);
                    return;
                }
                applySchemaChange(tablePath, dropColumnEvent);
            } else {
                throw new SeaTunnelException(
                        "Unsupported schemaChangeEvent : " + event.getEventType());
            }
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableChangeColumnEvent event)
            throws IOException {
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("RENAME COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getOldColumn()))
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()));
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String changeColumnSQL = sqlBuilder.toString();
        if (!execute(changeColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table change column, SQL:" + changeColumnSQL);
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableModifyColumnEvent event)
            throws IOException {
        BasicTypeDefine typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("MODIFY COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String modifyColumnSQL = sqlBuilder.toString();
        if (!execute(modifyColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table modify column, SQL:" + modifyColumnSQL);
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableAddColumnEvent event)
            throws IOException {
        BasicTypeDefine typeDefine = DorisTypeConverterV2.INSTANCE.reconvert(event.getColumn());
        StringBuilder sqlBuilder =
                new StringBuilder()
                        .append("ALTER TABLE")
                        .append(" ")
                        .append(tablePath.getFullName())
                        .append(" ")
                        .append("ADD COLUMN")
                        .append(" ")
                        .append(quoteIdentifier(event.getColumn().getName()))
                        .append(" ")
                        .append(typeDefine.getColumnType());
        if (event.getColumn().getDefaultValue() != null
                && isSupportDefaultValue(event.getColumn())) {
            sqlBuilder
                    .append(" DEFAULT ")
                    .append(quoteDefaultValue(event.getColumn().getDefaultValue()));
        }
        if (event.getColumn().getComment() != null) {
            sqlBuilder
                    .append(" ")
                    .append("COMMENT ")
                    .append("'")
                    .append(event.getColumn().getComment())
                    .append("'");
        }
        if (event.getAfterColumn() != null) {
            sqlBuilder.append(" ").append("AFTER ").append(quoteIdentifier(event.getAfterColumn()));
        }

        String addColumnSQL = sqlBuilder.toString();
        if (!execute(addColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table add column, SQL:" + addColumnSQL);
        }
    }

    /**
     * Support Default Value
     *
     * @param column
     * @return
     */
    // todo support more type
    private boolean isSupportDefaultValue(Column column) {
        switch (column.getDataType().getSqlType()) {
            case STRING:
            case BIGINT:
            case INT:
            case TIMESTAMP:
                return true;
            default:
                return false;
        }
    }

    public void applySchemaChange(TablePath tablePath, AlterTableDropColumnEvent event)
            throws IOException {
        String dropColumnSQL =
                String.format(
                        "ALTER TABLE %s DROP COLUMN %s",
                        tablePath.getFullName(), quoteIdentifier(event.getColumn()));
        if (!execute(dropColumnSQL, tablePath.getDatabaseName())) {
            log.warn("Failed to alter table drop column, SQL:" + dropColumnSQL);
        }
    }

    /** execute sql in doris. */
    public boolean execute(String ddl, String database)
            throws IOException, IllegalArgumentException {
        String responseEntity = executeThenReturnResponse(ddl, database);
        return handleSchemaChange(responseEntity);
    }

    private String executeThenReturnResponse(String ddl, String database)
            throws IOException, IllegalArgumentException {
        if (StringUtils.isEmpty(ddl)) {
            throw new IllegalArgumentException("ddl can not be null or empty string!");
        }
        log.info("Execute SQL: {}", ddl);
        HttpPost httpPost = buildHttpPost(ddl, database);
        return handleResponse(httpPost);
    }

    private boolean handleSchemaChange(String responseEntity) throws JsonProcessingException {
        Map<String, Object> responseMap = objectMapper.readValue(responseEntity, Map.class);
        String code = responseMap.getOrDefault("code", "-1").toString();
        if (code.equals("0")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Check if the column exists in the table
     *
     * @param tablePath
     * @param column
     * @return
     */
    public boolean columnExists(TablePath tablePath, String column) throws IOException {
        String selectColumnSQL =
                buildColumnExistsQuery(
                        tablePath.getDatabaseName(), tablePath.getTableName(), column);
        return sendCheckColumnHttpPostRequest(selectColumnSQL, tablePath.getDatabaseName());
    }

    public static String buildColumnExistsQuery(String database, String table, String column) {
        return String.format(CHECK_COLUMN_EXISTS, database, table, column);
    }

    public static String quoteIdentifier(String identifier) {
        return "`" + identifier + "`";
    }

    public static String quoteDefaultValue(Object defaultValue) {
        // DEFAULT current_timestamp not need quote
        if (defaultValue.toString().startsWith("current_timestamp")) {
            return "current_timestamp";
        }
        return "'" + defaultValue + "'";
    }

    private boolean sendCheckColumnHttpPostRequest(String sql, String database)
            throws IOException, IllegalArgumentException {
        HttpPost httpPost = buildHttpPost(sql, database);
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(httpPost);
            final int statusCode = response.getStatusLine().getStatusCode();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                log.info(
                        "http post response success. statusCode: {}, loadResult: {}",
                        statusCode,
                        loadResult);
                JsonNode responseNode = objectMapper.readTree(loadResult);
                String code = responseNode.get("code").asText("-1");
                if (code.equals("0")) {
                    JsonNode data = responseNode.get("data").get("data");
                    if (!data.isEmpty()) {
                        return true;
                    }
                }
            } else {
                log.warn("http post response failed. statusCode: {}", statusCode);
            }
        } catch (Exception e) {
            log.error(
                    "send http post request error {}, default return false, SQL:{}",
                    e.getMessage(),
                    sql);
            log.error(e.getMessage(), e);
        }
        return false;
    }

    public HttpPost buildHttpPost(String ddl, String database)
            throws IllegalArgumentException, IOException {
        Map<String, String> param = new HashMap<>();
        param.put("stmt", ddl);
        List<String> feNodes = Arrays.asList(dorisSinkConfig.getFrontends().split(","));
        Collections.shuffle(feNodes);
        String requestUrl = String.format(SCHEMA_CHANGE_API, feNodes.get(0), database);
        HttpPost httpPost = new HttpPost(requestUrl);
        httpPost.setHeader(HttpHeaders.AUTHORIZATION, authHeader());
        httpPost.setHeader(
                HttpHeaders.CONTENT_TYPE,
                String.format("application/json;charset=%s", charsetEncoding));
        httpPost.setEntity(
                new StringEntity(objectMapper.writeValueAsString(param), charsetEncoding));
        return httpPost;
    }

    private String handleResponse(HttpUriRequest request) {
        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
            CloseableHttpResponse response = httpclient.execute(request);
            final int statusCode = response.getStatusLine().getStatusCode();
            final String reasonPhrase = response.getStatusLine().getReasonPhrase();
            if (statusCode == 200 && response.getEntity() != null) {
                String loadResult = EntityUtils.toString(response.getEntity());
                log.info(
                        "http post response success. statusCode: {}, loadResult: {}",
                        statusCode,
                        loadResult);
                return loadResult;
            } else {
                throw new DorisSchemaChangeException(
                        DorisConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                        "Failed to schemaChange, status: "
                                + statusCode
                                + ", reason: "
                                + reasonPhrase);
            }
        } catch (Exception e) {
            log.error("SchemaChange request error,", e);
            throw new DorisSchemaChangeException(
                    DorisConnectorErrorCode.SCHEMA_CHANGE_FAILED,
                    "SchemaChange request error with " + e.getMessage());
        }
    }

    private String authHeader() {
        return "Basic "
                + new String(
                        Base64.encodeBase64(
                                (dorisSinkConfig.getUsername()
                                                + ":"
                                                + dorisSinkConfig.getPassword())
                                        .getBytes(StandardCharsets.UTF_8)));
    }
}
