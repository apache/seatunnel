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

package org.apache.seatunnel.connectors.seatunnel.neo4j.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSinkQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.constants.CypherEnum;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;
import org.apache.seatunnel.connectors.seatunnel.neo4j.internal.SeaTunnelRowNeo4jValue;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Query;
import org.neo4j.driver.Session;
import org.neo4j.driver.SessionConfig;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.Neo4jException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jCommonConfig.PLUGIN_NAME;

@Slf4j
public class Neo4jSinkWriter implements SinkWriter<SeaTunnelRow, Void, Void> {

    private final Neo4jSinkQueryInfo neo4jSinkQueryInfo;
    private final transient Driver driver;
    private final transient Session session;

    private final SeaTunnelRowType seaTunnelRowType;
    private final List<SeaTunnelRowNeo4jValue> writeBuffer;
    private final Integer maxBatchSize;

    public Neo4jSinkWriter(
            Neo4jSinkQueryInfo neo4jSinkQueryInfo, SeaTunnelRowType seaTunnelRowType) {
        this.neo4jSinkQueryInfo = neo4jSinkQueryInfo;
        this.driver = this.neo4jSinkQueryInfo.getDriverBuilder().build();
        this.session =
                driver.session(
                        SessionConfig.forDatabase(
                                neo4jSinkQueryInfo.getDriverBuilder().getDatabase()));
        this.seaTunnelRowType = seaTunnelRowType;
        this.maxBatchSize = Optional.ofNullable(neo4jSinkQueryInfo.getMaxBatchSize()).orElse(0);
        this.writeBuffer = new ArrayList<>(maxBatchSize);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (neo4jSinkQueryInfo.batchMode()) {
            writeByBatchSize(element);
        } else {
            writeOneByOne(element);
        }
    }

    private void writeOneByOne(SeaTunnelRow element) {
        final Map<String, Object> queryParamPosition =
                neo4jSinkQueryInfo.getQueryParamPosition().entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e -> element.getField((Integer) e.getValue())));
        final Query query = new Query(neo4jSinkQueryInfo.getQuery(), queryParamPosition);
        writeByQuery(query);
    }

    private void writeByBatchSize(SeaTunnelRow element) {
        writeBuffer.add(new SeaTunnelRowNeo4jValue(seaTunnelRowType, element));
        tryWriteByBatchSize();
    }

    private void tryWriteByBatchSize() {
        if (!writeBuffer.isEmpty() && writeBuffer.size() >= maxBatchSize) {
            Query query = batchQuery();
            writeByQuery(query);
            writeBuffer.clear();
        }
    }

    private Query batchQuery() {
        try {
            Value batchValues = Values.parameters(CypherEnum.BATCH.getValue(), writeBuffer);
            return new Query(neo4jSinkQueryInfo.getQuery(), batchValues);
        } catch (ClientException e) {
            log.error("Failed to build cypher statement", e);
            throw new Neo4jConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            PLUGIN_NAME, PluginType.SINK, e.getMessage()));
        }
    }

    private void writeByQuery(Query query) {
        try {
            session.writeTransaction(
                    tx -> {
                        tx.run(query);
                        return null;
                    });
        } catch (Neo4jException e) {
            throw new Neo4jConnectorException(
                    Neo4jConnectorErrorCode.DATE_BASE_ERROR, e.getMessage());
        }
    }

    @Override
    public Optional<Void> prepareCommit() throws IOException {
        return Optional.empty();
    }

    @Override
    public void abortPrepare() {}

    @Override
    public void close() throws IOException {
        flushWriteBuffer();
        session.close();
        driver.close();
    }

    private void flushWriteBuffer() {
        if (!writeBuffer.isEmpty()) {
            Query query = batchQuery();
            writeByQuery(query);
            writeBuffer.clear();
        }
    }
}
