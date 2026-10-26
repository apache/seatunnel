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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.DefaultSeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.serialize.SeaTunnelRowSerializer;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import java.io.IOException;
import java.util.Optional;

public class AmazonDynamoDBWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private final DynamoDbSinkClient dynamoDbSinkClient;
    private final SeaTunnelRowSerializer serializer;
    private final AmazonDynamoDBConfig amazondynamodbConfig;

    public AmazonDynamoDBWriter(
            AmazonDynamoDBConfig amazondynamodbConfig,
            CatalogTable catalogTable,
            DynamoDbSinkClient dynamoDbSinkClient) {
        this.amazondynamodbConfig = amazondynamodbConfig;
        this.dynamoDbSinkClient = dynamoDbSinkClient;

        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();
        this.serializer = new DefaultSeaTunnelRowSerializer(seaTunnelRowType, amazondynamodbConfig);
    }

    public AmazonDynamoDBWriter(
            AmazonDynamoDBConfig amazondynamodbConfig, CatalogTable catalogTable) {

        this.amazondynamodbConfig = amazondynamodbConfig;

        SeaTunnelRowType seaTunnelRowType = catalogTable.getSeaTunnelRowType();

        dynamoDbSinkClient = new DynamoDbSinkClient(amazondynamodbConfig);
        serializer = new DefaultSeaTunnelRowSerializer(seaTunnelRowType, amazondynamodbConfig);
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        // In multi-table pipelines, row.tableId identifies the target table.
        // Falls back to the configured table name for single-table usage.
        String tableName = element.getTableId();

        if (StringUtils.isEmpty(tableName)) {
            tableName = amazondynamodbConfig.getTable();
        }

        dynamoDbSinkClient.write(serializer.serialize(element), tableName);
    }

    @Override
    public void close() throws IOException {
        dynamoDbSinkClient.close();
    }

    @Override
    public Optional<Void> prepareCommit() {
        dynamoDbSinkClient.flush();
        return Optional.empty();
    }
}
