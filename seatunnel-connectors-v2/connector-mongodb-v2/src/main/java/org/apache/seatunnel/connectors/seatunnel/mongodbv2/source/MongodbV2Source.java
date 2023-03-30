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

package org.apache.seatunnel.connectors.seatunnel.mongodbv2.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.source.enumerator.MongoSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.source.reader.MongoReader;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodbv2.source.split.SamplingSplitStrategy;

import org.bson.BsonDocument;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

@AutoService(SeaTunnelSource.class)
public class MongodbV2Source
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>,
                SupportColumnProjection {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<SeaTunnelRow> deserializer;

    private MongoSplitStrategy splitStrategy;

    private SeaTunnelRowType rowType;

    @Override
    public String getPluginName() {
        return "MongodbV2";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.hasPath(MongodbConfig.CONNECTION.key())
                && pluginConfig.hasPath(MongodbConfig.DATABASE.key())
                && pluginConfig.hasPath(MongodbConfig.COLLECTION.key())) {
            String connection = pluginConfig.getString(MongodbConfig.CONNECTION.key());
            String database = pluginConfig.getString(MongodbConfig.DATABASE.key());
            String collection = pluginConfig.getString(MongodbConfig.COLLECTION.key());
            clientProvider =
                    MongoColloctionProviders.getBuilder()
                            .connectionString(connection)
                            .database(database)
                            .collection(collection)
                            .build();
        }
        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
        }
        deserializer = new DocumentRowDataDeserializer(rowType.getFieldNames(), rowType);
        splitStrategy =
                SamplingSplitStrategy.builder()
                        .setMatchQuery(
                                pluginConfig.hasPath(MongodbConfig.MATCHQUERY.key())
                                        ? BsonDocument.parse(
                                                pluginConfig.getString(
                                                        MongodbConfig.MATCHQUERY.key()))
                                        : new BsonDocument())
                        .setClientProvider(clientProvider)
                        .setSplitKey(
                                pluginConfig.hasPath(MongodbConfig.SPLIT_KEY.key())
                                        ? pluginConfig.getString(MongodbConfig.SPLIT_KEY.key())
                                        : MongodbConfig.SPLIT_KEY.defaultValue())
                        .setSizePerSplit(
                                pluginConfig.hasPath(MongodbConfig.SPLIT_SIZE.key())
                                        ? pluginConfig.getLong(MongodbConfig.SPLIT_SIZE.key())
                                        : MongodbConfig.SPLIT_SIZE.defaultValue())
                        .setProjection(
                                pluginConfig.hasPath(MongodbConfig.PROJECTION.key())
                                        ? BsonDocument.parse(
                                                pluginConfig.getString(
                                                        MongodbConfig.PROJECTION.key()))
                                        : new BsonDocument())
                        .build();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return rowType;
    }

    @Override
    public SourceReader<SeaTunnelRow, MongoSplit> createReader(SourceReader.Context readerContext)
            throws Exception {
        return new MongoReader(readerContext, clientProvider, deserializer);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) throws Exception {
        return new MongoSplitEnumerator(enumeratorContext, clientProvider, splitStrategy);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState)
            throws Exception {
        return new MongoSplitEnumerator(
                enumeratorContext, clientProvider, splitStrategy, checkpointState);
    }
}
