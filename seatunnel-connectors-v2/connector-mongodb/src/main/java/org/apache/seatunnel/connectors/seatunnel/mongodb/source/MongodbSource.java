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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

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
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongoReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator.MongodbSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader.MongoReader;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.SamplingSplitStrategy;

import org.bson.BsonDocument;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;

@AutoService(SeaTunnelSource.class)
public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>>,
                SupportColumnProjection {

    private static final long serialVersionUID = 1L;

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<SeaTunnelRow> deserializer;

    private MongoSplitStrategy splitStrategy;

    private SeaTunnelRowType rowType;

    private MongoReadOptions mongoReadOptions;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
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

        mongoReadOptions =
                MongoReadOptions.builder()
                        .setMaxTimeMS(
                                pluginConfig.hasPath(MongodbConfig.MAX_TIME_MIN.key())
                                        ? pluginConfig.getLong(MongodbConfig.MAX_TIME_MIN.key())
                                        : MongodbConfig.MAX_TIME_MIN.defaultValue())
                        .setFetchSize(
                                pluginConfig.hasPath(MongodbConfig.FETCH_SIZE.key())
                                        ? pluginConfig.getInt(MongodbConfig.FETCH_SIZE.key())
                                        : MongodbConfig.FETCH_SIZE.defaultValue())
                        .setNoCursorTimeout(
                                pluginConfig.hasPath(MongodbConfig.CURSO_NO_TIMEOUT.key())
                                        ? pluginConfig.getBoolean(
                                                MongodbConfig.CURSO_NO_TIMEOUT.key())
                                        : MongodbConfig.CURSO_NO_TIMEOUT.defaultValue())
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
        return new MongoReader(readerContext, clientProvider, deserializer, mongoReadOptions);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> createEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext) throws Exception {
        return new MongodbSplitEnumerator(enumeratorContext, clientProvider, splitStrategy);
    }

    @Override
    public SourceSplitEnumerator<MongoSplit, ArrayList<MongoSplit>> restoreEnumerator(
            SourceSplitEnumerator.Context<MongoSplit> enumeratorContext,
            ArrayList<MongoSplit> checkpointState)
            throws Exception {
        return new MongodbSplitEnumerator(
                enumeratorContext, clientProvider, splitStrategy, checkpointState);
    }
}
