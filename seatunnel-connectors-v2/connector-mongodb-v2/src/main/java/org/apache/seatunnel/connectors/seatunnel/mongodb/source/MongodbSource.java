package org.apache.seatunnel.connectors.seatunnel.mongodb.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongoColloctionProviders;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentRowDataDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.enumerator.MongoSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader.MongoReader;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplitStrategy;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.SamplingSplitStrategy;

import org.bson.BsonDocument;

import com.google.auto.service.AutoService;

import java.util.ArrayList;

import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.COLLECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.CONNECTOR_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.DATABASE;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.MATCHQUERY;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.PROJECTION;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.SPLIT_KEY;
import static org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbConfig.SPLIT_SIZE;

@AutoService(SeaTunnelSource.class)
public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>> {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<SeaTunnelRow> deserializer;

    private MongoSplitStrategy splitStrategy;

    private SeaTunnelRowType rowType;

    @Override
    public String getPluginName() {
        return CONNECTOR_IDENTITY;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (pluginConfig.hasPath(CONNECTION.key())
                && pluginConfig.hasPath(DATABASE.key())
                && pluginConfig.hasPath(COLLECTION.key())) {
            String connection = pluginConfig.getString(CONNECTION.key());
            String database = pluginConfig.getString(DATABASE.key());
            String collection = pluginConfig.getString(COLLECTION.key());
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
                                pluginConfig.hasPath(MATCHQUERY.key())
                                        ? BsonDocument.parse(
                                                pluginConfig.getString(MATCHQUERY.key()))
                                        : new BsonDocument())
                        .setClientProvider(clientProvider)
                        .setSplitKey(
                                pluginConfig.hasPath(SPLIT_KEY.key())
                                        ? pluginConfig.getString(SPLIT_KEY.key())
                                        : SPLIT_KEY.defaultValue())
                        .setSizePerSplit(
                                pluginConfig.hasPath(SPLIT_SIZE.key())
                                        ? pluginConfig.getLong(SPLIT_SIZE.key())
                                        : SPLIT_SIZE.defaultValue())
                        .setProjection(
                                pluginConfig.hasPath(PROJECTION.key())
                                        ? BsonDocument.parse(
                                                pluginConfig.getString(PROJECTION.key()))
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
