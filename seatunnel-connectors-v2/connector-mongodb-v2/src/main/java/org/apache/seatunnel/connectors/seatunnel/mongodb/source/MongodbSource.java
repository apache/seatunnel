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

import com.google.auto.service.AutoService;

import java.util.ArrayList;

@AutoService(SeaTunnelSource.class)
public class MongodbSource
        implements SeaTunnelSource<SeaTunnelRow, MongoSplit, ArrayList<MongoSplit>> {

    private MongoClientProvider clientProvider;

    private DocumentDeserializer<SeaTunnelRow> deserializer;

    private MongoSplitStrategy splitStrategy;

    private SeaTunnelRowType rowType;

    @Override
    public String getPluginName() {
        return "Mongodb";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        String CONNECT_STRING = String.format("mongodb://%s:%d/%s", "localhost", 27017, "test");
        clientProvider =
                MongoColloctionProviders.getBuilder()
                        .connectionString(CONNECT_STRING)
                        .database("test")
                        .collection("users")
                        .build();

        if (pluginConfig.hasPath(CatalogTableUtil.SCHEMA.key())) {
            this.rowType = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
        } else {
            this.rowType = CatalogTableUtil.buildSimpleTextSchema();
        }
        deserializer =
                new DocumentRowDataDeserializer(
                        new String[] {
                            "_id", "userId", "userName", "age", "score", "user_id", "gold", "level"
                        },
                        rowType);
        //        deserializer = document -> new SeaTunnelRow(document.values().toArray());

        splitStrategy =
                SamplingSplitStrategy.builder()
                        // .setMatchQuery(gte("user_id", 1000).toBsonDocument())
                        .setClientProvider(clientProvider)
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
