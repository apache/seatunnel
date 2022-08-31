package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

public class Neo4jSource implements SeaTunnelSource<SeaTunnelRow, Neo4jSourceSplit, Neo4jSourceState> {
    @Override
    public String getPluginName() {
        return null;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        SeaTunnelSource.super.setSeaTunnelContext(seaTunnelContext);
    }

    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }

    @Override
    public SourceReader<SeaTunnelRow, Neo4jSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public Serializer<Neo4jSourceSplit> getSplitSerializer() {
        return SeaTunnelSource.super.getSplitSerializer();
    }

    @Override
    public SourceSplitEnumerator<Neo4jSourceSplit, Neo4jSourceState> createEnumerator(SourceSplitEnumerator.Context<Neo4jSourceSplit> enumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator<Neo4jSourceSplit, Neo4jSourceState> restoreEnumerator(SourceSplitEnumerator.Context<Neo4jSourceSplit> enumeratorContext, Neo4jSourceState checkpointState) throws Exception {
        return null;
    }

    @Override
    public Serializer<Neo4jSourceState> getEnumeratorStateSerializer() {
        return SeaTunnelSource.super.getEnumeratorStateSerializer();
    }
}
