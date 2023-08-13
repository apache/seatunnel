package org.apache.seatunnel.connectors.seatunnel.amazonsqs.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.io.Serializable;

public class AmazonSqsSource implements SeaTunnelSource<SeaTunnelRow> {
    @Override
    public Boundedness getBoundedness() {
        return null;
    }

    @Override
    public SeaTunnelDataType getProducedType() {
        return null;
    }

    @Override
    public SourceReader createReader(SourceReader.Context readerContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator createEnumerator(SourceSplitEnumerator.Context enumeratorContext) throws Exception {
        return null;
    }

    @Override
    public SourceSplitEnumerator restoreEnumerator(SourceSplitEnumerator.Context enumeratorContext, Serializable checkpointState) throws Exception {
        return null;
    }

    @Override
    public String getPluginName() {
        return null;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

    }
}
