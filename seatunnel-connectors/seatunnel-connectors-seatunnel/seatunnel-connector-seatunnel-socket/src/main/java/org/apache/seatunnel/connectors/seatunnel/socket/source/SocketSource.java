package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelContext;
import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.serialization.Serializer;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowTypeInfo;
import org.apache.seatunnel.connectors.seatunnel.socket.state.SocketState;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigBeanFactory;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class SocketSource implements SeaTunnelSource<SeaTunnelRow, SocketSourceSplit, SocketState> {
    private SocketSourceParameter parameter;
    private SeaTunnelContext seaTunnelContext;
    @Override
    public String getPluginName() {
        return "SocketSource";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        this.parameter = ConfigBeanFactory.create(pluginConfig, SocketSourceParameter.class);
    }

    @Override
    public SeaTunnelContext getSeaTunnelContext() {
        return this.seaTunnelContext;
    }

    @Override
    public void setSeaTunnelContext(SeaTunnelContext seaTunnelContext) {
        this.seaTunnelContext = seaTunnelContext;
    }

    @Override
    public SeaTunnelRowTypeInfo getRowTypeInfo() {
        return new SeaTunnelRowTypeInfo(
                new String[]{"value"},
                new SeaTunnelDataType<?>[]{BasicType.STRING});
    }

    @Override
    public SourceReader<SeaTunnelRow, SocketSourceSplit> createReader(SourceReader.Context readerContext) throws Exception {
        return new SocketSourceReader(this.parameter, readerContext);
    }

    @Override
    public SourceSplitEnumerator<SocketSourceSplit, SocketState> createEnumerator(SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext) throws Exception {
        return new SocketSourceSplitEnumerator(enumeratorContext);
    }

    @Override
    public SourceSplitEnumerator<SocketSourceSplit, SocketState> restoreEnumerator(SourceSplitEnumerator.Context<SocketSourceSplit> enumeratorContext, SocketState checkpointState) throws Exception {
        return null;
    }

    @Override
    public Serializer<SocketState> getEnumeratorStateSerializer() {
        return new DefaultSerializer<>();
    }
}
