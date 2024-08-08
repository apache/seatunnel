package org.apache.seatunnel.connectors.seatunnel.sls.source;

import com.google.common.collect.Lists;
import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.*;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.sls.state.SlsSourceState;

import java.util.List;

public class SlsSource implements SeaTunnelSource<SeaTunnelRow, SlsSourceSplit, SlsSourceState>,SupportParallelism {

    private JobContext jobContext;

    private final SlsSourceConfig slsSourceConfig;

    public SlsSource(ReadonlyConfig readonlyConfig){
        this.slsSourceConfig = new SlsSourceConfig(readonlyConfig);

    }
    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public SourceReader<SeaTunnelRow, SlsSourceSplit> createReader(SourceReader.Context readContext) throws Exception {
        return new SlsSourceReader(slsSourceConfig);
    }

    @Override
    public SourceSplitEnumerator<SlsSourceSplit, SlsSourceState> createEnumerator(SourceSplitEnumerator.Context<SlsSourceSplit> enumeratorContext) throws Exception {
        return new SlsSourceSplitEnumerator(slsSourceConfig, enumeratorContext);
    }


    @Override
    public SourceSplitEnumerator<SlsSourceSplit, SlsSourceState> restoreEnumerator(
            SourceSplitEnumerator.Context<SlsSourceSplit> enumeratorContext,
            SlsSourceState checkpointState)
            throws Exception {
        return new SlsSourceSplitEnumerator(slsSourceConfig, enumeratorContext, checkpointState);
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Lists.newArrayList(slsSourceConfig.getCatalogTable());
    }


    @Override
    public String getPluginName() {
        return org.apache.seatunnel.connectors.seatunnel.sls.config.Config.CONNECTOR_IDENTITY;
    }
}
