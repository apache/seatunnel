package org.apache.seatunnel.connectors.doris.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.sink.committer.DorisCommitInfo;
import org.apache.seatunnel.connectors.doris.sink.writer.DorisSinkState;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public class DorisSinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo> {
    @Override
    public String factoryIdentifier() {
        return "Doris";
    }

    @Override
    public OptionRule optionRule() {
        return DorisOptions.SINK_RULE.build();
    }

    @Override
    public TableSink<SeaTunnelRow, DorisSinkState, DorisCommitInfo, DorisCommitInfo> createSink(
            TableFactoryContext context) {
        ReadonlyConfig config = context.getOptions();
        CatalogTable catalogTable = context.getCatalogTable();
        return () -> new DorisSink(config, catalogTable);
    }
}
