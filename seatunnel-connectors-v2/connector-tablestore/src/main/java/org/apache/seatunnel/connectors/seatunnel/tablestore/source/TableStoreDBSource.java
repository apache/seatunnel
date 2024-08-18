package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceReader.Context;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreConfig;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;
import org.apache.seatunnel.connectors.seatunnel.tablestore.exception.TablestoreConnectorException;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class TableStoreDBSource
        implements SeaTunnelSource<SeaTunnelRow, TableStoreDBSourceSplit, TableStoreDBSourceState>,
                SupportParallelism,
                SupportColumnProjection {

    private TablestoreOptions tablestoreOptions;
    private SeaTunnelRowType typeInfo;

    @Override
    public String getPluginName() {
        return "Tablestore";
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        List<CatalogTable> list = new ArrayList<>();
        String[] tables = this.tablestoreOptions.getTable().split(",");

        return SeaTunnelSource.super.getProducedCatalogTables();
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {

        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        TablestoreConfig.END_POINT.key(),
                        TablestoreConfig.INSTANCE_NAME.key(),
                        TablestoreConfig.ACCESS_KEY_ID.key(),
                        TablestoreConfig.ACCESS_KEY_SECRET.key(),
                        TablestoreConfig.TABLE.key());

        if (!result.isSuccess()) {
            throw new TablestoreConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SOURCE, result.getMsg()));
        }
        tablestoreOptions = new TablestoreOptions(pluginConfig);
        CatalogTableUtil.buildWithConfig(pluginConfig);
        typeInfo = CatalogTableUtil.buildWithConfig(pluginConfig).getSeaTunnelRowType();
    }

    @Override
    public Boundedness getBoundedness() {
        // 这里是流式处理，无边界
        return Boundedness.UNBOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.typeInfo;
    }

    @Override
    public SourceReader<SeaTunnelRow, TableStoreDBSourceSplit> createReader(Context readerContext)
            throws Exception {
        return new TableStoreDBSourceReader(readerContext, tablestoreOptions, typeInfo);
    }

    @Override
    public SourceSplitEnumerator<TableStoreDBSourceSplit, TableStoreDBSourceState> createEnumerator(
            org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<TableStoreDBSourceSplit>
                    enumeratorContext)
            throws Exception {
        return new TableStoreDBSourceSplitEnumerator(enumeratorContext, tablestoreOptions);
    }

    @Override
    public SourceSplitEnumerator<TableStoreDBSourceSplit, TableStoreDBSourceState>
            restoreEnumerator(
                    org.apache.seatunnel.api.source.SourceSplitEnumerator.Context<
                                    TableStoreDBSourceSplit>
                            enumeratorContext,
                    TableStoreDBSourceState checkpointState)
                    throws Exception {
        return new TableStoreDBSourceSplitEnumerator(
                enumeratorContext, tablestoreOptions, checkpointState);
    }
}
