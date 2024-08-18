package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.tablestore.config.TablestoreOptions;

import com.alicloud.openservices.tablestore.SyncClient;
import com.alicloud.openservices.tablestore.TunnelClient;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.CreateTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DeleteTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelRequest;
import com.alicloud.openservices.tablestore.model.tunnel.DescribeTunnelResponse;
import com.alicloud.openservices.tablestore.model.tunnel.TunnelType;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorker;
import com.alicloud.openservices.tablestore.tunnel.worker.TunnelWorkerConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedDeque;

@Slf4j
public class TableStoreDBSourceReader
        implements SourceReader<SeaTunnelRow, TableStoreDBSourceSplit> {

    protected SourceReader.Context context;
    protected TablestoreOptions tablestoreOptions;
    protected SeaTunnelRowType seaTunnelRowType;
    Queue<TableStoreDBSourceSplit> pendingSplits = new ConcurrentLinkedDeque<>();
    private SyncClient client;
    private volatile boolean noMoreSplit;
    private TunnelClient tunnelClient;

    public TableStoreDBSourceReader(
            SourceReader.Context context,
            TablestoreOptions options,
            SeaTunnelRowType seaTunnelRowType) {

        this.context = context;
        this.tablestoreOptions = options;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public void open() throws Exception {
        client =
                new SyncClient(
                        tablestoreOptions.getEndpoint(),
                        tablestoreOptions.getAccessKeyId(),
                        tablestoreOptions.getAccessKeySecret(),
                        tablestoreOptions.getInstanceName());
        tunnelClient =
                new TunnelClient(
                        tablestoreOptions.getEndpoint(),
                        tablestoreOptions.getAccessKeyId(),
                        tablestoreOptions.getAccessKeySecret(),
                        tablestoreOptions.getInstanceName());
    }

    @Override
    public void close() throws IOException {
        // client
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            TableStoreDBSourceSplit split = pendingSplits.poll();
            if (Objects.nonNull(split)) {
                read(split, output);
            }
            /*if (split == null) {
                log.info(
                        "TableStore Source Reader [{}] waiting for splits",
                        context.getIndexOfSubtask());
            }*/
            if (noMoreSplit) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded tablestore source");
                context.signalNoMoreElement();
                Thread.sleep(2000L);
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    private void read(TableStoreDBSourceSplit split, Collector<SeaTunnelRow> output) {
        String tunnelId = getTunel(split);
        TableStoreProcessor processor =
                new TableStoreProcessor(split.getTableName(), split.getPrimaryKey(), output);
        TunnelWorkerConfig workerConfig = new TunnelWorkerConfig(processor);
        // 配置TunnelWorker，并启动自动化的数据处理任务。
        TunnelWorker worker = new TunnelWorker(tunnelId, tunnelClient, workerConfig);
        try {
            worker.connectAndWorking();
        } catch (Exception e) {
            log.error("Start OTS tunnel failed.", e);
            worker.shutdown();
        }
    }

    public String getTunel(TableStoreDBSourceSplit split) {
        deleteTunel(split);
        String tunnelId = null;
        String tunnelName = split.getTableName() + "_migration2aws_tunnel4" + split.getSplitId();

        try {
            DescribeTunnelRequest drequest = new DescribeTunnelRequest("test", tunnelName);
            DescribeTunnelResponse dresp = tunnelClient.describeTunnel(drequest);
            tunnelId = dresp.getTunnelInfo().getTunnelId();
        } catch (Exception be) {
            CreateTunnelRequest crequest =
                    new CreateTunnelRequest(
                            split.getTableName(), tunnelName, TunnelType.valueOf("BaseAndStream"));
            CreateTunnelResponse cresp = tunnelClient.createTunnel(crequest);
            // tunnelId用于后续TunnelWorker的初始化，该值也可以通过ListTunnel或者DescribeTunnel获取。
            tunnelId = cresp.getTunnelId();
        }
        log.info("Tunnel found, Id: " + tunnelId);
        return tunnelId;
    }

    public void deleteTunel(TableStoreDBSourceSplit split) {
        String tunnelName = split.getTableName() + "_migration2aws_tunnel4" + split.getSplitId();
        try {
            DeleteTunnelRequest drequest =
                    new DeleteTunnelRequest(split.getTableName(), tunnelName);
            DeleteTunnelResponse dresp = tunnelClient.deleteTunnel(drequest);
            log.info("Tunnel has been deleted: " + dresp.toString());
        } catch (Exception be) {
            log.warn("Tunnel deletion failed due to not found: " + tunnelName);
        }
    }

    @Override
    public List<TableStoreDBSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<TableStoreDBSourceSplit> splits) {
        this.pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader [{}] received noMoreSplit event.", context.getIndexOfSubtask());
        noMoreSplit = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
