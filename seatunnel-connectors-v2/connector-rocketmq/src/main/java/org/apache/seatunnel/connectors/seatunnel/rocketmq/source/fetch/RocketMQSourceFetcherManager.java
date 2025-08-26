package org.apache.seatunnel.connectors.seatunnel.rocketmq.source.fetch;

import org.apache.seatunnel.connectors.seatunnel.common.source.reader.RecordsWithSplitIds;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SingleThreadFetcherManager;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcher;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.fetcher.SplitFetcherTask;
import org.apache.seatunnel.connectors.seatunnel.common.source.reader.splitreader.SplitReader;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.OffsetCommitCallback;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.RocketMQPartitionSplit;
import org.apache.seatunnel.connectors.seatunnel.rocketmq.source.RocketMQPartitionSplitReader;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import lombok.SneakyThrows;

import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.function.Supplier;

/**
 * @author 02211659 bianxiang
 * @date 2025-08-19 10:06:05
 */
public class RocketMQSourceFetcherManager
        extends SingleThreadFetcherManager<MessageExt, RocketMQPartitionSplit> {

    public RocketMQSourceFetcherManager(
            BlockingQueue<RecordsWithSplitIds<MessageExt>> elementsQueue,
            Supplier<SplitReader<MessageExt, RocketMQPartitionSplit>> splitReaderSupplier) {
        super(elementsQueue, splitReaderSupplier);
    }

    public void commitOffsets(
            Map<MessageQueue, Long> committedOffsets, OffsetCommitCallback callback) {
        if (committedOffsets.isEmpty()) {
            return;
        }

        SplitFetcher<MessageExt, RocketMQPartitionSplit> splitFetcher = fetchers.get(0);
        if (splitFetcher != null) {
            commit(splitFetcher, committedOffsets, callback);
        } else {
            splitFetcher = createSplitFetcher();
            commit(splitFetcher, committedOffsets, callback);
            startFetcher(splitFetcher);
        }
    }

    private void commit(
            SplitFetcher<MessageExt, RocketMQPartitionSplit> splitFetcher,
            Map<MessageQueue, Long> committedOffsets,
            OffsetCommitCallback callback) {
        RocketMQPartitionSplitReader rocketMQReader =
                (RocketMQPartitionSplitReader) splitFetcher.getSplitReader();

        splitFetcher.addTask(
                new SplitFetcherTask() {
                    @SneakyThrows
                    @Override
                    public void run() {
                        rocketMQReader.notifyCheckpointComplete(committedOffsets, callback);
                    }

                    @Override
                    public void wakeUp() {}
                });
    }
}
