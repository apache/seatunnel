/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.doris.source.reader;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorErrorCode;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.rest.PartitionDefinition;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;
import org.apache.seatunnel.connectors.doris.source.split.DorisSourceSplit;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class DorisSourceReader implements SourceReader<SeaTunnelRow, DorisSourceSplit> {

    private final Context context;
    private final DorisSourceConfig dorisSourceConfig;

    private final Queue<DorisSourceSplit> splitsQueue;
    private volatile boolean noMoreSplits;

    private DorisValueReader valueReader;

    private final Map<TablePath, DorisSourceTable> tables;

    public DorisSourceReader(
            Context context,
            DorisSourceConfig dorisSourceConfig,
            Map<TablePath, DorisSourceTable> tables) {
        this.splitsQueue = new ArrayDeque<>();
        this.context = context;
        this.dorisSourceConfig = dorisSourceConfig;
        this.tables = tables;
    }

    @Override
    public void open() throws Exception {}

    @Override
    public void close() throws IOException {
        if (valueReader != null) {
            valueReader.close();
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        synchronized (output.getCheckpointLock()) {
            DorisSourceSplit nextSplit = splitsQueue.poll();
            if (nextSplit != null) {
                PartitionDefinition partition = nextSplit.getPartitionDefinition();
                DorisSourceTable dorisSourceTable =
                        tables.get(TablePath.of(partition.getDatabase(), partition.getTable()));
                if (dorisSourceTable == null) {
                    throw new DorisConnectorException(
                            DorisConnectorErrorCode.SHOULD_NEVER_HAPPEN,
                            String.format(
                                    "the table '%s.%s' cannot be found in table_list of job configuration.",
                                    partition.getDatabase(), partition.getTable()));
                }
                valueReader = new DorisValueReader(partition, dorisSourceConfig, dorisSourceTable);
                while (valueReader.hasNext()) {
                    SeaTunnelRow record = valueReader.next();
                    output.collect(record);
                }
            }
            if (Boundedness.BOUNDED.equals(context.getBoundedness())
                    && noMoreSplits
                    && splitsQueue.isEmpty()) {
                // signal to the source that we have reached the end of the data.
                log.info("Closed the bounded Doris source");
                context.signalNoMoreElement();
            }
        }
    }

    @Override
    public List<DorisSourceSplit> snapshotState(long checkpointId) throws Exception {
        return new ArrayList<>(splitsQueue);
    }

    @Override
    public void addSplits(List<DorisSourceSplit> splits) {
        this.splitsQueue.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplits = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
