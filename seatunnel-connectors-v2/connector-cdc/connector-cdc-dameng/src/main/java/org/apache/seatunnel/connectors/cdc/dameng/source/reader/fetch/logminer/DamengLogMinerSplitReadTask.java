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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.logminer;

import static org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffset.NO_STOPPING_OFFSET;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.dameng.config.DamengSourceConfig;
import org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffset;
import org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.snapshot.DamengSnapshotFetchTask;

import io.debezium.DebeziumException;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.connector.dameng.DamengStreamingChangeEventSource;
import io.debezium.connector.dameng.logminer.LogContent;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
public class DamengLogMinerSplitReadTask extends DamengStreamingChangeEventSource {

    private final IncrementalSplit split;
    private final DamengOffsetContext offsetContext;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private final ErrorHandler errorHandler;
    private ChangeEventSourceContext context;

    public DamengLogMinerSplitReadTask(DamengOffsetContext offsetContext,
                                       DamengSourceConfig sourceConfig,
                                       DamengConnection connection,
                                       JdbcSourceEventDispatcher eventDispatcher,
                                       ErrorHandler errorHandler,
                                       DamengDatabaseSchema databaseSchema,
                                       IncrementalSplit split) {
        super(sourceConfig, connection, split.getTableIds(), eventDispatcher,
            errorHandler, Clock.SYSTEM, databaseSchema);
        this.split = split;
        this.offsetContext = offsetContext;
        this.eventDispatcher = eventDispatcher;
        this.errorHandler = errorHandler;
    }

    @Override
    public void execute(ChangeEventSourceContext context, DamengOffsetContext offsetContext) {
        this.context = context;
        super.execute(context, this.offsetContext);
    }

    @Override
    protected void handleEvent(DamengOffsetContext offsetContext, LogContent event) {
        super.handleEvent(offsetContext, event);
        // check do we need to stop for fetch logminer for snapshot split.
        if (isBoundedRead()) {
            LogMinerOffset currentLogMinerOffset =
                getLogMinerPosition(offsetContext.getOffset());
            // reach the high watermark, the logminer fetcher should be finished
            if (currentLogMinerOffset.isAtOrAfter(split.getStopOffset())) {
                // send logminer end event
                try {
                    eventDispatcher.dispatchWatermarkEvent(
                        offsetContext.getPartition(),
                        split,
                        currentLogMinerOffset,
                        WatermarkKind.END);
                } catch (InterruptedException e) {
                    log.error("Send signal event error.", e);
                    errorHandler.setProducerThrowable(
                        new DebeziumException("Error processing logminer signal event", e));
                }
                // tell fetcher the logminer task finished
                ((DamengSnapshotFetchTask.SnapshotScnSplitChangeEventSourceContext) context).finished();
            }
        }
    }

    private boolean isBoundedRead() {
        return !NO_STOPPING_OFFSET.equals(split.getStopOffset());
    }

    public static LogMinerOffset getLogMinerPosition(Map<String, ?> offset) {
        Map<String, String> offsetStrMap = new HashMap<>();
        for (Map.Entry<String, ?> entry : offset.entrySet()) {
            offsetStrMap.put(
                entry.getKey(),
                entry.getValue() == null ? null : entry.getValue().toString());
        }
        return new LogMinerOffset(offsetStrMap);
    }
}
