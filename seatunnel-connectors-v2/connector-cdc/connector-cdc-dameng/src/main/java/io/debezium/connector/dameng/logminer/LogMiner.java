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

package io.debezium.connector.dameng.logminer;

import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.Scn;
import lombok.Builder;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.sql.SQLException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;

@Slf4j
@Builder
public class LogMiner implements Closeable {
    private static final int ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER = -2846;
    @NonNull private final DamengConnection connection;
    @NonNull private final Scn previousScn;
    @NonNull private final String[] schemas;
    private final String[] tables;
    private Scn fileFirstScn;
    private Scn fileNextScn;
    private Scn recordNextScn;

    public void startLoop(
            Supplier<Boolean> runningContext, long sleepMillis, Consumer<LogContent> consumer) {
        try {
            init();

            while (runningContext.get()) {
                Thread.sleep(sleepMillis);
                scanAndPush(consumer);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    public void init() throws SQLException {
        if (fileFirstScn == null) {
            fileFirstScn = connection.getFirstScn(previousScn, previousScn);
            fileNextScn = fileFirstScn;
            log.info("Initialize scan v$archived_log file first scn: {}", fileFirstScn);
        }
        if (recordNextScn == null) {
            recordNextScn = previousScn;
            log.info("Initialize scan log record start scn: {}", recordNextScn);
        }
    }

    public void scanAndPush(@NonNull Consumer<LogContent> consumer) throws SQLException {
        Optional<LogFile> logFile = connection.getLogFile(fileFirstScn, fileNextScn);
        if (!logFile.isPresent()) {
            return;
        }
        fileNextScn = logFile.get().getNextScn();
        log.debug("Update scan v$archived_log file next scn to {}", fileNextScn);

        try {
            connection.endLogMiner();
            log.debug("End current session log miner");
        } catch (SQLException e) {
            if (ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER != e.getErrorCode()) {
                throw e;
            }
        }

        connection.addLogFile(logFile.get());
        log.debug("Add log file to current session log miner, logFile: {}", logFile.get());

        connection.startLogMiner(recordNextScn);
        log.debug("Start current session log miner for start scn: {}", recordNextScn);

        AtomicBoolean hasNextRows = new AtomicBoolean();
        do {
            hasNextRows.set(false);
            connection.readLogContent(
                    recordNextScn,
                    schemas,
                    tables,
                    (Consumer<LogContent>)
                            logContent -> {
                                consumer.accept(logContent);

                                recordNextScn = logContent.getScn();
                                log.debug("Update scan log record next scn to {}", recordNextScn);

                                hasNextRows.set(true);
                            });
        } while (hasNextRows.get());
    }

    @Override
    public void close() {
        try {
            connection.endLogMiner();
            log.info("End current session log miner");
        } catch (SQLException e) {
            if (ERROR_CODE_FOR_NOT_ACTIVE_LOGMINER != e.getErrorCode()) {
                throw new RuntimeException(e);
            }
        }
    }
}
