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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSinkState;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commiter.IcebergCommitInfo;

import org.apache.iceberg.io.TaskWriter;
import org.apache.iceberg.io.WriteResult;

import lombok.SneakyThrows;

import java.io.IOException;
import java.util.Optional;

/** @Author: Liuli @Date: 2023/7/12 15:25 */
public class IcebergSinkWriter
        implements SinkWriter<SeaTunnelRow, IcebergCommitInfo, IcebergSinkState> {
    SeaTunnelRowDataTaskWriterFactory seaTunnelRowDataTaskWriterFactory;
    TaskWriter<SeaTunnelRow> seaTunnelRowTaskWriter;

    private final SinkWriter.Context context;

    private transient boolean isOpen = false;

    public IcebergSinkWriter(
            SeaTunnelRowDataTaskWriterFactory seaTunnelRowDataTaskWriterFactory,
            SinkWriter.Context context) {
        this.seaTunnelRowDataTaskWriterFactory = seaTunnelRowDataTaskWriterFactory;
        this.context = context;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        tryOpen();
        if (null == seaTunnelRowTaskWriter) {
            System.out.printf("Start to create task writer%n");
            seaTunnelRowTaskWriter = seaTunnelRowDataTaskWriterFactory.create();
        }
        seaTunnelRowTaskWriter.write(element);
    }

    @Override
    public Optional<IcebergCommitInfo> prepareCommit() throws IOException {
        if (null == seaTunnelRowTaskWriter) {
            return Optional.of(new IcebergCommitInfo(null));
        }
        WriteResult writeResult = seaTunnelRowTaskWriter.complete();
        seaTunnelRowTaskWriter = null;
        return Optional.of(new IcebergCommitInfo(writeResult));
    }

    @SneakyThrows
    @Override
    public void abortPrepare() {
        if (null != seaTunnelRowTaskWriter) {
            seaTunnelRowTaskWriter.abort();
        }
    }

    @Override
    public void close() throws IOException {
        if (null != seaTunnelRowDataTaskWriterFactory) seaTunnelRowDataTaskWriterFactory.close();
        if (null != seaTunnelRowTaskWriter) seaTunnelRowTaskWriter.close();
    }

    private void tryOpen() throws IOException {
        if (!isOpen) {
            seaTunnelRowDataTaskWriterFactory.initialize(context.getIndexOfSubtask(), 1);
            isOpen = true;
        }
    }
}
