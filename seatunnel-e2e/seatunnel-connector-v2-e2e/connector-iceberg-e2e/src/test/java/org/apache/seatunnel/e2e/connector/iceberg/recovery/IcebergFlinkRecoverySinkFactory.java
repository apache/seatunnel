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

package org.apache.seatunnel.e2e.connector.iceberg.recovery;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSink;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.IcebergSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitInfo;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergAggregatedCommitter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit.IcebergCommitInfo;

import java.io.IOException;
import java.net.URI;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/** Test-only one-shot gates immediately before/after the real Iceberg publication. */
public class IcebergFlinkRecoverySinkFactory extends IcebergSinkFactory {
    public static final String NAME = "IcebergFlinkRecovery";

    @Override
    public String factoryIdentifier() {
        return NAME;
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        IcebergSink original = (IcebergSink) super.createSink(context).createSink();
        return () -> new RecoverySink(context.getOptions(), original.getWriteCatalogTable().get());
    }

    public static class RecoverySink extends IcebergSink {
        private final IcebergSinkConfig config;
        private final CatalogTable table;

        public RecoverySink(ReadonlyConfig options, CatalogTable table) {
            super(options, table);
            this.config = new IcebergSinkConfig(options);
            this.table = table;
        }

        @Override
        public String getPluginName() {
            return NAME;
        }

        @Override
        public Optional<SinkAggregatedCommitter<IcebergCommitInfo, IcebergAggregatedCommitInfo>>
                createAggregatedCommitter() {
            return Optional.of(new GatedCommitter(config, table));
        }
    }

    public static class GatedCommitter extends IcebergAggregatedCommitter {
        private final String root;

        public GatedCommitter(IcebergSinkConfig config, CatalogTable table) {
            super(config, table);
            root =
                    Paths.get(URI.create(config.getCatalogProps().get("warehouse")))
                            .getParent()
                            .toString();
        }

        @Override
        public List<IcebergAggregatedCommitInfo> commit(List<IcebergAggregatedCommitInfo> infos)
                throws IOException {
            boolean data =
                    infos.stream()
                            .flatMap(info -> info.getCommitInfos().stream())
                            .filter(info -> info.getResults() != null)
                            .flatMap(info -> info.getResults().stream())
                            .anyMatch(
                                    result ->
                                            (result.getDataFiles() != null
                                                            && !result.getDataFiles().isEmpty())
                                                    || (result.getDeleteFiles() != null
                                                            && !result.getDeleteFiles().isEmpty()));
            Path directory = Paths.get(root);
            boolean claimed = false;
            if (data) {
                try {
                    Files.createFile(directory.resolve("publication-claimed"));
                    claimed = true;
                } catch (FileAlreadyExistsException replay) {
                    // The persisted marker only disables fault injection, never real commit/replay.
                }
            }
            boolean before =
                    claimed && "before".equals(Files.readString(directory.resolve("boundary")));
            if (claimed) {
                List<String> files = new ArrayList<>();
                for (IcebergAggregatedCommitInfo info : infos) {
                    for (IcebergCommitInfo writer : info.getCommitInfos()) {
                        if (writer.getResults() != null) {
                            writer.getResults()
                                    .forEach(
                                            result -> {
                                                if (result.getDataFiles() != null) {
                                                    result.getDataFiles().stream()
                                                            .filter(file -> file.recordCount() > 0)
                                                            .forEach(
                                                                    file ->
                                                                            files.add(
                                                                                    file
                                                                                            .location()));
                                                }
                                                if (result.getDeleteFiles() != null) {
                                                    result.getDeleteFiles().stream()
                                                            .filter(file -> file.recordCount() > 0)
                                                            .forEach(
                                                                    file ->
                                                                            files.add(
                                                                                    file
                                                                                            .location()));
                                                }
                                            });
                        }
                    }
                }
                Files.write(directory.resolve("pending-files"), files);
            }
            if (before) {
                gate(directory);
            }
            List<IcebergAggregatedCommitInfo> retry = super.commit(infos);
            if (claimed && !before) {
                gate(directory);
            }
            return retry;
        }

        private static void gate(Path directory) throws IOException {
            Files.createFile(directory.resolve("publication-gated"));
            long deadline = System.nanoTime() + TimeUnit.MINUTES.toNanos(3);
            while (System.nanoTime() < deadline) {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Publication gate interrupted", e);
                }
            }
            throw new IOException("Expected TaskManager crash did not reach publication gate");
        }
    }
}
