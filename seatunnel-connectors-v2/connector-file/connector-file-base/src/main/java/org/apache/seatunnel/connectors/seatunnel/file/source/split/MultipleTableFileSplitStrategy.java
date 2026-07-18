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
package org.apache.seatunnel.connectors.seatunnel.file.source.split;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class MultipleTableFileSplitStrategy implements FileSplitStrategy, Closeable {

    private final Map<String, FileSplitStrategy> delegateStrategies;
    private final FileSplitStrategy fallbackStrategy;

    public MultipleTableFileSplitStrategy(Map<String, FileSplitStrategy> delegateStrategies) {
        this.delegateStrategies = Objects.requireNonNull(delegateStrategies, "delegateStrategies");
        this.fallbackStrategy = new DefaultFileSplitStrategy();
    }

    @Override
    public java.util.List<FileSourceSplit> split(String tableId, String filePath) {
        FileSplitStrategy delegate = delegateStrategies.get(tableId);
        if (delegate == null) {
            return fallbackStrategy.split(tableId, filePath);
        }
        return delegate.split(tableId, filePath);
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        Set<FileSplitStrategy> uniqueStrategies = new HashSet<>(delegateStrategies.values());
        for (FileSplitStrategy strategy : uniqueStrategies) {
            try {
                if (strategy instanceof Closeable) {
                    ((Closeable) strategy).close();
                    continue;
                }
                if (strategy instanceof AutoCloseable) {
                    ((AutoCloseable) strategy).close();
                }
            } catch (Exception e) {
                IOException current =
                        e instanceof IOException ? (IOException) e : new IOException(e);
                if (exception == null) {
                    exception = current;
                } else {
                    exception.addSuppressed(current);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
