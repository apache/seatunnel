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

package org.apache.seatunnel.api.datasource;

import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Abstract base class for {@link DataSourceProvider} implementations.
 *
 * <p>This class provides thread-safe caching for data source mappers, using double-checked locking
 * with {@link ReentrantLock} for better performance in high-concurrency scenarios.
 *
 * <p>Subclasses only need to implement {@link #createDataSourceMappers()} to provide the mapper
 * list, which will be cached after the first call.
 *
 * <h2>Usage Example</h2>
 *
 * <pre>
 * &#64;AutoService(DataSourceProvider.class)
 * public class MyDataSourceProvider extends AbstractDataSourceProvider {
 *
 *     &#64;Override
 *     public String kind() {
 *         return "my-provider";
 *     }
 *
 *     &#64;Override
 *     protected List&lt;DataSourceMapper&gt; createDataSourceMappers() {
 *         return Arrays.asList(new MyMapper1(), new MyMapper2());
 *     }
 *
 *     // implement init() and close()
 * }
 * </pre>
 */
public abstract class AbstractDataSourceProvider implements DataSourceProvider {

    private volatile List<DataSourceMapper> cachedMappers;
    private final Lock lock = new ReentrantLock();

    @Override
    public final List<DataSourceMapper> dataSourceMappers() {
        if (cachedMappers == null) {
            lock.lock();
            try {
                if (cachedMappers == null) {
                    cachedMappers = createDataSourceMappers();
                }
            } finally {
                lock.unlock();
            }
        }
        return cachedMappers;
    }

    /**
     * Creates the list of data source mappers supported by this provider.
     *
     * <p>This method is called once (lazily) when {@link #dataSourceMappers()} is first invoked.
     * Subclasses should implement this method to provide their mappers.
     *
     * <p>The returned list is cached and reused for subsequent calls. Implementations should return
     * an immutable or thread-safe list if mappers will be shared across threads.
     *
     * @return list of supported data source mappers
     */
    protected abstract List<DataSourceMapper> createDataSourceMappers();
}
