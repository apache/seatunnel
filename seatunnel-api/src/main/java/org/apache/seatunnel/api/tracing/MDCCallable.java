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

package org.apache.seatunnel.api.tracing;

import java.util.concurrent.Callable;
import java.util.function.Supplier;

/**
 * Callable that sets MDC context before calling the delegate and clears it afterwards.
 *
 * @param <V>
 */
public class MDCCallable<V> implements Callable<V> {
    private final Supplier<MDCContext> contextSupplier;
    private final Callable<V> delegate;

    public MDCCallable(Callable<V> delegate) {
        this(MDCContext.current(), delegate);
    }

    public MDCCallable(MDCContext context, Callable<V> delegate) {
        this(() -> context, delegate);
    }

    public MDCCallable(Supplier<MDCContext> contextSupplier, Callable<V> delegate) {
        this.contextSupplier = contextSupplier;
        this.delegate = delegate;
    }

    @Override
    public V call() throws Exception {
        try (MDCContext ignored = contextSupplier.get().activate()) {
            return delegate.call();
        }
    }
}
