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

import java.util.function.Function;
import java.util.function.Supplier;

public class MDCFunction<T, R> implements Function<T, R> {
    private final Supplier<MDCContext> contextSupplier;
    protected final Function<T, R> delegate;

    public MDCFunction(Function<T, R> delegate) {
        this(MDCContext.current(), delegate);
    }

    public MDCFunction(MDCContext context, Function<T, R> delegate) {
        this(() -> context, delegate);
    }

    public MDCFunction(Supplier<MDCContext> contextSupplier, Function<T, R> delegate) {
        this.contextSupplier = contextSupplier;
        this.delegate = delegate;
    }

    @Override
    public R apply(T t) {
        try (MDCContext ignored = contextSupplier.get().activate()) {
            return delegate.apply(t);
        }
    }
}
