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

import java.util.function.Predicate;
import java.util.function.Supplier;

public class MDCPredicate<T> implements Predicate<T> {
    private final Supplier<MDCContext> contextSupplier;
    private final Predicate<T> delegate;

    public MDCPredicate(Predicate<T> delegate) {
        this(MDCContext.current(), delegate);
    }

    public MDCPredicate(MDCContext context, Predicate<T> delegate) {
        this(() -> context, delegate);
    }

    public MDCPredicate(Supplier<MDCContext> contextSupplier, Predicate<T> delegate) {
        this.contextSupplier = contextSupplier;
        this.delegate = delegate;
    }

    @Override
    public boolean test(T t) {
        try (MDCContext ignored = contextSupplier.get().activate()) {
            return delegate.test(t);
        }
    }
}
