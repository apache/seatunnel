/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.common.utils;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

/** A future which prevents completion by outside caller */
public class PassiveCompletableFuture<T> extends CompletableFuture<T> {

    public PassiveCompletableFuture() {}

    public PassiveCompletableFuture(java.util.concurrent.CompletableFuture<T> chainedFuture) {
        this(new CompletableFuture<>(chainedFuture));
    }

    public PassiveCompletableFuture(CompletableFuture<T> chainedFuture) {
        if (chainedFuture != null) {
            chainedFuture.whenComplete(
                    (r, t) -> {
                        if (t != null) {
                            internalCompleteExceptionally(t);
                        } else {
                            internalComplete(r);
                        }
                    });
        }
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        throw new UnsupportedOperationException(
                "This future can't be completed by an outside caller");
    }

    @Override
    public boolean complete(T value) {
        throw new UnsupportedOperationException(
                "This future can't be completed by an outside caller");
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        throw new UnsupportedOperationException(
                "This future can't be cancelled by an outside caller");
    }

    @Override
    public void obtrudeException(Throwable ex) {
        throw new UnsupportedOperationException(
                "This future can't be completed by an outside caller");
    }

    @Override
    public void obtrudeValue(T value) {
        throw new UnsupportedOperationException(
                "This future can't be completed by an outside caller");
    }

    private void internalComplete(T value) {
        super.complete(value);
    }

    private void internalCompleteExceptionally(Throwable ex) {
        super.completeExceptionally(ex);
    }
}
