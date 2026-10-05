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

package org.apache.seatunnel.benchmark;

/** Immutable parameters for one full-pipeline benchmark invocation. */
public final class PipelineBenchmarkOptions {

    private final long totalRows;
    private final long offeredRatePerSecond;
    private final int parallelism;
    private final int payloadSize;
    private final int transformOperations;

    public PipelineBenchmarkOptions(
            long totalRows,
            long offeredRatePerSecond,
            int parallelism,
            int payloadSize,
            int transformOperations) {
        if (totalRows <= 0) {
            throw new IllegalArgumentException("totalRows must be greater than zero");
        }
        if (offeredRatePerSecond < 0) {
            throw new IllegalArgumentException("offeredRatePerSecond must not be negative");
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException("parallelism must be greater than zero");
        }
        if (payloadSize < 0) {
            throw new IllegalArgumentException("payloadSize must not be negative");
        }
        if (transformOperations < 0) {
            throw new IllegalArgumentException("transformOperations must not be negative");
        }
        this.totalRows = totalRows;
        this.offeredRatePerSecond = offeredRatePerSecond;
        this.parallelism = parallelism;
        this.payloadSize = payloadSize;
        this.transformOperations = transformOperations;
    }

    public long getTotalRows() {
        return totalRows;
    }

    public long getOfferedRatePerSecond() {
        return offeredRatePerSecond;
    }

    public int getParallelism() {
        return parallelism;
    }

    public int getPayloadSize() {
        return payloadSize;
    }

    public int getTransformOperations() {
        return transformOperations;
    }
}
