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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.split;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.Serializable;

public abstract class AbstractParametersProvider implements JdbcParameterValuesProvider {

    protected final long minVal;
    protected final long maxVal;

    protected long batchSize;
    protected int batchNum;

    /**
     * NumericBetweenParametersProviderJdbc constructor.
     *
     * @param minVal the lower bound of the produced "from" values
     * @param maxVal the upper bound of the produced "to" values
     */
    public AbstractParametersProvider(long minVal, long maxVal) {
        checkArgument(minVal <= maxVal, "minVal must not be larger than maxVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
    }

    /**
     * NumericBetweenParametersProviderJdbc constructor.
     *
     * @param fetchSize the max distance between the produced from/to pairs
     * @param minVal    the lower bound of the produced "from" values
     * @param maxVal    the upper bound of the produced "to" values
     */
    public AbstractParametersProvider(long fetchSize, long minVal, long maxVal) {
        checkArgument(minVal <= maxVal, "minVal must not be larger than maxVal");
        this.minVal = minVal;
        this.maxVal = maxVal;
        ofBatchSize(fetchSize);
    }

    public void ofBatchSize(long batchSize) {
        checkArgument(batchSize > 0, "Batch size must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchSize > maxElemCount) {
            batchSize = maxElemCount;
        }
        this.batchSize = batchSize;
        this.batchNum = new Double(Math.ceil((double) maxElemCount / batchSize)).intValue();
    }

    public AbstractParametersProvider ofBatchNum(int batchNum) {
        checkArgument(batchNum > 0, "Batch number must be positive");

        long maxElemCount = (maxVal - minVal) + 1;
        if (batchNum > maxElemCount) {
            batchNum = (int) maxElemCount;
        }
        this.batchNum = batchNum;
        this.batchSize = new Double(Math.ceil((double) maxElemCount / batchNum)).longValue();
        return this;
    }

    public abstract Serializable[][] getParameterValues();
}
