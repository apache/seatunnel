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

import static com.google.common.base.Preconditions.checkState;

import java.io.Serializable;
import java.sql.Date;

public class JdbcDateBetweenParametersProvider extends AbstractParametersProvider {

    /**
     * JdbcDateBetweenParametersProvider constructor.
     *
     * @param minVal the lower bound of the produced "from" values
     * @param maxVal the upper bound of the produced "to" values
     */
    public JdbcDateBetweenParametersProvider(long minVal, long maxVal) {
        super(minVal, maxVal);
    }

    /**
     * JdbcDateBetweenParametersProvider constructor.
     *
     * @param fetchSize the max distance between the produced from/to pairs
     * @param minVal    the lower bound of the produced "from" values
     * @param maxVal    the upper bound of the produced "to" values
     */
    public JdbcDateBetweenParametersProvider(long fetchSize, long minVal, long maxVal) {
        super(fetchSize, minVal, maxVal);
    }

    @Override
    public Serializable[][] getParameterValues() {
        checkState(
            batchSize > 0,
            "Batch size and batch number must be positive. Have you called `ofBatchSize` or `ofBatchNum`?");

        long maxElemCount = (maxVal - minVal) + 1;
        long bigBatchNum = maxElemCount - (batchSize - 1) * batchNum;

        Serializable[][] parameters = new Serializable[batchNum][2];
        long start = minVal;
        for (int i = 0; i < batchNum; i++) {
            long end = start + batchSize - 1 - (i >= bigBatchNum ? 1 : 0);
            parameters[i] = new Date[]{new Date(start), new Date(end)};
            start = end + 1;
        }
        return parameters;
    }
}
