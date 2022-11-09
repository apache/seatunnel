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

package org.apache.seatunnel.api.table.factory;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.sink.SinkCommonOptions;
import org.apache.seatunnel.api.table.connector.TableSink;

/**
 * This is an SPI interface, used to create {@link TableSink}. Each plugin need to have it own implementation.
 *
 * @param <IN>                    row type
 * @param <StateT>                state type
 * @param <CommitInfoT>           commit info type
 * @param <AggregatedCommitInfoT> aggregated commit info type
 */
public interface TableSinkFactory<IN, StateT, CommitInfoT, AggregatedCommitInfoT> extends Factory {

    /**
     * We will never use this method now. So gave a default implement and return null.
     *
     * @param context TableFactoryContext
     * @return
     */
    default TableSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT> createSink(TableFactoryContext context) {
        throw new UnsupportedOperationException("unsupported now");
    }

    /**
     * This method is called by SeaTunnel Web to get the full option rule of a sink.
     * Please don't overwrite this method.
     * @return
     */
    default OptionRule fullOptionRule() {
        OptionRule optionRule = optionRule();
        if (optionRule == null) {
            throw new FactoryException("OptionRule can not be null");
        }

        OptionRule sinkCommonOptionRule =
            OptionRule.builder().optional(SinkCommonOptions.SOURCE_TABLE_NAME, SinkCommonOptions.PARALLELISM).build();
        optionRule.getOptionalOptions().addAll(sinkCommonOptionRule.getOptionalOptions());
        return optionRule;
    }
}
