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

package org.apache.seatunnel.e2e.source.checkpointable;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import com.google.auto.service.AutoService;

import java.io.Serializable;

@AutoService(Factory.class)
public class CheckpointableSequenceSourceFactory implements TableSourceFactory {

    public static final Option<Long> START_OFFSET =
            Options.key("start_offset").longType().defaultValue(0L);

    public static final Option<Long> END_OFFSET =
            Options.key("end_offset").longType().defaultValue(Long.MAX_VALUE);

    public static final Option<Integer> SPLIT_NUM =
            Options.key("split_num").intType().defaultValue(1);

    public static final Option<Integer> RECORDS_PER_POLL =
            Options.key("records_per_poll").intType().defaultValue(1);

    public static final Option<Long> EMIT_INTERVAL_MS =
            Options.key("emit_interval_ms").longType().defaultValue(50L);

    @Override
    public String factoryIdentifier() {
        return "CheckpointableSequenceSource";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(START_OFFSET, END_OFFSET, SPLIT_NUM, RECORDS_PER_POLL, EMIT_INTERVAL_MS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new CheckpointableSequenceSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return CheckpointableSequenceSource.class;
    }
}
