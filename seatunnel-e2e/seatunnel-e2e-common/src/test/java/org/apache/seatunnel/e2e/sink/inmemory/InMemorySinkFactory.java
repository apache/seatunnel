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

package org.apache.seatunnel.e2e.sink.inmemory;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

import java.util.List;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@AutoService(Factory.class)
public class InMemorySinkFactory
        implements TableSinkFactory<
                SeaTunnelRow, InMemoryState, InMemoryCommitInfo, InMemoryAggregatedCommitInfo> {

    public static final Option<Boolean> THROW_EXCEPTION =
            Options.key("throw_exception").booleanType().defaultValue(false);

    public static final Option<Boolean> THROW_OUT_OF_MEMORY =
            Options.key("throw_out_of_memory").booleanType().defaultValue(false);
    public static final Option<Boolean> CHECKPOINT_SLEEP =
            Options.key("checkpoint_sleep").booleanType().defaultValue(false);

    public static final Option<Boolean> THROW_EXCEPTION_OF_COMMITTER =
            Options.key("throw_exception_of_committer").booleanType().defaultValue(false);
    public static final Option<String> ASSERT_OPTIONS_KEY =
            Options.key("assert_options_key").stringType().noDefaultValue();
    public static final Option<String> ASSERT_OPTIONS_VALUE =
            Options.key("assert_options_value").stringType().noDefaultValue();

    public static final Option<List<String>> THROW_RUNTIME_EXCEPTION_LIST =
            Options.key("throw_runtime_exception_list").listType().noDefaultValue();

    @Override
    public String factoryIdentifier() {
        return "InMemory";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        THROW_EXCEPTION,
                        THROW_OUT_OF_MEMORY,
                        CHECKPOINT_SLEEP,
                        THROW_EXCEPTION_OF_COMMITTER,
                        ASSERT_OPTIONS_KEY,
                        ASSERT_OPTIONS_VALUE)
                .build();
    }

    @Override
    public TableSink<SeaTunnelRow, InMemoryState, InMemoryCommitInfo, InMemoryAggregatedCommitInfo>
            createSink(TableSinkFactoryContext context) {
        if (context.getOptions().getOptional(ASSERT_OPTIONS_KEY).isPresent()) {
            String key = context.getOptions().get(ASSERT_OPTIONS_KEY);
            String value = context.getOptions().get(ASSERT_OPTIONS_VALUE);
            checkArgument(
                    key.equals(value),
                    String.format(
                            "assert key and value not match! key = %s, value = %s", key, value));
        }
        return () -> new InMemorySink(context.getCatalogTable(), context.getOptions());
    }
}
