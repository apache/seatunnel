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

package org.apache.seatunnel.engine.e2e.retrybackoff;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;

import com.google.auto.service.AutoService;

/**
 * Test-only factory whose single responsibility is to publish the always-failing fixture sink to
 * the engine's sink SPI.
 *
 * <p>{@link RetryBackoffFailingSink} is not a connector, so there is nothing to validate or evolve
 * here: no options are declared ({@link #optionRule()} is empty on purpose), no catalog or table
 * metadata is read, and every statement the retry-backoff test makes is recorded by {@link
 * RetryBackoffFailingSinkWriter}, never by this factory. The factory exists only so the engine can
 * discover the sink through {@code TableSinkFactory} and {@code @AutoService}, exactly like a
 * production sink.
 *
 * <p>Keep it that way: adding connector behavior (buffering, options, retry handling) would make
 * the fixture fail less plainly and would let the test pass or fail for reasons unrelated to the
 * engine retry backoff.
 */
@AutoService(Factory.class)
public class RetryBackoffFailingSinkFactory implements TableSinkFactory {

    public static final String IDENTIFIER = "RetryBackoffFail";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        return () -> new RetryBackoffFailingSink();
    }
}
