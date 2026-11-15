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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.fake.source.FakeSourceFactory;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * E2E-only source factory proving that the source factory context receives the whole job env block.
 * Behaves exactly like {@link FakeSourceFactory} but records the env probe value seen at factory
 * time.
 */
@AutoService(Factory.class)
public class EnvFakeSourceFactory extends FakeSourceFactory {

    public static final String IDENTIFIER = "EnvFakeSource";

    private static final AtomicReference<String> CAPTURED_ENV = new AtomicReference<>();

    public static void reset() {
        CAPTURED_ENV.set(null);
    }

    public static String capturedEnv() {
        return CAPTURED_ENV.get();
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        CAPTURED_ENV.set(EnvProbeSupport.probe(context.getEnvOptions()));
        return super.createSource(context);
    }
}
