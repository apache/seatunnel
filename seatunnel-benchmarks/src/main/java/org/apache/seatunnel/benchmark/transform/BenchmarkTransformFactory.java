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

package org.apache.seatunnel.benchmark.transform;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import com.google.auto.service.AutoService;

/** Factory for {@link BenchmarkTransform}. */
@AutoService(Factory.class)
public final class BenchmarkTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return BenchmarkTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .optional(
                        BenchmarkTransformOptions.OPERATIONS_PER_ROW,
                        Conditions.greaterOrEqual(BenchmarkTransformOptions.OPERATIONS_PER_ROW, 0))
                .optional(BenchmarkTransformOptions.COPY_ROW)
                .build();
    }

    @Override
    public TableTransform<SeaTunnelRow> createTransform(TableTransformFactoryContext context) {
        if (context.getCatalogTables().size() != 1) {
            throw new IllegalArgumentException(
                    "BenchmarkTransform requires exactly one input table");
        }
        return () ->
                new BenchmarkTransform(
                        context.getCatalogTables().get(0),
                        context.getOptions().get(BenchmarkTransformOptions.OPERATIONS_PER_ROW),
                        context.getOptions().get(BenchmarkTransformOptions.COPY_ROW));
    }
}
