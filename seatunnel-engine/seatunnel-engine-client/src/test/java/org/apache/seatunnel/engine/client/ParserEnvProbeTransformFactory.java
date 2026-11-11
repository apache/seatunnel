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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import com.google.auto.service.AutoService;

import java.util.Collections;
import java.util.List;

/** Test-only transform factory that captures the env options from the factory context. */
@AutoService(Factory.class)
public class ParserEnvProbeTransformFactory implements TableTransformFactory {

    public static final String IDENTIFIER = "ParserEnvProbeTransform";

    private static volatile ReadonlyConfig capturedEnvOptions;

    public static void reset() {
        capturedEnvOptions = null;
    }

    public static ReadonlyConfig capturedEnvOptions() {
        return capturedEnvOptions;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder().build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        capturedEnvOptions = context.getEnvOptions();
        CatalogTable input = context.getCatalogTables().get(0);
        return () -> new ParserEnvProbeTransform(input);
    }

    private static final class ParserEnvProbeTransform implements SeaTunnelTransform<SeaTunnelRow> {

        private static final long serialVersionUID = 1L;

        private final CatalogTable catalogTable;

        private ParserEnvProbeTransform(CatalogTable catalogTable) {
            this.catalogTable = catalogTable;
        }

        @Override
        public String getPluginName() {
            return IDENTIFIER;
        }

        @Override
        public CatalogTable getProducedCatalogTable() {
            return catalogTable;
        }

        @Override
        public List<CatalogTable> getProducedCatalogTables() {
            return Collections.singletonList(catalogTable);
        }
    }
}
