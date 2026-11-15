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

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;

import com.google.auto.service.AutoService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** E2E-only transform factory proving the transform factory context receives the whole env. */
@AutoService(Factory.class)
public class EnvProbeTransformFactory implements TableTransformFactory {

    public static final String IDENTIFIER = "EnvProbeTransform";

    public static final String ENV_PROBE_FIELD = "env_probe";

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
        // Recommended pattern: read env in the factory, copy it into the serializable transform
        // object that the job carries to the workers.
        String probeValue = EnvProbeSupport.probe(context.getEnvOptions());
        return () -> new EnvProbeTransform(context.getCatalogTables().get(0), probeValue);
    }

    /** Appends one {@code env_probe} column carrying the env value captured at factory time. */
    public static final class EnvProbeTransform extends AbstractCatalogSupportFlatMapTransform {

        private static final long serialVersionUID = 1L;

        private final String probeValue;

        public EnvProbeTransform(CatalogTable inputCatalogTable, String probeValue) {
            super(inputCatalogTable);
            this.probeValue = probeValue;
        }

        @Override
        protected org.apache.seatunnel.api.table.catalog.TableIdentifier
                transformTableIdentifier() {
            return inputCatalogTable.getTableId().copy();
        }

        @Override
        public String getPluginName() {
            return IDENTIFIER;
        }

        @Override
        protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
            Object[] fields = Arrays.copyOf(inputRow.getFields(), inputRow.getArity() + 1);
            fields[inputRow.getArity()] = probeValue;
            return Collections.singletonList(new SeaTunnelRow(fields));
        }

        @Override
        protected TableSchema transformTableSchema() {
            List<Column> columns = new ArrayList<>(inputCatalogTable.getTableSchema().getColumns());
            columns.add(
                    PhysicalColumn.of(ENV_PROBE_FIELD, BasicType.STRING_TYPE, 0, true, null, null));
            TableSchema.Builder builder = TableSchema.builder().columns(columns);
            PrimaryKey primaryKey = inputCatalogTable.getTableSchema().getPrimaryKey();
            if (primaryKey != null) {
                builder.primaryKey(primaryKey.copy());
            }
            inputCatalogTable
                    .getTableSchema()
                    .getConstraintKeys()
                    .forEach(constraintKey -> builder.constraintKey(constraintKey.copy()));
            return builder.build();
        }
    }
}
