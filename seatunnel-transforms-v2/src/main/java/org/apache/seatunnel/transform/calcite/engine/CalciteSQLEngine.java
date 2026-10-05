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

package org.apache.seatunnel.transform.calcite.engine;

import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.seatunnel.shade.org.apache.calcite.avatica.util.Casing;
import org.apache.seatunnel.shade.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.seatunnel.shade.org.apache.calcite.rel.RelNode;
import org.apache.seatunnel.shade.org.apache.calcite.rel.RelRoot;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.seatunnel.shade.org.apache.calcite.runtime.Bindable;
import org.apache.seatunnel.shade.org.apache.calcite.schema.SchemaPlus;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlNode;
import org.apache.seatunnel.shade.org.apache.calcite.sql.parser.SqlParser;
import org.apache.seatunnel.shade.org.apache.calcite.tools.FrameworkConfig;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Frameworks;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Planner;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Programs;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.transform.calcite.adapter.SeaTunnelDataContext;
import org.apache.seatunnel.transform.calcite.adapter.SeaTunnelScannableTable;
import org.apache.seatunnel.transform.calcite.type.CalciteValueConverter;
import org.apache.seatunnel.transform.calcite.type.OutputRowTypeDeriver;
import org.apache.seatunnel.transform.calcite.udf.BuiltinFunctions;
import org.apache.seatunnel.transform.calcite.udf.CalciteUdfContext;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Core Calcite SQL engine that parses, validates, compiles and executes SQL against a single
 * SeaTunnel row. The execution plan is compiled once and reused for each row.
 */
@Slf4j
public class CalciteSQLEngine implements AutoCloseable {

    private final String sql;
    private final String tableName;
    private final SeaTunnelRowType inputRowType;

    private SchemaPlus rootSchema;
    private SeaTunnelScannableTable scannableTable;
    private Bindable<Object[]> bindable;

    @Getter private SeaTunnelRowType outputRowType;
    private BuiltinFunctions builtinFunctions;
    private RelDataTypeFactory typeFactory;
    private SeaTunnelDataContext dataContext;

    public CalciteSQLEngine(String sql, String tableName, SeaTunnelRowType inputRowType) {
        this.sql = sql;
        this.tableName = tableName;
        this.inputRowType = inputRowType;
    }

    /**
     * Initializes the engine: parses, validates and compiles the SQL into a reusable Bindable plan.
     * Must be called before {@link #execute(SeaTunnelRow)}.
     */
    @SuppressWarnings("unchecked")
    public void init() {
        Planner planner = null;
        try {
            rootSchema = Frameworks.createRootSchema(true);
            typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

            scannableTable =
                    CalciteSchemaFactory.registerTable(
                            rootSchema, tableName, inputRowType, typeFactory);

            builtinFunctions = new BuiltinFunctions();
            builtinFunctions.discoverAndRegister(rootSchema);
            dataContext = new SeaTunnelDataContext(rootSchema, typeFactory);

            SqlParser.Config parserConfig =
                    SqlParser.config()
                            .withCaseSensitive(false)
                            .withQuotedCasing(Casing.UNCHANGED)
                            .withUnquotedCasing(Casing.UNCHANGED);

            FrameworkConfig frameworkConfig =
                    Frameworks.newConfigBuilder()
                            .defaultSchema(rootSchema)
                            .parserConfig(parserConfig)
                            .programs(Programs.standard())
                            .build();

            planner = Frameworks.getPlanner(frameworkConfig);
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);

            RelRoot relRoot = planner.rel(validated);
            RelNode logicalPlan = relRoot.rel;
            RelDataType validatedRowType = relRoot.validatedRowType;

            RelNode enumerablePlan =
                    Programs.standard()
                            .run(
                                    logicalPlan.getCluster().getPlanner(),
                                    logicalPlan,
                                    logicalPlan
                                            .getTraitSet()
                                            .replace(EnumerableConvention.INSTANCE),
                                    Collections.emptyList(),
                                    Collections.emptyList());

            bindable =
                    EnumerableInterpretable.toBindable(
                            Collections.emptyMap(),
                            null,
                            (EnumerableRel) enumerablePlan,
                            EnumerableRel.Prefer.ARRAY);

            outputRowType =
                    new OutputRowTypeDeriver(inputRowType).derive(validated, validatedRowType);

            log.info("Calcite SQL engine initialized successfully for table '{}'", tableName);
        } catch (TransformException e) {
            close();
            throw e;
        } catch (Exception e) {
            close();
            throw TransformCommonError.sqlExpressionError(sql, e);
        } finally {
            if (planner != null) {
                planner.close();
            }
        }
    }

    /**
     * Executes the pre-compiled SQL plan against a single input row. Returns a list of output rows
     * (typically 1, but UNNEST may produce 0-N rows).
     */
    public List<SeaTunnelRow> execute(SeaTunnelRow inputRow) {
        Object[] calciteRow = toCalciteRow(inputRow);
        scannableTable.setCurrentRow(calciteRow);
        dataContext.refreshNow();

        List<SeaTunnelRow> results = new ArrayList<>();
        try (CalciteUdfContext.Scope ignored =
                CalciteUdfContext.enter(inputRow.getTableId(), inputRow.getRowKind())) {
            for (Object rawRow : bindable.bind(dataContext)) {
                Object[] row;
                if (rawRow instanceof Object[]) {
                    row = (Object[]) rawRow;
                } else {
                    row = new Object[] {rawRow};
                }
                results.add(toSeaTunnelRow(row, inputRow));
            }
        } catch (Exception e) {
            throw TransformCommonError.sqlExpressionError(sql, e);
        } finally {
            scannableTable.setCurrentRow(null);
        }
        return results;
    }

    private Object[] toCalciteRow(SeaTunnelRow row) {
        Object[] values = new Object[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            values[i] = CalciteValueConverter.toCalcite(row.getField(i));
        }
        return values;
    }

    private SeaTunnelRow toSeaTunnelRow(Object[] calciteRow, SeaTunnelRow inputRow) {
        Object[] values = new Object[calciteRow.length];
        SeaTunnelDataType<?>[] fieldTypes = outputRowType.getFieldTypes();
        for (int i = 0; i < calciteRow.length; i++) {
            values[i] = CalciteValueConverter.fromCalcite(calciteRow[i], fieldTypes[i]);
        }
        SeaTunnelRow result = new SeaTunnelRow(values);
        result.setTableId(inputRow.getTableId());
        result.setRowKind(inputRow.getRowKind());
        result.setOptions(inputRow.getOptions());
        return result;
    }

    @Override
    public void close() {
        if (builtinFunctions != null) {
            builtinFunctions.close();
            builtinFunctions = null;
        }
        rootSchema = null;
        scannableTable = null;
        bindable = null;
        dataContext = null;
        typeFactory = null;
        outputRowType = null;
    }
}
