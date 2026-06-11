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

import org.apache.seatunnel.shade.org.apache.calcite.DataContext;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.avatica.util.Casing;
import org.apache.seatunnel.shade.org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.QueryProvider;
import org.apache.seatunnel.shade.org.apache.calcite.rel.RelNode;
import org.apache.seatunnel.shade.org.apache.calcite.rel.RelRoot;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeField;
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
import org.apache.seatunnel.transform.calcite.adapter.SeaTunnelScannableTable;
import org.apache.seatunnel.transform.calcite.type.TypeConverter;
import org.apache.seatunnel.transform.calcite.udf.BuiltinFunctions;
import org.apache.seatunnel.transform.calcite.udf.ZetaUdfBridge;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.extern.slf4j.Slf4j;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
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
    private RelDataType validatedRowType;
    private SeaTunnelRowType outputRowType;
    private BuiltinFunctions builtinFunctions;
    private ZetaUdfBridge zetaUdfBridge;
    private RelDataTypeFactory typeFactory;

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
        try {
            rootSchema = Frameworks.createRootSchema(true);
            typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT);

            scannableTable =
                    CalciteSchemaFactory.registerTable(
                            rootSchema, tableName, inputRowType, typeFactory);

            builtinFunctions = new BuiltinFunctions();
            builtinFunctions.discoverAndRegister(rootSchema);
            zetaUdfBridge = new ZetaUdfBridge();
            zetaUdfBridge.loadAndRegister(rootSchema);

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

            Planner planner = Frameworks.getPlanner(frameworkConfig);
            SqlNode parsed = planner.parse(sql);
            SqlNode validated = planner.validate(parsed);

            RelRoot relRoot = planner.rel(validated);
            RelNode logicalPlan = relRoot.rel;
            validatedRowType = relRoot.validatedRowType;

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

            outputRowType = deriveOutputRowType();

            planner.close();

            log.info(
                    "Calcite SQL engine initialized successfully for table '{}', SQL: {}",
                    tableName,
                    sql);
        } catch (TransformException e) {
            throw e;
        } catch (Exception e) {
            throw TransformCommonError.sqlExpressionError(sql, e);
        }
    }

    /**
     * Executes the pre-compiled SQL plan against a single input row. Returns a list of output rows
     * (typically 1, but UNNEST may produce 0-N rows).
     */
    public List<SeaTunnelRow> execute(SeaTunnelRow inputRow) {
        Object[] calciteRow = toCalciteRow(inputRow);
        scannableTable.setCurrentRow(calciteRow);

        DataContextImpl ctx = new DataContextImpl(rootSchema, typeFactory);

        List<SeaTunnelRow> results = new ArrayList<>();
        try {
            for (Object rawRow : bindable.bind(ctx)) {
                Object[] row;
                if (rawRow instanceof Object[]) {
                    row = (Object[]) rawRow;
                } else {
                    row = new Object[] {rawRow};
                }
                results.add(toSeaTunnelRow(row, inputRow.getTableId()));
            }
        } catch (Exception e) {
            throw TransformCommonError.sqlExpressionError(sql, e);
        }
        return results;
    }

    /** Returns the output row type derived from the validated SQL. */
    public SeaTunnelRowType getOutputRowType() {
        return outputRowType;
    }

    private SeaTunnelRowType deriveOutputRowType() {
        List<RelDataTypeField> fields = validatedRowType.getFieldList();
        String[] names = new String[fields.size()];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            names[i] = fields.get(i).getName();
            types[i] = TypeConverter.toSeaTunnelType(fields.get(i).getType());
        }
        return new SeaTunnelRowType(names, types);
    }

    private Object[] toCalciteRow(SeaTunnelRow row) {
        Object[] values = new Object[row.getArity()];
        for (int i = 0; i < row.getArity(); i++) {
            values[i] = convertToCalciteValue(row.getField(i));
        }
        return values;
    }

    private Object convertToCalciteValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof LocalDate) {
            return Date.valueOf((LocalDate) value);
        }
        if (value instanceof LocalTime) {
            return Time.valueOf((LocalTime) value);
        }
        if (value instanceof LocalDateTime) {
            return Timestamp.valueOf((LocalDateTime) value);
        }
        if (value instanceof OffsetDateTime) {
            return Timestamp.from(((OffsetDateTime) value).toInstant());
        }
        return value;
    }

    private SeaTunnelRow toSeaTunnelRow(Object[] calciteRow, String tableId) {
        Object[] values = new Object[calciteRow.length];
        SeaTunnelDataType<?>[] fieldTypes = outputRowType.getFieldTypes();
        for (int i = 0; i < calciteRow.length; i++) {
            values[i] = convertFromCalciteValue(calciteRow[i], fieldTypes[i]);
        }
        SeaTunnelRow result = new SeaTunnelRow(values);
        result.setTableId(tableId);
        return result;
    }

    private Object convertFromCalciteValue(Object value, SeaTunnelDataType<?> targetType) {
        if (value == null) {
            return null;
        }
        switch (targetType.getSqlType()) {
            case DATE:
                if (value instanceof Date) {
                    return ((Date) value).toLocalDate();
                }
                if (value instanceof Number) {
                    return LocalDate.ofEpochDay(((Number) value).longValue());
                }
                return value;
            case TIME:
                if (value instanceof Time) {
                    return ((Time) value).toLocalTime();
                }
                if (value instanceof Number) {
                    return LocalTime.ofSecondOfDay(((Number) value).longValue() / 1000);
                }
                return value;
            case TIMESTAMP:
                if (value instanceof Timestamp) {
                    return ((Timestamp) value).toLocalDateTime();
                }
                if (value instanceof Number) {
                    return new Timestamp(((Number) value).longValue()).toLocalDateTime();
                }
                return value;
            case TIMESTAMP_TZ:
                if (value instanceof Timestamp) {
                    return ((Timestamp) value).toInstant().atOffset(ZoneOffset.UTC);
                }
                if (value instanceof Number) {
                    return Instant.ofEpochMilli(((Number) value).longValue())
                            .atOffset(ZoneOffset.UTC);
                }
                return value;
            case TINYINT:
                if (value instanceof Number) {
                    return ((Number) value).byteValue();
                }
                return value;
            case SMALLINT:
                if (value instanceof Number) {
                    return ((Number) value).shortValue();
                }
                return value;
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                }
                return value;
            case BIGINT:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                }
                return value;
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                }
                return value;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                }
                return value;
            default:
                return value;
        }
    }

    @Override
    public void close() {
        if (builtinFunctions != null) {
            builtinFunctions.close();
            builtinFunctions = null;
        }
        if (zetaUdfBridge != null) {
            zetaUdfBridge.close();
            zetaUdfBridge = null;
        }
        rootSchema = null;
        scannableTable = null;
        bindable = null;
    }

    /**
     * Minimal DataContext implementation for Bindable execution without a full CalciteConnection.
     */
    private static class DataContextImpl implements DataContext {

        private final SchemaPlus rootSchema;
        private final RelDataTypeFactory typeFactory;

        DataContextImpl(SchemaPlus rootSchema, RelDataTypeFactory typeFactory) {
            this.rootSchema = rootSchema;
            this.typeFactory = typeFactory;
        }

        @Override
        public SchemaPlus getRootSchema() {
            return rootSchema;
        }

        @Override
        public JavaTypeFactory getTypeFactory() {
            return (JavaTypeFactory) typeFactory;
        }

        @Override
        public QueryProvider getQueryProvider() {
            return null;
        }

        @Override
        public Object get(String name) {
            return null;
        }
    }
}
