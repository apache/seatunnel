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
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlOrderBy;
import org.apache.seatunnel.shade.org.apache.calcite.sql.SqlSelect;
import org.apache.seatunnel.shade.org.apache.calcite.sql.parser.SqlParser;
import org.apache.seatunnel.shade.org.apache.calcite.tools.FrameworkConfig;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Frameworks;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Planner;
import org.apache.seatunnel.shade.org.apache.calcite.tools.Programs;

import org.apache.seatunnel.api.table.type.RowKind;
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

    /** Whether an UPDATE_AFTER that the WHERE clause rejects is emitted as a DELETE. */
    private final boolean whereRejectedUpdateAsDelete;

    private SchemaPlus rootSchema;
    private SeaTunnelScannableTable scannableTable;
    private Bindable<Object[]> bindable;

    /**
     * The query without its WHERE clause, or null when {@link #whereRejectedUpdateAsDelete} is off,
     * the query has no WHERE clause or is not a single SELECT. It builds the DELETE image of an
     * UPDATE_AFTER that the WHERE clause rejects.
     */
    private Bindable<Object[]> withoutWhereBindable;

    /** Whether a failed DELETE image was already logged at WARN. */
    private boolean deleteImageFailureLogged = false;

    @Getter private SeaTunnelRowType outputRowType;
    private BuiltinFunctions builtinFunctions;
    private RelDataTypeFactory typeFactory;
    private SeaTunnelDataContext dataContext;

    public CalciteSQLEngine(String sql, String tableName, SeaTunnelRowType inputRowType) {
        this(sql, tableName, inputRowType, false);
    }

    /**
     * @param whereRejectedUpdateAsDelete whether an UPDATE_AFTER that the WHERE clause rejects is
     *     emitted as a DELETE of its projected row instead of being dropped
     */
    public CalciteSQLEngine(
            String sql,
            String tableName,
            SeaTunnelRowType inputRowType,
            boolean whereRejectedUpdateAsDelete) {
        this.sql = sql;
        this.tableName = tableName;
        this.inputRowType = inputRowType;
        this.whereRejectedUpdateAsDelete = whereRejectedUpdateAsDelete;
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
            RelDataType validatedRowType = relRoot.validatedRowType;

            bindable = toBindable(relRoot.rel);
            withoutWhereBindable =
                    whereRejectedUpdateAsDelete
                            ? compileWithoutWhere(frameworkConfig, validatedRowType)
                            : null;

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
     * Converts a validated logical plan into the enumerable convention and compiles it.
     *
     * <p>The given plan is only read. The returned {@link Bindable} holds no row state: it can be
     * bound again for every input row, and it reads the current row from the data context passed to
     * {@link Bindable#bind}.
     *
     * @param logicalPlan the logical plan of a validated query
     * @return the compiled plan, reusable for every row
     */
    private static Bindable<Object[]> toBindable(RelNode logicalPlan) {
        RelNode enumerablePlan =
                Programs.standard()
                        .run(
                                logicalPlan.getCluster().getPlanner(),
                                logicalPlan,
                                logicalPlan.getTraitSet().replace(EnumerableConvention.INSTANCE),
                                Collections.emptyList(),
                                Collections.emptyList());
        return EnumerableInterpretable.toBindable(
                Collections.emptyMap(),
                null,
                (EnumerableRel) enumerablePlan,
                EnumerableRel.Prefer.ARRAY);
    }

    /**
     * Compiles the query without its WHERE clause. Only a single SELECT without GROUP BY or HAVING,
     * optionally with ORDER BY, is supported; for any other query, or a query without a WHERE
     * clause, it returns null.
     */
    private Bindable<Object[]> compileWithoutWhere(
            FrameworkConfig frameworkConfig, RelDataType validatedRowType) throws Exception {
        Planner planner = Frameworks.getPlanner(frameworkConfig);
        try {
            SqlNode parsed = planner.parse(sql);
            SqlNode query = parsed instanceof SqlOrderBy ? ((SqlOrderBy) parsed).query : parsed;
            if (!(query instanceof SqlSelect)) {
                return null;
            }
            SqlSelect select = (SqlSelect) query;
            if (select.getWhere() == null
                    || select.getGroup() != null
                    || select.getHaving() != null) {
                return null;
            }
            select.setWhere(null);
            RelRoot relRoot = planner.rel(planner.validate(parsed));
            if (relRoot.validatedRowType.getFieldCount() != validatedRowType.getFieldCount()) {
                return null;
            }
            return toBindable(relRoot.rel);
        } finally {
            planner.close();
        }
    }

    /**
     * Executes the pre-compiled SQL plan against a single input row. Returns a list of output rows
     * (typically 1, but UNNEST may produce 0-N rows).
     *
     * <p>When {@code where_rejected_update_as_delete} is enabled and the WHERE clause rejects an
     * UPDATE_AFTER, the update may have moved the row out of the condition: its UPDATE_BEFORE
     * matched and was kept, but a sink that writes UPDATE_AFTER as an upsert skips UPDATE_BEFORE
     * and would keep the old row. The query without its WHERE clause then builds the new image,
     * which is emitted as a DELETE so that the sink removes the row. When the option is disabled,
     * the rejected row is dropped like any other row.
     */
    public List<SeaTunnelRow> execute(SeaTunnelRow inputRow) {
        List<SeaTunnelRow> results = execute(bindable, inputRow, inputRow.getRowKind());
        if (!results.isEmpty()
                || inputRow.getRowKind() != RowKind.UPDATE_AFTER
                || withoutWhereBindable == null) {
            return results;
        }
        try {
            return execute(withoutWhereBindable, inputRow, RowKind.DELETE);
        } catch (RuntimeException e) {
            // The WHERE clause may guard the projection (e.g. "10 / x ... WHERE x <> 0"), so a
            // failed projection of a rejected row must not fail the job. Drop it as before.
            logDeleteImageFailure(e);
            return results;
        }
    }

    private void logDeleteImageFailure(RuntimeException e) {
        String message =
                "Dropped an UPDATE_AFTER row that the WHERE clause rejected, because its"
                        + " projection failed, so it cannot be sent as a DELETE. A keyed sink may"
                        + " keep the old row. Query: "
                        + sql;
        if (!deleteImageFailureLogged) {
            deleteImageFailureLogged = true;
            log.warn(message, e);
        } else {
            log.debug(message, e);
        }
    }

    private List<SeaTunnelRow> execute(
            Bindable<Object[]> plan, SeaTunnelRow inputRow, RowKind rowKind) {
        Object[] calciteRow = toCalciteRow(inputRow);
        scannableTable.setCurrentRow(calciteRow);
        dataContext.refreshNow();

        List<SeaTunnelRow> results = new ArrayList<>();
        try (CalciteUdfContext.Scope ignored =
                CalciteUdfContext.enter(inputRow.getTableId(), inputRow.getRowKind())) {
            for (Object rawRow : plan.bind(dataContext)) {
                Object[] row;
                if (rawRow instanceof Object[]) {
                    row = (Object[]) rawRow;
                } else {
                    row = new Object[] {rawRow};
                }
                results.add(toSeaTunnelRow(row, inputRow, rowKind));
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

    private SeaTunnelRow toSeaTunnelRow(
            Object[] calciteRow, SeaTunnelRow inputRow, RowKind rowKind) {
        Object[] values = new Object[calciteRow.length];
        SeaTunnelDataType<?>[] fieldTypes = outputRowType.getFieldTypes();
        for (int i = 0; i < calciteRow.length; i++) {
            values[i] = CalciteValueConverter.fromCalcite(calciteRow[i], fieldTypes[i]);
        }
        SeaTunnelRow result = new SeaTunnelRow(values);
        result.setTableId(inputRow.getTableId());
        result.setRowKind(rowKind);
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
        withoutWhereBindable = null;
        dataContext = null;
        typeFactory = null;
        outputRowType = null;
    }
}
