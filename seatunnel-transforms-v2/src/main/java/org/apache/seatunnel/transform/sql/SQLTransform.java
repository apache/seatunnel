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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableColumnsEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableCommentEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableEvent;
import org.apache.seatunnel.api.table.schema.event.AlterTableNameEvent;
import org.apache.seatunnel.api.table.schema.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformCommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.transform.sql.SQLEngineFactory.EngineType.ZETA;

@Slf4j
public class SQLTransform extends AbstractCatalogSupportFlatMapTransform
        implements SupportRowLevelErrorClassifier<SeaTunnelRow> {
    public static final String PLUGIN_NAME = "Sql";

    public static final Option<String> KEY_QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("The query SQL");

    public static final Option<String> KEY_ENGINE =
            Options.key("engine")
                    .stringType()
                    .defaultValue(ZETA.name())
                    .withDescription("The SQL engine type");

    /** Reason reported when an engine hand-off disagrees with the schema change event. */
    private static final String UPSTREAM_MISMATCH =
            "upstream produced schema does not match the schema change event";

    private final String query;

    private final EngineType engineType;

    private SeaTunnelRowType outRowType;

    private transient SQLEngine sqlEngine;

    private final String inputTableName;

    /**
     * Output slot descriptors of the live produced schema, rebuilt whenever the produced schema is
     * derived. Schema-change translation pairs them with the descriptors of the candidate output.
     */
    private transient List<SQLOutputSlot> outputSlots;

    /**
     * Input columns the query text references outside of star projections. Filled when the engine
     * opens and replaced on every commit; a schema change that removes one of them fails fast.
     */
    private transient Set<String> referencedInputColumns;

    /**
     * Pre-event state staged by an engine hand-off ({@link #setInputCatalogTable}) until the schema
     * change event that follows it is mapped. It never survives one engine dispatch: the engine
     * hands the input over and dispatches the event in the same call. A data row arriving while it
     * is set is a contract violation and is rejected.
     */
    private transient PendingSchemaChange pending;

    public SQLTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.query = config.get(KEY_QUERY);
        if (config.getOptional(KEY_ENGINE).isPresent()) {
            this.engineType = EngineType.valueOf(config.get(KEY_ENGINE).toUpperCase());
        } else {
            this.engineType = ZETA;
        }

        List<String> pluginInputIdentifiers = config.get(ConnectorCommonOptions.PLUGIN_INPUT);
        if (pluginInputIdentifiers != null && !pluginInputIdentifiers.isEmpty()) {
            this.inputTableName = pluginInputIdentifiers.get(0);
        } else {
            this.inputTableName = catalogTable.getTableId().getTableName();
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        sqlEngine = createSqlEngine();
        sqlEngine.init(
                inputTableName,
                inputCatalogTable.getTableId().getTableName(),
                inputCatalogTable.getSeaTunnelRowType(),
                query);
        referencedInputColumns = new HashSet<>(sqlEngine.referencedInputColumns());
    }

    /**
     * Creates the SQL engine this transform evaluates queries with. Every candidate schema of a
     * schema change is evaluated on its own engine created here and either committed or closed, so
     * tests can count engine lifecycles by overriding this hook.
     *
     * @return a new, not yet initialised engine
     */
    protected SQLEngine createSqlEngine() {
        return SQLEngineFactory.getSQLEngine(engineType);
    }

    private void tryOpen() {
        if (sqlEngine == null) {
            open();
        }
    }

    @Override
    public List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        if (pending != null) {
            throw new IllegalStateException(
                    "SQL transform received a data row while a schema hand-off is pending for table "
                            + inputCatalogTable.getTablePath()
                            + "; the schema change event must be dispatched before any row");
        }
        tryOpen();
        return sqlEngine.transformBySQL(inputRow, outRowType);
    }

    @Override
    public RowErrorClassification classifyRowError(Throwable t, SeaTunnelRow row) {
        TransformException transformException = findTransformException(t);
        if (transformException == null) {
            return RowErrorClassification.SYSTEM_ERROR;
        }
        if (transformException.getSeaTunnelErrorCode()
                == TransformCommonErrorCode.EXPRESSION_EXECUTE_ERROR) {
            return RowErrorClassification.ROW_ERROR;
        }
        if (transformException.getSeaTunnelErrorCode()
                        == TransformCommonErrorCode.WHERE_STATEMENT_ERROR
                && !hasUnsupportedOperationCause(transformException)) {
            return RowErrorClassification.ROW_ERROR;
        }
        return RowErrorClassification.SYSTEM_ERROR;
    }

    static boolean hasUnsupportedOperationCause(Throwable t) {
        Throwable current = t.getCause();
        while (current != null) {
            if (current instanceof SeaTunnelRuntimeException
                    && ((SeaTunnelRuntimeException) current).getSeaTunnelErrorCode()
                            == CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private TransformException findTransformException(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof TransformException) {
                return (TransformException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    @Override
    protected TableSchema transformTableSchema() {
        tryOpen();
        List<String> inputColumnsMapping = new ArrayList<>();
        outRowType = sqlEngine.typeMapping(inputColumnsMapping);
        outputSlots = sqlEngine.describeOutputSlots();
        return buildOutputSchema(inputCatalogTable, outRowType, inputColumnsMapping);
    }

    /**
     * Derives the produced schema of a query from the engine's output row type. The method reads
     * nothing from and writes nothing to the transform, so candidate schemas of a schema change can
     * be derived on isolated engines without touching live state.
     *
     * @param input the input table the engine was initialised with
     * @param outRowType the engine's output row type
     * @param inputColumnsMapping per output column, the input column it copies, or null
     * @return the produced schema
     */
    static TableSchema buildOutputSchema(
            CatalogTable input, SeaTunnelRowType outRowType, List<String> inputColumnsMapping) {
        List<String> outputColumns = Arrays.asList(outRowType.getFieldNames());

        TableSchema.Builder builder = TableSchema.builder();
        if (input.getTableSchema().getPrimaryKey() != null
                && outputColumns.containsAll(
                        input.getTableSchema().getPrimaryKey().getColumnNames())) {
            builder.primaryKey(input.getTableSchema().getPrimaryKey().copy());
        }

        List<ConstraintKey> outputConstraintKeys =
                input.getTableSchema().getConstraintKeys().stream()
                        .filter(
                                key -> {
                                    List<String> constraintColumnNames =
                                            key.getColumnNames().stream()
                                                    .map(
                                                            ConstraintKey.ConstraintKeyColumn
                                                                    ::getColumnName)
                                                    .collect(Collectors.toList());
                                    return outputColumns.containsAll(constraintColumnNames);
                                })
                        .map(ConstraintKey::copy)
                        .collect(Collectors.toList());

        builder.constraintKey(outputConstraintKeys);

        String[] fieldNames = outRowType.getFieldNames();
        SeaTunnelDataType<?>[] fieldTypes = outRowType.getFieldTypes();
        List<Column> columns = new ArrayList<>(fieldNames.length);
        for (int i = 0; i < fieldNames.length; i++) {
            Column simpleColumn = null;
            String inputColumnName = inputColumnsMapping.get(i);
            if (inputColumnName != null) {
                for (Column inputColumn : input.getTableSchema().getColumns()) {
                    if (inputColumnName.equals(inputColumn.getName())) {
                        simpleColumn = inputColumn;
                        break;
                    }
                }
            }
            Column column;
            if (simpleColumn != null) {
                column =
                        new PhysicalColumn(
                                fieldNames[i],
                                fieldTypes[i],
                                simpleColumn.getColumnLength(),
                                simpleColumn.getScale(),
                                simpleColumn.isNullable(),
                                simpleColumn.getDefaultValue(),
                                simpleColumn.getComment(),
                                simpleColumn.getSourceType(),
                                simpleColumn.getOptions());
            } else {
                column = PhysicalColumn.of(fieldNames[i], fieldTypes[i], 0, true, null, null);
            }
            columns.add(column);
        }
        return builder.columns(columns).build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    /**
     * Translates an upstream schema change into the change of this transform's own output.
     *
     * <p>Column-level events are applied one by one to an identity-annotated copy of the pre-event
     * input; the final input is evaluated on an isolated engine; the net effect on every output
     * column is emitted as events that replay onto the pre-event produced schema exactly. Live
     * state is committed only after every check passed, so a rejected event leaves input, produced
     * schema and engine untouched. Returns {@code null} when the output did not change.
     *
     * @param event the upstream event
     * @return the event to forward, or null when absorbed
     */
    @Override
    public SchemaChangeEvent mapSchemaChangeEvent(SchemaChangeEvent event) {
        if (!(event instanceof AlterTableEvent)) {
            return event;
        }
        PendingSchemaChange ctx = pending;
        pending = null;
        AlterTableEvent alterEvent = (AlterTableEvent) event;
        List<AlterTableColumnEvent> hints = SQLSchemaChangeTranslator.flatten(event);
        if (hints.isEmpty()) {
            return mapTableLevelEvent(alterEvent, ctx);
        }
        CatalogTable preInput = ctx != null ? ctx.preEventInput : inputCatalogTable;
        TableSchema preOutput = ctx != null ? ctx.preEventOutput : currentOutputSchema();
        List<SQLOutputSlot> preSlots = ctx != null ? ctx.preEventSlots : currentOutputSlots();

        SQLLineageSchema initialLineage = SQLLineageSchema.initial(preInput.getTableSchema());
        SQLLineageSchema lineage = initialLineage;
        Set<Integer> protectedIdentities =
                SQLSchemaChangeTranslator.protectedIdentities(
                        preSlots, preOutput, preInput.getPartitionKeys(), initialLineage);
        for (AlterTableColumnEvent hint : hints) {
            try {
                SQLSchemaChangeTranslator.rejectProtectedColumnChange(
                        lineage, hint, protectedIdentities);
            } catch (IllegalArgumentException e) {
                throw incompatible(alterEvent, e.getMessage(), e);
            }
            try {
                lineage = lineage.apply(hint);
            } catch (IllegalArgumentException e) {
                throw incompatible(
                        alterEvent,
                        "schema change does not apply to the input schema: " + e.getMessage(),
                        e);
            }
        }
        TableSchema finalSchema = lineage.getSchema();
        List<String> missing =
                referencedColumns().stream()
                        .filter(name -> !finalSchema.contains(name))
                        .sorted()
                        .collect(Collectors.toList());
        if (!missing.isEmpty()) {
            throw incompatible(
                    alterEvent,
                    "referenced columns " + missing + " do not exist after the change",
                    null);
        }
        CatalogTable finalInput;
        if (ctx != null) {
            if (!finalSchema.equals(ctx.handedInput.getTableSchema())) {
                throw incompatible(alterEvent, UPSTREAM_MISMATCH, null);
            }
            finalInput = ctx.handedInput;
        } else {
            finalInput = withSchema(preInput, finalSchema);
        }
        SQLOutputCandidate candidate = evaluate(finalInput, alterEvent);
        try {
            List<AlterTableColumnEvent> out;
            try {
                out =
                        SQLSchemaChangeTranslator.translate(
                                transformTableIdentifier(),
                                preSlots,
                                preOutput,
                                initialLineage,
                                candidate.slots,
                                candidate.outputSchema,
                                lineage);
            } catch (IllegalArgumentException e) {
                throw incompatible(alterEvent, e.getMessage(), e);
            }
            SQLSchemaChangeTranslator.verifyReplay(preOutput, candidate.outputSchema, out);
            commit(candidate);
            if (out.isEmpty()) {
                log.info(
                        "SQL transform absorbed schema change {} for query [{}]; the output schema is unchanged",
                        event,
                        query);
                return null;
            }
            CatalogTable produced = getProducedCatalogTable();
            SchemaChangeEvent outgoing =
                    SQLSchemaChangeTranslator.rebuild(
                            alterEvent, produced.getTableId(), out, produced);
            log.info(
                    "SQL transform translated schema change {} into {} for query [{}]",
                    event,
                    outgoing,
                    query);
            return outgoing;
        } finally {
            candidate.close();
        }
    }

    /**
     * Stages an engine hand-off. The engine hands a transform the produced table of the previous
     * transform before it dispatches the schema change event; nothing is evaluated or committed
     * here so that a rejected event leaves the transform untouched. A hand-off whose schema equals
     * the live input schema (unaffected tables, repeated hand-offs) is a no-op.
     *
     * @param handedInput the produced table of the previous transform after the event
     */
    @Override
    public void setInputCatalogTable(@NonNull CatalogTable handedInput) {
        if (handedInput.getTableSchema().equals(inputCatalogTable.getTableSchema())) {
            return;
        }
        if (pending == null) {
            pending =
                    new PendingSchemaChange(
                            inputCatalogTable, currentOutputSchema(), currentOutputSlots());
        }
        pending.handedInput = handedInput;
    }

    @Override
    public void close() {
        if (sqlEngine != null) {
            sqlEngine.close();
        }
    }

    private SchemaChangeEvent mapTableLevelEvent(AlterTableEvent event, PendingSchemaChange ctx) {
        if (event instanceof AlterTableColumnsEvent
                || event instanceof AlterTableNameEvent
                || event instanceof AlterTableCommentEvent) {
            if (ctx != null) {
                throw incompatible(event, UPSTREAM_MISMATCH, null);
            }
            if (event instanceof AlterTableCommentEvent) {
                applyTableComment(((AlterTableCommentEvent) event).getNewComment());
            }
            return event;
        }
        if (event.getChangeAfter() == null) {
            throw incompatible(
                    event, "unsupported schema change event " + event.getClass().getName(), null);
        }
        // Any other table-level event that carries the whole table, such as the restore event
        // emitted after a failover, resynchronises the input. No DDL is derived from it.
        CatalogTable restored =
                CatalogTable.of(inputCatalogTable.getTableId(), event.getChangeAfter());
        if (ctx != null && !restored.getTableSchema().equals(ctx.handedInput.getTableSchema())) {
            throw incompatible(event, UPSTREAM_MISMATCH, null);
        }
        SQLOutputCandidate candidate = evaluate(restored, event);
        try {
            commit(candidate);
        } finally {
            candidate.close();
        }
        event.setChangeAfter(getProducedCatalogTable());
        log.info(
                "SQL transform resynchronised its input from schema change event {} for query [{}]",
                event,
                query);
        return event;
    }

    private void applyTableComment(String newComment) {
        inputCatalogTable =
                CatalogTable.of(
                        inputCatalogTable.getTableId(),
                        inputCatalogTable.getTableSchema(),
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        newComment,
                        inputCatalogTable.getTableId().getCatalogName(),
                        inputCatalogTable.getMetadataSchema());
        outputCatalogTable = null;
    }

    /**
     * Evaluates the query against a candidate input on a fresh engine. Nothing here touches live
     * state; the returned candidate owns its engine until it is committed or closed.
     */
    private SQLOutputCandidate evaluate(CatalogTable candidateInput, AlterTableEvent event) {
        SQLEngine engine = createSqlEngine();
        boolean built = false;
        try {
            engine.init(
                    inputTableName,
                    candidateInput.getTableId().getTableName(),
                    candidateInput.getSeaTunnelRowType(),
                    query);
            List<String> inputColumnsMapping = new ArrayList<>();
            SeaTunnelRowType candidateRowType = engine.typeMapping(inputColumnsMapping);
            TableSchema outputSchema =
                    buildOutputSchema(candidateInput, candidateRowType, inputColumnsMapping);
            requireUniqueNames(outputSchema);
            List<SQLOutputSlot> slots = engine.describeOutputSlots();
            if (slots.size() != outputSchema.getColumns().size()) {
                throw new IllegalStateException(
                        String.format(
                                "the SQL engine described %d output slots for %d produced columns",
                                slots.size(), outputSchema.getColumns().size()));
            }
            engine.validateFilterTypes();
            SQLOutputCandidate candidate =
                    new SQLOutputCandidate(
                            candidateInput, engine, candidateRowType, outputSchema, slots);
            built = true;
            return candidate;
        } catch (IllegalArgumentException e) {
            throw incompatible(event, e.getMessage(), e);
        } catch (IllegalStateException e) {
            throw e;
        } catch (RuntimeException e) {
            throw incompatible(
                    event, "output schema could not be recomputed: " + e.getMessage(), e);
        } finally {
            if (!built) {
                engine.close();
            }
        }
    }

    /** Swaps the live state to the candidate and closes the retired engine exactly once. */
    private void commit(SQLOutputCandidate candidate) {
        SQLEngine retired = sqlEngine;
        inputCatalogTable = candidate.input;
        sqlEngine = candidate.engine;
        outRowType = candidate.outRowType;
        outputSlots = candidate.slots;
        referencedInputColumns = new HashSet<>(candidate.engine.referencedInputColumns());
        outputCatalogTable =
                CatalogTable.of(
                        transformTableIdentifier(),
                        candidate.outputSchema,
                        inputCatalogTable.getOptions(),
                        inputCatalogTable.getPartitionKeys(),
                        inputCatalogTable.getComment(),
                        inputCatalogTable.getTableId().getCatalogName(),
                        inputCatalogTable.getMetadataSchema());
        candidate.committed = true;
        if (retired != null && retired != candidate.engine) {
            retired.close();
        }
    }

    private static CatalogTable withSchema(CatalogTable base, TableSchema schema) {
        return CatalogTable.of(
                base.getTableId(),
                schema,
                base.getOptions(),
                base.getPartitionKeys(),
                base.getComment(),
                base.getTableId().getCatalogName(),
                base.getMetadataSchema());
    }

    private TableSchema currentOutputSchema() {
        return getProducedCatalogTable().getTableSchema();
    }

    private List<SQLOutputSlot> currentOutputSlots() {
        getProducedCatalogTable();
        if (outputSlots == null) {
            tryOpen();
            outputSlots = sqlEngine.describeOutputSlots();
        }
        return outputSlots;
    }

    private Set<String> referencedColumns() {
        if (referencedInputColumns == null) {
            tryOpen();
            referencedInputColumns = new HashSet<>(sqlEngine.referencedInputColumns());
        }
        return referencedInputColumns;
    }

    private static void requireUniqueNames(TableSchema schema) {
        Set<String> seen = new HashSet<>();
        List<String> duplicates = new ArrayList<>();
        for (String name : schema.getFieldNames()) {
            if (name == null || name.isEmpty()) {
                throw new IllegalArgumentException("output column names must not be empty");
            }
            if (!seen.add(name)) {
                duplicates.add(name);
            }
        }
        if (!duplicates.isEmpty()) {
            throw new IllegalArgumentException(
                    "output column names are not unique after the change: " + duplicates);
        }
    }

    private TransformException incompatible(AlterTableEvent event, String reason, Throwable cause) {
        String table = inputCatalogTable.getTablePath().toString();
        if (cause == null) {
            return TransformCommonError.sqlSchemaChangeIncompatible(
                    query, table, event.getStatement(), reason);
        }
        return TransformCommonError.sqlSchemaChangeIncompatible(
                query, table, event.getStatement(), reason, cause);
    }

    /**
     * Pre-event state and the handed-over input kept between an engine hand-off and the schema
     * change event that follows it.
     */
    private static final class PendingSchemaChange {
        private final CatalogTable preEventInput;
        private final TableSchema preEventOutput;
        private final List<SQLOutputSlot> preEventSlots;
        private CatalogTable handedInput;

        private PendingSchemaChange(
                CatalogTable preEventInput,
                TableSchema preEventOutput,
                List<SQLOutputSlot> preEventSlots) {
            this.preEventInput = preEventInput;
            this.preEventOutput = preEventOutput;
            this.preEventSlots = preEventSlots;
        }
    }

    /**
     * Everything derived from one candidate input. Nothing here is live until {@link
     * SQLTransform#commit}; the candidate owns its engine until then and closes it when discarded.
     */
    private static final class SQLOutputCandidate implements AutoCloseable {
        private final CatalogTable input;
        private final SQLEngine engine;
        private final SeaTunnelRowType outRowType;
        private final TableSchema outputSchema;
        private final List<SQLOutputSlot> slots;
        private boolean committed;

        private SQLOutputCandidate(
                CatalogTable input,
                SQLEngine engine,
                SeaTunnelRowType outRowType,
                TableSchema outputSchema,
                List<SQLOutputSlot> slots) {
            this.input = input;
            this.engine = engine;
            this.outRowType = outRowType;
            this.outputSchema = outputSchema;
            this.slots = slots;
        }

        @Override
        public void close() {
            if (!committed) {
                engine.close();
            }
        }
    }
}
