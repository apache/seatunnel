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

package org.apache.seatunnel.transform.sql.zeta;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformCommonError;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngine;

import org.apache.commons.collections4.CollectionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.TrimFunction;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.LateralView;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectItem;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;

public class ZetaSQLEngine implements SQLEngine {
    private static final Logger log = LoggerFactory.getLogger(ZetaSQLEngine.class);
    public static final String ESCAPE_IDENTIFIER = "`";

    private String inputTableName;
    @Nullable private String catalogTableName;
    private SeaTunnelRowType inputRowType;
    private SeaTunnelRowType outRowType;

    private String sql;
    private PlainSelect selectBody;

    private ZetaSQLFunction zetaSQLFunction;
    private ZetaSQLFilter zetaSQLFilter;
    private ZetaSQLType zetaSQLType;
    private List<ZetaUDF> udfList = Collections.emptyList();
    private ZetaUDFContext udfContext;

    private Integer allColumnsCount = null;
    private ZetaUDFContext initializationContext;
    private final List<ZetaUDF> openedUdfs = new ArrayList<>();
    private volatile Set<ZetaUDF> openedUdfSet = Collections.emptySet();

    /**
     * Field indexes for the common {@code SELECT source_column AS target_column} projection.
     * Keeping this plan avoids evaluating a full SQL expression tree for every row.
     */
    @Nullable private int[] directProjectionIndexes;

    public ZetaSQLEngine() {}

    @Override
    public void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql) {
        init(inputTableName, catalogTableName, inputRowType, sql, null);
    }

    @Override
    public void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql,
            CatalogTable inputCatalogTable) {
        this.inputTableName = inputTableName;
        this.catalogTableName = catalogTableName;
        this.inputRowType = inputRowType;
        this.sql = sql;

        udfList = loadUDFs();
        udfContext = new ZetaUDFContext(inputCatalogTable);
        initializationContext = udfContext.initializationContext();

        this.zetaSQLType = new ZetaSQLType(inputRowType, udfList);
        this.zetaSQLFunction =
                new ZetaSQLFunction(inputRowType, zetaSQLType, udfList, udfContext, this::openUdf);
        this.zetaSQLFilter = new ZetaSQLFilter(zetaSQLFunction, zetaSQLType);

        parseSQL();
    }

    protected List<ZetaUDF> loadUDFs() {
        List<ZetaUDF> loadedUdfs = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ZetaUDF.class, classLoader).forEach(loadedUdfs::add);
        return loadedUdfs;
    }

    private void openUdf(ZetaUDF udf) {
        if (openedUdfSet.contains(udf)) {
            return;
        }
        synchronized (this) {
            if (openedUdfSet.contains(udf)) {
                return;
            }
            try {
                udf.open(initializationContext);
                openedUdfs.add(udf);
                Set<ZetaUDF> snapshot = Collections.newSetFromMap(new IdentityHashMap<>());
                snapshot.addAll(openedUdfs);
                openedUdfSet = Collections.unmodifiableSet(snapshot);
            } catch (Exception failure) {
                closeUdf(udf);
                close();
                throw new UdfOpenException(udf.functionName(), failure);
            }
        }
    }

    @Override
    public void openReferencedUdfs() {
        Set<String> names = new LinkedHashSet<>();
        ExpressionVisitorAdapter visitor =
                new ExpressionVisitorAdapter() {
                    @Override
                    public void visit(Function function) {
                        if (!ZetaSQLFunction.isBuiltInFunction(function.getName())) {
                            names.add(function.getName());
                        }
                        super.visit(function);
                    }

                    @Override
                    public void visit(TrimFunction function) {
                        if (function.getExpression() != null) {
                            function.getExpression().accept(this);
                        }
                    }
                };
        for (SelectItem<?> item : selectBody.getSelectItems()) {
            item.getExpression().accept(visitor);
        }
        if (selectBody.getWhere() != null) {
            selectBody.getWhere().accept(visitor);
        }
        if (selectBody.getLateralViews() != null) {
            for (LateralView lateralView : selectBody.getLateralViews()) {
                Function generator = lateralView.getGeneratorFunction();
                if (generator.getParameters() != null) {
                    for (Expression expression : generator.getParameters().getExpressions()) {
                        expression.accept(visitor);
                    }
                }
            }
        }
        for (String name : names) {
            for (ZetaUDF udf : udfList) {
                if (udf.functionName().equalsIgnoreCase(name)) {
                    openUdf(udf);
                    break;
                }
            }
        }
    }

    private void parseSQL() {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            // validate SQL statement
            validateSQL(statement);
            this.selectBody = (PlainSelect) ((Select) statement).getSelectBody();
            prepareDirectProjection();
        } catch (JSQLParserException e) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    /**
     * Enables the fast path only for plain column projections. Any expression that needs the
     * original evaluator, such as literals, nested fields, functions, or wildcards, leaves this
     * disabled.
     */
    private void prepareDirectProjection() {
        directProjectionIndexes = null;
        if (selectBody.getWhere() != null
                || selectBody.getHaving() != null
                || selectBody.getDistinct() != null) {
            return;
        }

        List<SelectItem<?>> selectItems = selectBody.getSelectItems();
        int[] projectionIndexes = new int[selectItems.size()];
        for (int i = 0; i < selectItems.size(); i++) {
            Expression expression = selectItems.get(i).getExpression();
            if (!(expression instanceof Column)) {
                return;
            }

            String columnName = ((Column) expression).getColumnName();
            int index = inputRowType.indexOf(columnName, false);
            if (index == -1
                    && columnName.startsWith(ESCAPE_IDENTIFIER)
                    && columnName.endsWith(ESCAPE_IDENTIFIER)) {
                columnName = columnName.substring(1, columnName.length() - 1);
                index = inputRowType.indexOf(columnName, false);
            }
            if (index == -1) {
                return;
            }
            projectionIndexes[i] = index;
        }
        directProjectionIndexes = projectionIndexes;
    }

    private void validateSQL(Statement statement) {
        try {
            if (!(statement instanceof Select)) {
                throw new IllegalArgumentException("Only supported DQL(select) SQL");
            }
            Select select = (Select) statement;
            if (!(select.getSelectBody() instanceof PlainSelect)) {
                throw new IllegalArgumentException("Unsupported SQL syntax");
            }
            PlainSelect selectBody = (PlainSelect) select.getSelectBody();

            FromItem fromItem = selectBody.getFromItem();
            if (fromItem instanceof Table) {
                Table table = (Table) fromItem;
                if (table.getSchemaName() != null) {
                    throw new IllegalArgumentException("Unsupported schema syntax");
                }
                if (table.getAlias() != null) {
                    throw new IllegalArgumentException("Unsupported table alias name syntax");
                }
                String tableName = table.getName();
                if (!inputTableName.equalsIgnoreCase(tableName)
                        && !tableName.equalsIgnoreCase(catalogTableName)
                        && !"DUAL".equalsIgnoreCase(tableName)) {
                    log.warn(
                            "SQL table name {} is not equal to input table name {} or catalog table name {}",
                            tableName,
                            inputTableName,
                            catalogTableName);
                }
            } else {
                throw new IllegalArgumentException("Unsupported sub table syntax");
            }

            if (selectBody.getJoins() != null) {
                throw new IllegalArgumentException("Unsupported table join syntax");
            }

            if (selectBody.getOrderByElements() != null) {
                throw new IllegalArgumentException("Unsupported ORDER BY syntax");
            }

            if (selectBody.getGroupBy() != null) {
                throw new IllegalArgumentException("Unsupported GROUP BY syntax");
            }

            if (selectBody.getLimit() != null || selectBody.getOffset() != null) {
                throw new IllegalArgumentException("Unsupported LIMIT,OFFSET syntax");
            }
        } catch (Exception e) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("SQL validate failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    @Override
    public SeaTunnelRowType typeMapping(List<String> inputColumnsMapping) {
        List<SelectItem<?>> selectItems = selectBody.getSelectItems();

        // count number of all columns
        int columnsSize = countColumnsSize(selectItems);

        String[] fieldNames = new String[columnsSize];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[columnsSize];
        if (inputColumnsMapping != null) {
            for (int i = 0; i < columnsSize; i++) {
                inputColumnsMapping.add(null);
            }
        }

        List<String> inputColumnNames =
                Arrays.stream(inputRowType.getFieldNames()).collect(Collectors.toList());

        int idx = 0;
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                for (int i = 0; i < inputRowType.getFieldNames().length; i++) {
                    fieldNames[idx] = cleanEscape(inputRowType.getFieldName(i));
                    seaTunnelDataTypes[idx] = inputRowType.getFieldType(i);
                    if (inputColumnsMapping != null) {
                        inputColumnsMapping.set(idx, inputRowType.getFieldName(i));
                    }
                    idx++;
                }
            } else {
                Expression expression = selectItem.getExpression();
                if (selectItem.getAlias() != null) {
                    String aliasName = selectItem.getAlias().getName();
                    fieldNames[idx] = cleanEscape(aliasName);
                } else {
                    if (expression instanceof Column) {
                        fieldNames[idx] = cleanEscape(((Column) expression).getColumnName());
                    } else {
                        fieldNames[idx] = cleanEscape(expression.toString());
                    }
                }

                if (inputColumnsMapping != null
                        && expression instanceof Column
                        && inputColumnNames.contains(((Column) expression).getColumnName())) {
                    inputColumnsMapping.set(idx, ((Column) expression).getColumnName());
                }

                seaTunnelDataTypes[idx] = zetaSQLType.getExpressionType(expression);
                idx++;
            }
        }
        List<LateralView> lateralViews = selectBody.getLateralViews();
        if (CollectionUtils.isEmpty(lateralViews)) {
            outRowType = new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
        } else {
            outRowType =
                    zetaSQLFunction.lateralViewMapping(
                            fieldNames, seaTunnelDataTypes, lateralViews, inputColumnsMapping);
        }
        return outRowType;
    }

    private static String cleanEscape(String columnName) {
        if (columnName.startsWith(ESCAPE_IDENTIFIER) && columnName.endsWith(ESCAPE_IDENTIFIER)) {
            columnName = columnName.substring(1, columnName.length() - 1);
        }
        return columnName;
    }

    @Override
    public List<SeaTunnelRow> transformBySQL(SeaTunnelRow inputRow, SeaTunnelRowType outRowType) {
        // ------Physical Query Plan Execution------
        // Scan Table
        Object[] inputFields = scanTable(inputRow);
        zetaSQLFunction.updateUDFContext(inputFields, inputRow);

        // Filter
        try {
            boolean retain = zetaSQLFilter.executeFilter(selectBody.getWhere(), inputFields);
            if (!retain) {
                return null;
            }
        } catch (UdfOpenException e) {
            throw e;
        } catch (Exception e) {
            throw TransformCommonError.sqlWhereStatementError(selectBody.getWhere().toString(), e);
        }

        // Project
        Object[] outputFields = project(inputFields);

        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputFields);
        seaTunnelRow.setRowKind(inputRow.getRowKind());
        seaTunnelRow.setTableId(inputRow.getTableId());
        seaTunnelRow.setOptions(inputRow.getOptions());
        List<LateralView> lateralViews = selectBody.getLateralViews();
        if (CollectionUtils.isEmpty(lateralViews)) {
            return Lists.newArrayList(seaTunnelRow);
        }
        return zetaSQLFunction.lateralView(
                Lists.newArrayList(seaTunnelRow), lateralViews, outRowType);
    }

    private Object[] scanTable(SeaTunnelRow inputRow) {
        // do nothing, only return the input fields
        return inputRow.getFields();
    }

    private Object[] project(Object[] inputFields) {
        if (directProjectionIndexes != null) {
            Object[] fields = new Object[directProjectionIndexes.length];
            List<SelectItem<?>> selectItems = selectBody.getSelectItems();
            for (int i = 0; i < directProjectionIndexes.length; i++) {
                try {
                    fields[i] = inputFields[directProjectionIndexes[i]];
                } catch (Exception e) {
                    throw TransformCommonError.sqlExpressionError(
                            selectItems.get(i).getExpression().toString(), e);
                }
            }
            return fields;
        }

        List<SelectItem<?>> selectItems = selectBody.getSelectItems();

        int columnsSize = countColumnsSize(selectItems);

        Object[] fields = new Object[columnsSize];

        int idx = 0;
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                for (Object inputField : inputFields) {
                    fields[idx] = inputField;
                    idx++;
                }
            } else {
                Expression expression = selectItem.getExpression();
                try {
                    fields[idx] = zetaSQLFunction.computeForValue(expression, inputFields);
                    idx++;
                } catch (UdfOpenException e) {
                    throw e;
                } catch (Exception e) {
                    throw TransformCommonError.sqlExpressionError(expression.toString(), e);
                }
            }
        }
        return fields;
    }

    private int countColumnsSize(List<SelectItem<?>> selectItems) {
        if (allColumnsCount != null) {
            return allColumnsCount;
        }
        int allColumnsCnt = 0;
        for (SelectItem selectItem : selectItems) {
            if (selectItem.getExpression() instanceof AllColumns) {
                allColumnsCnt++;
            }
        }
        allColumnsCount =
                selectItems.size()
                        + inputRowType.getFieldNames().length * allColumnsCnt
                        - allColumnsCnt;
        return allColumnsCount;
    }

    @Override
    public synchronized void close() {
        for (int i = openedUdfs.size() - 1; i >= 0; i--) {
            closeUdf(openedUdfs.get(i));
        }
        openedUdfs.clear();
        openedUdfSet = Collections.emptySet();
    }

    private void closeUdf(ZetaUDF udf) {
        try {
            udf.close();
        } catch (Exception failure) {
            log.warn("Close udf {} failed", udf.functionName(), failure);
        }
    }

    private static final class UdfOpenException extends TransformException {
        private UdfOpenException(String functionName, Exception cause) {
            super(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Open udf %s failed: %s", functionName, cause.getMessage()));
            initCause(cause);
        }
    }
}
