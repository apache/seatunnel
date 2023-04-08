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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngine;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;

import java.util.ArrayList;
import java.util.List;
import java.util.ServiceLoader;

public class ZetaSQLEngine implements SQLEngine {
    private String inputTableName;

    private String sql;
    private PlainSelect selectBody;

    private ZetaSQLFunction zetaSQLFunction;
    private ZetaSQLFilter zetaSQLFilter;
    private ZetaSQLType zetaSQLType;

    public ZetaSQLEngine() {}

    @Override
    public void init(String inputTableName, SeaTunnelRowType inputRowType, String sql) {
        this.inputTableName = inputTableName;
        this.sql = sql;

        List<ZetaUDF> udfList = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ZetaUDF.class, classLoader).forEach(udfList::add);

        this.zetaSQLType = new ZetaSQLType(inputRowType, udfList);
        this.zetaSQLFunction = new ZetaSQLFunction(inputRowType, zetaSQLType, udfList);
        this.zetaSQLFilter = new ZetaSQLFilter(zetaSQLFunction);

        parseSQL();
    }

    private void parseSQL() {
        try {
            Statement statement = CCJSqlParserUtil.parse(sql);
            // validate SQL statement
            validateSQL(statement);
            this.selectBody = (PlainSelect) ((Select) statement).getSelectBody();
        } catch (JSQLParserException e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
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
                if (!inputTableName.equalsIgnoreCase(tableName)) {
                    throw new IllegalArgumentException(
                            String.format("Table name: %s not found", tableName));
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

            for (SelectItem selectItem : selectBody.getSelectItems()) {
                if (selectItem instanceof AllColumns) {
                    throw new IllegalArgumentException("Unsupported all columns select syntax");
                }
            }
        } catch (Exception e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("SQL validate failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    @Override
    public SeaTunnelRowType typeMapping() {
        List<SelectItem> selectItems = selectBody.getSelectItems();

        String[] fieldNames = new String[selectItems.size()];
        SeaTunnelDataType<?>[] seaTunnelDataTypes = new SeaTunnelDataType<?>[selectItems.size()];

        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);
            if (selectItem instanceof SelectExpressionItem) {
                SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
                Expression expression = expressionItem.getExpression();

                if (expressionItem.getAlias() != null) {
                    fieldNames[i] = expressionItem.getAlias().getName();
                } else {
                    if (expression instanceof Column) {
                        fieldNames[i] = ((Column) expression).getColumnName();
                    } else {
                        fieldNames[i] = expression.toString();
                    }
                }

                seaTunnelDataTypes[i] = zetaSQLType.getExpressionType(expression);
            }
        }
        return new SeaTunnelRowType(fieldNames, seaTunnelDataTypes);
    }

    @Override
    public SeaTunnelRow transformBySQL(SeaTunnelRow inputRow) {
        // ------Physical Query Plan Execution------
        // Scan Table
        Object[] inputFields = scanTable(inputRow);

        // Filter
        boolean retain = zetaSQLFilter.executeFilter(selectBody.getWhere(), inputFields);
        if (!retain) {
            return null;
        }

        // Project
        Object[] outputFields = project(inputFields);

        return new SeaTunnelRow(outputFields);
    }

    private Object[] scanTable(SeaTunnelRow inputRow) {
        // do nothing, only return the input fields
        return inputRow.getFields();
    }

    private Object[] project(Object[] inputFields) {
        List<SelectItem> selectItems = selectBody.getSelectItems();
        Object[] fields = new Object[selectItems.size()];
        for (int i = 0; i < selectItems.size(); i++) {
            SelectItem selectItem = selectItems.get(i);
            SelectExpressionItem expressionItem = (SelectExpressionItem) selectItem;
            Expression expression = expressionItem.getExpression();
            fields[i] = zetaSQLFunction.computeForValue(expression, inputFields);
        }
        return fields;
    }
}
