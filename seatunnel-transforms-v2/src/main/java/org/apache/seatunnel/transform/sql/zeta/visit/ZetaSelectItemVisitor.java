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

package org.apache.seatunnel.transform.sql.zeta.visit;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import net.sf.jsqlparser.expression.Alias;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItemVisitorAdapter;

import java.util.Collections;
import java.util.List;

public class ZetaSelectItemVisitor extends SelectItemVisitorAdapter {
    private final ZetaStatementVisitor zetaStatementVisitor;
    private final ZetaExpressionVisitor zetaExpressionVisitor;

    public ZetaSelectItemVisitor(
            ZetaStatementVisitor zetaStatementVisitor,
            ZetaExpressionVisitor zetaExpressionVisitor) {
        this.zetaStatementVisitor = zetaStatementVisitor;
        this.zetaExpressionVisitor = zetaExpressionVisitor;
    }

    /** select * */
    @Override
    public void visit(AllColumns columns) {
        SeaTunnelRowType inputRowType = zetaStatementVisitor.getInputRowType();
        List<String> outputFieldNames = zetaStatementVisitor.getOutputFieldNames();
        Collections.addAll(outputFieldNames, inputRowType.getFieldNames());

        List<SeaTunnelDataType<?>> outputDataTypes = zetaStatementVisitor.getOutputDataTypes();
        Collections.addAll(outputDataTypes, inputRowType.getFieldTypes());

        List<Object> outputFields = zetaStatementVisitor.getOutputFields();
        Collections.addAll(outputFields, zetaStatementVisitor.getInputFields());
    }

    /** select t1.* */
    @Override
    public void visit(AllTableColumns columns) {
        throw new IllegalArgumentException("Unsupported table alias name syntax");
    }

    /** select id */
    @Override
    public void visit(SelectExpressionItem item) {
        List<String> outputFieldNames = zetaStatementVisitor.getOutputFieldNames();
        int index = zetaStatementVisitor.getIndex();
        Expression expression = item.getExpression();
        Alias alias = item.getAlias();
        if (alias != null) {
            outputFieldNames.add(alias.getName());
        } else {
            if (expression instanceof Column) {
                String columnName = ((Column) expression).getColumnName();
                outputFieldNames.add(columnName);
            } else {
                outputFieldNames.add(expression.toString());
            }
        }
        zetaStatementVisitor.setIndex(++index);
        expression.accept(zetaExpressionVisitor);
    }
}
