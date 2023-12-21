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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.transform.exception.TransformException;

import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitorAdapter;
import net.sf.jsqlparser.statement.select.LateralSubSelect;
import net.sf.jsqlparser.statement.select.ParenthesisFromItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.TableFunction;
import net.sf.jsqlparser.statement.select.ValuesList;

public class ZetaFromItemVisitor extends FromItemVisitorAdapter {
    ZetaStatementVisitor zetaStatementVisitor;

    public ZetaFromItemVisitor(ZetaStatementVisitor zetaStatementVisitor) {
        this.zetaStatementVisitor = zetaStatementVisitor;
    }

    @Override
    public void visit(Table table) {
        if (table.getSchemaName() != null) {
            throw new IllegalArgumentException("Unsupported from schema syntax");
        }
        if (table.getAlias() != null) {
            throw new IllegalArgumentException("Unsupported from table alias syntax");
        }
        String inputTableName = zetaStatementVisitor.getInputTableName();
        String catalogTableName = zetaStatementVisitor.getCatalogTableName();
        String tableName = table.getName();
        if (!tableName.equalsIgnoreCase(inputTableName)
                && !tableName.equalsIgnoreCase(catalogTableName)) {
            throw new TransformException(
                    CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                    String.format("Table name: %s not found", tableName));
        }
    }

    @Override
    public void visit(SubSelect subSelect) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(SubJoin subjoin) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(LateralSubSelect lateralSubSelect) {
        throw new IllegalArgumentException("Unsupported from sub table syntax");
    }

    @Override
    public void visit(ValuesList valuesList) {
        throw new IllegalArgumentException("Unsupported from value list syntax");
    }

    @Override
    public void visit(TableFunction valuesList) {
        throw new IllegalArgumentException("Unsupported from table function syntax");
    }

    @Override
    public void visit(ParenthesisFromItem aThis) {
        throw new IllegalArgumentException("Unsupported from parenthesis syntax");
    }
}
