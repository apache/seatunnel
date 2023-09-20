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

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;
import org.apache.seatunnel.transform.sql.SQLEngine;
import org.apache.seatunnel.transform.sql.zeta.visit.ZetaStatementVisitor;

import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.ServiceLoader;

public class ZetaSQLEngine implements SQLEngine {
    private String sql;
    private Statement statement;
    private ZetaStatementVisitor zetaStatementVisitor;

    public ZetaSQLEngine() {}

    @Override
    public void init(
            String inputTableName,
            String catalogTableName,
            SeaTunnelRowType inputRowType,
            String sql) {
        this.sql = sql;

        List<ZetaUDF> udfList = new ArrayList<>();
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        ServiceLoader.load(ZetaUDF.class, classLoader).forEach(udfList::add);

        this.zetaStatementVisitor =
                new ZetaStatementVisitor(
                        inputTableName, catalogTableName, inputRowType, null, udfList);
    }

    @Override
    public SeaTunnelRowType typeMapping(List<String> inputColumnsMapping) {
        try {
            zetaStatementVisitor.clean();
            zetaStatementVisitor.setNeedWhere(false);
            Statement statement = CCJSqlParserUtil.parse(sql);
            // validate SQL statement
            statement.accept(zetaStatementVisitor);
            String[] array = zetaStatementVisitor.getOutputFieldNames().toArray();
            Collections.addAll(inputColumnsMapping, array);
            return zetaStatementVisitor.getResultRowType();
        } catch (Exception e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
    }

    @Override
    public SeaTunnelRow transformBySQL(SeaTunnelRow inputRow) {

        try {
            if (statement == null) {
                statement = CCJSqlParserUtil.parse(sql);
            }
            // validate SQL statement
            zetaStatementVisitor.clean();
            zetaStatementVisitor.setNeedWhere(true);
            zetaStatementVisitor.setInputFields(inputRow.getFields());
            statement.accept(zetaStatementVisitor);
            Object[] outputFields = zetaStatementVisitor.getOutputFields().toArray();
            SeaTunnelRow seaTunnelRow = new SeaTunnelRow(outputFields);
            seaTunnelRow.setRowKind(inputRow.getRowKind());
            seaTunnelRow.setTableId(inputRow.getTableId());

            return seaTunnelRow;
        } catch (Exception e) {
            throw new TransformException(
                    CommonErrorCode.UNSUPPORTED_OPERATION,
                    String.format("SQL parse failed: %s, cause: %s", sql, e.getMessage()));
        }
    }
}
