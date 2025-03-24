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

package org.apache.seatunnel.e2e.transform;

import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestSQLIT extends TestSuiteBase {

    @TestTemplate
    public void testSQLTransform(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult sqlTransform = container.executeJob("/sql_transform.conf");
        Assertions.assertEquals(0, sqlTransform.getExitCode());
        Container.ExecResult sqlBinaryExpression =
                container.executeJob("/sql_transform/binary_expression.conf");
        Assertions.assertEquals(0, sqlBinaryExpression.getExitCode());
        Container.ExecResult sqlFuncString =
                container.executeJob("/sql_transform/func_string.conf");
        Assertions.assertEquals(0, sqlFuncString.getExitCode());
        Container.ExecResult sqlFuncNumeric =
                container.executeJob("/sql_transform/func_numeric.conf");
        Assertions.assertEquals(0, sqlFuncNumeric.getExitCode());
        Container.ExecResult sqlFuncDatetime =
                container.executeJob("/sql_transform/func_datetime.conf");
        Assertions.assertEquals(0, sqlFuncDatetime.getExitCode());
        Container.ExecResult sqlFuncSystem =
                container.executeJob("/sql_transform/func_system.conf");
        Assertions.assertEquals(0, sqlFuncSystem.getExitCode());
        Container.ExecResult sqlFuncFromUnixtime =
                container.executeJob("/sql_transform/func_from_unixtime.conf");
        Assertions.assertEquals(0, sqlFuncFromUnixtime.getExitCode());
        Container.ExecResult sqlCriteriaFilter =
                container.executeJob("/sql_transform/criteria_filter.conf");
        Assertions.assertEquals(0, sqlCriteriaFilter.getExitCode());
        Container.ExecResult sqlAllColumns =
                container.executeJob("/sql_transform/sql_all_columns.conf");
        Assertions.assertEquals(0, sqlAllColumns.getExitCode());
        Container.ExecResult caseWhenSql = container.executeJob("/sql_transform/case_when.conf");
        Assertions.assertEquals(0, caseWhenSql.getExitCode());

        Container.ExecResult execResultBySql =
                container.executeJob("/sql_transform/explode_transform.conf");
        Assertions.assertEquals(0, execResultBySql.getExitCode());

        Container.ExecResult execResultBySqlWithoutOuter =
                container.executeJob("/sql_transform/explode_transform_without_outer.conf");
        Assertions.assertEquals(0, execResultBySqlWithoutOuter.getExitCode());

        Container.ExecResult execResultBySqlWithOuter =
                container.executeJob("/sql_transform/explode_transform_with_outer.conf");
        Assertions.assertEquals(0, execResultBySqlWithOuter.getExitCode());

        Container.ExecResult arraySql = container.executeJob("/sql_transform/func_array.conf");
        Assertions.assertEquals(0, arraySql.getExitCode());

        Container.ExecResult splitSql = container.executeJob("/sql_transform/func_split.conf");
        Assertions.assertEquals(0, splitSql.getExitCode());

        Container.ExecResult maxMinSql =
                container.executeJob("/sql_transform/func_array_max_min.conf");
        Assertions.assertEquals(0, maxMinSql.getExitCode());
    }

    @TestTemplate
    public void testSQLTransformMultiTable(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult sqlTransform = container.executeJob("/sql_transform.conf");
        Assertions.assertEquals(0, sqlTransform.getExitCode());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.SPARK},
            disabledReason = "Spark translation has some issue on map convert")
    public void testInnerQuery(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult innerQuerySql =
                container.executeJob("/sql_transform/inner_query.conf");
        Assertions.assertEquals(0, innerQuerySql.getExitCode());
    }
}
