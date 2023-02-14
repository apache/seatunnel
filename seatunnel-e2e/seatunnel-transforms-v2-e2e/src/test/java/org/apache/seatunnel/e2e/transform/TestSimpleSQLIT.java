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

import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.spark.AbstractTestSparkContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import java.io.IOException;

public class TestSimpleSQLIT extends TestSuiteBase {

    @TestTemplate
    public void testSimpleSQL(TestContainer container) throws IOException, InterruptedException {
        if (container instanceof AbstractTestSparkContainer) {
            return; // skip test for spark, because some problems of Timestamp convert unresolved.
        }

        Container.ExecResult execResult = container.executeJob("/simple_sql_transform.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // -------------binary expression-------------
        execResult = container.executeJob("/simple_sql_functions/binary_expression.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // -------------start test functions-------------
        execResult = container.executeJob("/simple_sql_functions/func_ascii.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_bit_length.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_char_length.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_octet_length.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_chr.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_concat.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_hextoraw.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_rawtohex.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_insert.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_lower.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_upper.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_left.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_right.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_lpad.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_rpad.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_ltrim.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_rtrim.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_trim.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_regexp_replace.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_regexp_like.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_regexp_substr.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_repeat.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_replace.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_soundex.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_space.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_substr.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_to_char.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_translate.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_abs.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_acos.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_asin.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_atan.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_cos.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_cosh.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_sin.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_sinh.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_tan.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_tanh.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_mod.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_ceil.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_exp.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_floor.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_ln.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_log.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_log10.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_radians.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_sqrt.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_pi.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_power.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_rand.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_round.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_sign.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_trunc.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_current_date.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_current_time.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_current_timestamp.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_dateadd.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_datediff.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_date_trunc.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_dayname.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_day_of_week.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_day_of_year.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_extract.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_formatdatetime.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_hour.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_minute.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_month.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_monthname.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_parsedatetime.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_quarter.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_second.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_week.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_year.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_cast.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_coalesce.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_ifnull.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/func_nullif.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // -------------start test filter-------------
        execResult = container.executeJob("/simple_sql_functions/filter_equals_to.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_not_equals_to.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_great_than.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_great_than_equals.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_minor_than.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_minor_than_equals.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_is_null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_is_not_null.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_in.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_not_in.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_function.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        execResult = container.executeJob("/simple_sql_functions/filter_and_or.conf");
        Assertions.assertEquals(0, execResult.getExitCode());
    }
}
