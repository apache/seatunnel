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

package org.apache.seatunnel.connectors.seatunnel.hudi;

import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.hudi.exception.HudiError;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HudiErrorTest {

    @Test
    void testHudiError() {
        // TODO test HudiError on hudi e2e after hudi e2e is ready
        SeaTunnelRuntimeException exception = HudiError.cannotFindParquetFile("test.table1");
        Assertions.assertEquals("HUDI-01", exception.getSeaTunnelErrorCode().getCode());
        Assertions.assertEquals(
                "ErrorCode:[HUDI-01], ErrorDescription:[Hudi connector can not find parquet file in table path 'test.table1', please check!]",
                exception.getMessage());
    }
}
