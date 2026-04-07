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

package org.apache.seatunnel.connectors.doris.util;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class DorisRedirectExceptionBuilderTest {

    @Test
    void testBuildContainsRoutingContextAndHint() {
        String message =
                DorisRedirectExceptionBuilder.build(
                        "http://fe1:8030/api/test_db/test_table/_stream_load",
                        "http://be1:8040/api/test_db/test_table/_stream_load",
                        true,
                        true,
                        "commit");

        Assertions.assertTrue(message.contains("307 Temporary Redirect"));
        Assertions.assertTrue(
                message.contains("request=http://fe1:8030/api/test_db/test_table/_stream_load"));
        Assertions.assertTrue(
                message.contains("Location=http://be1:8040/api/test_db/test_table/_stream_load"));
        Assertions.assertTrue(message.contains("direct_to_be=true"));
        Assertions.assertTrue(message.contains("2pc=true"));
        Assertions.assertTrue(message.contains("stage=commit"));
        Assertions.assertTrue(message.contains("BE reachability"));
    }

    @Test
    void testBuildFollowUpFailureContainsRedirectAndCause() {
        String message =
                DorisRedirectExceptionBuilder.buildFollowUpFailure(
                        "http://fe1:8030/api/test_db/test_table/_stream_load",
                        "http://be1:8040/api/test_db/test_table/_stream_load",
                        true,
                        true,
                        "stream-load-write",
                        "Connection refused");

        Assertions.assertTrue(message.contains("redirect follow-up failed"));
        Assertions.assertTrue(
                message.contains("Location=http://be1:8040/api/test_db/test_table/_stream_load"));
        Assertions.assertTrue(message.contains("stage=stream-load-write"));
        Assertions.assertTrue(message.contains("cause=Connection refused"));
    }
}
