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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.lineage.flink.LineageJobStatusHook;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Binds the hook class name that the common starter can only spell as a literal to the real class.
 *
 * <p>{@code seatunnel-flink-starter-common} compiles against Flink 1.15 and therefore cannot
 * resolve {@link LineageJobStatusHook}, which implements a 1.16+ interface. It nevertheless has to
 * name it: the diagnostic for a JobManager that is missing the lineage jar recognises the failure
 * by matching this name in a stack trace relayed as text over REST. A rename that left the literal
 * behind would silently turn that diagnostic off, and the operator would be back to a bare {@code
 * ClassNotFoundException}. This module can resolve both, so it asserts they agree.
 */
class FlinkLineageHookNamingTest {

    @Test
    void theCommonStarterNamesTheRealHookClass() {
        Assertions.assertEquals(
                LineageJobStatusHook.class.getName(),
                FlinkLineageSupport.HOOK_HANDLER_CLASS,
                "the missing-deployment diagnostic must name the class the JobManager loads");
    }
}
