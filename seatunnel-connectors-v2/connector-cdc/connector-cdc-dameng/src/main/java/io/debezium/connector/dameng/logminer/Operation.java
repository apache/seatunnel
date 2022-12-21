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

package io.debezium.connector.dameng.logminer;

import lombok.RequiredArgsConstructor;

import java.util.Arrays;

@RequiredArgsConstructor
public enum Operation {
    INTERNAL(0),
    INSERT(1),
    DELETE(2),
    UPDATE(3),
    BATCH_UPDATE(4),
    DDL(5),
    START(6),
    COMMIT(7),
    SEL_LOB_LOCATOR(9),
    LOB_WRITE(10),
    LOB_TRIM(11),
    SELECT_FOR_UPDATE(25),
    LOB_ERASE(28),
    MISSING_SCN(34),
    ROLLBACK(36),
    SEQ_MODIFY(37),
    XA_COMMIT(38),
    UNSUPPORTED(255);

    private final int code;

    public static Operation parse(int operationCode) throws IllegalArgumentException {
        return Arrays.stream(Operation.values())
            .filter(o -> o.code == operationCode)
            .findFirst()
            .orElseThrow(() -> new IllegalArgumentException("Illegal operationCode: " + operationCode));
    }
}
