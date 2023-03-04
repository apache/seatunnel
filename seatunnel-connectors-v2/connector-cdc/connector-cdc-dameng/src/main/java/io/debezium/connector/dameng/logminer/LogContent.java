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

import io.debezium.connector.dameng.Scn;
import lombok.Builder;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;

import java.util.Date;

@Builder
@Getter
@ToString
@RequiredArgsConstructor
public class LogContent {
    private final Scn scn;
    private final Scn startScn;
    private final Scn commitScn;
    private final Date timestamp;
    private final Date startTimestamp;
    private final Date commitTimestamp;
    private final String xid;
    private final boolean rollBack;
    private final String operation;
    private final int operationCode;
    private final String segOwner;
    private final String tableName;
    private final String sqlRedo;
    private final String sqlUndo;
    private final int ssn;
    private final int csf;
    private final int status;
}
