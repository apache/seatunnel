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
package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class JdbcSinkConfigChecker {

    public static void check(ReadonlyConfig readonlyConfig) {
        if (readonlyConfig.get(JdbcSinkOptions.USE_SQLSERVER_BULK_COPY)) {
            if (readonlyConfig.get(JdbcSinkOptions.AUTO_COMMIT)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, auto_commit is true and does not take effect.");
            }
            if (readonlyConfig.get(JdbcSinkOptions.IS_EXACTLY_ONCE)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, is_exactly_once is true and does not take effect.");
            }
            if (readonlyConfig.get(JdbcSinkOptions.ENABLE_UPSERT)) {
                log.warn(
                        "When use_sqlserver_bulk_copy is enabled, enable_upsert is true and does not take effect.");
            }
            if (StringUtils.isBlank(readonlyConfig.get(JdbcSinkOptions.TABLE))) {
                // throw new SeaTunnelRuntimeException();
            }
        }
    }
}
