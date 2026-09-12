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

package org.apache.seatunnel.connectors.seatunnel.syslog.sink;

import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableSink;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSinkFactory;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.syslog.config.SyslogSinkOptions;

import com.google.auto.service.AutoService;

@AutoService(Factory.class)
public final class SyslogSinkFactory implements TableSinkFactory {
    @Override
    public String factoryIdentifier() {
        return SyslogSinkOptions.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(SyslogSinkOptions.HOST, Conditions.notBlank(SyslogSinkOptions.HOST))
                .optional(
                        SyslogSinkOptions.PORT,
                        Conditions.greaterOrEqual(SyslogSinkOptions.PORT, 1)
                                .and(Conditions.lessOrEqual(SyslogSinkOptions.PORT, 65535)))
                .optional(
                        SyslogSinkOptions.CONNECT_TIMEOUT,
                        Conditions.greaterThan(SyslogSinkOptions.CONNECT_TIMEOUT, 0))
                .optional(
                        SyslogSinkOptions.WRITE_TIMEOUT,
                        Conditions.greaterThan(SyslogSinkOptions.WRITE_TIMEOUT, 0))
                .optional(
                        SyslogSinkOptions.MAX_MESSAGE_BYTES,
                        Conditions.greaterThan(SyslogSinkOptions.MAX_MESSAGE_BYTES, 0)
                                .and(
                                        Conditions.lessOrEqual(
                                                SyslogSinkOptions.MAX_MESSAGE_BYTES, 1048576)))
                .optional(SyslogSinkOptions.CA_CERT, Conditions.notBlank(SyslogSinkOptions.CA_CERT))
                .optional(
                        SyslogSinkOptions.KEY_STORE,
                        Conditions.notBlank(SyslogSinkOptions.KEY_STORE))
                .optional(SyslogSinkOptions.KEY_STORE_PASSWORD, SyslogSinkOptions.KEY_STORE_TYPE)
                .build();
    }

    @Override
    public TableSink createSink(TableSinkFactoryContext context) {
        SyslogSink sink =
                new SyslogSink(
                        new SyslogSinkConfig(context.getOptions()), context.getCatalogTable());
        return () -> sink;
    }
}
