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

package org.apache.seatunnel.connectors.seatunnel.hudi.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;

public class HudiSourceConfig {
    public static final Option<String> TABLE_PATH =
            Options.key("table.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi table path");

    public static final Option<String> TABLE_TYPE =
            Options.key("table.type")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi table type. default hudi table type is cow. mor is not support yet");

    public static final Option<String> CONF_FILES =
            Options.key("conf.files")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi conf files ");

    public static final Option<Boolean> USE_KERBEROS =
            Options.key("use.kerberos")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("hudi use.kerberos");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos.principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi kerberos.principal");

    public static final Option<String> KERBEROS_PRINCIPAL_FILE =
            Options.key("kerberos.principal.file")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("hudi kerberos.principal.file ");

}
