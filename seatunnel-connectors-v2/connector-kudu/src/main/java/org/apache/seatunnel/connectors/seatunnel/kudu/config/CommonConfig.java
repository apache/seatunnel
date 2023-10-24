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

package org.apache.seatunnel.connectors.seatunnel.kudu.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.kudu.client.AsyncKuduClient;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@ToString
public class CommonConfig implements Serializable {

    public static final Option<String> MASTER =
            Options.key("kudu_masters")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu master address. Separated by ','");

    public static final Option<String> TABLE_NAME =
            Options.key("table_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kudu table name");

    public static final Option<Integer> WORKER_COUNT =
            Options.key("client_worker_count")
                    .intType()
                    .defaultValue(2 * Runtime.getRuntime().availableProcessors())
                    .withDescription(
                            "Kudu worker count. Default value is twice the current number of cpu cores");

    public static final Option<Long> OPERATION_TIMEOUT =
            Options.key("client_default_operation_timeout_ms")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu normal operation time out");

    public static final Option<Long> ADMIN_OPERATION_TIMEOUT =
            Options.key("client_default_admin_operation_timeout_ms")
                    .longType()
                    .defaultValue(AsyncKuduClient.DEFAULT_OPERATION_TIMEOUT_MS)
                    .withDescription("Kudu admin operation time out");

    public static final Option<Boolean> ENABLE_KERBEROS =
            Options.key("enable_kerberos")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Kerberos principal enable.");
    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kerberos principal. Note that all zeta nodes require have this file.");

    public static final Option<String> KERBEROS_KEYTAB =
            Options.key("kerberos_keytab")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kerberos keytab. Note that all zeta nodes require have this file.");

    public static final Option<String> KERBEROS_KRB5_CONF =
            Options.key("kerberos_krb5conf")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Kerberos krb5 conf. Note that all zeta nodes require have this file.");

    protected String masters;

    protected String table;

    protected Integer workerCount;

    protected Long operationTimeout;

    protected Long adminOperationTimeout;

    protected Boolean enableKerberos;
    protected String principal;
    protected String keytab;
    protected String krb5conf;

    public CommonConfig(ReadonlyConfig config) {
        this.masters = config.get(MASTER);
        this.table = config.get(TABLE_NAME);
        this.workerCount = config.get(WORKER_COUNT);
        this.operationTimeout = config.get(OPERATION_TIMEOUT);
        this.adminOperationTimeout = config.get(ADMIN_OPERATION_TIMEOUT);
        this.enableKerberos = config.get(ENABLE_KERBEROS);
        this.principal = config.get(KERBEROS_PRINCIPAL);
        this.keytab = config.get(KERBEROS_KEYTAB);
        this.krb5conf = config.get(KERBEROS_KEYTAB);
    }
}
