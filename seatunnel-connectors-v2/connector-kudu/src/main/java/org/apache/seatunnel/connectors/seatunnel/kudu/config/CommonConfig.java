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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;

@Getter
@ToString
public class CommonConfig implements Serializable {

    protected String masters;
    protected Integer workerCount;

    protected Long operationTimeout;

    protected Long adminOperationTimeout;

    protected Boolean enableKerberos;
    protected String principal;
    protected String keytab;
    protected String krb5conf;

    public CommonConfig(ReadonlyConfig config) {
        this.masters = config.get(KuduBaseOptions.MASTER);
        this.workerCount = config.get(KuduBaseOptions.WORKER_COUNT);
        this.operationTimeout = config.get(KuduBaseOptions.OPERATION_TIMEOUT);
        this.adminOperationTimeout = config.get(KuduBaseOptions.ADMIN_OPERATION_TIMEOUT);
        this.enableKerberos = config.get(KuduBaseOptions.ENABLE_KERBEROS);
        this.principal = config.get(KuduBaseOptions.KERBEROS_PRINCIPAL);
        this.keytab = config.get(KuduBaseOptions.KERBEROS_KEYTAB);
        this.krb5conf = config.get(KuduBaseOptions.KERBEROS_KRB5_CONF);
    }
}
