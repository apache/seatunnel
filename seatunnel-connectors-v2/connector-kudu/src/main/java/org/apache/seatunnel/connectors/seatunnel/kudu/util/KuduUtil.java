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

package org.apache.seatunnel.connectors.seatunnel.kudu.util;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.kudu.Schema;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import lombok.extern.slf4j.Slf4j;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;

@Slf4j
public class KuduUtil {

    private static final String ERROR_MESSAGE =
            "principal and keytab can not be null current principal %s keytab %s";

    public static final String KRB5_CONF_KEY = "java.security.krb5.conf";

    public static final String HADOOP_AUTH_KEY = "hadoop.security.authentication";

    public static final String KRB = "kerberos";

    public static KuduClient getKuduClient(CommonConfig config) {
        try {
            if (config.getEnableKerberos()) {
                synchronized (UserGroupInformation.class) {
                    UserGroupInformation ugi = loginAndReturnUgi(config);
                    return ugi.doAs(
                            (PrivilegedExceptionAction<KuduClient>)
                                    () -> getKuduClientInternal(config));
                }
            }
            return getKuduClientInternal(config);

        } catch (IOException | InterruptedException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
        }
    }

    private static UserGroupInformation loginAndReturnUgi(CommonConfig config) throws IOException {
        if (StringUtils.isBlank(config.getPrincipal()) || StringUtils.isBlank(config.getKeytab())) {
            throw new KuduConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT,
                    String.format(ERROR_MESSAGE, config.getPrincipal(), config.getKeytab()));
        }
        if (StringUtils.isNotBlank(config.getKrb5conf())) {
            reloadKrb5conf(config.getKrb5conf());
        }
        Configuration conf = new Configuration();
        conf.set(HADOOP_AUTH_KEY, KRB);
        UserGroupInformation.setConfiguration(conf);
        log.info(
                "Start Kerberos authentication using principal {} and keytab {}",
                config.getPrincipal(),
                config.getKeytab());
        return UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                config.getPrincipal(), config.getKeytab());
    }

    private static void reloadKrb5conf(String krb5conf) {
        System.setProperty(KRB5_CONF_KEY, krb5conf);
        try {
            Config.refresh();
            KerberosName.resetDefaultRealm();
        } catch (KrbException e) {
            log.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }

    private static KuduClient getKuduClientInternal(CommonConfig config) {
        return new AsyncKuduClient.AsyncKuduClientBuilder(
                        Arrays.asList(config.getMasters().split(",")))
                .workerCount(config.getWorkerCount())
                .defaultAdminOperationTimeoutMs(config.getAdminOperationTimeout())
                .defaultOperationTimeoutMs(config.getOperationTimeout())
                .build()
                .syncClient();
    }

    public static List<KuduScanToken> getKuduScanToken(
            KuduSourceConfig kuduSourceConfig, String[] columnName) throws IOException {
        try (KuduClient client = KuduUtil.getKuduClient(kuduSourceConfig)) {
            KuduTable kuduTable = client.openTable(kuduSourceConfig.getTable());
            List<String> columnNameList = Arrays.asList(columnName);

            KuduScanToken.KuduScanTokenBuilder builder =
                    client.newScanTokenBuilder(kuduTable)
                            .batchSizeBytes(kuduSourceConfig.getBatchSizeBytes())
                            .setTimeout(kuduSourceConfig.getQueryTimeout())
                            .setProjectedColumnNames(columnNameList);

            addPredicates(builder, kuduSourceConfig.getFilter(), kuduTable.getSchema());

            return builder.build();
        } catch (Exception e) {
            throw new IOException("Get ScanToken error", e);
        }
    }

    private static void addPredicates(
            KuduScanToken.KuduScanTokenBuilder kuduScanTokenBuilder, String filter, Schema schema) {
        // todo Support for where condition
    }
}
