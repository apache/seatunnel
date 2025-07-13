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

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.CommonConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSourceTableConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.Type;
import org.apache.kudu.client.AsyncKuduClient;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduPredicate;
import org.apache.kudu.client.KuduScanToken;
import org.apache.kudu.client.KuduTable;

import lombok.extern.slf4j.Slf4j;
import sun.security.krb5.Config;
import sun.security.krb5.KrbException;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
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
            KuduClient kuduClient,
            KuduSourceConfig kuduSourceConfig,
            KuduSourceTableConfig kuduSourceTableConfig)
            throws IOException {
        KuduTable kuduTable =
                kuduClient.openTable(kuduSourceTableConfig.getTablePath().getFullName());
        List<String> columnNameList =
                Arrays.asList(
                        kuduSourceTableConfig
                                .getCatalogTable()
                                .getSeaTunnelRowType()
                                .getFieldNames());
        KuduScanToken.KuduScanTokenBuilder builder =
                kuduClient
                        .newScanTokenBuilder(kuduTable)
                        .batchSizeBytes(kuduSourceConfig.getBatchSizeBytes())
                        .setTimeout(kuduSourceConfig.getQueryTimeout())
                        .setProjectedColumnNames(columnNameList);

        addPredicates(builder, kuduSourceTableConfig.getFilter(), kuduTable.getSchema());
        return builder.build();
    }

    private static void addPredicates(
            KuduScanToken.KuduScanTokenBuilder kuduScanTokenBuilder, String filter, Schema schema) {

        log.info("Adding predicates to Kudu scan token: {}", filter);

        List<ColumnSchema> columns = schema.getColumns();
        for (ColumnSchema column : columns) {
            log.info(" column name " + column.getName());
        }

        if (StringUtils.isBlank(filter)) {
            return;
        }

        List<String> conditions = Arrays.asList(filter.trim().split("\\s+AND\\s+"));

        Pattern pattern = Pattern.compile("(\\w+)\\s*([=><]=?|<=|>=)\\s*(.+)");
        for (String condition : conditions) {
            Matcher matcher = pattern.matcher(condition.trim());

            String column = null;
            String op = null;
            String value = null;

            if (matcher.matches()) {
                column = matcher.group(1);
                op = matcher.group(2);
                value = matcher.group(3);
            } else {
                throw new IllegalArgumentException("Invalid filter condition: " + condition);
            }

            if (!schema.hasColumn(column)) {
                throw new KuduConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Column not found in Kudu schema: " + column);
            }

            Type type = schema.getColumn(column).getType();

            KuduPredicate.ComparisonOp comparisonOp = null;
            switch (op) {
                case "=":
                    comparisonOp = KuduPredicate.ComparisonOp.EQUAL;
                    break;
                case ">":
                    comparisonOp = KuduPredicate.ComparisonOp.GREATER;
                    break;
                case ">=":
                    comparisonOp = KuduPredicate.ComparisonOp.GREATER_EQUAL;
                    break;
                case "<":
                    comparisonOp = KuduPredicate.ComparisonOp.LESS;
                    break;
                case "<=":
                    comparisonOp = KuduPredicate.ComparisonOp.LESS_EQUAL;
                    break;
                default:
                    throw new KuduConnectorException(
                            CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                            "Unsupported operator: " + op);
            }

            Object parsedValue = parseValue(type, value);

            KuduPredicate predicate =
                    KuduPredicate.newComparisonPredicate(
                            schema.getColumn(column), comparisonOp, parsedValue);
            kuduScanTokenBuilder.addPredicate(predicate);
        }
    }

    private static Object parseValue(Type type, String value) {
        try {
            switch (type.getDataType()) {
                case INT8:
                    return Byte.valueOf(value);
                case INT16:
                    return Short.valueOf(value);
                case INT32:
                    return Integer.valueOf(value);
                case INT64:
                    return Long.valueOf(value);
                case STRING:
                    return value.startsWith("'") && value.endsWith("'")
                            ? value.substring(1, value.length() - 1)
                            : value;
                case BOOL:
                    return Boolean.valueOf(value);
                case UNIXTIME_MICROS:
                    return new java.sql.Timestamp(Long.parseLong(value));
                case FLOAT:
                    return Float.valueOf(value);
                case DOUBLE:
                    return Double.valueOf(value);
                default:
                    throw new IllegalArgumentException("Unsupported type: " + type);
            }
        } catch (NumberFormatException e) {
            throw new KuduConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Failed to parse value '" + value + "' as type " + type,
                    e);
        }
    }
}
