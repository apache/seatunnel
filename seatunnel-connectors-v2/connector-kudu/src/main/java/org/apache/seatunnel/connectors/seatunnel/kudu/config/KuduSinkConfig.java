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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.commons.lang3.StringUtils;

import lombok.Data;
import lombok.NonNull;

@Data
public class KuduSinkConfig {

    public static final Option<String> KUDU_MASTER =
            Options.key("kudu_master")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu master address");

    public static final Option<SaveMode> KUDU_SAVE_MODE =
            Options.key("save_mode")
                    .enumType(SaveMode.class)
                    .noDefaultValue()
                    .withDescription("Storage mode,append is now supported");

    public static final Option<String> KUDU_TABLE_NAME =
            Options.key("kudu_table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("kudu table name");

    public static final Option<String> KRB5_CONF_PATH =
            Options.key("krb5.conf.path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("krb5 conf path");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos principal");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Kerberos keytab file path");


    private SaveMode saveMode;

    private String kuduMaster;

    /**
     * Specifies the name of the table
     */
    private String kuduTableName;

    private String kerberosPrincipal;

    private String kerberosKeytabPath;

    private String krb5ConfPath;

    public enum SaveMode {
        APPEND(),
        OVERWRITE();

        public static SaveMode fromStr(String str) {
            if ("overwrite".equals(str)) {
                return OVERWRITE;
            } else {
                return APPEND;
            }
        }
    }

    public KuduSinkConfig(@NonNull Config pluginConfig) {
        if (pluginConfig.hasPath(KUDU_SAVE_MODE.key())
                && pluginConfig.hasPath(KUDU_MASTER.key())
                && pluginConfig.hasPath(KUDU_TABLE_NAME.key())) {
            this.saveMode =
                    StringUtils.isBlank(pluginConfig.getString(KUDU_SAVE_MODE.key()))
                            ? SaveMode.APPEND
                            : SaveMode.fromStr(pluginConfig.getString(KUDU_SAVE_MODE.key()));
            this.kuduMaster = pluginConfig.getString(KUDU_MASTER.key());
            this.kuduTableName = pluginConfig.getString(KUDU_TABLE_NAME.key());
        } else {
            throw new KuduConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            "Kudu", PluginType.SINK, "Missing Sink configuration parameters"));
        }
        if (pluginConfig.hasPath(KRB5_CONF_PATH.key())
                && pluginConfig.hasPath(KERBEROS_PRINCIPAL.key())
                && pluginConfig.hasPath(KERBEROS_KEYTAB_PATH.key())) {
            this.krb5ConfPath = pluginConfig.getString(KRB5_CONF_PATH.key());
            this.kerberosPrincipal = pluginConfig.getString(KERBEROS_PRINCIPAL.key());
            this.kerberosKeytabPath = pluginConfig.getString(KERBEROS_KEYTAB_PATH.key());
        }
    }
}
