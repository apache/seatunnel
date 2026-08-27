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

package org.apache.seatunnel.connectors.seatunnel.jdbc.config;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import java.util.Map;

public class JdbcCommonOptions {

    public static final Option<String> URL =
            Options.key("url")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("base-url")
                    .withDescription("url");

    public static final Option<String> DRIVER =
            Options.key("driver").stringType().noDefaultValue().withDescription("driver");

    public static final Option<String> SCHEMA =
            Options.key("schema")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "for databases that support the schema parameter, give it priority.");

    public static final Option<Integer> CONNECTION_CHECK_TIMEOUT_SEC =
            Options.key("connection_check_timeout_sec")
                    .intType()
                    .defaultValue(30)
                    .withDescription("connection check time second");

    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(1000 * 60 * 60 * 24)
                    .withDescription(
                            "Socket timeout in milliseconds for reading data from the server. Default is 24h. Set to 0 for no timeout.");

    public static final Option<Integer> CONNECT_TIMEOUT_MS =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(1000 * 60 * 60 * 24)
                    .withDescription(
                            "Connection timeout in milliseconds for establishing connection to the server. Default is 24h. Set to 0 for no timeout.");

    public static final Option<String> COMPATIBLE_MODE =
            Options.key("compatible_mode")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("compatibleMode")
                    .withDescription(
                            "The compatible mode of database, required when the database supports multiple compatible modes. For example, when using OceanBase database, you need to set it to 'mysql' or 'oracle'.");

    public static final Option<String> DIALECT =
            Options.key("dialect")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The appointed dialect, if it does not exist, is still obtained according to the url");

    public static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withFallbackKeys("user")
                    .withDescription("user");

    public static final Option<String> PASSWORD =
            Options.key("password").stringType().noDefaultValue().withDescription("password");

    public static final Option<String> QUERY =
            Options.key("query").stringType().noDefaultValue().withDescription("query");

    public static final Option<Boolean> DECIMAL_TYPE_NARROWING =
            Options.key("decimal_type_narrowing")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "decimal type narrowing, if true, the decimal type will be narrowed to the int or long type if without loss of precision. Only support for Oracle at now.");

    public static final Option<Boolean> INT_TYPE_NARROWING =
            Options.key("int_type_narrowing")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "int type narrowing, if true, the tinyint(1) type will be narrowed to the boolean type if without loss of precision. Support for MySQL at now.");

    public static final Option<Boolean> HANDLE_BLOB_AS_STRING =
            Options.key("handle_blob_as_string")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "If true, BLOB type will be converted to STRING type. Only support for Oracle at now.");

    public static final Option<Boolean> USE_KERBEROS =
            Options.key("use_kerberos")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable Kerberos, default is false.");

    public static final Option<String> KERBEROS_PRINCIPAL =
            Options.key("kerberos_principal")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When use kerberos, we should set kerberos principal such as 'test_user@xxx'.");

    public static final Option<String> KERBEROS_KEYTAB_PATH =
            Options.key("kerberos_keytab_path")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "When use kerberos, we should set kerberos principal file path such as '/home/test/test_user.keytab'.");

    public static final Option<String> KRB5_PATH =
            Options.key("krb5_path")
                    .stringType()
                    .defaultValue("/etc/krb5.conf")
                    .withDescription(
                            "When use kerberos, we should set krb5 path file path such as '/seatunnel/krb5.conf' or use the default path '/etc/krb5.conf");

    public static final Option<Map<String, String>> PROPERTIES =
            Options.key("properties")
                    .mapType()
                    .noDefaultValue()
                    .withDescription("additional connection configuration parameters");
    public static final Option<String> ACCESS_KEY_ID =
            Options.key("access_key_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("access_key_id");

    public static final Option<String> SECRET_ACCESS_KEY =
            Options.key("secret_access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("secret_access_key");

    public static final Option<String> REGION =
            Options.key("region").stringType().noDefaultValue().withDescription("region");

    /**
     * Returns a fresh {@link OptionRule.Builder} with the base validation rules shared by standard
     * JDBC catalog factories (MySQL, PostgreSQL, etc.) that use generic {@code host:port/database}
     * URL format and require username/password authentication.
     *
     * <p>Catalog factories with non-standard URL formats (SqlServer, Oracle, SapHana) should use
     * {@link #baseCatalogRule(ConditionExtension)} with their own URL validator.
     *
     * <p>Catalog factories that do not require authentication (e.g. DuckDB) should define their own
     * {@code optionRule()} directly.
     */
    public static OptionRule.Builder baseCatalogRule() {
        return baseCatalogRule(new UrlContainsDatabaseValidator());
    }

    /**
     * Returns a fresh {@link OptionRule.Builder} with a custom URL validator. Use this for
     * databases whose JDBC URL does not follow the standard {@code host:port/database} format (e.g.
     * SqlServer, Oracle, SapHana).
     */
    public static OptionRule.Builder baseCatalogRule(ConditionExtension<String> urlValidator) {
        return OptionRule.builder()
                .required(URL, Conditions.extension(URL, urlValidator))
                .required(USERNAME, PASSWORD)
                .optional(SCHEMA, DECIMAL_TYPE_NARROWING, HANDLE_BLOB_AS_STRING);
    }

    /**
     * Validates that the JDBC URL has a valid format with at least a host component. Database name
     * is optional to maintain backward compatibility with connectors (e.g. StarRocks, Doris) that
     * specify the database in the query or table_path instead of the URL.
     */
    public static class UrlContainsDatabaseValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "JDBC URL must be a valid format: jdbc:<scheme>://host:port[/database]";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String url) {
            if (url == null || url.trim().isEmpty()) {
                return false;
            }
            try {
                JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(url);
                return StringUtils.isNotBlank(urlInfo.getHost());
            } catch (IllegalArgumentException e) {
                throw new OptionValidationException(
                        String.format(
                                "Invalid JDBC URL format: [%s], "
                                        + "expected pattern: jdbc:<scheme>://host:port[/database]",
                                url));
            }
        }
    }
}
