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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

import static org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode.KERBEROS_AUTHENTICATION_FAILED;

@Slf4j
public class HiveJdbcUtils {

    public static synchronized void doKerberosAuthentication(JdbcConnectionConfig jdbcConfig) {
        String principal = jdbcConfig.kerberosPrincipal;
        String keytabPath = jdbcConfig.kerberosKeytabPath;
        String krb5Path = jdbcConfig.krb5Path;
        System.setProperty("java.security.krb5.conf", krb5Path);
        Configuration configuration = new Configuration();

        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
            log.warn(
                    "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
                    principal,
                    keytabPath);
        } else {
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                log.info(
                        "Start Kerberos authentication using principal {} and keytab {}",
                        principal,
                        keytabPath);
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                log.info("Kerberos authentication successful");
            } catch (IOException e) {
                String errorMsg =
                        String.format(
                                "Kerberos authentication failed using this "
                                        + "principal [%s] and keytab path [%s]",
                                principal, keytabPath);
                throw new JdbcConnectorException(KERBEROS_AUTHENTICATION_FAILED, errorMsg, e);
            }
        }
    }
}
