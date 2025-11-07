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

package org.apache.seatunnel.connectors.seatunnel.file.hadoop;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;

public class HadoopLoginFactory {

    /** Login with kerberos, and do the given action after login successfully. */
    public static <T> T loginWithKerberos(
            Configuration configuration,
            String krb5FilePath,
            String kerberosPrincipal,
            String kerberosKeytabPath,
            LoginFunction<T> action)
            throws IOException, InterruptedException {
        if (!configuration.get("hadoop.security.authentication").equals("kerberos")) {
            throw new IllegalArgumentException("hadoop.security.authentication must be kerberos");
        }
        // Use global lock to avoid multiple threads to execute setConfiguration at the same time
        synchronized (UserGroupInformation.class) {
            if (StringUtils.isNotEmpty(krb5FilePath)) {
                System.setProperty("java.security.krb5.conf", krb5FilePath);
            }
            // init configuration
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation userGroupInformation =
                    UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                            kerberosPrincipal, kerberosKeytabPath);
            return userGroupInformation.doAs(
                    (PrivilegedExceptionAction<T>)
                            () -> action.run(configuration, userGroupInformation));
        }
    }

    /** Login with remote user, and do the given action after login successfully. */
    public static <T> T loginWithRemoteUser(
            Configuration configuration, String remoteUser, LoginFunction<T> action)
            throws Exception {

        // Use global lock to avoid multiple threads to execute setConfiguration at the same time
        synchronized (UserGroupInformation.class) {
            // init configuration
            UserGroupInformation userGroupInformation =
                    UserGroupInformation.createRemoteUser(remoteUser);
            return userGroupInformation.doAs(
                    (PrivilegedExceptionAction<T>)
                            () -> action.run(configuration, userGroupInformation));
        }
    }

    public interface LoginFunction<T> {

        T run(Configuration configuration, UserGroupInformation userGroupInformation)
                throws Exception;
    }
}
