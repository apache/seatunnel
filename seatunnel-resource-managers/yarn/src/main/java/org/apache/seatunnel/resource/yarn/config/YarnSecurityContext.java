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

package org.apache.seatunnel.resource.yarn.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.resource.yarn.exception.YarnClientException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.security.token.TokenIdentifier;
import org.apache.hadoop.yarn.client.util.YarnClientUtils;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;

/** Owns the submitter identity and the filesystem credentials carried by YARN containers. */
public final class YarnSecurityContext {
    private final Configuration configuration;
    private final String simpleUserName;
    private final String principal;
    private final String keytab;
    private UserGroupInformation identity;

    public YarnSecurityContext(Configuration configuration, ReadonlyConfig options) {
        this.configuration = configuration;
        simpleUserName = options.get(YarnOptions.HADOOP_USER_NAME);
        principal = options.get(YarnOptions.KERBEROS_PRINCIPAL);
        keytab = options.get(YarnOptions.KERBEROS_KEYTAB);
        if (isKerberos(configuration) && principal.isEmpty() != keytab.isEmpty()) {
            throw new IllegalArgumentException(
                    "Configure yarn.kerberos.principal and yarn.kerberos.keytab together");
        }
    }

    public static boolean isKerberos(Configuration configuration) {
        return SecurityUtil.getAuthenticationMethod(configuration)
                == UserGroupInformation.AuthenticationMethod.KERBEROS;
    }

    /**
     * Runs each descriptor operation under the same Hadoop identity. Simple mode uses the current
     * Hadoop user or the explicitly selected user. Kerberos uses the current ticket or keytab.
     *
     * @param action operation to run under the submission identity
     * @param <T> result type of the operation
     * @return the result of the action
     * @throws Exception if establishing the identity or executing the action fails
     */
    public <T> T run(PrivilegedExceptionAction<T> action) throws Exception {
        try {
            UserGroupInformation user = submitter();
            if (isKerberos(configuration) && user.isFromKeytab()) {
                user.checkTGTAndReloginFromKeytab();
            }
            return user.doAs(action);
        } catch (IOException failure) {
            throw new YarnClientException(
                    "Could not run YARN operation as the Hadoop submitter", failure);
        } catch (UndeclaredThrowableException wrapped) {
            // UGI wraps checked application exceptions other than IOException and
            // InterruptedException.
            if (wrapped.getUndeclaredThrowable() instanceof Exception) {
                throw (Exception) wrapped.getUndeclaredThrowable();
            }
            throw wrapped;
        }
    }

    private synchronized UserGroupInformation submitter() throws IOException {
        if (identity == null) {
            if (!isKerberos(configuration)) {
                identity = simpleUser(simpleUserName);
                return identity;
            }
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation user =
                    principal.isEmpty()
                            ? UserGroupInformation.getCurrentUser()
                            : UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                                    principal, keytab);
            if (!user.hasKerberosCredentials()) {
                throw new IOException(
                        "YARN submission requires a Kerberos ticket from kinit or yarn.kerberos.principal and yarn.kerberos.keytab");
            }
            identity = user;
        }
        return identity;
    }

    /** Preserves an existing doAs subject and its credentials when it already owns the request. */
    static UserGroupInformation simpleUser(String configuredName) {
        try {
            UserGroupInformation current = UserGroupInformation.getCurrentUser();
            if (configuredName == null) {
                return current;
            }
            String name = configuredName.trim();
            if (name.isEmpty()) {
                throw new IllegalArgumentException(
                        YarnOptions.HADOOP_USER_NAME.key() + " must not be empty");
            }
            if (name.equals(current.getShortUserName()) || name.equals(current.getUserName())) {
                return current;
            }
            return UserGroupInformation.createRemoteUser(name);
        } catch (IOException failure) {
            throw new YarnClientException("Could not resolve the current Hadoop user", failure);
        }
    }

    /**
     * Obtains delegation tokens for the staging filesystem under the submitter's identity and sets
     * the ResourceManager principal as their renewer.
     *
     * @param configuration Hadoop configuration used for authentication and RM principal lookup
     * @param staging private application directory that the AM and workers must read
     * @return serialized credentials for the AM launch context, or {@code null} in simple mode
     * @throws IOException if resolving the renewer, fetching tokens or serializing credentials
     *     fails
     */
    public static ByteBuffer masterTokens(Configuration configuration, Path staging)
            throws IOException {
        if (!isKerberos(configuration)) {
            return null;
        }
        String renewer = YarnClientUtils.getRmPrincipal(configuration);
        if (renewer == null || renewer.isEmpty()) {
            throw new IOException(
                    "yarn.resourcemanager.principal is required to obtain HDFS delegation tokens");
        }
        Credentials credentials = new Credentials();
        try (FileSystem fs = FileSystem.newInstance(staging.toUri(), configuration)) {
            fs.addDelegationTokens(renewer, credentials);
        }
        return serialize(credentials);
    }

    /**
     * Copies the AM's current HDFS delegation tokens into one worker launch. The AM retains its
     * ResourceManager control token.
     *
     * @param configuration container configuration that selects the authentication mode
     * @return serialized worker credentials, or {@code null} in simple mode
     * @throws IOException if reading or serializing the AM's credentials fails
     */
    public static ByteBuffer workerTokens(Configuration configuration) throws IOException {
        if (!isKerberos(configuration)) {
            return null;
        }
        Credentials credentials = new Credentials();
        for (Token<? extends TokenIdentifier> token :
                UserGroupInformation.getCurrentUser().getTokens()) {
            if (DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(token.getKind())) {
                credentials.addToken(token.getService(), token);
            }
        }
        return serialize(credentials);
    }

    private static ByteBuffer serialize(Credentials credentials) throws IOException {
        try (DataOutputBuffer output = new DataOutputBuffer()) {
            credentials.writeTokenStorageToStream(output);
            return ByteBuffer.wrap(output.getData(), 0, output.getLength());
        }
    }
}
