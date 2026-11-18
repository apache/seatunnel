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

package org.apache.seatunnel.resource.e2e.yarn;

import org.apache.seatunnel.shade.com.google.common.io.Closer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;
import org.apache.seatunnel.resource.yarn.config.YarnOptions;
import org.apache.seatunnel.resource.yarn.config.YarnSecurityContext;
import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.YarnContainerLaunchContextFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.datatransfer.sasl.SaslDataTransferTestCase;
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.minikdc.MiniKdc;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.kerby.kerberos.kerb.client.KrbClient;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.ByteBuffer;
import java.security.PrivilegedExceptionAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Exercises the two submitter login paths and token-only HDFS access from a worker. */
@DisabledOnOs(
        value = OS.WINDOWS,
        disabledReason = "The YARN tests use a Unix container environment")
public class YarnKerberosIT extends TestSuiteBase {

    @TempDir File temporary;

    @Test
    void ticketCacheAndKeytabCanLocalizeFromSecureHdfs() throws Exception {
        SaslDataTransferTestCase.initKdc();
        try {
            HdfsConfiguration conf = new SecureHdfs().configuration();
            conf.set(MiniDFSCluster.HDFS_MINIDFS_BASEDIR, new File(temporary, "hdfs").getPath());
            conf.set(YarnConfiguration.RM_PRINCIPAL, SaslDataTransferTestCase.getHdfsPrincipal());
            UserGroupInformation.setConfiguration(conf);
            MiniDFSCluster cluster = new MiniDFSCluster.Builder(conf).numDataNodes(1).build();
            try {
                cluster.waitActive();
                FileSystem fs = cluster.getFileSystem();
                Path staging = fs.makeQualified(new Path("/app"));
                Path source = new Path(staging, "distribution.zip");
                try (FSDataOutputStream output = fs.create(source)) {
                    output.write(42);
                }
                Path manifest = new Path(staging, "distribution.properties");
                try (FSDataOutputStream output = fs.create(manifest)) {
                    output.writeBytes("archive=distribution.zip\nroot=\n");
                }
                fs.create(new Path(staging, YarnConstants.LOCALIZED_SPECIFICATION_NAME)).close();
                fs.create(new Path(staging, YarnConstants.LOCALIZED_HADOOP_CONFIG_NAME)).close();
                String principal = SaslDataTransferTestCase.getUserPrincipal();
                File keytab = new File(SaslDataTransferTestCase.getUserKeyTab());
                Map<String, Object> options = new HashMap<>();
                options.put(YarnOptions.KERBEROS_PRINCIPAL.key(), principal);
                options.put(YarnOptions.KERBEROS_KEYTAB.key(), keytab.getAbsolutePath());
                assertContainerAccess(
                        new YarnSecurityContext(conf, ReadonlyConfig.fromMap(options)),
                        conf,
                        source);

                File cache = new File(temporary, "ticket-cache");
                File krb5 = new File(System.getProperty(MiniKdc.JAVA_SECURITY_KRB5_CONF));
                KrbClient kinit = new KrbClient(krb5.getParentFile());
                kinit.init();
                kinit.storeTicket(kinit.requestTgt(principal, keytab), cache);
                UserGroupInformation cached =
                        UserGroupInformation.getUGIFromTicketCache(cache.getPath(), principal);
                cached.doAs(
                        (PrivilegedExceptionAction<Void>)
                                () -> {
                                    assertContainerAccess(
                                            new YarnSecurityContext(
                                                    conf,
                                                    ReadonlyConfig.fromMap(Collections.emptyMap())),
                                            conf,
                                            source);
                                    return null;
                                });
            } finally {
                cluster.close();
            }
        } finally {
            SaslDataTransferTestCase.shutdownKdc();
            UserGroupInformation.reset();
        }
    }

    private void assertContainerAccess(
            YarnSecurityContext security, Configuration conf, Path source) throws Exception {
        Credentials issued =
                security.run(
                        () -> {
                            assertEquals(
                                    SaslDataTransferTestCase.getUserPrincipal(),
                                    UserGroupInformation.getCurrentUser().getUserName());
                            ContainerLaunchContext master =
                                    YarnContainerLaunchContextFactory.master(
                                            conf, source.getParent(), 512, "simple-user");
                            assertTrue(
                                    master.getLocalResources()
                                            .containsKey(
                                                    YarnConstants.LOCALIZED_SPECIFICATION_NAME));
                            return credentials(master);
                        });
        Token<?> stagingToken =
                issued.getAllTokens().stream()
                        .filter(
                                token ->
                                        DelegationTokenIdentifier.HDFS_DELEGATION_KIND.equals(
                                                token.getKind()))
                        .findFirst()
                        .orElseThrow(
                                () -> new AssertionError("Missing staging HDFS delegation token"));
        UserGroupInformation renewer =
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(
                        conf.get(YarnConfiguration.RM_PRINCIPAL),
                        SaslDataTransferTestCase.getHdfsKeytab());
        try {
            long expiry =
                    renewer.doAs((PrivilegedExceptionAction<Long>) () -> stagingToken.renew(conf));
            assertTrue(expiry > System.currentTimeMillis());
        } finally {
            renewer.logoutUserFromKeytab();
        }
        Text rmService = new Text("application-master");
        issued.addToken(
                rmService,
                new Token<>(
                        new byte[] {1}, new byte[] {2}, AMRMTokenIdentifier.KIND_NAME, rmService));
        UserGroupInformation master = UserGroupInformation.createRemoteUser("am");
        master.addCredentials(issued);
        Credentials forwarded =
                master.doAs(
                        (PrivilegedExceptionAction<Credentials>)
                                () -> {
                                    ContainerLaunchContext worker =
                                            YarnContainerLaunchContextFactory.worker(
                                                    conf,
                                                    source.getParent(),
                                                    "app",
                                                    "am:5801",
                                                    new WorkerSpecification(512, 1, 1),
                                                    "simple-user",
                                                    "/am");
                                    assertFalse(
                                            worker.getLocalResources()
                                                    .containsKey(
                                                            YarnConstants
                                                                    .LOCALIZED_SPECIFICATION_NAME));
                                    return credentials(worker);
                                });
        assertNull(forwarded.getToken(rmService));
        UserGroupInformation worker = UserGroupInformation.createRemoteUser("worker");
        worker.addCredentials(forwarded);
        worker.doAs(
                (PrivilegedExceptionAction<Void>)
                        () -> {
                            Closer closer = Closer.create();
                            try {
                                FileSystem fs =
                                        closer.register(
                                                FileSystem.newInstance(source.toUri(), conf));
                                FSDataInputStream input = closer.register(fs.open(source));
                                assertEquals(42, input.read());
                            } catch (Throwable failure) {
                                throw closer.rethrow(failure, Exception.class);
                            } finally {
                                closer.close();
                            }
                            return null;
                        });
    }

    private static Credentials credentials(ContainerLaunchContext context) throws Exception {
        assertFalse(context.getEnvironment().containsKey(YarnConstants.HADOOP_USER_NAME_ENV));
        ByteBuffer tokens = context.getTokens().duplicate();
        byte[] bytes = new byte[tokens.remaining()];
        tokens.get(bytes);
        try (DataInputBuffer input = new DataInputBuffer()) {
            input.reset(bytes, bytes.length);
            Credentials credentials = new Credentials();
            credentials.readTokenStorageStream(input);
            return credentials;
        }
    }

    private static final class SecureHdfs extends SaslDataTransferTestCase {
        HdfsConfiguration configuration() throws Exception {
            return createSecureConfig("authentication");
        }
    }
}
