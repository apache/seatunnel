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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonBaseOptions;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.commons.collections.CollectionUtils;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.ResolvingFileIO;
import org.apache.paimon.privilege.FileBasedPrivilegeManagerLoader;
import org.apache.paimon.privilege.PrivilegeType;
import org.apache.paimon.privilege.PrivilegedCatalog;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.MinIOContainer;

import io.minio.BucketExistsArgs;
import io.minio.MakeBucketArgs;
import io.minio.MinioClient;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class PaimonWithS3IT extends SeaTunnelContainer {

    private static final String MINIO_DOCKER_IMAGE = "minio/minio:RELEASE.2024-06-13T22-53-53Z";
    private static final String HOST = "minio";
    private static final int MINIO_PORT = 9000;
    private static final String MINIO_USER_NAME = "minio";
    private static final String MINIO_USER_PASSWORD = "miniominio";

    private static final String BUCKET = "test";
    private static final String PRIVILEGE_BUCKET = "privilegetest";

    private MinIOContainer container;
    private MinioClient minioClient;

    private String warehouse = "s3a://privilegetest/";
    private String rootUser = "root";
    private String rootPassword = "123456";
    private String paimonUser = "paimon";
    private String paimonUserPassword = "123456";

    private PrivilegedCatalog privilegedCatalog;
    private final String DATABASE_NAME = "seatunnel_namespace11";
    private final String TABLE_NAME = "st_test";

    protected static final String AWS_SDK_DOWNLOAD =
            "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.271/aws-java-sdk-bundle-1.11.271.jar";
    protected static final String HADOOP_AWS_DOWNLOAD =
            "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.1.4/hadoop-aws-3.1.4.jar";

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        container =
                new MinIOContainer(MINIO_DOCKER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(HOST)
                        .withUserName(MINIO_USER_NAME)
                        .withPassword(MINIO_USER_PASSWORD)
                        .withExposedPorts(MINIO_PORT);
        container.start();

        String s3URL = container.getS3URL();

        // configuringClient
        minioClient =
                MinioClient.builder()
                        .endpoint(s3URL)
                        .credentials(container.getUserName(), container.getPassword())
                        .build();

        // create bucket
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(BUCKET).build());
        minioClient.makeBucket(MakeBucketArgs.builder().bucket(PRIVILEGE_BUCKET).build());

        BucketExistsArgs existsArgs = BucketExistsArgs.builder().bucket(BUCKET).build();
        Assertions.assertTrue(minioClient.bucketExists(existsArgs));
        BucketExistsArgs privExistsArgs =
                BucketExistsArgs.builder().bucket(PRIVILEGE_BUCKET).build();
        Assertions.assertTrue(minioClient.bucketExists(privExistsArgs));

        initPrivilege();

        super.startUp();
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();
        if (container != null) {
            container.close();
        }
    }

    @Override
    protected String[] buildStartCommand() {
        return new String[] {
            "bash",
            "-c",
            "wget -P "
                    + SEATUNNEL_HOME
                    + "lib "
                    + AWS_SDK_DOWNLOAD
                    + " &&"
                    + "wget -P "
                    + SEATUNNEL_HOME
                    + "lib "
                    + HADOOP_AWS_DOWNLOAD
                    + " &&"
                    + ContainerUtil.adaptPathForWin(
                            Paths.get(SEATUNNEL_HOME, "bin", SERVER_SHELL).toString())
        };
    }

    @Override
    protected boolean isIssueWeAlreadyKnow(String threadName) {
        return super.isIssueWeAlreadyKnow(threadName)
                // Paimon with s3
                || threadName.startsWith("s3a-transfer");
    }

    @Test
    public void testFakeCDCSinkPaimonWithS3Filesystem() throws Exception {
        Container.ExecResult execResult = executeJob("/fake_to_paimon_with_s3.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult readResult = executeJob("/paimon_with_s3_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    @Test
    public void testFakeCDCSinkPaimonWithCheckpointInBatchModeWithS3Filesystem() throws Exception {
        Container.ExecResult execResult =
                executeJob("/fake_to_paimon_with_s3_with_checkpoint.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Container.ExecResult readResult = executeJob("/fake_2_paimon_with_s3_to_assert.conf");
        Assertions.assertEquals(0, readResult.getExitCode());
    }

    private void initPrivilege() {
        org.apache.paimon.options.Options catalogOptions = new org.apache.paimon.options.Options();
        catalogOptions.set(PaimonBaseOptions.WAREHOUSE.key(), warehouse);
        catalogOptions.set("fs.s3a.endpoint", container.getS3URL());
        catalogOptions.set("fs.s3a.access-key", MINIO_USER_NAME);
        catalogOptions.set("fs.s3a.secret-key", MINIO_USER_PASSWORD);
        catalogOptions.set("fs.s3a.buffer.dir", "/tmp/s3abuffer");
        catalogOptions.set("fs.s3a.change.detection.mode", "NONE");
        catalogOptions.set("fs.s3a.change.detection.version.required", "false");
        catalogOptions.set("fs.s3a.path.style.access", "true");
        catalogOptions.set(
                "fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        final CatalogContext catalogContext = CatalogContext.create(catalogOptions);

        FileIO fileIO = new ResolvingFileIO();
        fileIO.configure(catalogContext);

        privilegedCatalog =
                new PrivilegedCatalog(
                        CatalogFactory.createCatalog(catalogContext),
                        new FileBasedPrivilegeManagerLoader(
                                warehouse, fileIO, rootUser, rootPassword));
        if (!privilegedCatalog.privilegeManager().privilegeEnabled()) {
            privilegedCatalog.privilegeManager().initializePrivilege(rootPassword);
        }

        // create user and grant privilege on table
        privilegedCatalog.privilegeManager().createUser(paimonUser, paimonUserPassword);
        String fullTableName = Identifier.create(DATABASE_NAME, TABLE_NAME).getFullName();
        privilegedCatalog.privilegeManager().grant(paimonUser, "", PrivilegeType.CREATE_DATABASE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, DATABASE_NAME, PrivilegeType.DROP_DATABASE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, fullTableName, PrivilegeType.DROP_TABLE);
        privilegedCatalog
                .privilegeManager()
                .grant(paimonUser, DATABASE_NAME, PrivilegeType.CREATE_TABLE);
    }

    private void grantPrivilege(List<PrivilegeType> privilegeTypes) {
        String fullTableName = Identifier.create(DATABASE_NAME, TABLE_NAME).getFullName();
        if (!CollectionUtils.isEmpty(privilegeTypes)) {
            for (PrivilegeType type : privilegeTypes) {
                privilegedCatalog.privilegeManager().grant(paimonUser, fullTableName, type);
            }
        }
    }

    private void revokePrivilege(List<PrivilegeType> privilegeTypes) {
        String fullTableName = Identifier.create(DATABASE_NAME, TABLE_NAME).getFullName();
        if (!CollectionUtils.isEmpty(privilegeTypes)) {
            for (PrivilegeType type : privilegeTypes) {
                privilegedCatalog.privilegeManager().revoke(paimonUser, fullTableName, type);
            }
        }
    }

    /** User not grant read privilege read data test cases for the Paimon table. */
    @Test
    public void privilegeEnabledPaimonSourceAuthorized() throws Exception {
        List<PrivilegeType> privilegeTypes = new ArrayList<>();
        privilegeTypes.add(PrivilegeType.SELECT);
        privilegeTypes.add(PrivilegeType.INSERT);
        grantPrivilege(privilegeTypes);
        // fake to paimon
        Container.ExecResult execResult = executeJob("/fake_to_paimon_with_s3_with_privilege.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // paimon to paimon
        Container.ExecResult execResult1 =
                executeJob("/paimon_to_paimon_with_s3_with_privilege.conf");
        Assertions.assertEquals(0, execResult1.getExitCode());
        revokePrivilege(privilegeTypes);
    }

    /** User not grant read privilege read data test cases for the Paimon table. */
    @Test
    public void privilegeEnabledPaimonSourceUnAuthorized() throws Exception {
        List<PrivilegeType> privilegeTypes = new ArrayList<>();
        privilegeTypes.add(PrivilegeType.INSERT);
        grantPrivilege(privilegeTypes);
        // fake to paimon
        Container.ExecResult execResult = executeJob("/fake_to_paimon_with_s3_with_privilege.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // paimon to paimon
        Container.ExecResult execResult1 =
                executeJob("/paimon_to_paimon_with_s3_with_privilege.conf");
        Assertions.assertEquals(1, execResult1.getExitCode());
        revokePrivilege(privilegeTypes);
    }
}
