/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.DefaultSinkWriterContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcConnectionConfig;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.connection.DataSourceUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import javax.sql.XADataSource;
import javax.transaction.xa.XAException;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import java.util.stream.Stream;

import static javax.transaction.xa.XAResource.TMSTARTRSCAN;

@Slf4j
@Disabled(
        "Temporary fast fix, reason: JdbcDatabaseContainer: ClassNotFoundException: com.mysql.jdbc.Driver")
class XaGroupOpsImplIT {

    private static final String MYSQL_DOCKER_IMAGE = "mysql:8.0.29";

    private MySQLContainer<?> mc;
    private XaGroupOps xaGroupOps;
    private SemanticXidGenerator xidGenerator;
    private JdbcConnectionConfig jdbcConnectionConfig;
    private XaFacade xaFacade;
    private XAResource xaResource;

    @BeforeEach
    void before() throws Exception {
        // Non-root users need to grant XA_RECOVER_ADMIN permission
        mc =
                new MySQLContainer<>(DockerImageName.parse(MYSQL_DOCKER_IMAGE))
                        .withUsername("root")
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(MYSQL_DOCKER_IMAGE)));
        Startables.deepStart(Stream.of(mc)).join();

        jdbcConnectionConfig =
                JdbcConnectionConfig.builder()
                        .url(mc.getJdbcUrl())
                        .username(mc.getUsername())
                        .password(mc.getPassword())
                        .xaDataSourceClassName("com.mysql.cj.jdbc.MysqlXADataSource")
                        .build();

        xidGenerator = new SemanticXidGenerator();
        xidGenerator.open();
        xaFacade = new XaFacadeImplAutoLoad(jdbcConnectionConfig);
        xaFacade.open();
        xaGroupOps = new XaGroupOpsImpl(xaFacade);

        XADataSource xaDataSource =
                (XADataSource) DataSourceUtils.buildCommonDataSource(jdbcConnectionConfig);
        xaResource = xaDataSource.getXAConnection().getXAResource();
    }

    @Test
    void testRecoverAndRollback() throws Exception {
        JobContext jobContext = new JobContext();
        SinkWriter.Context writerContext1 = new DefaultSinkWriterContext(1, 1);
        Xid xid1 = xidGenerator.generateXid(jobContext, writerContext1, System.currentTimeMillis());
        Xid xid2 =
                xidGenerator.generateXid(
                        jobContext, writerContext1, System.currentTimeMillis() + 1);

        xaFacade.start(xid1);
        xaFacade.endAndPrepare(xid1);

        xaFacade.start(xid2);
        xaFacade.endAndPrepare(xid2);

        Assertions.assertTrue(checkPreparedXid(xid1));
        Assertions.assertTrue(checkPreparedXid(xid2));

        xaGroupOps.recoverAndRollback(jobContext, writerContext1, xidGenerator, xid2);

        Assertions.assertFalse(checkPreparedXid(xid1));
        Assertions.assertTrue(checkPreparedXid(xid2));
    }

    private boolean checkPreparedXid(Xid xidCrr) throws XAException {
        Xid[] recover = xaResource.recover(TMSTARTRSCAN);
        for (Xid value : recover) {
            XidImpl xid =
                    new XidImpl(
                            value.getFormatId(),
                            value.getGlobalTransactionId(),
                            value.getBranchQualifier());
            if (xid.equals(xidCrr)) {
                return true;
            }
        }
        return false;
    }

    @AfterEach
    public void closePostgreSqlContainer() {
        if (mc != null) {
            mc.stop();
        }
    }
}
