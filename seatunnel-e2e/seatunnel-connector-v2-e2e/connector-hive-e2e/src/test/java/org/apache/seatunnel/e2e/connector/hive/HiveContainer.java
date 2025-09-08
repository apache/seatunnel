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

package org.apache.seatunnel.e2e.connector.hive;

import org.apache.seatunnel.e2e.common.util.ContainerUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.security.UserGroupInformation;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Properties;

public class HiveContainer extends GenericContainer<HiveContainer> {
    public static final String IMAGE = "apache/hive";
    public static final String DEFAULT_TAG = "3.1.3";

    private static final DockerImageName DEFAULT_IMAGE_NAME = DockerImageName.parse(IMAGE);

    public static final int HIVE_SERVER_PORT = 10000;

    public static final int HMS_PORT = 9083;

    private static final String SERVICE_NAME_ENV = "SERVICE_NAME";

    private static final String DRIVER_CLASS_NAME = "org.apache.hive.jdbc.HiveDriver";

    public HiveContainer(Role role) {
        super(DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG));
        this.addExposedPorts(role.exposePort);
        this.addEnv(SERVICE_NAME_ENV, role.serviceName);
        this.setWaitStrategy(role.waitStrategy);
        this.withLogConsumer(
                new Slf4jLogConsumer(
                        DockerLoggerFactory.getLogger(
                                DEFAULT_IMAGE_NAME.withTag(DEFAULT_TAG).toString())));
    }

    public static HiveContainer hmsStandalone() {
        return new HiveContainer(Role.HMS_STANDALONE);
    }

    public static HiveContainer hiveServer() {
        return new HiveContainer(Role.HIVE_SERVER_WITH_EMBEDDING_HMS);
    }

    public String getMetastoreUri() {
        return String.format("thrift://%s:%s", getHost(), getMappedPort(HMS_PORT));
    }

    public String getHiveJdbcUri(boolean enableKerberos) {
        if (enableKerberos) {
            return String.format(
                    "jdbc:hive2://%s:%s/default;principal=hive/metastore.seatunnel@EXAMPLE.COM",
                    getHost(), getMappedPort(HIVE_SERVER_PORT));
        } else {
            return String.format(
                    "jdbc:hive2://%s:%s/default", getHost(), getMappedPort(HIVE_SERVER_PORT));
        }
    }

    public HiveMetaStoreClient createMetaStoreClient() throws MetaException {
        return this.createMetaStoreClient(false);
    }

    public HiveMetaStoreClient createMetaStoreClient(boolean enableKerberos) throws MetaException {
        HiveConf conf = new HiveConf();
        conf.set("hive.metastore.uris", getMetastoreUri());
        if (enableKerberos) {
            conf.addResource("kerberos/hive-site.xml");
        }
        return new HiveMetaStoreClient(conf);
    }

    public Connection getConnection()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    SQLException {
        return getConnection(false);
    }

    public Connection getConnection(boolean enableKerberos)
            throws ClassNotFoundException, InstantiationException, IllegalAccessException,
                    SQLException {
        Driver driver = loadHiveJdbcDriver();
        if (!enableKerberos) {
            return driver.connect(getHiveJdbcUri(false), getJdbcConnectionConfig());
        }
        Configuration authConf = new Configuration();
        authConf.set("hadoop.security.authentication", "kerberos");
        Configuration configuration = new Configuration();
        System.setProperty(
                "java.security.krb5.conf",
                ContainerUtil.getResourcesFile("/kerberos/krb5_local.conf").getPath());
        configuration.set("hadoop.security.authentication", "KERBEROS");
        try {
            UserGroupInformation.setConfiguration(configuration);
            UserGroupInformation.loginUserFromKeytab(
                    "hive/metastore.seatunnel@EXAMPLE.COM", "/tmp/hive.keytab");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return driver.connect(getHiveJdbcUri(true), getJdbcConnectionConfig());
    }

    public Driver loadHiveJdbcDriver()
            throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        return (Driver) Class.forName(DRIVER_CLASS_NAME).newInstance();
    }

    public Properties getJdbcConnectionConfig() {
        Properties props = new Properties();

        return props;
    }

    public enum Role {
        HIVE_SERVER_WITH_EMBEDDING_HMS(
                "hiveserver2", HIVE_SERVER_PORT, Wait.forLogMessage(".*Starting HiveServer2.*", 1)),
        HMS_STANDALONE(
                "metastore", HMS_PORT, Wait.forLogMessage(".*Starting Hive Metastore Server.*", 1));

        private final String serviceName;
        private final int exposePort;
        private final WaitStrategy waitStrategy;

        Role(String serviceName, int exposePort, WaitStrategy waitStrategy) {
            this.serviceName = serviceName;
            this.exposePort = exposePort;
            this.waitStrategy = waitStrategy;
        }
    }
}
