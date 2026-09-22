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

package org.apache.seatunnel.e2e.connector.woocommerce;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.woocommerce.source.WooCommerceSource;
import org.apache.seatunnel.connectors.seatunnel.woocommerce.source.WooCommerceSourceFactory;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.CertificateFactory;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/** Runs against real WordPress/WooCommerce with HTTPS and a read-only REST key. */
public class WooCommerceIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> database;
    private GenericContainer<?> store;
    private Path truststore;

    @BeforeEach
    @Override
    public void startUp() throws Exception {
        database =
                new GenericContainer<>(DockerImageName.parse("mariadb:11.4.8"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("woocommerce-db")
                        .withEnv("MARIADB_ROOT_PASSWORD", "fixture-root")
                        .withEnv("MARIADB_DATABASE", "wordpress")
                        .withEnv("MARIADB_USER", "wordpress")
                        .withEnv("MARIADB_PASSWORD", "fixture-password")
                        .waitingFor(Wait.forLogMessage(".*ready for connections.*", 2));
        database.start();
        store =
                new GenericContainer<>(DockerImageName.parse("wordpress:6.8.3-php8.3-apache"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("woocommerce-fixture")
                        .withEnv("WORDPRESS_DB_HOST", "woocommerce-db")
                        .withEnv("WORDPRESS_DB_USER", "wordpress")
                        .withEnv("WORDPRESS_DB_PASSWORD", "fixture-password")
                        .withEnv("WORDPRESS_DB_NAME", "wordpress")
                        .withExposedPorts(80, 443)
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("docker/setup.php"),
                                "/tmp/setup.php")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("docker/seed.php"),
                                "/tmp/seed.php")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("docker/ssl.conf"),
                                "/etc/apache2/sites-available/woocommerce-ssl.conf")
                        .waitingFor(
                                Wait.forHttp("/wp-admin/install.php")
                                        .forPort(80)
                                        .forStatusCode(200)
                                        .withStartupTimeout(Duration.ofMinutes(3)));
        store.start();
        command(
                "curl",
                "--fail",
                "--silent",
                "--show-error",
                "--max-time",
                "120",
                "https://downloads.wordpress.org/plugin/woocommerce.9.9.5.zip",
                "-o",
                "/tmp/woocommerce.zip");
        command("php", "/tmp/setup.php");
        command(
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "2",
                "-subj",
                "/CN=woocommerce-fixture",
                "-addext",
                "subjectAltName=DNS:woocommerce-fixture,DNS:localhost,IP:127.0.0.1",
                "-keyout",
                "/tmp/woocommerce.key",
                "-out",
                "/tmp/woocommerce.crt");
        command("a2enmod", "ssl");
        command("a2ensite", "woocommerce-ssl");
        command("apache2ctl", "-k", "graceful");
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        store.copyFileFromContainer(
                "/tmp/woocommerce.crt",
                input -> {
                    try {
                        trust.setCertificateEntry(
                                "woocommerce-fixture",
                                CertificateFactory.getInstance("X.509").generateCertificate(input));
                    } catch (Exception ex) {
                        throw new IllegalStateException(ex);
                    }
                    return null;
                });
        truststore = Files.createTempFile("woocommerce-fixture-", ".jks");
        try (OutputStream output = Files.newOutputStream(truststore)) {
            trust.store(output, "fixture-store".toCharArray());
        }
    }

    private void command(String... command) throws Exception {
        Container.ExecResult result = store.execInContainer(command);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    @Test
    void realOrdersRespectUtcBoundariesAndPageThroughFactoryReader() throws Exception {
        command("php", "/tmp/seed.php", "cpt");
        readAndVerifyOrders();
    }

    @Test
    void hposOrdersRespectTheSameUtcAndPaginationContract() throws Exception {
        command("php", "/tmp/seed.php", "hpos");
        readAndVerifyOrders();
    }

    private void readAndVerifyOrders() throws Exception {
        String previous = System.getProperty("javax.net.ssl.trustStore");
        String password = System.getProperty("javax.net.ssl.trustStorePassword");
        System.setProperty("javax.net.ssl.trustStore", truststore.toString());
        System.setProperty("javax.net.ssl.trustStorePassword", "fixture-store");
        try {
            Map<String, Object> options = new LinkedHashMap<>();
            options.put("url", "https://" + store.getHost() + ":" + store.getMappedPort(443));
            options.put("consumer_key", "ck_0000000000000000000000000000000000000001");
            options.put("consumer_secret", "cs_0000000000000000000000000000000000000002");
            options.put("start_date", "2026-01-01T00:00:00Z");
            options.put("end_date", "2026-02-01T00:00:00Z");
            options.put("page_size", 2);
            options.put("decimal_places", 3);
            Map<String, Object> fields = new LinkedHashMap<>();
            fields.put("id", "bigint");
            fields.put("total", "decimal(12,3)");
            fields.put("billing", Collections.singletonMap("email", "string"));
            fields.put("line_items", "array<map<string,string>>");
            fields.put("date_created_gmt", "string");
            options.put("schema", Collections.singletonMap("fields", fields));
            Object created =
                    new WooCommerceSourceFactory()
                            .createSource(
                                    new TableSourceFactoryContext(
                                            ReadonlyConfig.fromMap(options),
                                            getClass().getClassLoader()))
                            .createSource();
            WooCommerceSource source = (WooCommerceSource) created;
            SourceReader.Context context = mock(SourceReader.Context.class);
            List<SeaTunnelRow> rows = new ArrayList<>();
            try (AbstractSingleSplitReader<SeaTunnelRow> reader =
                    source.createReader(new SingleSplitReaderContext(context))) {
                reader.open();
                reader.pollNext(
                        new Collector<SeaTunnelRow>() {
                            public void collect(SeaTunnelRow row) {
                                rows.add(row);
                            }

                            public Object getCheckpointLock() {
                                return rows;
                            }
                        });
            }
            Assertions.assertEquals(
                    3,
                    rows.size(),
                    "Orders exactly on either UTC boundary are excluded even in a non-UTC store");
            List<String> creationDates = new ArrayList<>();
            for (SeaTunnelRow row : rows) {
                Assertions.assertEquals(new BigDecimal("12.345"), row.getField(1));
                Assertions.assertEquals(
                        "buyer@example.test", ((SeaTunnelRow) row.getField(2)).getField(0));
                Assertions.assertEquals("12.345", ((Map<?, ?>[]) row.getField(3))[0].get("total"));
                creationDates.add((String) row.getField(4));
            }
            Assertions.assertEquals(
                    Arrays.asList(
                            "2026-01-05T12:00:00", "2026-01-06T12:00:00", "2026-01-07T12:00:00"),
                    creationDates);
            verify(context, times(1)).signalNoMoreElement();
        } finally {
            restore("javax.net.ssl.trustStore", previous);
            restore("javax.net.ssl.trustStorePassword", password);
        }
    }

    @Test
    void packagedZetaSourceReadsOrdersOverVerifiedTls() throws Exception {
        command("php", "/tmp/seed.php", "hpos");
        SeaTunnelContainer engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/woocommerce-fixture.jks -Djavax.net.ssl.trustStorePassword=fixture-store";
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> runtime) {
                        runtime.withCopyFileToContainer(
                                MountableFile.forHostPath(truststore),
                                "/tmp/woocommerce-fixture.jks");
                    }
                };
        try {
            engine.startUp();
            Container.ExecResult result = engine.executeJob("/woocommerce_to_assert.conf");
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        } finally {
            engine.tearDown();
        }
    }

    private static void restore(String name, String value) {
        if (value == null) {
            System.clearProperty(name);
        } else {
            System.setProperty(name, value);
        }
    }

    @AfterEach
    @Override
    public void tearDown() throws Exception {
        try {
            if (store != null) {
                store.stop();
            }
        } finally {
            try {
                if (database != null) {
                    database.stop();
                }
            } finally {
                if (truststore != null) {
                    Files.deleteIfExists(truststore);
                }
            }
        }
    }
}
