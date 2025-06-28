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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.apache.commons.lang3.tuple.Pair;

import org.junit.jupiter.api.Assertions;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;

import lombok.extern.slf4j.Slf4j;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DuckDBIT extends AbstractJdbcIT {

    // 使用通用Ubuntu镜像，DuckDB是嵌入式数据库不需要专门的服务器镜像
    private static final String DUCKDB_IMAGE = "ubuntu:22.04";
    private static final String DUCKDB_CONTAINER_HOST = "duckdb-host";
    private static final String DUCKDB_DATABASE = "";
    private static final String DUCKDB_SOURCE = "test_source";
    private static final String DUCKDB_SINK = "test_sink";
    private static final String DUCKDB_USERNAME = "";
    private static final String DUCKDB_PASSWORD = "";
    private static final int DUCKDB_PORT = 0; // DuckDB does not use network ports
    private static final String DUCKDB_URL = "jdbc:duckdb:/tmp/test_database.db";
    private static final String DRIVER_CLASS = "org.duckdb.DuckDBDriver";

    // DuckDB table structure using standard SQL types
    private static final String CREATE_SQL =
            "CREATE TABLE IF NOT EXISTS %s ("
                    + "id INTEGER PRIMARY KEY, "
                    + "name VARCHAR(255) NOT NULL, "
                    + "age INTEGER, "
                    + "score DECIMAL(10,2), "
                    + "is_active BOOLEAN, "
                    + "birthday DATE, "
                    + "created_at TIMESTAMP "
                    + ");";

    @TestContainerExtension
    private final ContainerExtendedFactory extendedFactory =
            container -> {
                try {
                    log.info("Setting up DuckDB environment in container");

                    // Update package manager and install necessary tools
                    Container.ExecResult updateResult =
                            container.execInContainer("apt-get", "update");
                    Assertions.assertEquals(
                            0,
                            updateResult.getExitCode(),
                            "Failed to update package list: " + updateResult.getStderr());

                    Container.ExecResult installResult =
                            container.execInContainer(
                                    "apt-get",
                                    "install",
                                    "-y",
                                    "wget",
                                    "openjdk-11-jdk",
                                    "curl",
                                    "python3",
                                    "python3-pip",
                                    "unzip");
                    Assertions.assertEquals(
                            0,
                            installResult.getExitCode(),
                            "Failed to install packages: " + installResult.getStderr());

                    // Install DuckDB Python library (more reliable method)
                    Container.ExecResult pipResult =
                            container.execInContainer("pip3", "install", "duckdb==0.10.0");
                    Assertions.assertEquals(
                            0,
                            pipResult.getExitCode(),
                            "Failed to install DuckDB Python: " + pipResult.getStderr());

                    // Create directories
                    container.execInContainer("mkdir", "-p", "/tmp/seatunnel/plugins/Jdbc/lib");
                    container.execInContainer("mkdir", "-p", "/tmp");

                    // Use locally available DuckDB JDBC driver (avoid network download issues)
                    // If network download fails, use Python DuckDB as fallback
                    Container.ExecResult downloadResult =
                            container.execInContainer(
                                    "wget",
                                    "-O",
                                    "/tmp/seatunnel/plugins/Jdbc/lib/duckdb_jdbc-0.10.0.jar",
                                    "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.0/duckdb_jdbc-0.10.0.jar",
                                    "--timeout=30",
                                    "--tries=3",
                                    "--no-check-certificate");

                    if (downloadResult.getExitCode() != 0) {
                        log.warn(
                                "Failed to download DuckDB JDBC driver, using Python fallback: {}",
                                downloadResult.getStderr());
                        // Create a Python script to initialize the database
                        createPythonInitScript(container);
                    } else {
                        log.info("DuckDB JDBC driver downloaded successfully");
                    }

                    // Initialize database and tables
                    initializeDuckDBDatabase(container);

                    log.info("DuckDB setup completed successfully");

                } catch (Exception e) {
                    log.error("Failed to setup DuckDB environment", e);
                    throw new RuntimeException("DuckDB setup failed", e);
                }
            };

    private void createPythonInitScript(GenericContainer<?> container) throws Exception {
        String initScript =
                "#!/usr/bin/env python3\n"
                        + "import duckdb\n"
                        + "import sys\n"
                        + "import os\n"
                        + "\n"
                        + "def init_database():\n"
                        + "    try:\n"
                        + "        # Create database connection\n"
                        + "        conn = duckdb.connect('/tmp/test_database.db')\n"
                        + "        \n"
                        + "        # Create source table\n"
                        + "        conn.execute('''\n"
                        + "            CREATE TABLE IF NOT EXISTS \"test_source\" (\n"
                        + "                id INTEGER PRIMARY KEY,\n"
                        + "                name VARCHAR(255) NOT NULL,\n"
                        + "                age INTEGER,\n"
                        + "                score DECIMAL(10,2),\n"
                        + "                is_active BOOLEAN,\n"
                        + "                birthday DATE,\n"
                        + "                created_at TIMESTAMP\n"
                        + "            );\n"
                        + "        ''')\n"
                        + "        \n"
                        + "        # Create target table\n"
                        + "        conn.execute('''\n"
                        + "            CREATE TABLE IF NOT EXISTS \"test_sink\" (\n"
                        + "                id INTEGER PRIMARY KEY,\n"
                        + "                name VARCHAR(255) NOT NULL,\n"
                        + "                age INTEGER,\n"
                        + "                score DECIMAL(10,2),\n"
                        + "                is_active BOOLEAN,\n"
                        + "                birthday DATE,\n"
                        + "                created_at TIMESTAMP\n"
                        + "            );\n"
                        + "        ''')\n"
                        + "        \n"
                        + "        # Insert test data\n"
                        + "        conn.execute('''\n"
                        + "            INSERT OR REPLACE INTO \"test_source\" (id, name, age, score, is_active, birthday, created_at) VALUES\n"
                        + "            (1, 'Alice', 25, 95.5, true, '1998-05-15', '2023-01-01 10:00:00'),\n"
                        + "            (2, 'Bob', 30, 87.3, false, '1993-08-20', '2023-01-02 11:30:00'),\n"
                        + "            (3, 'Charlie', 35, 92.8, true, '1988-12-10', '2023-01-03 14:15:00'),\n"
                        + "            (4, 'Diana', 28, 89.9, true, '1995-03-25', '2023-01-04 16:45:00'),\n"
                        + "            (5, 'Eve', 22, 94.1, false, '2001-11-30', '2023-01-05 09:20:00');\n"
                        + "        ''')\n"
                        + "        \n"
                        + "        # Validate data\n"
                        + "        result = conn.execute('SELECT COUNT(*) FROM \"test_source\"').fetchone()\n"
                        + "        print(f'Source table created with {result[0]} rows')\n"
                        + "        \n"
                        + "        conn.close()\n"
                        + "        print('DuckDB database initialized successfully')\n"
                        + "        return True\n"
                        + "        \n"
                        + "    except Exception as e:\n"
                        + "        print(f'Error initializing database: {e}')\n"
                        + "        return False\n"
                        + "\n"
                        + "if __name__ == '__main__':\n"
                        + "    success = init_database()\n"
                        + "    sys.exit(0 if success else 1)\n";

        // 将脚本写入容器
        container.execInContainer(
                "sh", "-c", "cat > /tmp/init_duckdb.py << 'EOF'\n" + initScript + "\nEOF");
        container.execInContainer("chmod", "+x", "/tmp/init_duckdb.py");
    }

    private void initializeDuckDBDatabase(GenericContainer<?> container) throws Exception {
        // 运行Python初始化脚本
        Container.ExecResult initResult =
                container.execInContainer("python3", "/tmp/init_duckdb.py");
        if (initResult.getExitCode() != 0) {
            log.error("Failed to initialize DuckDB database: {}", initResult.getStderr());
            throw new RuntimeException("Database initialization failed: " + initResult.getStderr());
        }
        log.info("DuckDB database initialized: {}", initResult.getStdout());

        // 验证数据库文件存在
        Container.ExecResult checkResult =
                container.execInContainer("ls", "-la", "/tmp/test_database.db");
        if (checkResult.getExitCode() == 0) {
            log.info("Database file verified: {}", checkResult.getStdout());
        } else {
            log.warn("Database file check failed: {}", checkResult.getStderr());
        }
    }

    @Override
    public String quoteIdentifier(String field) {
        // DuckDB uses double quotes for identifier quoting
        return "\"" + field + "\"";
    }

    @Override
    public void startUp() {
        log.info("Starting DuckDB IT test setup");
    }

    @Override
    public void tearDown() {
        log.info("DuckDB IT test cleanup completed");
    }

    @Override
    JdbcCase getJdbcCase() {
        Map<String, String> containerEnv = new HashMap<>();
        containerEnv.put("JAVA_HOME", "/usr/lib/jvm/java-11-openjdk-amd64");

        Pair<String[], List<SeaTunnelRow>> testDataSet = initTestData();
        String[] fieldNames = testDataSet.getKey();

        String insertSql = insertTable(DUCKDB_DATABASE, DUCKDB_SOURCE, fieldNames);

        return JdbcCase.builder()
                .dockerImage(DUCKDB_IMAGE)
                .networkAliases(DUCKDB_CONTAINER_HOST)
                .containerEnv(containerEnv)
                .driverClass(DRIVER_CLASS)
                .host(HOST)
                .port(DUCKDB_PORT)
                .localPort(DUCKDB_PORT)
                .jdbcTemplate(DUCKDB_URL)
                .jdbcUrl(DUCKDB_URL)
                .userName(DUCKDB_USERNAME)
                .password(DUCKDB_PASSWORD)
                .database(DUCKDB_DATABASE)
                .sourceTable(DUCKDB_SOURCE)
                .sinkTable(DUCKDB_SINK)
                .createSql(CREATE_SQL)
                .configFile(Arrays.asList("/jdbc_duckdb_source_and_sink.conf"))
                .insertSql(insertSql)
                .testData(testDataSet)
                .catalogDatabase(DUCKDB_DATABASE)
                .catalogTable(DUCKDB_SINK)
                .tablePathFullName(DUCKDB_SOURCE)
                .build();
    }

    @Override
    String driverUrl() {
        // 保持接口兼容性，但实际使用容器内安装的驱动
        return "https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/0.10.0/duckdb_jdbc-0.10.0.jar";
    }

    @Override
    Pair<String[], List<SeaTunnelRow>> initTestData() {
        String[] fieldNames =
                new String[] {"id", "name", "age", "score", "is_active", "birthday", "created_at"};

        List<SeaTunnelRow> rows = new ArrayList<>();

        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            1,
                            "Alice",
                            25,
                            new BigDecimal("95.5"),
                            true,
                            Date.valueOf(LocalDate.of(1998, 5, 15)),
                            Timestamp.valueOf(LocalDateTime.of(2023, 1, 1, 10, 0, 0))
                        }));

        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            2,
                            "Bob",
                            30,
                            new BigDecimal("87.3"),
                            false,
                            Date.valueOf(LocalDate.of(1993, 8, 20)),
                            Timestamp.valueOf(LocalDateTime.of(2023, 1, 2, 11, 30, 0))
                        }));

        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            3,
                            "Charlie",
                            35,
                            new BigDecimal("92.8"),
                            true,
                            Date.valueOf(LocalDate.of(1988, 12, 10)),
                            Timestamp.valueOf(LocalDateTime.of(2023, 1, 3, 14, 15, 0))
                        }));

        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            4,
                            "Diana",
                            28,
                            new BigDecimal("89.9"),
                            true,
                            Date.valueOf(LocalDate.of(1995, 3, 25)),
                            Timestamp.valueOf(LocalDateTime.of(2023, 1, 4, 16, 45, 0))
                        }));

        rows.add(
                new SeaTunnelRow(
                        new Object[] {
                            5,
                            "Eve",
                            22,
                            new BigDecimal("94.1"),
                            false,
                            Date.valueOf(LocalDate.of(2001, 11, 30)),
                            Timestamp.valueOf(LocalDateTime.of(2023, 1, 5, 9, 20, 0))
                        }));

        return Pair.of(fieldNames, rows);
    }

    @Override
    protected GenericContainer<?> initContainer() {
        GenericContainer<?> container =
                new GenericContainer<>(DUCKDB_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(DUCKDB_CONTAINER_HOST)
                        .withCommand("tail", "-f", "/dev/null");

        container.setPortBindings(new ArrayList<>());
        return container;
    }

    @Override
    protected void checkResult(
            String executeKey, TestContainer container, Container.ExecResult execResult) {
        if (execResult.getExitCode() != 0) {
            log.error(
                    "DuckDB test execution failed. Key: {}, Error: {}",
                    executeKey,
                    execResult.getStderr());
            log.error("DuckDB test stdout: {}", execResult.getStdout());
        }
        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                String.format(
                        "DuckDB test [%s] should execute successfully, but got error: %s",
                        executeKey, execResult.getStderr()));
    }

    @Override
    protected void createNeededTables() {
        try {
            log.info("Creating DuckDB tables with connection: {}", connection);

            // 创建源表
            String createSourceSql = String.format(CREATE_SQL, quoteIdentifier(DUCKDB_SOURCE));
            log.info("Creating source table with SQL: {}", createSourceSql);
            connection.createStatement().execute(createSourceSql);

            // 创建目标表
            String createSinkSql = String.format(CREATE_SQL, quoteIdentifier(DUCKDB_SINK));
            log.info("Creating sink table with SQL: {}", createSinkSql);
            connection.createStatement().execute(createSinkSql);

            connection.commit();
            log.info("DuckDB tables created successfully");

            // 验证表是否创建成功
            boolean sourceExists =
                    connection
                            .createStatement()
                            .executeQuery(
                                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_source'")
                            .next();
            boolean sinkExists =
                    connection
                            .createStatement()
                            .executeQuery(
                                    "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_sink'")
                            .next();

            log.info(
                    "Table verification - Source exists: {}, Sink exists: {}",
                    sourceExists,
                    sinkExists);

        } catch (Exception e) {
            log.error("Failed to create DuckDB tables", e);
            throw new RuntimeException("Failed to create tables: " + e.getMessage(), e);
        }
    }

    @Override
    protected void beforeStartUP() {
        log.info("DuckDB IT: beforeStartUP - Preparing DuckDB environment");
    }

    @Override
    protected void clearTable(String database, String schema, String table) {
        try {
            String clearSql = String.format("DELETE FROM %s", quoteIdentifier(table));
            log.info("Clearing table with SQL: {}", clearSql);
            connection.createStatement().execute(clearSql);
            connection.commit();
            log.info("Table {} cleared successfully", table);
        } catch (Exception e) {
            log.error("Failed to clear table: {}", table, e);
            throw new RuntimeException("Failed to clear table: " + e.getMessage(), e);
        }
    }
}
