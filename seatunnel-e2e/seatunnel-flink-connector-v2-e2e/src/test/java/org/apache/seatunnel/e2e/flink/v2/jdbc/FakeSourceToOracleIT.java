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
package org.apache.seatunnel.e2e.flink.v2.jdbc;


import com.google.common.collect.Lists;
import org.apache.seatunnel.e2e.flink.FlinkContainer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.OracleContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.sql.*;
import java.util.List;
import java.util.stream.Stream;

public class FakeSourceToOracleIT extends FlinkContainer {
	private static final Logger LOGGER = LoggerFactory.getLogger(FakeSourceToOracleIT.class);
	private OracleContainer oracleContainer;

	@SuppressWarnings("checkstyle:MagicNumber")
	@BeforeEach
	public void startOracleContainer() throws InterruptedException, ClassNotFoundException {
		oracleContainer = new OracleContainer(DockerImageName.parse("gvenzl/oracle-xe:18.4.0-slim"))
				.withNetwork(NETWORK)
				.withNetworkAliases("oracle")
				.withLogConsumer(new Slf4jLogConsumer(LOGGER));
		Startables.deepStart(Stream.of(oracleContainer)).join();
		LOGGER.info("Oracle container started");
		Thread.sleep(5000L);
		Class.forName(oracleContainer.getDriverClassName());
		initializeJdbcTable();
	}

	private void initializeJdbcTable() {
		try (Connection connection = DriverManager.getConnection(oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword())) {
			Statement statement = connection.createStatement();
			String sql = "CREATE TABLE test (\n" +
					"  name varchar(255) NOT NULL\n" +
					")";
			statement.execute(sql);
		} catch (SQLException e) {
			throw new RuntimeException("Initializing Oracle table failed!", e);
		}
	}

	@Test
	public void testFakeSourceToOracleSink() throws SQLException, IOException, InterruptedException {
		Container.ExecResult execResult = executeSeaTunnelFlinkJob("/jdbc/fakesource_to_jdbc.conf");
		Assertions.assertEquals(0, execResult.getExitCode());
		// query result
		String sql = "select * from test";
		try (Connection connection = DriverManager.getConnection(oracleContainer.getJdbcUrl(), oracleContainer.getUsername(), oracleContainer.getPassword())) {
			Statement statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery(sql);
			List<String> result = Lists.newArrayList();
			while (resultSet.next()) {
				result.add(resultSet.getString("name"));
			}
			Assertions.assertFalse(result.isEmpty());
		}
	}

	@AfterEach
	public void closeClickHouseContainer() {
		if (oracleContainer != null) {
			oracleContainer.stop();
		}
	}
}
