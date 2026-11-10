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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.benchmark.jdbc.JdbcRowConverterFixture;

import org.openjdk.jmh.Main;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

/** Measures JDBC row conversion and parameter binding with prepared inputs. */
@Threads(1)
@Fork(
        value = 3,
        jvmArgsAppend = {
            "-Xms4g",
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-XX:+AlwaysPreTouch",
            "-XX:+DisableExplicitGC",
            "-XX:ActiveProcessorCount=4",
            "-Djava.net.preferIPv4Stack=true"
        })
public class JdbcRowConverterBenchmark extends BenchmarkBase {

    @Param({"5", "10", "15", "20"})
    private int columns;

    private JdbcRowConverterFixture fixture;

    public static void main(String[] args) throws IOException {
        String[] options = new String[args.length + 1];
        options[0] = JdbcRowConverterBenchmark.class.getCanonicalName();
        System.arraycopy(args, 0, options, 1, args.length);
        Main.main(options);
    }

    @Setup(Level.Trial)
    public void setUp() throws SQLException {
        fixture = new JdbcRowConverterFixture(columns);
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    public SeaTunnelRow toInternal() throws SQLException {
        return fixture.readRow();
    }

    @Benchmark
    @OutputTimeUnit(TimeUnit.SECONDS)
    public PreparedStatement toExternal() throws SQLException {
        return fixture.bindRow();
    }

    @TearDown(Level.Trial)
    public void tearDown() throws SQLException {
        if (fixture != null) {
            fixture.close();
        }
    }
}
