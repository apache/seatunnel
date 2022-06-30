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

package org.apache.seatunnel.core.flink;

import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;

import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;

public class FlinkEnvParameterParserTest {

    static final String APP_CONF_PATH = ClassLoader.getSystemResource("app.conf").getPath();

    @Test
    public void getEnvParameters() throws FileNotFoundException {
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(APP_CONF_PATH);
        flinkCommandArgs.setVariables(Arrays.asList("name=tom", "age=1"));

        List<String> envParameters = FlinkEnvParameterParser.getEnvParameters(flinkCommandArgs);
        envParameters.sort(String::compareTo);

        String[] expected = {"-Dexecution.parallelism=1",
            "-Dexecution.checkpoint.interval=10000", "-Dexecution.checkpoint.data-uri=hdfs://localhost:9000/checkpoint", "-Dname=tom", "-Dage=1"};
        Arrays.sort(expected);
        Assert.assertArrayEquals(expected, envParameters.toArray());
    }
}
