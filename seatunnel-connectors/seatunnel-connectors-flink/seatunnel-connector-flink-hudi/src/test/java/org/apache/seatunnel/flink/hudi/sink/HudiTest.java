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

package org.apache.seatunnel.flink.hudi.sink;

import org.apache.seatunnel.core.base.Seatunnel;
import org.apache.seatunnel.core.base.command.Command;
import org.apache.seatunnel.core.flink.args.FlinkCommandArgs;
import org.apache.seatunnel.core.flink.command.FlinkCommandBuilder;

import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.util.FlinkClientUtil;
import org.junit.Assert;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class HudiTest {

    public static String getTestConfigFile(String configFile) throws FileNotFoundException, URISyntaxException {
        URL resource = HudiTest.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }

    @Test
    @SuppressWarnings("magicnumber")
    public void testWriteToHudi() throws Exception {
        String os = System.getProperty("os.name");
        //is windows
        if (os != null && os.toLowerCase().startsWith("windows")) {
            // Due to this is ut will thrown exception 'null\bin\winutils.exe in the Hadoop binaries' on windows
            // So just skip it for windows os.
            return;
        }
        String configFile = getTestConfigFile("/fake_to_hudi.conf");
        FlinkCommandArgs flinkCommandArgs = new FlinkCommandArgs();
        flinkCommandArgs.setConfigFile(configFile);
        flinkCommandArgs.setCheckConfig(false);
        flinkCommandArgs.setVariables(null);
        Command<FlinkCommandArgs> flinkCommand =
                new FlinkCommandBuilder().buildCommand(flinkCommandArgs);
        new Thread(() -> Seatunnel.run(flinkCommand)).start();

        //Due to the stream job cannot be stopped, so checked if there are data files in the specified directory after 20 seconds.
        //If there are data files, sink to hudi successfully
        Thread.sleep(20000L);

        final long hasComplete = FlinkClientUtil.createMetaClient("file:///tmp/seatunnel/hudi").getCommitTimeline().getInstants().filter(HoodieInstant::isCompleted).count();
        Assert.assertTrue(hasComplete > 0);
    }
}
