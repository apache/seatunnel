/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.sql;

import org.apache.seatunnel.config.CommandLineArgs;
import org.apache.seatunnel.config.CommandLineUtils;
import org.apache.seatunnel.core.sql.job.Executor;
import org.apache.seatunnel.core.sql.job.JobInfo;
import org.apache.commons.io.FileUtils;

import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scopt.OptionParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SeatunnelSql {
    public static void main(String[] args) throws Exception {
        JobInfo jobInfo = parseJob(args);
        Executor.runJob(jobInfo);
    }

    private static JobInfo parseJob(String[] args) throws IOException {
        OptionParser<CommandLineArgs> parser = CommandLineUtils.flinkParser();
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs(
                "client", "application.conf", false, new scala.collection.immutable.HashMap()));
        if (option.isDefined()) {
            CommandLineArgs commandLineArgs = option.get();
            String configFilePath = commandLineArgs.configFile();
            String jobContent = FileUtils.readFileToString(new File(configFilePath), StandardCharsets.UTF_8);
            return new JobInfo(jobContent);
        } else {
            throw new RuntimeException("Please specify application config file");
        }
    }

}
