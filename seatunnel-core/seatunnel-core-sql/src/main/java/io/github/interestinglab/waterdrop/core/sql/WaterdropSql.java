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

package io.github.interestinglab.waterdrop.core.sql;

import io.github.interestinglab.waterdrop.config.CommandLineArgs;
import io.github.interestinglab.waterdrop.config.CommandLineUtils;
import io.github.interestinglab.waterdrop.core.sql.job.Executor;
import io.github.interestinglab.waterdrop.core.sql.job.JobInfo;
import org.apache.commons.io.FileUtils;
import scala.Option;
import scala.collection.JavaConverters;
import scala.collection.Seq;
import scopt.OptionParser;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class WaterdropSql {
    public static void main(String[] args) throws Exception {
        JobInfo jobInfo = parseJob(args);
        Executor.runJob(jobInfo);
    }

    private static JobInfo parseJob(String[] args) throws IOException {
        OptionParser<CommandLineArgs> parser = CommandLineUtils.flinkParser();
        Seq<String> seq = JavaConverters.asScalaIteratorConverter(Arrays.asList(args).iterator()).asScala().toSeq();
        Option<CommandLineArgs> option = parser.parse(seq, new CommandLineArgs("client", "application.conf", false));
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
