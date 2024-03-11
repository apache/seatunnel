/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.core.starter.flink;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

public class TestFlinkJobCommandReturnV13 {
    @Test
    public void testSparkJobCommandReturnV13() throws Exception {
        String flink_job_command = null;
        String flink_job_command_return = null;
        String flink_job_command_return_error = null;
        String flink_home_dir = null;
        String flink_submit_job_cmd = null;
        String seatunnel_home_dir = null;
        String seatunnel_submit_flink_job_jar_list = null;
        String seatunnel_submit_flink_job_cmd = null;
        String seatunnel_submit_flink_job_cmd_paras = null;
        String seatunnel_submit_flink_job_cmd_full_path = null;

        StringBuilder sb_cmd_final = new StringBuilder();
        String seatunnel_submit_flink_job_cmd_final = null;

        final String separator = "/";
        Properties prop = new Properties();

        try {
            InputStream in =
                    this.getClass()
                            .getClassLoader()
                            .getResourceAsStream("flink13-job-command.properties");
            if (in == null) {
                throw new FileNotFoundException(
                        "Resource file not found. Make sure the file exists in src/test/resources.");
            }
            prop.load(in);

            flink_home_dir = prop.getProperty("FLINK13_HOME");
            flink_submit_job_cmd = prop.getProperty("FLINK13_SUBMIT_JOB_CMD");
            seatunnel_home_dir = prop.getProperty("SEATUNNEL_HOME");
            seatunnel_submit_flink_job_jar_list =
                    prop.getProperty("SEATUNNEL_SUBMIT_FLINK13_JOB_JAR_LIST");
            seatunnel_submit_flink_job_cmd = prop.getProperty("SEATUNNEL_SUBMIT_FLINK13_JOB_CMD");

            sb_cmd_final.append(seatunnel_home_dir);
            sb_cmd_final.append(separator);
            sb_cmd_final.append("bin");
            sb_cmd_final.append(separator);
            sb_cmd_final.append(seatunnel_submit_flink_job_cmd);
            sb_cmd_final.append(" --config ");
            sb_cmd_final.append(seatunnel_home_dir);
            sb_cmd_final.append(separator);
            sb_cmd_final.append("/config/v2.streaming.conf.template");

            seatunnel_submit_flink_job_cmd_final = sb_cmd_final.toString();
            // debug
            // System.out.println("Final command:" + seatunnel_submit_flink_job_cmd_final);

            Process process = Runtime.getRuntime().exec(seatunnel_submit_flink_job_cmd_final);
            process.waitFor(); // wait for the command to finish

            // Read the stdout of command
            flink_job_command_return = readStream(process.getInputStream());

            // Read the stderror of command
            flink_job_command_return_error = readStream(process.getErrorStream());
            Assertions.assertNull(flink_job_command_return_error);

            /* if (flink_job_command_return_error == null) {
                System.out.println("Seatunnel flink job submutted successfully.");
            } else {
                System.out.println("Seatunnel flink job submutted failed, error messages is:");
                System.out.println(flink_job_command_return_error);
            }
            */

            process.destroy();
            // debug
            System.out.println(
                    "Auto closed [start-seatunnel-flink-13-connector-v2.cmd] successfully.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static String readStream(InputStream inputStream) throws IOException {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream))) {
            String line;
            StringBuilder sb = new StringBuilder();
            String str_final = "";

            while ((line = reader.readLine()) != null) {
                System.out.println(line);
                sb.append(line);
            }
            str_final = sb.toString();
            if (!(str_final.isEmpty())) {
                return str_final;
            } else {
                return null;
            }
        }
    }
}
