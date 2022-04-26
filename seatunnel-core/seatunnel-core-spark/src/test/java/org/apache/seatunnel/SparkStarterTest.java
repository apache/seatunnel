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

package org.apache.seatunnel;

import static org.junit.Assert.assertEquals;

import com.beust.jcommander.JCommander;
import org.apache.seatunnel.command.SparkCommandArgs;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class SparkStarterTest {

    @Test
    public void testGetSparkConf() throws URISyntaxException, FileNotFoundException {
        URI uri = ClassLoader.getSystemResource("spark_application.conf").toURI();
        String file = new File(uri).toString();
        Map<String, String> sparkConf = SparkStarter.getSparkConf(file);
        assertEquals("SeaTunnel", sparkConf.get("spark.app.name"));
        assertEquals("1", sparkConf.get("spark.executor.cores"));
    }
    @Test
    public void testChangeFileLocation() throws Exception {
        URI uri = ClassLoader.getSystemResource("spark_application.conf").toURI();
        String file = new File(uri).toString();
        String[] args = {"--master" , "yarn" , "--deploy-mode" , "cluster" ,"-c" ,file};
        Class SparkStarterClass =  Class.forName ("org.apache.seatunnel.SparkStarter");
        Constructor con = SparkStarterClass.getDeclaredConstructor(String[].class,SparkCommandArgs.class);
        con.setAccessible(true);
        SparkCommandArgs commandArgs = new SparkCommandArgs();
        JCommander.newBuilder()
                .programName("start-seatunnel-spark.sh")
                .addObject(commandArgs)
                .args(args)
                .build();
        SparkStarter starter = (SparkStarter) con.newInstance(args,commandArgs);
        starter.changeFileLocation();
        String[] finalArgs = starter.args;
        for (int i = 1; i < finalArgs.length; i++) {
            if ("-c".equals(finalArgs[i - 1]) || "--config".equals(finalArgs[i - 1])) {
                assertEquals("spark_application.conf",finalArgs[i]);
            }
        }
    }
}
