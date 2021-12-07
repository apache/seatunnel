/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop;

import io.github.interestinglab.waterdrop.doctor.ClassLoaderUtils;

import java.net.URL;

/**
 * Doctor
 *   command [findClassJar]... // find jar file location of specific class
 *   command [dignose]...    // waterdrop已经挂了，然后收集一些信息，包括系统信息，启动命令，配置文件，yarn logs
 *   command [detectPerf]... // detect performance issues.(Waterdrop 运行时增加统计信息，结合运行时去查看spark运行时job, stage的信息)
 *   command [benchmark] ... // waterdrop baseline benchmark.
 *
 *   param:
 *   (1) engine: spark, flink.
 */
public class WaterdropDoctor {

    public static void main(String[] args) throws Exception {
        System.out.println("Hello World");

//        URL url = org.apache.hadoop.hdfs.web.HftpFileSystem.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(url);
//        URL url = org.apache.hadoop.fs.FileSystem.class.getProtectionDomain().getCodeSource().getLocation();
//        System.out.println(url);

        String clsName = args[0];
        try {
            URL url = ClassLoaderUtils.getJarURL(clsName);
            System.out.println(url);
        } catch (java.lang.ClassNotFoundException e) {
            System.out.println("class not found in runtime jars: " + clsName);
        }
    }
}
