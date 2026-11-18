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

package org.apache.seatunnel.resource.yarn.launch.resources;

import org.apache.seatunnel.resource.yarn.config.YarnApplicationConfiguration;
import org.apache.seatunnel.resource.yarn.launch.YarnDistribution;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.util.List;

/** Inputs shared by the launch steps while the submitter owns the private application directory. */
@Getter
@RequiredArgsConstructor
public final class YarnApplicationStaging {

    private final FileSystem fileSystem;
    private final Path directory;
    private final YarnDistribution distribution;
    private final YarnApplicationConfiguration deployment;
    private final Configuration hadoopConfiguration;

    /** Writes each declared file to the application's private staging directory in order. */
    public void stage(List<YarnLocalResource> files) throws IOException {
        for (YarnLocalResource file : files) {
            file.stage(this);
        }
    }

    /** Materializes the AM's selected job file. */
    public void stageJobFile(Path target) throws IOException {
        deployment.getJobFile().stage(fileSystem, target, hadoopConfiguration);
    }

    /** Materializes a named runtime file from its configured URI or inline contents. */
    public void stageShippedFile(String name, Path target) throws IOException {
        deployment.getShipFiles().get(name).stage(fileSystem, target, hadoopConfiguration);
    }
}
