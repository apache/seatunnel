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

package org.apache.seatunnel.resource.yarn.launch.decorators;

import org.apache.seatunnel.shade.com.google.common.io.Closer;

import org.apache.seatunnel.resource.yarn.launch.YarnConstants;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnApplicationStaging;
import org.apache.seatunnel.resource.yarn.launch.resources.YarnLocalResource;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import lombok.RequiredArgsConstructor;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes the private application specification and optional job and exposes them only to the AM.
 */
@RequiredArgsConstructor
public final class ApplicationSpecificationDecorator extends AbstractYarnStepDecorator {

    private final boolean hasJobFile;

    @Override
    public List<YarnLocalResource> build() {
        YarnLocalResource specification =
                YarnLocalResource.file(
                        YarnConstants.LOCALIZED_SPECIFICATION_NAME, this::stageSpecification);
        if (!hasJobFile) {
            return List.of(specification);
        }
        YarnLocalResource job =
                YarnLocalResource.file(
                        YarnConstants.LOCALIZED_JOB_CONFIG_NAME,
                        YarnApplicationStaging::stageJobFile);
        return List.of(specification, job);
    }

    private void stageSpecification(YarnApplicationStaging staging, Path path) throws IOException {
        try {
            writeSpecification(staging, path);
        } catch (IOException failure) {
            throw new IOException(
                    "Could not write YARN application specification to " + path, failure);
        }
    }

    private void writeSpecification(YarnApplicationStaging staging, Path path) throws IOException {
        Closer closer = Closer.create();
        try {
            FSDataOutputStream output = staging.getFileSystem().create(path, false);
            Writer writer = closer.register(new OutputStreamWriter(output, StandardCharsets.UTF_8));
            staging.getDeployment().write(writer);
        } catch (Throwable failure) {
            throw closer.rethrow(failure);
        } finally {
            closer.close();
        }
    }
}
