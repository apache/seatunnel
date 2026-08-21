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

package org.apache.seatunnel.e2e.connector.paimon;

import org.apache.seatunnel.e2e.common.util.DependencyJar;

import org.testcontainers.containers.GenericContainer;

import java.io.IOException;

final class PaimonDependencies {

    private PaimonDependencies() {}

    static void copyHiveTo(GenericContainer<?> container, String targetDirectory)
            throws IOException, InterruptedException {
        DependencyJar.staged("hive-exec.jar").copyTo(container, targetDirectory);
        DependencyJar.staged("libfb303.jar").copyTo(container, targetDirectory);
    }

    /**
     * Put S3A in the container's {@code lib/} the same way the distribution does, with the shaded
     * {@code seatunnel-hadoop-aws} jar.
     *
     * <p>This used to add {@code hadoop-aws} and the AWS SDK v1 bundle separately. Against the
     * {@code hadoop-common} the uber jar now ships, {@code hadoop-aws} 3.1.4's {@code
     * S3AFileSystem.create} fails with {@code NoSuchMethodError} on {@code
     * SemaphoredDelegatingExecutor.<init>(ListeningExecutorService,int,boolean)}, whose guava-typed
     * constructor was removed - so every write through {@code
     * org.apache.paimon.s3.HadoopCompliantFileIO} failed. The shaded jar carries {@code hadoop-aws}
     * together with its own SDK, which is what {@code lib/seatunnel-hadoop-aws.jar} is in a real
     * distribution.
     */
    static void addS3To(GenericContainer<?> container, String targetDirectory) {
        DependencyJar.staged("seatunnel-hadoop-aws.jar").addTo(container, targetDirectory);
    }
}
