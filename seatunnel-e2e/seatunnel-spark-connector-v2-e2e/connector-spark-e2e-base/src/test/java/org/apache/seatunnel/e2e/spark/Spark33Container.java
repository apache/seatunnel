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

package org.apache.seatunnel.e2e.spark;

import java.nio.file.Paths;

/**
 * This class is the base class of SparkEnvironment test. The before method will create a Spark master, and after method will close the Spark master.
 * You can use {@link AbstractSparkContainer#executeSeaTunnelSparkJob} to submit a seatunnel conf and a seatunnel spark job.
 */
public abstract class Spark33Container extends AbstractSparkContainer {

    private final String translationJarName = "seatunnel-translation-spark-3.3-dist.jar";

    @Override
    protected String getTranslationJarPath() {
        return Paths.get(PROJECT_ROOT_PATH.toString(),
                "seatunnel-translation", "seatunnel-translation-spark",
                "seatunnel-translation-spark-3.3-dist", "target",
                translationJarName).toString();
    }

    @Override
    protected String getTranslationJarTargetPath() {
        return Paths.get(SEATUNNEL_HOME, "plugins", "translation-spark-3.3", "lib",
                translationJarName).toString();
    }

    @Override
    protected String getSparkDockerImage() {
        return "bitnami/spark:3.3.0";
    }
}
