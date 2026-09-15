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

package org.apache.seatunnel.e2e.common.util;

import org.testcontainers.images.builder.ImageFromDockerfile;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Derives Java 11 flavours of the published engine images the e2e suite pins.
 *
 * <p>SeaTunnel jars are compiled to Java 11 bytecode, but several of the Flink and Spark images
 * still ship a Java 8 runtime, so loading any SeaTunnel class inside them fails with {@code
 * UnsupportedClassVersionError: class file version 55.0}. No Java 11 tags are published for those
 * images, and falling back to the vanilla upstream images is not an option either: the {@code
 * *_hadoop27} Flink images carry the shaded Hadoop jars the file based connectors need on the Flink
 * classpath, which is exactly why they were adopted in the first place (see issue #2291).
 *
 * <p>So the original image is kept byte for byte and only its JVM is replaced: a Java 11 JDK is
 * copied in and the image's own {@code JAVA_HOME} is re-pointed at it through a symlink. Because
 * the swap happens at the path the image already advertises, every hardcoded {@code JAVA_HOME}
 * reference and every {@code PATH} entry baked into the image keeps resolving, which matters for
 * the bitnami based Spark images whose startup scripts export {@code JAVA_HOME} themselves.
 *
 * <p>A full JDK rather than a JRE is required because {@link ContainerUtil} inspects running
 * servers with {@code jps}, {@code jstack} and {@code jmap}.
 *
 * <p>Images that already ship Java 11 must not be routed through this class: deriving them would
 * cost a docker build for no benefit.
 */
public final class EngineImageJdkUpgrader {

    /** Source of the Java 11 runtime that replaces the JVM of the derived images. */
    private static final String JDK_IMAGE = "eclipse-temurin:11-jdk";

    /** Path {@link #JDK_IMAGE} unpacks its JDK to, and the path it is copied from. */
    private static final String JDK_IMAGE_JAVA_HOME = "/opt/java/openjdk";

    /** Where the Java 11 JDK is installed before the image's own JAVA_HOME is linked to it. */
    private static final String JDK_INSTALL_PATH = "/opt/seatunnel-e2e-jdk11";

    /** Prefix of the derived image names, kept stable so repeated runs reuse the same build. */
    private static final String DERIVED_IMAGE_PREFIX = "seatunnel-e2e-jdk11-";

    /**
     * Derived image name per source image. A docker build is expensive, so each source image is
     * derived at most once per JVM and the daemon keeps the result for the rest of the job.
     */
    private static final Map<String, String> DERIVED_IMAGES = new ConcurrentHashMap<>();

    private EngineImageJdkUpgrader() {}

    /**
     * Returns a Java 11 flavour of {@code sourceImage}, building it on first use.
     *
     * @param sourceImage published engine image that still ships a Java 8 runtime
     * @return name of the derived image, usable anywhere the source image name was used
     */
    public static String toJava11(String sourceImage) {
        return DERIVED_IMAGES.computeIfAbsent(sourceImage, EngineImageJdkUpgrader::build);
    }

    private static String build(String sourceImage) {
        String dockerfile =
                "FROM "
                        + JDK_IMAGE
                        + " AS jdk\n"
                        + "FROM "
                        + sourceImage
                        + "\n"
                        // bitnami based images build as an unprivileged user, so take root before
                        // replacing the JDK. The e2e containers already run as root at runtime.
                        + "USER root\n"
                        + "COPY --from=jdk "
                        + JDK_IMAGE_JAVA_HOME
                        + " "
                        + JDK_INSTALL_PATH
                        + "\n"
                        // Fail the build loudly rather than silently keeping Java 8 if the source
                        // image does not declare JAVA_HOME. The trailing java -version check closes
                        // a gap the JAVA_HOME test alone does not: some base images resolve the
                        // bare
                        // `java` on PATH through /usr/bin/java -> /etc/alternatives/java rather
                        // than
                        // through $JAVA_HOME/bin, so the symlink swap above can succeed while the
                        // java a caller actually invokes is still the original Java 8 one.
                        + "RUN test -n \"$JAVA_HOME\" && rm -rf \"$JAVA_HOME\" && ln -s "
                        + JDK_INSTALL_PATH
                        + " \"$JAVA_HOME\""
                        + " && java -version 2>&1 | grep -q 'version \"11'\n";
        return new ImageFromDockerfile(derivedImageName(sourceImage), false)
                .withFileFromString("Dockerfile", dockerfile)
                .get();
    }

    /** Turns an image reference into a deterministic, docker compatible derived image name. */
    private static String derivedImageName(String sourceImage) {
        return DERIVED_IMAGE_PREFIX
                + sourceImage.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9._-]", "-")
                + ":latest";
    }
}
