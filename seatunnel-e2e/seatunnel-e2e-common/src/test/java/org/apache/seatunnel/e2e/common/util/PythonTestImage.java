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

import org.testcontainers.DockerClientFactory;
import org.testcontainers.images.RemoteDockerImage;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.utility.DockerImageName;

import com.github.dockerjava.api.command.InspectImageResponse;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** Prepares Python in test images before runtime containers need package mirror access. */
public final class PythonTestImage {

    private static final Map<String, String> IMAGES = new ConcurrentHashMap<>();

    private PythonTestImage() {}

    /**
     * Returns the original engine image with Python available before any runtime JVM starts.
     * Package mirrors are needed only while building an image that does not already have Python.
     */
    public static String withPython(String sourceImage) {
        return IMAGES.computeIfAbsent(sourceImage, PythonTestImage::build);
    }

    private static String build(String sourceImage) {
        String pulledImage = new RemoteDockerImage(DockerImageName.parse(sourceImage)).get();
        InspectImageResponse image =
                DockerClientFactory.lazyClient().inspectImageCmd(pulledImage).exec();
        return new ImageFromDockerfile()
                .withFileFromString(
                        "Dockerfile",
                        dockerfile(
                                pulledImage,
                                image.getOs() + "/" + image.getArch(),
                                image.getConfig().getUser()))
                .get();
    }

    static String dockerfile(String sourceImage, String platform, String user) {
        return "FROM --platform="
                + platform
                + " "
                + sourceImage
                + "\n"
                + (user == null || user.isEmpty() ? "" : "USER root\n")
                + "RUN set -eu; "
                + "if command -v python3 >/dev/null 2>&1; then :; "
                + "elif command -v apt-get >/dev/null 2>&1; then "
                + "apt-get update && DEBIAN_FRONTEND=noninteractive "
                + "apt-get install -y --no-install-recommends python3; "
                + "elif command -v dnf >/dev/null 2>&1; then dnf install -y python3; "
                + "elif command -v microdnf >/dev/null 2>&1; then microdnf install -y python3; "
                + "elif command -v yum >/dev/null 2>&1; then yum install -y python3; "
                + "elif command -v apk >/dev/null 2>&1; then apk add --no-cache python3; "
                + "else echo 'Unsupported package manager for installing python3' >&2; exit 1; fi; "
                + "python3 --version\n"
                + (user == null || user.isEmpty() ? "" : "USER " + user + "\n");
    }
}
