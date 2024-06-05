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

package org.apache.seatunnel.engine.common.env;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

import static org.apache.seatunnel.engine.common.Constant.PROP_FILE;

@Slf4j
public class EnvironmentUtil {

    private static String getProperty(Properties properties, String key, String defaultValue) {
        String value = properties.getProperty(key);
        if (value == null || value.charAt(0) == '$') {
            return defaultValue;
        }
        return value;
    }

    public static Version getVersion() {

        Version version = new Version();
        ClassLoader classLoader = EnvironmentUtil.class.getClassLoader();

        try (InputStream propFile = classLoader.getResourceAsStream(PROP_FILE)) {

            if (propFile != null) {
                Properties properties = new Properties();

                properties.load(propFile);

                version.setProjectVersion(
                        getProperty(properties, "project.version", version.getProjectVersion()));
                version.setGitCommitId(
                        getProperty(properties, "git.commit.id", version.getGitCommitId()));
                version.setGitCommitAbbrev(
                        getProperty(
                                properties, "git.commit.id.abbrev", version.getGitCommitAbbrev()));

                DateTimeFormatter gitDateTimeFormatter =
                        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssZ");

                DateTimeFormatter systemDefault =
                        DateTimeFormatter.ISO_OFFSET_DATE_TIME.withZone(ZoneId.systemDefault());

                version.setBuildTime(
                        systemDefault.format(
                                gitDateTimeFormatter.parse(
                                        getProperty(
                                                properties,
                                                "git.build.time",
                                                version.getBuildTime()))));
                version.setCommitTime(
                        systemDefault.format(
                                gitDateTimeFormatter.parse(
                                        getProperty(
                                                properties,
                                                "git.commit.time",
                                                version.getCommitTime()))));
            }

        } catch (IOException ioException) {
            log.info("Unable to read version property file: {}", ioException.getMessage());
        }

        return version;
    }
}
