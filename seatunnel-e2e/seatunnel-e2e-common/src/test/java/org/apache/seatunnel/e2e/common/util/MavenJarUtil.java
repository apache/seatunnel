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

import org.apache.maven.model.Model;
import org.apache.maven.model.io.xpp3.MavenXpp3Reader;
import org.apache.maven.settings.Settings;
import org.apache.maven.settings.building.DefaultSettingsBuilder;
import org.apache.maven.settings.building.DefaultSettingsBuilderFactory;
import org.apache.maven.settings.building.DefaultSettingsBuildingRequest;
import org.apache.maven.settings.building.SettingsBuildingException;

import org.codehaus.plexus.util.xml.pull.XmlPullParserException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Optional;
import java.util.Properties;

public class MavenJarUtil {

    public static final String MAVEN_LOCAL_REPOSITORY_PATH = getLocalRepositoryPath();

    private static Properties getPomProperties() {
        MavenXpp3Reader reader = new MavenXpp3Reader();
        String pomFilePath = ContainerUtil.PROJECT_ROOT_PATH + "/pom.xml";
        try (FileReader fileReader = new FileReader(pomFilePath)) {
            Model model = reader.read(fileReader);
            return model.getProperties();
        } catch (IOException | XmlPullParserException e) {
            throw new RuntimeException(e);
        }
    }

    private static String getLocalRepositoryPath() {
        DefaultSettingsBuilder settingsBuilder = new DefaultSettingsBuilderFactory().newInstance();
        DefaultSettingsBuildingRequest request = new DefaultSettingsBuildingRequest();

        File userSettingsFile = new File(System.getProperty("user.home"), ".m2/settings.xml");
        File globalSettingsFile = new File(System.getenv("M2_HOME"), "conf/settings.xml");

        request.setUserSettingsFile(userSettingsFile.exists() ? userSettingsFile : null);
        request.setGlobalSettingsFile(globalSettingsFile.exists() ? globalSettingsFile : null);

        Settings settings = null;
        try {
            settings = settingsBuilder.build(request).getEffectiveSettings();
        } catch (SettingsBuildingException e) {
            throw new RuntimeException(e);
        }
        return Optional.ofNullable(settings.getLocalRepository())
                .orElse(System.getProperty("user.home") + "/.m2/repository");
    }

    public static String getMavenJarPath(String groupId, String artifactId, String version) {
        return MAVEN_LOCAL_REPOSITORY_PATH
                + File.separator
                + groupId.replace(".", File.separator)
                + File.separator
                + artifactId
                + File.separator
                + version
                + File.separator
                + artifactId
                + "-"
                + version
                + ".jar";
    }

    public static String getHadoop3UberJarPath() {
        Properties properties = getPomProperties();
        String shadeVersion = properties.getProperty("seatunnel.shade.version");
        String hadoopVersion = properties.getProperty("seatunnel.shade.hadoop.version");
        String groupId = "org.apache.seatunnel";
        String artifactId = "seatunnel-shade-hadoop3-uber";
        String version = hadoopVersion + "-" + shadeVersion;
        return getMavenJarPath(groupId, artifactId, version);
    }

    public static String getHadoop3UberJarName() {
        Properties properties = getPomProperties();
        String shadeVersion = properties.getProperty("seatunnel.shade.version");
        String hadoopVersion = properties.getProperty("seatunnel.shade.hadoop.version");
        String artifactId = "seatunnel-shade-hadoop3-uber";
        String version = hadoopVersion + "-" + shadeVersion;
        return String.format("%s-%s.jar", artifactId, version);
    }

    @Test
    public void testMavenJarPath() {
        String groupId = "org.apache.seatunnel";
        String artifactId = "seatunnel-shade-hadoop3-uber";
        String version = "3.1.1-2.1.0.0";
        String expectedPath =
                MAVEN_LOCAL_REPOSITORY_PATH
                        + File.separator
                        + groupId.replace(".", File.separator)
                        + File.separator
                        + artifactId
                        + File.separator
                        + version
                        + File.separator
                        + artifactId
                        + "-"
                        + version
                        + ".jar";
        Assertions.assertEquals(expectedPath, getMavenJarPath(groupId, artifactId, version));
    }

    @Test
    public void testHadoop3UberJarExists() {
        File file = new File(getHadoop3UberJarPath());
        Assertions.assertTrue(
                file.exists(), String.format("File %s not exists", file.getAbsolutePath()));
    }
}
