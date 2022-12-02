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

package org.apache.seatunnel.core.starter.utils;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.core.starter.command.AbstractCommandArgs;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

@Slf4j
public class FileUtils {

    private FileUtils() {
        throw new UnsupportedOperationException("This class cannot be instantiated");
    }

    /**
     * Get the seatunnel config path.
     * In client mode, the path to the config file is directly given by user.
     * In cluster mode, the path to the config file is the `executor path/config file name`.
     *
     * @param args args
     * @return path of the seatunnel config file.
     */
    public static Path getConfigPath(AbstractCommandArgs args) {
        checkNotNull(args, "args");
        checkNotNull(args.getDeployMode(), "deploy mode");
        switch (args.getDeployMode()) {
            case CLIENT:
                return Paths.get(args.getConfigFile());
            case CLUSTER:
                return Paths.get(getFileName(args.getConfigFile()));
            default:
                throw new IllegalArgumentException("Unsupported deploy mode: " + args.getDeployMode());
        }
    }

    /**
     * Check whether the conf file exists.
     *
     * @param configFile the path of the config file
     */
    public static void checkConfigExist(Path configFile) {
        if (!configFile.toFile().exists()) {
            String message = "Can't find config file: " + configFile;
            throw new SeaTunnelRuntimeException(CommonErrorCode.FILE_OPERATION_FAILED, message);
        }
    }

    /**
     * Get the file name from the given path.
     * e.g. seatunnel/conf/config.conf -> config.conf
     *
     * @param filePath the path to the file
     * @return file name
     */
    private static String getFileName(String filePath) {
        checkNotNull(filePath, "file path");
        return filePath.substring(filePath.lastIndexOf(File.separatorChar) + 1);
    }

    private static <T> void checkNotNull(T arg, String argName) {
        if (arg == null) {
            throw new IllegalArgumentException(argName + " cannot be null");
        }
    }
}
