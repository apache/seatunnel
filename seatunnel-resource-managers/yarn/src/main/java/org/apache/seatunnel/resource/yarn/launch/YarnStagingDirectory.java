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

package org.apache.seatunnel.resource.yarn.launch;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/** Owns cleanup of application artifacts shared by the submitting client and ApplicationMaster. */
public final class YarnStagingDirectory {
    private YarnStagingDirectory() {}

    /** Resolves the application-owned remote directory supplied by the NodeManager launcher. */
    public static Path fromEnvironment() {
        String staging = System.getenv(YarnConstants.STAGING_DIRECTORY_ENV);
        if (staging == null || staging.isEmpty()) {
            throw new IllegalStateException("YARN application staging environment is missing");
        }
        return new Path(staging);
    }

    /** Deletes only the application's staging directory using an independently owned filesystem. */
    public static void cleanup(Configuration configuration, Path staging) throws Exception {
        Configuration cleanupConfiguration = new Configuration(configuration);
        // The AM shutdown hook owns this client; Hadoop's concurrent global hook must not close it.
        cleanupConfiguration.setBoolean("fs.automatic.close", false);
        try (FileSystem fileSystem =
                FileSystem.newInstance(staging.toUri(), cleanupConfiguration)) {
            if (!fileSystem.delete(staging, true) && fileSystem.exists(staging)) {
                throw new IOException(
                        "Could not remove YARN application staging directory " + staging);
            }
        }
    }
}
