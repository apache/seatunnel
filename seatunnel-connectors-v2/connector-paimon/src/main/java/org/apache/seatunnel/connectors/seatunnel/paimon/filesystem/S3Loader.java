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

package org.apache.seatunnel.connectors.seatunnel.paimon.filesystem;

import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.FileIOLoader;
import org.apache.paimon.fs.Path;
import org.apache.paimon.s3.S3FileIO;

import java.util.ArrayList;
import java.util.List;

public class S3Loader implements FileIOLoader {
    @Override
    public String getScheme() {
        return "s3a";
    }

    @Override
    public List<String[]> requiredOptions() {
        List<String[]> options = new ArrayList<>();
        options.add(new String[] {"fs.s3a.access-key", "fs.s3a.access.key"});
        options.add(new String[] {"fs.s3a.secret-key", "fs.s3a.secret.key"});
        options.add(new String[] {"fs.s3a.endpoint", "fs.s3a.endpoint"});
        return options;
    }

    @Override
    public FileIO load(Path path) {
        return new S3FileIO();
    }
}
