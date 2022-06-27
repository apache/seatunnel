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

package org.apache.seatunnel.connectors.seatunnel.hive.source.file.reader.format;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

import org.apache.seatunnel.connectors.seatunnel.hive.exception.HivePluginException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.HadoopConf;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class AbstractReadStrategy implements ReadStrategy {

    HadoopConf hadoopConf;

    @Override
    public void init(HadoopConf conf) {
        this.hadoopConf = conf;
    }

    @Override
    public Configuration getConfiguration(HadoopConf hadoopConf) {
        Configuration configuration = new Configuration();
        configuration.set(FS_DEFAULT_NAME_KEY, hadoopConf.getHdfsNameKey());
        configuration.set("fs.hdfs.impl", hadoopConf.getFsHdfsImpl());
        return configuration;
    }

    Configuration getConfiguration() throws HivePluginException {
        if (null == hadoopConf) {
            throw new HivePluginException("Not init read config");
        }
        return getConfiguration(hadoopConf);
    }

    boolean checkFileType(String path) throws IOException, HivePluginException {
        return false;
    }

    @Override
    public List<String> getFileNamesByPath(HadoopConf hadoopConf, String path) throws IOException {
        Configuration configuration = getConfiguration(hadoopConf);
        List<String> fileNames = new ArrayList<>();
        FileSystem hdfs = FileSystem.get(configuration);
        Path listFiles = new Path(path);
        FileStatus[] stats = hdfs.listStatus(listFiles);
        for (FileStatus fileStatus : stats) {
            if (fileStatus.isDirectory()) {
                fileNames.addAll(getFileNamesByPath(hadoopConf, fileStatus.getPath().toString()));
                continue;
            }
            if (fileStatus.isFile()) {

                fileNames.add(fileStatus.getPath().toString());
            }
        }
        return fileNames;
    }

}
