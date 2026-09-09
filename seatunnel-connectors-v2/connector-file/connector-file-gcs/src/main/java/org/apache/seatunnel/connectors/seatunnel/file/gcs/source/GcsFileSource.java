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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.gcs.source.config.MultipleTableGcsFileSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.source.BaseMultipleTableFileSource;

import java.util.List;

/** File source implementation for objects stored in Google Cloud Storage. */
public class GcsFileSource extends BaseMultipleTableFileSource {

    /** Creates a GCS file source for the configured catalog tables. */
    public GcsFileSource(
            ReadonlyConfig readonlyConfig, List<CatalogTable> catalogTablesFromConfig) {
        this(new MultipleTableGcsFileSourceConfig(readonlyConfig, catalogTablesFromConfig));
    }

    private GcsFileSource(MultipleTableGcsFileSourceConfig sourceConfig) {
        super(sourceConfig, initFileSplitStrategy(sourceConfig));
    }

    @Override
    public String getPluginName() {
        return FileSystemType.GCS.getFileSystemPluginName();
    }
}
