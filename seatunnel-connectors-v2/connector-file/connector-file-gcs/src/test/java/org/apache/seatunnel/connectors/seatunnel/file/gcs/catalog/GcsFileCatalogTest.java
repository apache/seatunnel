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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.catalog;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.Mockito;

class GcsFileCatalogTest {

    private static final String SINK_PATH = "gs://test-bucket/warehouse/orders";

    @Test
    void shouldRejectExistingPartitionDataInErrorMode() throws Exception {
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(fileSystemProxy.hasAnyFile(SINK_PATH, true)).thenReturn(true);
        GcsFileCatalog catalog = new GcsFileCatalog(fileSystemProxy, SINK_PATH, "GcsFile");
        DefaultSaveModeHandler handler =
                new DefaultSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.ERROR_WHEN_DATA_EXISTS,
                        catalog,
                        CatalogTableUtil.buildSimpleTextTable(),
                        null);

        Assertions.assertThrows(SeaTunnelRuntimeException.class, handler::handleDataSaveMode);

        Mockito.verify(fileSystemProxy).hasAnyFile(SINK_PATH, true);
        Mockito.verifyNoMoreInteractions(fileSystemProxy);
    }

    @Test
    void shouldPreserveExistingDataInAppendMode() {
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        GcsFileCatalog catalog = new GcsFileCatalog(fileSystemProxy, SINK_PATH, "GcsFile");
        DefaultSaveModeHandler handler =
                new DefaultSaveModeHandler(
                        SchemaSaveMode.IGNORE,
                        DataSaveMode.APPEND_DATA,
                        catalog,
                        CatalogTableUtil.buildSimpleTextTable(),
                        null);

        handler.handleDataSaveMode();

        Mockito.verifyNoInteractions(fileSystemProxy);
    }

    @Test
    void shouldNotReportDataWhenListingIsEmpty() throws Exception {
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(fileSystemProxy.hasAnyFile(SINK_PATH, true)).thenReturn(false);
        GcsFileCatalog catalog = new GcsFileCatalog(fileSystemProxy, SINK_PATH, "GcsFile");

        Assertions.assertFalse(catalog.isExistsData(null));

        Mockito.verify(fileSystemProxy).hasAnyFile(SINK_PATH, true);
    }

    @Test
    void shouldTruncateOnlyTheConfiguredPrefix() throws Exception {
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        GcsFileCatalog catalog = new GcsFileCatalog(fileSystemProxy, SINK_PATH, "GcsFile");

        catalog.truncateTable(null, false);

        InOrder operations = Mockito.inOrder(fileSystemProxy);
        operations.verify(fileSystemProxy).deleteFile(SINK_PATH);
        operations.verify(fileSystemProxy).createDir(SINK_PATH);
        Mockito.verifyNoMoreInteractions(fileSystemProxy);
    }

    @Test
    void shouldCheckForExistingDataRecursively() throws Exception {
        HadoopFileSystemProxy fileSystemProxy = Mockito.mock(HadoopFileSystemProxy.class);
        Mockito.when(fileSystemProxy.hasAnyFile(SINK_PATH, true)).thenReturn(true);
        GcsFileCatalog catalog = new GcsFileCatalog(fileSystemProxy, SINK_PATH, "GcsFile");

        Assertions.assertTrue(catalog.isExistsData(null));

        Mockito.verify(fileSystemProxy).hasAnyFile(SINK_PATH, true);
    }
}
