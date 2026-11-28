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

package org.apache.seatunnel.e2e.transform.udf;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDFContext;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Exposes the table seen during startup and holds a worker until close so the container can detect
 * missing cleanup.
 */
@AutoService(ZetaUDF.class)
public final class CatalogContextUdf implements ZetaUDF {
    private String tablePath;
    private CountDownLatch closed;

    @Override
    public String functionName() {
        return "CATALOG_CONTEXT";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public void open(ZetaUDFContext context) {
        CatalogTable table = context.getInputCatalogTable();
        if (table == null || context.getRawTableId() != null || context.getAllFields().length > 0) {
            throw new IllegalStateException("UDF startup requires catalog metadata and no row");
        }
        if (tablePath != null) {
            throw new IllegalStateException("UDF initialized more than once");
        }
        tablePath = table.getTablePath().toString();
        CountDownLatch stop = new CountDownLatch(1);
        closed = stop;
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                stop.await();
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        },
                        "udf-catalog-context-" + tablePath);
        worker.setDaemon(true);
        worker.start();
    }

    @Override
    public Object evaluate(List<Object> args) {
        if (tablePath == null) {
            throw new IllegalStateException("UDF evaluated before startup");
        }
        return tablePath + ":" + args.get(0);
    }

    @Override
    public void close() {
        if (closed != null) {
            closed.countDown();
            closed = null;
        }
        tablePath = null;
    }
}
