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

package org.apache.seatunnel.transform.sql;

import org.apache.seatunnel.api.common.error.RowErrorClassification;
import org.apache.seatunnel.api.common.error.SupportRowLevelErrorClassifier;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.SeaTunnelFlatMapTransform;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.api.transform.TransformOpenException;
import org.apache.seatunnel.transform.common.AbstractMultiCatalogFlatMapTransform;
import org.apache.seatunnel.transform.common.IdentityFlatMapTransform;
import org.apache.seatunnel.transform.exception.TransformCommonErrorCode;
import org.apache.seatunnel.transform.exception.TransformException;

import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class SQLMultiCatalogFlatMapTransform extends AbstractMultiCatalogFlatMapTransform
        implements SupportRowLevelErrorClassifier<SeaTunnelRow> {

    public SQLMultiCatalogFlatMapTransform(
            List<CatalogTable> inputCatalogTables, ReadonlyConfig config) {
        super(inputCatalogTables, config);
    }

    @Override
    public void open() {
        try {
            for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
                transform.open();
            }
        } catch (Exception failure) {
            close();
            throw new TransformOpenException("Failed to initialize SQL transform", failure);
        }
    }

    @Override
    public void close() {
        for (SeaTunnelTransform<SeaTunnelRow> transform : transformMap.values()) {
            try {
                transform.close();
            } catch (Exception failure) {
                log.warn("Failed to close a SQL table transform", failure);
            }
        }
    }

    @Override
    public String getPluginName() {
        return SQLTransform.PLUGIN_NAME;
    }

    @Override
    protected SeaTunnelFlatMapTransform<SeaTunnelRow> buildTransform(
            CatalogTable inputCatalogTable, ReadonlyConfig config) {
        return new SQLTransform(config, inputCatalogTable);
    }

    /** Mirrors the mandatory options of the transform-level rule declared by the factory. */
    @Override
    protected boolean hasTransformLevelRuleConfigured(ReadonlyConfig config) {
        return hasAnyOptionConfigured(config, SQLTransform.KEY_QUERY);
    }

    @Override
    public RowErrorClassification classifyRowError(Throwable t, SeaTunnelRow row) {
        TransformException transformException = findTransformException(t);
        if (transformException == null) {
            return RowErrorClassification.SYSTEM_ERROR;
        }
        if (transformException.getSeaTunnelErrorCode()
                == TransformCommonErrorCode.EXPRESSION_EXECUTE_ERROR) {
            return RowErrorClassification.ROW_ERROR;
        }
        if (transformException.getSeaTunnelErrorCode()
                        == TransformCommonErrorCode.WHERE_STATEMENT_ERROR
                && !SQLTransform.hasUnsupportedOperationCause(transformException)) {
            return RowErrorClassification.ROW_ERROR;
        }
        return RowErrorClassification.SYSTEM_ERROR;
    }

    private TransformException findTransformException(Throwable t) {
        Throwable current = t;
        while (current != null) {
            if (current instanceof TransformException) {
                return (TransformException) current;
            }
            current = current.getCause();
        }
        return null;
    }

    @Override
    protected SeaTunnelTransform<SeaTunnelRow> createIdentityTransform(CatalogTable catalogTable) {
        return new IdentityFlatMapTransform(catalogTable);
    }
}
