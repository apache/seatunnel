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

package org.apache.seatunnel.transform.quality;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportTransform;

import cn.hutool.core.lang.Assert;
import com.oceandatum.quality.common.rulebase.IRuleHandler;
import com.oceandatum.quality.common.rulebase.RuleFactory;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

@Slf4j
public class QualityRuleTransform extends AbstractCatalogSupportTransform {

    public static final String PLUGIN_NAME = "QualityRule";
    private static boolean resultType;
    private static IRuleHandler ruleHandler;
    private static List<Integer> columns;

    public QualityRuleTransform(
            @NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        initRuleCode(config);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    /**
     * 初始化质检代码
     *
     * @param config
     */
    private void initRuleCode(ReadonlyConfig config) {
        loadRule(config);
    }

    /**
     * 质检结果输出
     *
     * @param inputRow upstream input row data
     * @return
     */
    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        for (Integer index : columns) {
            Object value = inputRow.getField(index);
            boolean result = ruleHandler.doCheck(String.valueOf(value));
            if (result != resultType) {
                return null;
            }
        }
        return inputRow;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputCatalogTable.getTableSchema().copy();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId().copy();
    }

    private void loadRule(ReadonlyConfig config) {
        try {
            String ruleType = config.getOptional(QualityRuleTransformConfig.RULE_TYPE).get();
            String ruleCode = config.getOptional(QualityRuleTransformConfig.RULE_CODE).get();
            resultType = config.getOptional(QualityRuleTransformConfig.QUALITY_RESULT).get();
            columns = config.getOptional(QualityRuleTransformConfig.COLUMNS_INDEX).get();
            Assert.notNull(ruleType);
            Assert.notNull(ruleCode);
            Assert.notNull(resultType);
            Assert.notNull(columns);
            ruleHandler = RuleFactory.getRule(ruleType, ruleCode);
        } catch (Exception e) {
            throw new RuntimeException("load quality rule error：", e);
        }
    }
}
