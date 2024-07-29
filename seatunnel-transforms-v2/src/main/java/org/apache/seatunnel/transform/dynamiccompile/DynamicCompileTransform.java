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

package org.apache.seatunnel.transform.dynamiccompile;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;
import org.apache.seatunnel.transform.dynamiccompile.parse.AbstractParse;
import org.apache.seatunnel.transform.dynamiccompile.parse.GroovyClassParse;
import org.apache.seatunnel.transform.dynamiccompile.parse.JavaClassParse;
import org.apache.seatunnel.transform.exception.TransformException;

import java.nio.file.Paths;

import static org.apache.seatunnel.transform.dynamiccompile.CompileTransformErrorCode.COMPILE_TRANSFORM_ERROR_CODE;

public class DynamicCompileTransform extends MultipleFieldOutputTransform {
    public static final String PLUGIN_NAME = "DynamicCompile";

    public static final String getInlineOutputColumns = "getInlineOutputColumns";

    public static final String getInlineOutputFieldValues = "getInlineOutputFieldValues";

    private final String sourceCode;

    private final CompilePattern compilePattern;

    private AbstractParse DynamicCompileParse;

    public DynamicCompileTransform(ReadonlyConfig readonlyConfig, CatalogTable catalogTable) {
        super(catalogTable);
        CompileLanguage compileLanguage =
                readonlyConfig.get(DynamicCompileTransformConfig.COMPILE_LANGUAGE);
        // todo other compile
        if (CompileLanguage.GROOVY.equals(compileLanguage)) {
            DynamicCompileParse = new GroovyClassParse();
        } else if (CompileLanguage.JAVA.equals(compileLanguage)) {
            DynamicCompileParse = new JavaClassParse();
        }
        compilePattern = readonlyConfig.get(DynamicCompileTransformConfig.COMPILE_PATTERN);

        if (CompilePattern.SOURCE_CODE.equals(compilePattern)) {
            sourceCode = readonlyConfig.get(DynamicCompileTransformConfig.SOURCE_CODE);
        } else {
            // NPE will never happen because it is required in the ABSOLUTE_PATH mode
            sourceCode =
                    FileUtils.readFileToStr(
                            Paths.get(
                                    readonlyConfig.get(
                                            DynamicCompileTransformConfig.ABSOLUTE_PATH)));
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Column[] getOutputColumns() {
        Object result;
        try {
            result =
                    ReflectionUtils.invoke(
                            getCompileLanguageInstance(),
                            getInlineOutputColumns,
                            inputCatalogTable);

        } catch (Exception e) {
            throw new TransformException(COMPILE_TRANSFORM_ERROR_CODE, e.getMessage());
        }

        return (Column[]) result;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        Object result;
        try {
            result =
                    ReflectionUtils.invoke(
                            getCompileLanguageInstance(), getInlineOutputFieldValues, inputRow);

        } catch (Exception e) {
            throw new TransformException(COMPILE_TRANSFORM_ERROR_CODE, e.getMessage());
        }
        return (Object[]) result;
    }

    private Object getCompileLanguageInstance()
            throws InstantiationException, IllegalAccessException {
        Class<?> compileClass = DynamicCompileParse.parseClassSourceCode(sourceCode);
        return compileClass.newInstance();
    }
}
