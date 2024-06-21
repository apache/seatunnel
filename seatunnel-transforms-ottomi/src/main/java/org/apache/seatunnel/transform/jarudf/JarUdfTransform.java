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

package org.apache.seatunnel.transform.jarudf;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.transform.common.MultipleFieldOutputTransform;
import org.apache.seatunnel.transform.common.SeaTunnelRowAccessor;

import cn.hutool.core.util.ReflectUtil;
import cn.hutool.db.Db;
import cn.hutool.db.DbUtil;
import cn.hutool.db.Entity;
import cn.hutool.db.ds.simple.SimpleDataSource;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@Slf4j
public class JarUdfTransform extends MultipleFieldOutputTransform {

    private final ReadonlyConfig config;
    public static final String PLUGIN_NAME = "JarUdf";
    public static final String METHOD_NAME = "evaluate";
    public static final String QUERY_SQL = "select jar from ottomi_function where id = ?";
    public static final String JAR_COLUMN = "jar";
    private String className;
    private List<Integer> inputColumnIndex;
    private JarClassLoader jarClassLoader;

    public JarUdfTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public void open() {
        className = config.getOptional(JarUdfTransformConfig.JAR_CLASS_NAME).get();
        inputColumnIndex = config.getOptional(JarUdfTransformConfig.COLUMN_INDEX).get();
        try {
            loadJar();
        } catch (Exception e) {
            throw new RuntimeException("init load jar error:" + e.getMessage());
        }
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected Object[] getOutputFieldValues(SeaTunnelRowAccessor inputRow) {
        try {
            Class<?> loadClass = jarClassLoader.loadClass(className);
            Object instance = ReflectUtil.newInstance(loadClass);
            Method method = ReflectUtil.getMethod(loadClass, METHOD_NAME, Object[].class);
            Object[] inputData = inputColumnIndex.stream().map(i -> inputRow.getField(i)).toArray();
            Object result = ReflectUtil.invoke(instance, method, (Object) inputData);
            return new Object[] {result};
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Column[] getOutputColumns() {
        String outputFieldName = config.getOptional(JarUdfTransformConfig.OUT_FIELD_NAME).get();
        return Arrays.stream(new String[] {outputFieldName})
                .map(
                        fieldName ->
                                PhysicalColumn.of(
                                        fieldName, BasicType.STRING_TYPE, 255, true, "", ""))
                .toArray(Column[]::new);
    }

    private void loadJar() throws Exception {
        Map<String, String> props =
                config.getOptional(JarUdfTransformConfig.JAR_CONNECTION_PROPERTIES).get();
        SimpleDataSource simpleDataSource =
                new SimpleDataSource(
                        props.get("url"), props.get("username"), props.get("password"));
        Db use = DbUtil.use(simpleDataSource);
        Entity entity = use.queryOne(QUERY_SQL, props.get("id"));
        byte[] jarStream = (byte[]) entity.get(JAR_COLUMN);
        jarClassLoader = new JarClassLoader(jarStream);
    }
}
