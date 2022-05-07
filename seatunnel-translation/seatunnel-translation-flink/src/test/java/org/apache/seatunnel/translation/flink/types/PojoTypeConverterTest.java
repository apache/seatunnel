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

package org.apache.seatunnel.translation.flink.types;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DataType;
import org.apache.seatunnel.api.table.type.ListType;
import org.apache.seatunnel.api.table.type.PojoType;

import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;

public class PojoTypeConverterTest {

    @Test
    public void convert() {
        PojoTypeConverter<MockPojo> pojoTypeConverter = new PojoTypeConverter<>();
        Field[] fields = MockPojo.class.getDeclaredFields();
        DataType<?>[] fieldTypes = {BasicType.STRING, new ListType<>(BasicType.INTEGER)};
        PojoTypeInfo<MockPojo> pojoTypeInfo =
            pojoTypeConverter.convert(new PojoType<>(MockPojo.class, fields, fieldTypes));
        Assert.assertEquals(
            TypeExtractor.createTypeInfo(MockPojo.class),
            pojoTypeInfo);
    }

    public static class MockPojo {
        private String stringField;
        private List<Integer> listField;

        public String getStringField() {
            return stringField;
        }

        public void setStringField(String stringField) {
            this.stringField = stringField;
        }

        public List<Integer> getListField() {
            return listField;
        }

        public void setListField(List<Integer> listField) {
            this.listField = listField;
        }
    }
}
