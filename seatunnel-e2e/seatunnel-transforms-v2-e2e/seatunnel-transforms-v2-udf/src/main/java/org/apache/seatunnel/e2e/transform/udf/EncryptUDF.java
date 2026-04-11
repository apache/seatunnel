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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDF;
import org.apache.seatunnel.transform.sql.zeta.ZetaUDFContext;

import com.google.auto.service.AutoService;

import java.util.List;

@AutoService(ZetaUDF.class)
public class EncryptUDF implements ZetaUDF {

    private transient CryptoClient client;

    @Override
    public String functionName() {
        return "ENCRYPT";
    }

    @Override
    public SeaTunnelDataType<?> resultType(List<SeaTunnelDataType<?>> argsType) {
        return BasicType.STRING_TYPE;
    }

    @Override
    public void open() {
        this.client = new CryptoClient();
    }

    @Override
    public boolean requiresContext() {
        return true;
    }

    @Override
    public Object evaluate(List<Object> args) {
        throw new UnsupportedOperationException("ENCRYPT should be called with context");
    }

    @Override
    public Object evaluateWithContext(List<Object> args, ZetaUDFContext context) {
        if (client == null) {
            throw new IllegalStateException("open() was not called before evaluateWithContext()");
        }
        Object value = args.get(0);
        if (value == null) {
            return null;
        }
        String tableId = context.getRawTableId();
        return client.encrypt(value, tableId);
    }

    @Override
    public void close() {
        this.client = null;
    }

    private static class CryptoClient {
        private String encrypt(Object value, String tableId) {
            int keySeed = tableId == null ? 0 : tableId.hashCode();
            return "ENC(" + keySeed + "):" + value;
        }
    }
}
