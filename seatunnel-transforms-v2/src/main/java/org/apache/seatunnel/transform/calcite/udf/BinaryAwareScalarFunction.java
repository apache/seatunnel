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

package org.apache.seatunnel.transform.calcite.udf;

import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.CallImplementor;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.NotNullImplementor;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.NullPolicy;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.RexImpTable;
import org.apache.seatunnel.shade.org.apache.calcite.adapter.enumerable.RexToLixTranslator;
import org.apache.seatunnel.shade.org.apache.calcite.avatica.util.ByteString;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.tree.Expression;
import org.apache.seatunnel.shade.org.apache.calcite.linq4j.tree.Expressions;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataType;
import org.apache.seatunnel.shade.org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.seatunnel.shade.org.apache.calcite.rex.RexCall;
import org.apache.seatunnel.shade.org.apache.calcite.schema.ImplementableFunction;
import org.apache.seatunnel.shade.org.apache.calcite.schema.ScalarFunction;
import org.apache.seatunnel.shade.org.apache.calcite.schema.impl.ReflectiveFunctionBase;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/** Bridges {@code byte[]} eval parameters and return values with Calcite's {@link ByteString}. */
final class BinaryAwareScalarFunction extends ReflectiveFunctionBase
        implements ScalarFunction, ImplementableFunction {

    private final CallImplementor implementor;

    BinaryAwareScalarFunction(Method method) {
        super(method);
        this.implementor =
                RexImpTable.createImplementor(
                        new BinaryAwareNotNullImplementor(method), NullPolicy.STRICT, false);
    }

    @Override
    public RelDataType getReturnType(RelDataTypeFactory typeFactory) {
        return typeFactory.createJavaType(method.getReturnType());
    }

    @Override
    public CallImplementor getImplementor() {
        return implementor;
    }

    /**
     * Detects whether the given method's signature involves binary types ({@code byte[]}) that
     * require ByteString bridging.
     */
    static boolean requiresBinaryBridging(Method method) {
        if (method.getReturnType() == byte[].class) {
            return true;
        }
        for (Class<?> paramType : method.getParameterTypes()) {
            if (paramType == byte[].class) {
                return true;
            }
        }
        return false;
    }

    private static final class BinaryAwareNotNullImplementor implements NotNullImplementor {

        private final Method method;

        BinaryAwareNotNullImplementor(Method method) {
            this.method = method;
        }

        @Override
        public Expression implement(
                RexToLixTranslator translator, RexCall call, List<Expression> translatedOperands) {
            Class<?>[] paramTypes = method.getParameterTypes();
            List<Expression> args = new ArrayList<>(translatedOperands.size());
            for (int i = 0; i < translatedOperands.size(); i++) {
                Expression operand = translatedOperands.get(i);
                if (paramTypes[i] == byte[].class) {
                    operand = Expressions.call(operand, "getBytes");
                }
                args.add(operand);
            }
            Expression result = Expressions.call(method, args);
            if (method.getReturnType() == byte[].class) {
                result = Expressions.new_(ByteString.class, result);
            }
            return result;
        }
    }
}
