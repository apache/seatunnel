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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.driver;

import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;

import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;

public class DriverSelectionTest {

    @Test
    void assertDriver() {
        String url = "jdbc:mock://127.0.0.1:3306/test?useSSL=false";
        String driverName = OtherDriver.class.getName();
        String expectedDriverName = ExpectedDriver.class.getName();
        JdbcUrlUtil.UrlInfo MysqlUrlInfo = JdbcUrlUtil.getUrlInfo(url);
        MySqlCatalog mySqlCatalog =
                new MySqlCatalog("mock", "root", "123456", MysqlUrlInfo, expectedDriverName);
        try {
            Class.forName(driverName);
            Class.forName(expectedDriverName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        List<String> driverNames = new ArrayList<>();
        Enumeration<Driver> drivers = DriverManager.getDrivers();
        while (drivers.hasMoreElements()) {
            driverNames.add(drivers.nextElement().getClass().getName());
        }
        int expectedDriverIndex = driverNames.indexOf(expectedDriverName);
        int otherDriverIndex = driverNames.indexOf(driverName);
        assert expectedDriverIndex != -1 : "ExpectedDriver not registered in DriverManager";
        assert otherDriverIndex != -1 : "OtherDriver not registered in DriverManager";
        System.out.println(
                "expectedDriverIndex is "
                        + expectedDriverIndex
                        + " otherDriverIndex is "
                        + otherDriverIndex);
        assert expectedDriverIndex > otherDriverIndex
                : "ExpectedDriver should be registered after OtherDriver, but found ExpectedDriver at index "
                        + expectedDriverIndex
                        + " and OtherDriver at index "
                        + otherDriverIndex;
        /*
         * This test verifies that even when the driver is registered later in the DriverManager's list,
         * the system can still load the correct jar/driver based on the specified driverName parameter.
         * This ensures that our connection mechanism correctly prioritizes explicitly specified drivers
         * over the default driver discovery order in DriverManager.
         */
        Method getConnectionMethod = findGetConnectionMethod(mySqlCatalog.getClass());
        if (getConnectionMethod != null) {
            getConnectionMethod.setAccessible(true);
            Connection connection;
            try {
                connection = (Connection) getConnectionMethod.invoke(mySqlCatalog, url);
            } catch (IllegalAccessException | InvocationTargetException e) {
                throw new RuntimeException(e);
            }
            System.out.println(
                    "Connection class: "
                            + connection
                                    .getClass()
                                    .getName()
                                    .startsWith(ExpectedDriver.class.getName()));
            assert connection.getClass().getName().startsWith(ExpectedDriver.class.getName())
                    : "Connection should be created by "
                            + expectedDriverName
                            + " but was created by a class named "
                            + connection.getClass().getName();
        } else {
            assert false : "Could not find getConnection method";
        }
    }

    private Method findGetConnectionMethod(Class<?> clazz) {
        if (clazz == null) {
            return null;
        }
        try {
            return clazz.getDeclaredMethod("getConnection", String.class);
        } catch (NoSuchMethodException e) {
            return findGetConnectionMethod(clazz.getSuperclass());
        }
    }
}
