/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package io.github.interestinglab.waterdrop.utils;


import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidDataSourceFactory;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

public class JdbcConnectionPoll implements Serializable {

    private static JdbcConnectionPoll poll;

    private DruidDataSource dataSource;

    private JdbcConnectionPoll(Properties properties) throws Exception {
        dataSource = (DruidDataSource) DruidDataSourceFactory.createDataSource(properties);
    }

    public static JdbcConnectionPoll getPoll(Properties properties) throws Exception {
        if (poll == null){
            synchronized (JdbcConnectionPoll.class){
                if (poll == null){
                    poll = new JdbcConnectionPoll(properties);
                }
            }
        }
        return poll;
    }

    public Connection getConnection() throws SQLException {
       return dataSource.getConnection();
    }

}
