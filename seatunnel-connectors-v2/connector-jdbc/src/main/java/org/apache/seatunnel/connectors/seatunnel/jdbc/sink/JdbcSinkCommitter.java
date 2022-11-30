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

package org.apache.seatunnel.connectors.seatunnel.jdbc.sink;

import org.apache.seatunnel.api.sink.SinkCommitter;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.options.JdbcConnectionOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaFacade;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOps;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa.XaGroupOpsImpl;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import java.io.IOException;
import java.util.List;

public class JdbcSinkCommitter
    implements SinkCommitter<XidInfo> {
    private final XaFacade xaFacade;
    private final XaGroupOps xaGroupOps;
    private final JdbcConnectionOptions jdbcConnectionOptions;

    public JdbcSinkCommitter(
        JdbcConnectionOptions jdbcConnectionOptions
    )
        throws IOException {
        this.jdbcConnectionOptions = jdbcConnectionOptions;
        this.xaFacade = XaFacade.fromJdbcConnectionOptions(
            jdbcConnectionOptions);
        this.xaGroupOps = new XaGroupOpsImpl(xaFacade);
        try {
            xaFacade.open();
        } catch (Exception e) {
            new JdbcConnectorException(CommonErrorCode.WRITER_OPERATION_FAILED, "unable to open JDBC sink committer", e);
        }
    }

    @Override
    public List<XidInfo> commit(List<XidInfo> committables) {
        return xaGroupOps
            .commit(committables, false, jdbcConnectionOptions.getMaxCommitAttempts())
            .getForRetry();
    }

    @Override
    public void abort(List<XidInfo> commitInfos) {
        try {
            xaGroupOps.rollback(commitInfos);
        } catch (Exception e) {
            new JdbcConnectorException(JdbcConnectorErrorCode.XA_OPERATION_FAILED, "rollback failed", e);
        }
    }
}
