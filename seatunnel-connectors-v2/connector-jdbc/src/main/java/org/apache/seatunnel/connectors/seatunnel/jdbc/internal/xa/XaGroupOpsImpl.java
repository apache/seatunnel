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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.xa;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.state.XidInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.transaction.xa.Xid;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

public class XaGroupOpsImpl
    implements XaGroupOps {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(XaGroupOpsImpl.class);

    private final XaFacade xaFacade;

    public XaGroupOpsImpl(XaFacade xaFacade) {
        this.xaFacade = xaFacade;
    }

    @Override
    public GroupXaOperationResult<XidInfo> commit(
        List<XidInfo> xids, boolean allowOutOfOrderCommits, int maxCommitAttempts) {
        GroupXaOperationResult<XidInfo> result = new GroupXaOperationResult<>();
        int origSize = xids.size();
        LOG.debug("commit {} transactions", origSize);
        for (Iterator<XidInfo> i = xids.iterator();
             i.hasNext() && (result.hasNoFailures() || allowOutOfOrderCommits); ) {
            XidInfo x = i.next();
            i.remove();
            try {
                xaFacade.commit(x.getXid(), false);
                result.succeeded(x);
            } catch (XaFacade.TransientXaException e) {
                result.failedTransiently(x.withAttemptsIncremented(), e);
            } catch (Exception e) {
                result.failed(x, e);
            }
        }
        result.getForRetry().addAll(xids);
        result.throwIfAnyFailed("commit");
        throwIfAnyReachedMaxAttempts(result, maxCommitAttempts);
        result.getTransientFailure()
            .ifPresent(
                f ->
                    LOG.warn(
                        "failed to commit {} transactions out of {} (keep them to retry later)",
                        result.getForRetry().size(),
                        origSize,
                        f));
        return result;
    }

    @Override
    public void rollback(List<XidInfo> xids) {
        for (XidInfo x : xids) {
            xaFacade.rollback(x.getXid());
        }
    }

    @Override
    public GroupXaOperationResult<XidInfo> failAndRollback(Collection<XidInfo> xids) {
        GroupXaOperationResult<XidInfo> result = new GroupXaOperationResult<>();
        if (xids.isEmpty()) {
            return result;
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("rolling back {} transactions: {}", xids.size(), xids);
        }
        for (XidInfo x : xids) {
            try {
                xaFacade.failAndRollback(x.getXid());
                result.succeeded(x);
            } catch (XaFacade.TransientXaException e) {
                LOG.info("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failedTransiently(x, e);
            } catch (Exception e) {
                LOG.warn("unable to fail/rollback transaction, xid={}: {}", x, e.getMessage());
                result.failed(x, e);
            }
        }
        if (!result.getForRetry().isEmpty()) {
            LOG.info("failed to roll back {} transactions", result.getForRetry().size());
        }
        return result;
    }

    @Override
    public void recoverAndRollback(JobContext context, SinkWriter.Context sinkContext, XidGenerator xidGenerator,
                                   Xid excludeXid) {
        Collection<Xid> recovered = xaFacade.recover().stream()
            .map(x -> new XidImpl(x.getFormatId(), x.getGlobalTransactionId(), x.getBranchQualifier())).collect(
                Collectors.toList());
        recovered.remove(excludeXid);
        if (recovered.isEmpty()) {
            return;
        }
        LOG.warn("rollback {} recovered transactions", recovered.size());
        for (Xid xid : recovered) {
            if (xidGenerator.belongsToSubtask(xid, context, sinkContext)) {
                try {
                    xaFacade.rollback(xid);
                } catch (Exception e) {
                    LOG.info("unable to rollback recovered transaction, xid={}", xid, e);
                }
            }
        }
    }

    private static void throwIfAnyReachedMaxAttempts(
        GroupXaOperationResult<XidInfo> result, int maxAttempts) {
        List<XidInfo> reached = null;
        for (XidInfo x : result.getForRetry()) {
            if (x.getAttempts() >= maxAttempts) {
                if (reached == null) {
                    reached = new ArrayList<>();
                }
                reached.add(x);
            }
        }
        if (reached != null) {
            throw new JdbcConnectorException(JdbcConnectorErrorCode.XA_OPERATION_FAILED,
                String.format(
                    "reached max number of commit attempts (%d) for transactions: %s",
                    maxAttempts, reached));
        }
    }
}
