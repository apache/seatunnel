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

package org.apache.seatunnel.engine.server.joiner;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.JoinRequest;
import com.hazelcast.internal.cluster.impl.MulticastJoiner;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.internal.util.RandomPicker;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class LiteNodeDropOutMulticastJoiner extends MulticastJoiner {

    private static final long JOIN_RETRY_INTERVAL = 1000L;
    private final AtomicInteger currentTryCount = new AtomicInteger(0);
    private final AtomicInteger maxTryCount = new AtomicInteger(calculateTryCount());

    public LiteNodeDropOutMulticastJoiner(Node node) {
        super(node);
    }

    @Override
    public void doJoin() {
        long joinStartTime = Clock.currentTimeMillis();
        long maxJoinMillis = getMaxJoinMillis();
        Address thisAddress = node.getThisAddress();

        while (shouldRetry() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {

            // clear master node
            clusterService.setMasterAddressToJoin(null);

            Address masterAddress = getTargetAddress();
            if (masterAddress == null) {
                masterAddress = findMasterWithMulticast();
            }
            clusterService.setMasterAddressToJoin(masterAddress);

            if (masterAddress == null || thisAddress.equals(masterAddress)) {
                if (node.isLiteMember()) {
                    log.info("This node is lite member. No need to join to a master node.");
                    continue;
                } else {
                    clusterJoinManager.setThisMemberAsMaster();
                    return;
                }
            }

            logger.info("Trying to join to discovered node: " + masterAddress);
            joinMaster();
        }
    }

    private void joinMaster() {
        long maxMasterJoinTime = getMaxJoinTimeToMasterNode();
        long start = Clock.currentTimeMillis();

        while (shouldRetry() && Clock.currentTimeMillis() - start < maxMasterJoinTime) {

            Address master = clusterService.getMasterAddress();
            if (master != null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Joining to master " + master);
                }
                clusterJoinManager.sendJoinRequest(master);
            } else {
                break;
            }

            try {
                clusterService.blockOnJoin(JOIN_RETRY_INTERVAL);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }

            if (isBlacklisted(master)) {
                clusterService.setMasterAddressToJoin(null);
                return;
            }
        }
    }

    private Address findMasterWithMulticast() {
        try {
            if (this.logger.isFineEnabled()) {
                this.logger.fine("Searching for master node. Max tries: " + maxTryCount.get());
            }

            JoinRequest joinRequest = this.node.createJoinRequest((Address) null);

            while (this.node.isRunning()
                    && currentTryCount.incrementAndGet() <= maxTryCount.get()) {
                joinRequest.setTryCount(currentTryCount.get());
                this.node.multicastService.send(joinRequest);
                Address masterAddress = this.clusterService.getMasterAddress();
                if (masterAddress != null) {
                    Address var3 = masterAddress;
                    return var3;
                }

                Thread.sleep((long) this.getPublishInterval());
            }

            return null;
        } catch (Exception var7) {
            Exception e = var7;
            if (this.logger != null) {
                this.logger.warning(e);
            }

            return null;
        } finally {
            currentTryCount.set(0);
        }
    }

    private int calculateTryCount() {
        NetworkConfig networkConfig = ConfigAccessor.getActiveMemberNetworkConfig(this.config);
        long timeoutMillis =
                TimeUnit.SECONDS.toMillis(
                        (long)
                                networkConfig
                                        .getJoin()
                                        .getMulticastConfig()
                                        .getMulticastTimeoutSeconds());
        int avgPublishInterval = 125;
        int tryCount = (int) timeoutMillis / avgPublishInterval;
        String host = this.node.getThisAddress().getHost();

        int lastDigits;
        try {
            lastDigits = Integer.parseInt(host.substring(host.lastIndexOf(46) + 1));
        } catch (NumberFormatException var9) {
            lastDigits = RandomPicker.getInt(512);
        }

        int portDiff = this.node.getThisAddress().getPort() - networkConfig.getPort();
        tryCount += (lastDigits + portDiff) % 10;
        return tryCount;
    }

    private int getPublishInterval() {
        return RandomPicker.getInt(50, 200);
    }
}
