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
import com.hazelcast.config.Config;
import com.hazelcast.config.InterfacesConfig;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.TcpIpConfig;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.SplitBrainJoinMessage;
import com.hazelcast.internal.cluster.impl.TcpIpJoiner;
import com.hazelcast.internal.cluster.impl.operations.JoinMastershipClaimOp;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.server.ServerConnectionManager;
import com.hazelcast.internal.server.tcp.LinkedAddresses;
import com.hazelcast.internal.server.tcp.LocalAddressRegistry;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.internal.util.Clock;
import com.hazelcast.spi.impl.operationservice.impl.OperationServiceImpl;
import com.hazelcast.spi.properties.ClusterProperty;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.cluster.impl.ClusterServiceImpl.SERVICE_NAME;
import static com.hazelcast.internal.util.EmptyStatement.ignore;
import static com.hazelcast.internal.util.FutureUtil.RETHROW_EVERYTHING;
import static com.hazelcast.internal.util.FutureUtil.returnWithDeadline;

public class LiteNodeDropOutTcpIpJoiner extends TcpIpJoiner {

    private static final long JOIN_RETRY_WAIT_TIME = 1000L;
    private static final int MASTERSHIP_CLAIM_TIMEOUT = 10;

    private final int maxPortTryCount;
    private volatile boolean claimingMastership;
    private final JoinConfig joinConfig;

    public LiteNodeDropOutTcpIpJoiner(Node node) {
        super(node);
        int tryCount = node.getProperties().getInteger(ClusterProperty.TCP_JOIN_PORT_TRY_COUNT);
        if (tryCount <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be greater than zero! Current value: %d",
                            ClusterProperty.TCP_JOIN_PORT_TRY_COUNT, tryCount));
        }
        maxPortTryCount = tryCount;
        joinConfig = getActiveMemberNetworkConfig(config).getJoin();
    }

    @Override
    public boolean isClaimingMastership() {
        return claimingMastership;
    }

    private int getConnTimeoutSeconds() {
        return joinConfig.getTcpIpConfig().getConnectionTimeoutSeconds();
    }

    @Override
    public void doJoin() {
        final Address targetAddress = getTargetAddress();
        if (targetAddress != null) {
            long maxJoinMergeTargetMillis =
                    node.getProperties().getMillis(ClusterProperty.MAX_JOIN_MERGE_TARGET_SECONDS);
            joinViaTargetMember(targetAddress, maxJoinMergeTargetMillis);
            if (!clusterService.isJoined()) {
                joinViaPossibleMembers();
            }
        } else if (joinConfig.getTcpIpConfig().getRequiredMember() != null) {
            Address requiredMember = getRequiredMemberAddress();
            long maxJoinMillis = getMaxJoinMillis();
            joinViaTargetMember(requiredMember, maxJoinMillis);
        } else {
            joinViaPossibleMembers();
        }
    }

    private void joinViaTargetMember(Address targetAddress, long maxJoinMillis) {
        try {
            if (targetAddress == null) {
                throw new IllegalArgumentException("Invalid target address: NULL");
            }
            if (logger.isFineEnabled()) {
                logger.fine("Joining over target member " + targetAddress);
            }
            if (targetAddress.equals(node.getThisAddress()) || isLocalAddress(targetAddress)) {
                clusterJoinManager.setThisMemberAsMaster();
                return;
            }
            long joinStartTime = Clock.currentTimeMillis();
            Connection connection;
            while (shouldRetry() && (Clock.currentTimeMillis() - joinStartTime < maxJoinMillis)) {
                ServerConnectionManager connectionManager =
                        node.getServer().getConnectionManager(MEMBER);
                connection = connectionManager.getOrConnect(targetAddress);
                if (connection == null) {
                    connectionManager.blockOnConnect(targetAddress, JOIN_RETRY_WAIT_TIME, 0);
                    continue;
                }
                if (logger.isFineEnabled()) {
                    logger.fine("Sending joinRequest " + targetAddress);
                }
                clusterJoinManager.sendJoinRequest(targetAddress);

                if (!clusterService.isJoined()) {
                    clusterService.blockOnJoin(JOIN_RETRY_WAIT_TIME);
                }
            }
        } catch (final Exception e) {
            logger.warning(e);
        }
    }

    private void joinViaPossibleMembers() {
        try {
            Collection<Address> possibleAddresses = getPossibleAddressesForInitialJoin();

            long maxJoinMillis = getMaxJoinMillis();
            long startTime = Clock.currentTimeMillis();

            while (shouldRetry() && (Clock.currentTimeMillis() - startTime < maxJoinMillis)) {
                tryJoinAddresses(possibleAddresses);

                if (clusterService.isJoined()) {
                    return;
                }

                // update for seatunnel, lite member can not become master node
                if (isAllBlacklisted(possibleAddresses) && !node.isLiteMember()) {
                    logger.fine(
                            "This node will assume master role since none of the possible members accepted join request.");
                    clusterJoinManager.setThisMemberAsMaster();
                    return;
                }

                if (tryClaimMastership(possibleAddresses)) {
                    return;
                }

                clusterService.setMasterAddressToJoin(null);
            }
        } catch (Throwable t) {
            logger.severe(t);
        }
    }

    private boolean tryClaimMastership(Collection<Address> addresses) {
        boolean consensus = false;
        if (isThisNodeMasterCandidate(addresses)) {
            consensus = claimMastership(addresses);
            if (consensus) {
                if (logger.isFineEnabled()) {
                    Set<Address> votingEndpoints = new HashSet<>(addresses);
                    votingEndpoints.removeAll(blacklistedAddresses.keySet());
                    logger.fine(
                            "Setting myself as master after consensus! Voting endpoints: "
                                    + votingEndpoints);
                }
                clusterJoinManager.setThisMemberAsMaster();
            } else if (logger.isFineEnabled()) {
                Set<Address> votingEndpoints = new HashSet<>(addresses);
                votingEndpoints.removeAll(blacklistedAddresses.keySet());
                logger.fine(
                        "My claim to be master is rejected! Voting endpoints: " + votingEndpoints);
            }
        } else if (logger.isFineEnabled()) {
            logger.fine("Cannot claim myself as master! Will try to connect a possible master...");
        }
        claimingMastership = false;
        return consensus;
    }

    @Override
    protected Collection<Address> getPossibleAddressesForInitialJoin() {
        return getPossibleAddresses();
    }

    private boolean claimMastership(Collection<Address> possibleAddresses) {
        if (logger.isFineEnabled()) {
            Set<Address> votingEndpoints = new HashSet<>(possibleAddresses);
            votingEndpoints.removeAll(blacklistedAddresses.keySet());
            logger.fine("Claiming myself as master node! Asking to endpoints: " + votingEndpoints);
        }
        claimingMastership = true;
        OperationServiceImpl operationService = node.getNodeEngine().getOperationService();
        Collection<Future<Boolean>> futures = new LinkedList<>();
        for (Address address : possibleAddresses) {
            try {
                if (isBlacklisted(address) || isLocalAddress(address)) {
                    continue;
                }
            } catch (UnknownHostException e) {
                logger.warning(e);
                ignore(e);
            }

            Future<Boolean> future =
                    operationService
                            .createInvocationBuilder(
                                    SERVICE_NAME, new JoinMastershipClaimOp(), address)
                            .setTryCount(1)
                            .invoke();
            futures.add(future);
        }

        try {
            Collection<Boolean> responses =
                    returnWithDeadline(
                            futures,
                            MASTERSHIP_CLAIM_TIMEOUT,
                            TimeUnit.SECONDS,
                            RETHROW_EVERYTHING);
            for (Boolean response : responses) {
                if (!response) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            logger.fine(e);
            return false;
        }
    }

    @SuppressWarnings("checkstyle:NestedIfDepth")
    private boolean isThisNodeMasterCandidate(Collection<Address> addresses) {
        // update for seatunnel, lite node can not become master node.
        if (node.isLiteMember()) {
            return false;
        }
        int thisHashCode = node.getThisAddress().hashCode();
        for (Address address : addresses) {
            if (isBlacklisted(address)) {
                continue;
            }
            if (node.getServer().getConnectionManager(MEMBER).get(address) != null
                    && node.getClusterService().getMember(address) != null
                    && !node.getClusterService().getMember(address).isLiteMember()) {
                LocalAddressRegistry addressRegistry = node.getLocalAddressRegistry();
                UUID memberUuid = addressRegistry.uuidOf(address);
                if (memberUuid != null) {
                    Address primaryAddress = addressRegistry.getPrimaryAddress(memberUuid);
                    if (primaryAddress != null) {
                        if (thisHashCode > primaryAddress.hashCode()) {
                            return false;
                        }
                    }
                }
            }
        }
        return true;
    }

    private void tryJoinAddresses(Collection<Address> addresses) throws InterruptedException {
        long connectionTimeoutMillis = TimeUnit.SECONDS.toMillis(getConnTimeoutSeconds());
        long start = Clock.currentTimeMillis();

        while (!clusterService.isJoined()
                && Clock.currentTimeMillis() - start < connectionTimeoutMillis) {
            Address masterAddress = clusterService.getMasterAddress();
            if (isAllBlacklisted(addresses) && masterAddress == null) {
                return;
            }

            if (masterAddress != null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Sending join request to " + masterAddress);
                }
                clusterJoinManager.sendJoinRequest(masterAddress);
            } else {
                sendMasterQuestion(addresses);
            }

            if (!clusterService.isJoined()) {
                clusterService.blockOnJoin(JOIN_RETRY_WAIT_TIME);
            }

            addresses.removeIf(
                    address -> {
                        try {
                            return isLocalAddress(address);
                        } catch (UnknownHostException e) {
                            if (logger.isFineEnabled()) {
                                logger.fine("Error during resolving possible target address!", e);
                            }
                            ignore(e);
                            return false;
                        }
                    });
        }
    }

    private boolean isAllBlacklisted(Collection<Address> possibleAddresses) {
        return blacklistedAddresses.keySet().containsAll(possibleAddresses);
    }

    private void sendMasterQuestion(Collection<Address> addresses) {
        if (logger.isFineEnabled()) {
            logger.fine(
                    "NOT sending master question to blacklisted endpoints: "
                            + blacklistedAddresses);
        }
        for (Address address : addresses) {
            if (isBlacklisted(address)) {
                continue;
            }
            if (logger.isFineEnabled()) {
                logger.fine("Sending master question to " + address);
            }
            clusterJoinManager.sendMasterQuestion(address);
        }
    }

    private Address getRequiredMemberAddress() {
        TcpIpConfig tcpIpConfig = joinConfig.getTcpIpConfig();
        String host = tcpIpConfig.getRequiredMember();
        try {
            AddressUtil.AddressHolder addressHolder =
                    AddressUtil.getAddressHolder(
                            host, getActiveMemberNetworkConfig(config).getPort());
            if (AddressUtil.isIpAddress(addressHolder.getAddress())) {
                return new Address(addressHolder.getAddress(), addressHolder.getPort());
            }
            InterfacesConfig interfaces = getActiveMemberNetworkConfig(config).getInterfaces();
            if (interfaces.isEnabled()) {
                InetAddress[] inetAddresses = InetAddress.getAllByName(addressHolder.getAddress());
                if (inetAddresses.length > 1) {
                    for (InetAddress inetAddress : inetAddresses) {
                        if (AddressUtil.matchAnyInterface(
                                inetAddress.getHostAddress(), interfaces.getInterfaces())) {
                            return new Address(inetAddress, addressHolder.getPort());
                        }
                    }
                } else if (AddressUtil.matchAnyInterface(
                        inetAddresses[0].getHostAddress(), interfaces.getInterfaces())) {
                    return new Address(addressHolder.getAddress(), addressHolder.getPort());
                }
            } else {
                return new Address(addressHolder.getAddress(), addressHolder.getPort());
            }
        } catch (final Exception e) {
            logger.warning(e);
        }
        return null;
    }

    @SuppressWarnings({"checkstyle:npathcomplexity", "checkstyle:cyclomaticcomplexity"})
    @Override
    protected Collection<Address> getPossibleAddresses() {
        final Collection<String> possibleMembers = getMembers();
        final Set<Address> possibleAddresses = new HashSet<>();
        final NetworkConfig networkConfig = getActiveMemberNetworkConfig(config);
        for (String possibleMember : possibleMembers) {
            AddressUtil.AddressHolder addressHolder = AddressUtil.getAddressHolder(possibleMember);
            try {
                boolean portIsDefined =
                        addressHolder.getPort() != -1 || !networkConfig.isPortAutoIncrement();
                int count = portIsDefined ? 1 : maxPortTryCount;
                int port =
                        addressHolder.getPort() != -1
                                ? addressHolder.getPort()
                                : networkConfig.getPort();
                AddressUtil.AddressMatcher addressMatcher = null;
                try {
                    addressMatcher = AddressUtil.getAddressMatcher(addressHolder.getAddress());
                } catch (AddressUtil.InvalidAddressException ignore) {
                    ignore(ignore);
                }
                if (addressMatcher != null) {
                    final Collection<String> matchedAddresses;
                    if (addressMatcher.isIPv4()) {
                        matchedAddresses = AddressUtil.getMatchingIpv4Addresses(addressMatcher);
                    } else {
                        // for IPv6 we are not doing wildcard matching
                        matchedAddresses = Collections.singleton(addressHolder.getAddress());
                    }
                    for (String matchedAddress : matchedAddresses) {
                        addPossibleAddresses(
                                possibleAddresses,
                                null,
                                InetAddress.getByName(matchedAddress),
                                port,
                                count);
                    }
                } else {
                    final String host = addressHolder.getAddress();
                    final InterfacesConfig interfaces = networkConfig.getInterfaces();
                    if (interfaces.isEnabled()) {
                        final InetAddress[] inetAddresses = InetAddress.getAllByName(host);
                        for (InetAddress inetAddress : inetAddresses) {
                            if (AddressUtil.matchAnyInterface(
                                    inetAddress.getHostAddress(), interfaces.getInterfaces())) {
                                addPossibleAddresses(
                                        possibleAddresses, host, inetAddress, port, count);
                            }
                        }
                    } else {
                        addPossibleAddresses(possibleAddresses, host, null, port, count);
                    }
                }
            } catch (UnknownHostException e) {
                logger.warning(
                        "Cannot resolve hostname '"
                                + addressHolder.getAddress()
                                + "'. Please make sure host is valid and reachable.");
                if (logger.isFineEnabled()) {
                    logger.fine("Error during resolving possible target!", e);
                }
            }
        }

        possibleAddresses.remove(node.getThisAddress());
        return possibleAddresses;
    }

    private void addPossibleAddresses(
            final Set<Address> possibleAddresses,
            final String host,
            final InetAddress inetAddress,
            final int port,
            final int count)
            throws UnknownHostException {
        for (int i = 0; i < count; i++) {
            int currentPort = port + i;

            Address address;
            if (host != null && inetAddress != null) {
                address = new Address(host, inetAddress, currentPort);
            } else if (host != null) {
                address = new Address(host, currentPort);
            } else {
                address = new Address(inetAddress, currentPort);
            }
            if (!isLocalAddress(address)) {
                possibleAddresses.add(address);
            }
        }
    }

    private boolean isLocalAddress(final Address address) throws UnknownHostException {
        UUID memberUuid = node.getLocalAddressRegistry().uuidOf(address);
        if (memberUuid == null) {
            // also try to resolve this address
            Address resolvedAddress = new Address(address.getInetSocketAddress());
            memberUuid = node.getLocalAddressRegistry().uuidOf(resolvedAddress);
        }
        boolean local = memberUuid != null && memberUuid.equals(node.getThisUuid());

        if (logger.isFineEnabled()) {
            logger.fine(address + " is local? " + local);
        }
        return local;
    }

    @Override
    protected Collection<String> getMembers() {
        return getConfigurationMembers(config);
    }

    public static Collection<String> getConfigurationMembers(Config config) {
        return getConfigurationMembers(
                getActiveMemberNetworkConfig(config).getJoin().getTcpIpConfig());
    }

    public static Collection<String> getConfigurationMembers(TcpIpConfig tcpIpConfig) {
        final Collection<String> configMembers = tcpIpConfig.getMembers();
        final Set<String> possibleMembers = new HashSet<>();
        for (String member : configMembers) {
            // split members defined in tcp-ip configuration by comma(,) semi-colon(;) space( ).
            String[] members = member.split("[,; ]");
            Collections.addAll(possibleMembers, members);
        }
        return possibleMembers;
    }

    @Override
    public void searchForOtherClusters() {
        final Collection<Address> possibleAddresses;
        try {
            possibleAddresses = getPossibleAddresses();
        } catch (Throwable e) {
            logger.severe(e);
            return;
        }
        LocalAddressRegistry addressRegistry = node.getLocalAddressRegistry();
        possibleAddresses.removeAll(addressRegistry.getLocalAddresses());
        node.getClusterService()
                .getMembers()
                .forEach(
                        member -> {
                            LinkedAddresses addresses =
                                    addressRegistry.linkedAddressesOf(member.getUuid());
                            if (addresses != null) {
                                Set<Address> knownMemberAddresses = addresses.getAllAddresses();
                                possibleAddresses.removeAll(knownMemberAddresses);
                            } else {
                                // do not expect this case in the normal conditions, except for
                                // disconnections happens
                                // at the same time
                                possibleAddresses.remove(member.getAddress());
                            }
                        });

        if (possibleAddresses.isEmpty()) {
            return;
        }
        SplitBrainJoinMessage request = node.createSplitBrainJoinMessage();
        for (Address address : possibleAddresses) {
            SplitBrainJoinMessage.SplitBrainMergeCheckResult result =
                    sendSplitBrainJoinMessageAndCheckResponse(address, request);
            if (result
                    == SplitBrainJoinMessage.SplitBrainMergeCheckResult.LOCAL_NODE_SHOULD_MERGE) {
                logger.warning(node.getThisAddress() + " is merging [tcp/ip] to " + address);
                setTargetAddress(address);
                startClusterMerge(address, request.getMemberListVersion());
                return;
            }
        }
    }
}
