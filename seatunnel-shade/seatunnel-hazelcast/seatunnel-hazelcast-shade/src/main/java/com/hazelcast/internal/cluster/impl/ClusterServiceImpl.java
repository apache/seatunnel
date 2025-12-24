/*
 * Copyright (c) 2008-2022, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.auditlog.AuditlogTypeIds;
import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.ClusterState;
import com.hazelcast.cluster.InitialMembershipEvent;
import com.hazelcast.cluster.InitialMembershipListener;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.MemberSelector;
import com.hazelcast.cluster.MembershipEvent;
import com.hazelcast.cluster.MembershipListener;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.hotrestart.HotRestartService;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.LifecycleServiceImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.cluster.impl.operations.ExplicitSuspicionOp;
import com.hazelcast.internal.cluster.impl.operations.OnJoinOp;
import com.hazelcast.internal.cluster.impl.operations.PromoteLiteMemberOp;
import com.hazelcast.internal.cluster.impl.operations.ShutdownNodeOp;
import com.hazelcast.internal.cluster.impl.operations.TriggerExplicitSuspicionOp;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.metrics.Probe;
import com.hazelcast.internal.nio.Connection;
import com.hazelcast.internal.nio.ConnectionListener;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.TransactionalService;
import com.hazelcast.internal.util.Timer;
import com.hazelcast.internal.util.UuidUtil;
import com.hazelcast.internal.util.executor.ExecutorType;
import com.hazelcast.logging.ILogger;
import com.hazelcast.persistence.PersistenceService;
import com.hazelcast.spi.exception.RetryableHazelcastException;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.eventservice.EventPublishingService;
import com.hazelcast.spi.impl.eventservice.EventRegistration;
import com.hazelcast.spi.impl.eventservice.EventService;
import com.hazelcast.spi.impl.executionservice.ExecutionService;
import com.hazelcast.spi.impl.operationservice.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.transaction.TransactionOptions;
import com.hazelcast.transaction.TransactionalObject;
import com.hazelcast.transaction.impl.Transaction;
import com.hazelcast.version.Version;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

import static com.hazelcast.cluster.impl.MemberImpl.NA_MEMBER_LIST_JOIN_VERSION;
import static com.hazelcast.cluster.memberselector.MemberSelectors.NON_LOCAL_MEMBER_SELECTOR;
import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_METRIC_CLUSTER_SERVICE_SIZE;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_PREFIX;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_PREFIX_CLOCK;
import static com.hazelcast.internal.metrics.MetricDescriptorConstants.CLUSTER_PREFIX_HEARTBEAT;
import static com.hazelcast.internal.util.Preconditions.checkFalse;
import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static com.hazelcast.internal.util.Preconditions.checkTrue;
import static java.lang.String.format;

@SuppressWarnings({
    "checkstyle:methodcount",
    "checkstyle:classdataabstractioncoupling",
    "checkstyle:classfanoutcomplexity"
})
public class ClusterServiceImpl
        implements ClusterService,
                ConnectionListener,
                ManagedService,
                EventPublishingService<MembershipEvent, MembershipListener>,
                TransactionalService {

    public static final String SERVICE_NAME = "hz:core:clusterService";
    public static final String SPLIT_BRAIN_HANDLER_EXECUTOR_NAME = "hz:cluster:splitbrain";

    static final String CLUSTER_EXECUTOR_NAME = "hz:cluster";
    static final String MEMBERSHIP_EVENT_EXECUTOR_NAME = "hz:cluster:event";
    static final String VERSION_AUTO_UPGRADE_EXECUTOR_NAME = "hz:cluster:version:auto:upgrade";

    private static final int DEFAULT_MERGE_RUN_DELAY_MILLIS = 100;
    private static final long CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS = 1000;
    private static final boolean ASSERTION_ENABLED =
            ClusterServiceImpl.class.desiredAssertionStatus();
    private static final String TRANSACTION_OPTIONS_MUST_NOT_BE_NULL =
            "Transaction options must not be null!";
    private static final String STATE_MUST_NOT_BE_NULL = "State must not be null!";
    private static final String VERSION_MUST_NOT_BE_NULL = "Version must not be null!";

    private final Node node;
    private final ILogger logger;
    private final NodeEngineImpl nodeEngine;
    private final ClusterClockImpl clusterClock;
    private final MembershipManager membershipManager;
    private final ClusterJoinManager clusterJoinManager;
    private final ClusterStateManager clusterStateManager;
    private final ClusterHeartbeatManager clusterHeartbeatManager;
    private final ReentrantLock lock = new ReentrantLock();
    private final AtomicReference<JoinHolder> joined = new AtomicReference<>(new JoinHolder(false));

    private volatile UUID clusterId;
    private volatile Address masterAddress;
    private volatile MemberImpl localMember;

    private static class JoinHolder {
        private final CountDownLatch latch = new CountDownLatch(1);
        private final boolean isJoined;

        JoinHolder(boolean isJoined) {
            this.isJoined = isJoined;
        }
    }

    public ClusterServiceImpl(Node node, MemberImpl localMember) {
        this.node = node;
        this.localMember = localMember;
        nodeEngine = node.nodeEngine;

        logger = node.getLogger(ClusterService.class.getName());
        clusterClock = new ClusterClockImpl(logger);

        membershipManager = new MembershipManager(node, this, lock);
        clusterStateManager = new ClusterStateManager(node, lock);
        clusterJoinManager = new ClusterJoinManager(node, this, lock);
        clusterHeartbeatManager = new ClusterHeartbeatManager(node, this, lock);

        node.getServer().getConnectionManager(MEMBER).addConnectionListener(this);
        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.register(CLUSTER_EXECUTOR_NAME, 2, Integer.MAX_VALUE, ExecutorType.CACHED);
        executionService.register(
                SPLIT_BRAIN_HANDLER_EXECUTOR_NAME, 2, Integer.MAX_VALUE, ExecutorType.CACHED);
        // MEMBERSHIP_EVENT_EXECUTOR is a single threaded executor to ensure that events are
        // executed in correct order.
        executionService.register(
                MEMBERSHIP_EVENT_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        executionService.register(
                VERSION_AUTO_UPGRADE_EXECUTOR_NAME, 1, Integer.MAX_VALUE, ExecutorType.CACHED);
        registerMetrics();
    }

    private void registerMetrics() {
        MetricsRegistry metricsRegistry = node.nodeEngine.getMetricsRegistry();
        metricsRegistry.registerStaticMetrics(clusterClock, CLUSTER_PREFIX_CLOCK);
        metricsRegistry.registerStaticMetrics(clusterHeartbeatManager, CLUSTER_PREFIX_HEARTBEAT);
        metricsRegistry.registerStaticMetrics(this, CLUSTER_PREFIX);
    }

    @Override
    public void init(NodeEngine nodeEngine, Properties properties) {
        long mergeFirstRunDelayMs =
                node.getProperties()
                        .getPositiveMillisOrDefault(
                                ClusterProperty.MERGE_FIRST_RUN_DELAY_SECONDS,
                                DEFAULT_MERGE_RUN_DELAY_MILLIS);
        long mergeNextRunDelayMs =
                node.getProperties()
                        .getPositiveMillisOrDefault(
                                ClusterProperty.MERGE_NEXT_RUN_DELAY_SECONDS,
                                DEFAULT_MERGE_RUN_DELAY_MILLIS);

        ExecutionService executionService = nodeEngine.getExecutionService();
        executionService.scheduleWithRepetition(
                SPLIT_BRAIN_HANDLER_EXECUTOR_NAME,
                new SplitBrainHandler(node),
                mergeFirstRunDelayMs,
                mergeNextRunDelayMs,
                TimeUnit.MILLISECONDS);

        membershipManager.init();
        clusterHeartbeatManager.init();
    }

    public void sendLocalMembershipEvent() {
        membershipManager.sendMembershipEvents(
                Collections.emptySet(), Collections.singleton(getLocalMember()), false);
    }

    public void handleExplicitSuspicion(
            MembersViewMetadata expectedMembersViewMetadata, Address suspectedAddress) {
        membershipManager.handleExplicitSuspicion(expectedMembersViewMetadata, suspectedAddress);
    }

    public void handleExplicitSuspicionTrigger(
            Address caller,
            int callerMemberListVersion,
            MembersViewMetadata suspectedMembersViewMetadata) {
        membershipManager.handleExplicitSuspicionTrigger(
                caller, callerMemberListVersion, suspectedMembersViewMetadata);
    }

    public void suspectMember(Member suspectedMember, String reason, boolean destroyConnection) {
        membershipManager.suspectMember((MemberImpl) suspectedMember, reason, destroyConnection);
    }

    public void suspectAddressIfNotConnected(Address address) {
        lock.lock();
        try {
            MemberImpl member = getMember(address);
            if (member == null) {
                if (logger.isFineEnabled()) {
                    logger.fine("Cannot suspect " + address + ", since it's not a member.");
                }

                return;
            }

            Connection conn = node.getServer().getConnectionManager(MEMBER).get(address);
            if (conn != null && conn.isAlive()) {
                if (logger.isFineEnabled()) {
                    logger.fine(
                            "Cannot suspect "
                                    + member
                                    + ", since there's a live connection -> "
                                    + conn);
                }

                return;
            }
            suspectMember(member, "No connection", false);
        } finally {
            lock.unlock();
        }
    }

    void sendExplicitSuspicion(MembersViewMetadata endpointMembersViewMetadata) {
        Address endpoint = endpointMembersViewMetadata.getMemberAddress();
        if (endpoint.equals(node.getThisAddress())) {
            logger.warning(
                    "Cannot send explicit suspicion for "
                            + endpointMembersViewMetadata
                            + " to itself.");
            return;
        }

        if (!isJoined()) {
            if (logger.isFineEnabled()) {
                logger.fine("Cannot send explicit suspicion, not joined yet!");
            }

            return;
        }

        Version clusterVersion = getClusterVersion();
        assert !clusterVersion.isUnknown() : "Cluster version should not be unknown after join!";

        Operation op = new ExplicitSuspicionOp(endpointMembersViewMetadata);
        nodeEngine.getOperationService().send(op, endpoint);
    }

    void sendExplicitSuspicionTrigger(
            Address triggerTo, MembersViewMetadata endpointMembersViewMetadata) {
        if (triggerTo.equals(node.getThisAddress())) {
            logger.warning(
                    "Cannot send explicit suspicion trigger for "
                            + endpointMembersViewMetadata
                            + " to itself.");
            return;
        }

        int memberListVersion = membershipManager.getMemberListVersion();
        Operation op =
                new TriggerExplicitSuspicionOp(memberListVersion, endpointMembersViewMetadata);
        OperationService operationService = nodeEngine.getOperationService();
        operationService.send(op, triggerTo);
    }

    public MembersView handleMastershipClaim(
            @Nonnull Address candidateAddress, @Nonnull UUID candidateUuid) {
        checkNotNull(candidateAddress);
        checkNotNull(candidateUuid);
        checkFalse(
                getThisAddress().equals(candidateAddress),
                "cannot accept my own mastership claim!");

        lock.lock();
        try {
            checkTrue(
                    isJoined(),
                    candidateAddress + " claims mastership but this node is not joined!");
            checkFalse(
                    isMaster(), candidateAddress + " claims mastership but this node is master!");

            MemberImpl masterCandidate =
                    membershipManager.getMember(candidateAddress, candidateUuid);
            checkTrue(
                    masterCandidate != null,
                    candidateAddress + " claims mastership but it is not a member!");

            MemberMap memberMap = membershipManager.getMemberMap();
            if (!shouldAcceptMastership(memberMap, masterCandidate)) {
                String message =
                        "Cannot accept mastership claim of "
                                + candidateAddress
                                + " at the moment. There are more suitable master candidates in the member list.";
                logger.fine(message);
                throw new RetryableHazelcastException(message);
            }

            if (!membershipManager.clearMemberSuspicion(masterCandidate, "Mastership claim")) {
                throw new IllegalStateException(
                        "Cannot accept mastership claim of "
                                + candidateAddress
                                + ". "
                                + getMasterAddress()
                                + " is already master.");
            }

            setMasterAddress(masterCandidate.getAddress());

            MembersView response = memberMap.toTailMembersView(masterCandidate, true);

            logger.warning(
                    "Mastership of " + candidateAddress + " is accepted. Response: " + response);

            return response;
        } finally {
            lock.unlock();
        }
    }

    // called under cluster service lock
    // mastership is accepted when all members before the candidate is suspected or is lite node
    private boolean shouldAcceptMastership(MemberMap memberMap, MemberImpl candidate) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        for (MemberImpl member : memberMap.headMemberSet(candidate, false)) {
            // update for seatunnel, lite member can not become master node
            if (!member.isLiteMember() && !membershipManager.isMemberSuspected(member)) {
                if (logger.isFineEnabled()) {
                    logger.fine(
                            "Should not accept mastership claim of "
                                    + candidate
                                    + ", because "
                                    + member
                                    + " is not suspected at the moment and is before than "
                                    + candidate
                                    + " in the member list.");
                }

                return false;
            }
        }
        return true;
    }

    public void merge(Address newTargetAddress) {
        node.getJoiner().setTargetAddress(newTargetAddress);
        LifecycleServiceImpl lifecycleService = node.hazelcastInstance.getLifecycleService();
        lifecycleService.runUnderLifecycleLock(new ClusterMergeTask(node));
    }

    @Override
    public void reset() {
        lock.lock();
        try {
            resetJoinState();
            resetLocalMemberUuid();
            resetClusterId();
            clearInternalState();
        } finally {
            lock.unlock();
        }
    }

    private void resetLocalMemberUuid() {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        assert !isJoined() : "Cannot reset local member UUID when joined.";

        Map<EndpointQualifier, Address> addressMap = localMember.getAddressMap();
        UUID newUuid = UuidUtil.newUnsecureUUID();

        logger.warning(
                "Resetting local member UUID. Previous: "
                        + localMember.getUuid()
                        + ", new: "
                        + newUuid);
        node.setThisUuid(newUuid);
        localMember =
                new MemberImpl.Builder(addressMap)
                        .version(localMember.getVersion())
                        .localMember(true)
                        .uuid(newUuid)
                        .attributes(localMember.getAttributes())
                        .liteMember(localMember.isLiteMember())
                        .memberListJoinVersion(localMember.getMemberListJoinVersion())
                        .instance(node.hazelcastInstance)
                        .build();
        node.loggingService.setThisMember(localMember);
        node.getLocalAddressRegistry().setLocalUuid(newUuid);
    }

    public void resetJoinState() {
        lock.lock();
        try {
            setMasterAddress(null);
            setJoined(false);
        } finally {
            lock.unlock();
        }
    }

    @SuppressWarnings("checkstyle:parameternumber")
    public boolean finalizeJoin(
            MembersView membersView,
            Address callerAddress,
            UUID callerUuid,
            UUID targetUuid,
            UUID clusterId,
            ClusterState clusterState,
            Version clusterVersion,
            long clusterStartTime,
            long masterTime,
            OnJoinOp preJoinOp) {
        lock.lock();
        try {
            if (!checkValidMaster(callerAddress)) {
                if (logger.isFineEnabled()) {
                    logger.fine(
                            "Not finalizing join because caller: "
                                    + callerAddress
                                    + " is not known master: "
                                    + getMasterAddress());
                }
                MembersViewMetadata membersViewMetadata =
                        new MembersViewMetadata(
                                callerAddress, callerUuid, callerAddress, membersView.getVersion());
                sendExplicitSuspicion(membersViewMetadata);
                return false;
            }

            if (isJoined()) {
                if (logger.isFineEnabled()) {
                    logger.fine("Node is already joined... No need to finalize join...");
                }

                return false;
            }

            checkMemberUpdateContainsLocalMember(membersView, targetUuid);

            try {
                initialClusterState(clusterState, clusterVersion);
            } catch (VersionMismatchException e) {
                // node should shutdown since it cannot handle the cluster version
                // it is safe to do so here because no operations have been executed yet
                logger.severe(
                        format(
                                "This member will shutdown because it cannot join the cluster: %s",
                                e.getMessage()));
                node.shutdown(true);
                return false;
            }
            setClusterId(clusterId);
            ClusterClockImpl clusterClock = getClusterClock();
            clusterClock.setClusterStartTime(clusterStartTime);
            clusterClock.setMasterTime(masterTime);

            // run pre-join op before member list update, so operations other than join ops will be
            // refused by operation service
            if (preJoinOp != null) {
                nodeEngine.getOperationService().run(preJoinOp);
            }

            membershipManager.updateMembers(membersView);
            clusterHeartbeatManager.heartbeat();
            setJoined(true);
            node.getNodeExtension()
                    .getAuditlogService()
                    .eventBuilder(AuditlogTypeIds.CLUSTER_MEMBER_ADDED)
                    .message("Member joined")
                    .addParameter("membersView", membersView)
                    .addParameter("address", node.getThisAddress())
                    .log();
            return true;
        } finally {
            lock.unlock();
        }
    }

    public boolean updateMembers(
            MembersView membersView, Address callerAddress, UUID callerUuid, UUID targetUuid) {
        lock.lock();
        try {
            if (!isJoined()) {
                logger.warning(
                        "Not updating members received from caller: "
                                + callerAddress
                                + " because node is not joined! ");
                return false;
            }

            if (!checkValidMaster(callerAddress)) {
                logger.warning(
                        "Not updating members because caller: "
                                + callerAddress
                                + " is not known master: "
                                + getMasterAddress());
                MembersViewMetadata callerMembersViewMetadata =
                        new MembersViewMetadata(
                                callerAddress, callerUuid, callerAddress, membersView.getVersion());
                if (!clusterJoinManager.isMastershipClaimInProgress()) {
                    sendExplicitSuspicion(callerMembersViewMetadata);
                }
                return false;
            }

            checkMemberUpdateContainsLocalMember(membersView, targetUuid);

            if (!shouldProcessMemberUpdate(membersView)) {
                return false;
            }

            membershipManager.updateMembers(membersView);
            return true;
        } finally {
            lock.unlock();
        }
    }

    private void checkMemberUpdateContainsLocalMember(MembersView membersView, UUID targetUuid) {
        UUID thisUuid = getThisUuid();
        if (!thisUuid.equals(targetUuid)) {
            String msg =
                    "Not applying member update because target uuid: "
                            + targetUuid
                            + " is different! -> "
                            + membersView
                            + ", local member: "
                            + localMember;
            throw new IllegalArgumentException(msg);
        }

        Member localMember = getLocalMember();
        if (!membersView.containsMember(localMember.getAddress(), localMember.getUuid())) {
            String msg =
                    "Not applying member update because member list doesn't contain us! -> "
                            + membersView
                            + ", local member: "
                            + localMember;
            throw new IllegalArgumentException(msg);
        }
    }

    private boolean checkValidMaster(Address callerAddress) {
        return (callerAddress != null && callerAddress.equals(getMasterAddress()));
    }

    private boolean shouldProcessMemberUpdate(MembersView membersView) {
        int memberListVersion = membershipManager.getMemberListVersion();
        if (memberListVersion > membersView.getVersion()) {
            if (logger.isFineEnabled()) {
                logger.fine(
                        "Received an older member update, ignoring... Current version: "
                                + memberListVersion
                                + ", Received version: "
                                + membersView.getVersion());
            }

            return false;
        }

        if (memberListVersion == membersView.getVersion()) {
            if (ASSERTION_ENABLED) {
                MemberMap memberMap = membershipManager.getMemberMap();
                Collection<Address> currentAddresses = memberMap.getAddresses();
                Collection<Address> newAddresses = membersView.getAddresses();

                assert currentAddresses.size() == newAddresses.size()
                                && newAddresses.containsAll(currentAddresses)
                        : "Member view versions are same but new member view doesn't match the current!"
                                + " Current: "
                                + memberMap.toMembersView()
                                + ", New: "
                                + membersView;
            }

            if (logger.isFineEnabled()) {
                logger.fine(
                        "Received a periodic member update, ignoring... Version: "
                                + memberListVersion);
            }

            return false;
        }

        return true;
    }

    @Override
    public void connectionAdded(Connection connection) {}

    @Override
    public void connectionRemoved(Connection connection) {
        if (logger.isFineEnabled()) {
            logger.fine("Removed connection to " + connection.getRemoteAddress());
        }
        if (!isJoined()) {
            Address masterAddress = getMasterAddress();
            if (masterAddress != null && masterAddress.equals(connection.getRemoteAddress())) {
                setMasterAddressToJoin(null);
            }
        }
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    /**
     * Returns whether member with given identity (either {@code UUID} or {@code Address} depending
     * on Persistence is enabled or not) is a known missing member or not.
     *
     * @param address Address of the missing member
     * @param uuid Uuid of the missing member
     * @return true if it's a known missing member, false otherwise
     */
    public boolean isMissingMember(Address address, UUID uuid) {
        return membershipManager.isMissingMember(address, uuid);
    }

    public Collection<Member> getActiveAndMissingMembers() {
        return membershipManager.getActiveAndMissingMembers();
    }

    public void notifyForRemovedMember(MemberImpl member) {
        lock.lock();
        try {
            membershipManager.onMemberRemove(member);
        } finally {
            lock.unlock();
        }
    }

    public void shrinkMissingMembers(Collection<UUID> memberUuidsToRemove) {
        membershipManager.shrinkMissingMembers(memberUuidsToRemove);
    }

    @Override
    public MemberImpl getMember(Address address) {
        if (address == null) {
            return null;
        }
        return membershipManager.getMember(address);
    }

    @Override
    public MemberImpl getMember(UUID uuid) {
        if (uuid == null) {
            return null;
        }
        return membershipManager.getMember(uuid);
    }

    @Override
    public MemberImpl getMember(Address address, UUID uuid) {
        if (address == null || uuid == null) {
            return null;
        }
        return membershipManager.getMember(address, uuid);
    }

    @Override
    @Nonnull
    public Collection<MemberImpl> getMemberImpls() {
        return membershipManager.getMembers();
    }

    public Collection<Address> getMemberAddresses() {
        return membershipManager.getMemberMap().getAddresses();
    }

    @Override
    @Nonnull
    public Set<Member> getMembers() {
        return membershipManager.getMemberSet();
    }

    @Override
    public Collection<Member> getMembers(MemberSelector selector) {
        return (Collection) new MemberSelectingCollection(membershipManager.getMembers(), selector);
    }

    @Override
    public void shutdown(boolean terminate) {
        clearInternalState();
    }

    private void clearInternalState() {
        lock.lock();
        try {
            membershipManager.reset();
            clusterHeartbeatManager.reset();
            clusterStateManager.reset();
            clusterJoinManager.reset();
            resetJoinState();
        } finally {
            lock.unlock();
        }
    }

    public boolean setMasterAddressToJoin(final Address master) {
        lock.lock();
        try {
            if (isJoined()) {
                Address currentMasterAddress = getMasterAddress();
                if (!currentMasterAddress.equals(master)) {
                    logger.warning(
                            "Cannot set master address to "
                                    + master
                                    + " because node is already joined! Current master: "
                                    + currentMasterAddress);
                } else if (logger.isFineEnabled()) {
                    logger.fine("Master address is already set to " + master);
                }
                return false;
            }

            setMasterAddress(master);
            return true;
        } finally {
            lock.unlock();
        }
    }

    // should be called under lock
    void setMasterAddress(Address master) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        if (logger.isFineEnabled()) {
            logger.fine("Setting master address to " + master);
        }
        masterAddress = master;
        joined.getAndUpdate(holder -> new JoinHolder(holder.isJoined)).latch.countDown();
    }

    @Override
    public Address getMasterAddress() {
        return masterAddress;
    }

    @Override
    public boolean isMaster() {
        return node.getThisAddress().equals(masterAddress);
    }

    @Override
    @Nonnull
    public Address getThisAddress() {
        return node.getThisAddress();
    }

    @Override
    @Nonnull
    public UUID getThisUuid() {
        return node.getThisUuid();
    }

    @Override
    @Nonnull
    public MemberImpl getLocalMember() {
        return localMember;
    }

    // should be called under lock
    void setJoined(boolean val) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        joined.getAndUpdate(holder -> new JoinHolder(val)).latch.countDown();
    }

    @Override
    public boolean isJoined() {
        return joined.get().isJoined;
    }

    @Probe(name = CLUSTER_METRIC_CLUSTER_SERVICE_SIZE)
    @Override
    public int getSize() {
        return membershipManager.getMemberMap().size();
    }

    @Override
    public int getSize(MemberSelector selector) {
        int size = 0;
        for (MemberImpl member : membershipManager.getMembers()) {
            if (selector.select(member)) {
                size++;
            }
        }

        return size;
    }

    @Override
    @Nonnull
    public ClusterClockImpl getClusterClock() {
        return clusterClock;
    }

    @Override
    public long getClusterTime() {
        return clusterClock.getClusterTime();
    }

    @Override
    public UUID getClusterId() {
        return clusterId;
    }

    // called under cluster service lock
    void setClusterId(UUID newClusterId) {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        assert clusterId == null : "Cluster ID should be null: " + clusterId;
        clusterId = newClusterId;
    }

    // called under cluster service lock
    private void resetClusterId() {
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";
        clusterId = null;
    }

    @Nonnull
    public UUID addMembershipListener(@Nonnull MembershipListener listener) {
        checkNotNull(listener, "listener cannot be null");

        EventService eventService = nodeEngine.getEventService();
        EventRegistration registration;
        if (listener instanceof InitialMembershipListener) {
            lock.lock();
            try {
                ((InitialMembershipListener) listener)
                        .init(new InitialMembershipEvent(this, getMembers()));
                registration =
                        eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
            } finally {
                lock.unlock();
            }
        } else {
            registration = eventService.registerLocalListener(SERVICE_NAME, SERVICE_NAME, listener);
        }

        return registration.getId();
    }

    public boolean removeMembershipListener(@Nonnull UUID registrationId) {
        checkNotNull(registrationId, "registrationId cannot be null");

        EventService eventService = nodeEngine.getEventService();
        return eventService.deregisterListener(SERVICE_NAME, SERVICE_NAME, registrationId);
    }

    @Override
    public void dispatchEvent(MembershipEvent event, MembershipListener listener) {
        switch (event.getEventType()) {
            case MembershipEvent.MEMBER_ADDED:
                listener.memberAdded(event);
                break;
            case MembershipEvent.MEMBER_REMOVED:
                listener.memberRemoved(event);
                break;
            default:
                throw new IllegalArgumentException("Unhandled event: " + event);
        }
    }

    public String getMemberListString() {
        return membershipManager.memberListString();
    }

    void printMemberList() {
        logger.info(getMemberListString());
    }

    @Nonnull
    @Override
    public ClusterState getClusterState() {
        return clusterStateManager.getState();
    }

    @Override
    public <T extends TransactionalObject> T createTransactionalObject(
            String name, Transaction transaction) {
        throw new UnsupportedOperationException(
                SERVICE_NAME + " does not support TransactionalObjects!");
    }

    @Override
    public void rollbackTransaction(UUID transactionId) {
        clusterStateManager.rollbackClusterState(transactionId);
    }

    @Override
    public void changeClusterState(@Nonnull ClusterState newState) {
        checkNotNull(newState, STATE_MUST_NOT_BE_NULL);
        changeClusterState(newState, false);
    }

    private void changeClusterState(ClusterState newState, boolean isTransient) {
        long partitionStateStamp = getPartitionStateStamp();
        clusterStateManager.changeClusterState(
                ClusterStateChange.from(newState),
                membershipManager.getMemberMap(),
                partitionStateStamp,
                isTransient);
    }

    @Override
    public void changeClusterState(
            @Nonnull ClusterState newState, @Nonnull TransactionOptions options) {
        checkNotNull(newState, STATE_MUST_NOT_BE_NULL);
        checkNotNull(options, TRANSACTION_OPTIONS_MUST_NOT_BE_NULL);
        changeClusterState(newState, options, false);
    }

    private void changeClusterState(
            @Nonnull ClusterState newState,
            @Nonnull TransactionOptions options,
            boolean isTransient) {
        long partitionStateStamp = getPartitionStateStamp();
        clusterStateManager.changeClusterState(
                ClusterStateChange.from(newState),
                membershipManager.getMemberMap(),
                options,
                partitionStateStamp,
                isTransient);
    }

    @Override
    @Nonnull
    public Version getClusterVersion() {
        return clusterStateManager.getClusterVersion();
    }

    @Override
    public HotRestartService getHotRestartService() {
        return node.getNodeExtension().getHotRestartService();
    }

    @Override
    @Nonnull
    public PersistenceService getPersistenceService() {
        return node.getNodeExtension().getHotRestartService();
    }

    @Override
    public void changeClusterVersion(@Nonnull Version version) {
        checkNotNull(version, VERSION_MUST_NOT_BE_NULL);
        MemberMap memberMap = membershipManager.getMemberMap();
        changeClusterVersion(version, memberMap);
    }

    public void changeClusterVersion(@Nonnull Version version, @Nonnull MemberMap memberMap) {
        long partitionStateStamp = getPartitionStateStamp();
        clusterStateManager.changeClusterState(
                ClusterStateChange.from(version), memberMap, partitionStateStamp, false);
    }

    @Override
    public void changeClusterVersion(
            @Nonnull Version version, @Nonnull TransactionOptions options) {
        checkNotNull(version, VERSION_MUST_NOT_BE_NULL);
        checkNotNull(options, TRANSACTION_OPTIONS_MUST_NOT_BE_NULL);
        long partitionStateStamp = getPartitionStateStamp();
        clusterStateManager.changeClusterState(
                ClusterStateChange.from(version),
                membershipManager.getMemberMap(),
                options,
                partitionStateStamp,
                false);
    }

    private long getPartitionStateStamp() {
        return node.getPartitionService().getPartitionStateStamp();
    }

    @Override
    public int getMemberListJoinVersion() {
        lock.lock();
        try {
            if (!isJoined()) {
                throw new IllegalStateException(
                        "Member list join version is not available when not joined");
            }

            int joinVersion = localMember.getMemberListJoinVersion();
            if (joinVersion == NA_MEMBER_LIST_JOIN_VERSION) {
                // This can happen when the cluster was just upgraded to 3.10, but this member did
                // not yet learn
                // its node ID by an async call from master.
                throw new IllegalStateException("Member list join version is not yet available");
            }
            return joinVersion;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void shutdown() {
        shutdownCluster(null);
    }

    @Override
    public void shutdown(@Nullable TransactionOptions options) {
        shutdownCluster(options);
    }

    private void shutdownCluster(TransactionOptions options) {
        if (options == null) {
            changeClusterState(ClusterState.PASSIVE, true);
        } else {
            changeClusterState(ClusterState.PASSIVE, options, true);
        }

        node.getNodeExtension()
                .getAuditlogService()
                .eventBuilder(AuditlogTypeIds.CLUSTER_SHUTDOWN)
                .message("Shutting down the cluster")
                .log();
        long timeoutNanos =
                node.getProperties().getNanos(ClusterProperty.CLUSTER_SHUTDOWN_TIMEOUT_SECONDS);
        long startNanos = Timer.nanos();
        node.getNodeExtension()
                .getInternalHotRestartService()
                .waitPartitionReplicaSyncOnCluster(timeoutNanos, TimeUnit.NANOSECONDS);
        timeoutNanos -= (Timer.nanosElapsed(startNanos));

        if (node.config.getCPSubsystemConfig().getCPMemberCount() == 0) {
            shutdownNodesConcurrently(timeoutNanos);
        } else {
            shutdownNodesSerially(timeoutNanos);
        }
    }

    private void shutdownNodesConcurrently(final long timeoutNanos) {
        Operation op = new ShutdownNodeOp();
        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        long startTimeNanos = Timer.nanos();

        logger.info("Sending shut down operations to all members...");

        while (Timer.nanosElapsed(startTimeNanos) < timeoutNanos && !members.isEmpty()) {
            for (Member member : members) {
                nodeEngine.getOperationService().send(op, member.getAddress());
            }

            try {
                Thread.sleep(CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Shutdown sleep interrupted. ", e);
                break;
            }

            members = getMembers(NON_LOCAL_MEMBER_SELECTOR);
        }

        logger.info(
                "Number of other members remaining: "
                        + getSize(NON_LOCAL_MEMBER_SELECTOR)
                        + ". Shutting down itself.");

        HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        hazelcastInstance.getLifecycleService().shutdown();
    }

    private void shutdownNodesSerially(final long timeoutNanos) {
        Operation op = new ShutdownNodeOp();
        long startTimeNanos = Timer.nanos();
        Collection<Member> members = getMembers(NON_LOCAL_MEMBER_SELECTOR);

        logger.info("Sending shut down operations to other members one by one...");

        while (Timer.nanosElapsed(startTimeNanos) < timeoutNanos && !members.isEmpty()) {
            Member member = members.iterator().next();
            nodeEngine.getOperationService().send(op, member.getAddress());
            members = getMembers(NON_LOCAL_MEMBER_SELECTOR);

            try {
                Thread.sleep(CLUSTER_SHUTDOWN_SLEEP_DURATION_IN_MILLIS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warning("Shutdown sleep interrupted. ", e);
                break;
            }
        }

        logger.info(
                "Number of other members remaining: "
                        + getSize(NON_LOCAL_MEMBER_SELECTOR)
                        + ". Shutting down itself.");

        HazelcastInstanceImpl hazelcastInstance = node.hazelcastInstance;
        hazelcastInstance.getLifecycleService().shutdown();
    }

    private void initialClusterState(ClusterState clusterState, Version version) {
        if (isJoined()) {
            throw new IllegalStateException(
                    "Cannot set initial state after node joined! -> " + clusterState);
        }
        clusterStateManager.initialClusterState(clusterState, version);
    }

    public MembershipManager getMembershipManager() {
        return membershipManager;
    }

    public ClusterStateManager getClusterStateManager() {
        return clusterStateManager;
    }

    public ClusterJoinManager getClusterJoinManager() {
        return clusterJoinManager;
    }

    public ClusterHeartbeatManager getClusterHeartbeatManager() {
        return clusterHeartbeatManager;
    }

    @Override
    public void promoteLocalLiteMember() {
        MemberImpl member = getLocalMember();
        if (!member.isLiteMember()) {
            throw new IllegalStateException(member + " is not a lite member!");
        }

        MemberImpl master = getMasterMember();
        PromoteLiteMemberOp op = new PromoteLiteMemberOp();
        op.setCallerUuid(member.getUuid());

        InvocationFuture<MembersView> future =
                nodeEngine
                        .getOperationService()
                        .invokeOnTarget(SERVICE_NAME, op, master.getAddress());
        MembersView view = future.joinInternal();

        lock.lock();
        try {
            if (!member.getAddress().equals(master.getAddress())) {
                updateMembers(view, master.getAddress(), master.getUuid(), getThisUuid());
            }

            MemberImpl localMemberInMemberList = membershipManager.getMember(member.getAddress());
            boolean result = localMemberInMemberList.isLiteMember();
            node.getNodeExtension()
                    .getAuditlogService()
                    .eventBuilder(AuditlogTypeIds.CLUSTER_PROMOTE_MEMBER)
                    .message("Promotion of the lite member")
                    .addParameter("success", result)
                    .addParameter("address", node.getThisAddress())
                    .log();
            if (result) {
                throw new IllegalStateException(
                        "Cannot promote to data member! Previous master was: "
                                + master.getAddress()
                                + ", Current master is: "
                                + getMasterAddress());
            }
        } finally {
            lock.unlock();
        }
    }

    MemberImpl promoteAndGetLocalMember() {
        MemberImpl member = getLocalMember();
        assert member.isLiteMember() : "Local member is not lite member!";
        assert lock.isHeldByCurrentThread() : "Called without holding cluster service lock!";

        localMember =
                new MemberImpl.Builder(member.getAddressMap())
                        .version(member.getVersion())
                        .localMember(true)
                        .uuid(member.getUuid())
                        .attributes(member.getAttributes())
                        .memberListJoinVersion(member.getMemberListJoinVersion())
                        .instance(node.hazelcastInstance)
                        .build();
        node.loggingService.setThisMember(localMember);
        return localMember;
    }

    @Override
    public int getMemberListVersion() {
        return membershipManager.getMemberListVersion();
    }

    private MemberImpl getMasterMember() {
        MemberImpl master;
        lock.lock();
        try {
            Address masterAddress = getMasterAddress();
            if (masterAddress == null) {
                throw new IllegalStateException("Master is not known yet!");
            }

            master = getMember(masterAddress);
        } finally {
            lock.unlock();
        }
        return master;
    }

    @Override
    public String toString() {
        return "ClusterService" + "{address=" + getThisAddress() + '}';
    }

    /**
     * @param timeoutMillis the maximum time in millis to block on join
     * @return true is cluster has been joined, false if timed out
     * @throws InterruptedException
     */
    public boolean blockOnJoin(long timeoutMillis) throws InterruptedException {
        return joined.get().latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }
}
