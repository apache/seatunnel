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

package com.hazelcast.cluster.impl;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.internal.cluster.impl.ClusterDataSerializerHook;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.version.MemberVersion;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.hazelcast.instance.EndpointQualifier.MEMBER;
import static com.hazelcast.internal.util.Preconditions.isNotNull;

public final class MemberImpl extends AbstractMember
        implements Member, HazelcastInstanceAware, IdentifiedDataSerializable {

    /** Denotes that member list join version of a member is not known yet. */
    public static final int NA_MEMBER_LIST_JOIN_VERSION = -1;

    private boolean localMember;

    private volatile int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
    private volatile HazelcastInstanceImpl instance;
    private volatile ILogger logger;

    public MemberImpl() {}

    public MemberImpl(Address address, MemberVersion version, boolean localMember) {
        this(
                newHashMap(MEMBER, address),
                address,
                version,
                localMember,
                null,
                null,
                false,
                NA_MEMBER_LIST_JOIN_VERSION,
                null);
    }

    public MemberImpl(Address address, MemberVersion version, boolean localMember, UUID uuid) {
        this(
                newHashMap(MEMBER, address),
                address,
                version,
                localMember,
                uuid,
                null,
                false,
                NA_MEMBER_LIST_JOIN_VERSION,
                null);
    }

    private MemberImpl(
            Map<EndpointQualifier, Address> addresses,
            MemberVersion version,
            boolean localMember,
            UUID uuid,
            Map<String, String> attributes,
            boolean liteMember,
            int memberListJoinVersion,
            HazelcastInstanceImpl instance) {
        this(
                addresses,
                addresses.get(MEMBER),
                version,
                localMember,
                uuid,
                attributes,
                liteMember,
                memberListJoinVersion,
                instance);
    }

    public MemberImpl(MemberImpl member) {
        super(member);
        this.localMember = member.localMember;
        this.memberListJoinVersion = member.memberListJoinVersion;
        this.instance = member.instance;
    }

    private MemberImpl(
            Map<EndpointQualifier, Address> addresses,
            Address address,
            MemberVersion version,
            boolean localMember,
            UUID uuid,
            Map<String, String> attributes,
            boolean liteMember,
            int memberListJoinVersion,
            HazelcastInstanceImpl instance) {
        super(addresses, address, version, uuid, attributes, liteMember);
        this.memberListJoinVersion = memberListJoinVersion;
        this.localMember = localMember;
        this.instance = instance;
    }

    @Override
    protected ILogger getLogger() {
        return logger;
    }

    @Override
    public void setHazelcastInstance(HazelcastInstance hazelcastInstance) {
        if (hazelcastInstance instanceof HazelcastInstanceImpl) {
            instance = (HazelcastInstanceImpl) hazelcastInstance;
            localMember = instance.node.address.equals(address);
            logger = instance.node.getLogger(this.getClass().getName());
        }
    }

    @Override
    public boolean localMember() {
        return localMember;
    }

    @Override
    public String getAttribute(String key) {
        return attributes.get(key);
    }

    public void setMemberListJoinVersion(int memberListJoinVersion) {
        this.memberListJoinVersion = memberListJoinVersion;
    }

    public int getMemberListJoinVersion() {
        return memberListJoinVersion;
    }

    private void ensureLocalMember() {
        if (!localMember) {
            throw new UnsupportedOperationException(
                    "Attributes on remote members must not be changed");
        }
    }

    public void setAttribute(String key, String value) {
        ensureLocalMember();
        if (instance != null && instance.node.clusterService.isJoined()) {
            throw new UnsupportedOperationException(
                    "Attributes can not be changed after instance has started");
        }

        isNotNull(key, "key");
        isNotNull(value, "value");

        attributes.put(key, value);
    }

    public void updateAttribute(Map<String, String> tags) {
        ensureLocalMember();
        attributes.clear();
        if (tags.size() > 0) {
            attributes.putAll(tags);
        }
    }

    public int getFactoryId() {
        return ClusterDataSerializerHook.F_ID;
    }

    @Override
    public int getClassId() {
        return ClusterDataSerializerHook.MEMBER;
    }

    public static class Builder {
        private Address address;
        private Map<EndpointQualifier, Address> addressMap;

        private Map<String, String> attributes;
        private boolean localMember;
        private UUID uuid;
        private boolean liteMember;
        private MemberVersion version;
        private int memberListJoinVersion = NA_MEMBER_LIST_JOIN_VERSION;
        private HazelcastInstanceImpl instance;

        public Builder(Address address) {
            Preconditions.isNotNull(address, "address");
            this.address = address;
        }

        public Builder(Map<EndpointQualifier, Address> addresses) {
            Preconditions.isNotNull(addresses, "addresses");
            Preconditions.isNotNull(addresses.get(MEMBER), "addresses.get(MEMBER)");
            this.addressMap = addresses;
        }

        public Builder address(Address address) {
            this.address = Preconditions.isNotNull(address, "address");
            return this;
        }

        public Builder localMember(boolean localMember) {
            this.localMember = localMember;
            return this;
        }

        public Builder version(MemberVersion memberVersion) {
            this.version = memberVersion;
            return this;
        }

        public Builder uuid(UUID uuid) {
            this.uuid = uuid;
            return this;
        }

        public Builder attributes(Map<String, String> attributes) {
            this.attributes = attributes;
            return this;
        }

        public Builder memberListJoinVersion(int memberListJoinVersion) {
            this.memberListJoinVersion = memberListJoinVersion;
            return this;
        }

        public Builder liteMember(boolean liteMember) {
            this.liteMember = liteMember;
            return this;
        }

        public Builder instance(HazelcastInstanceImpl hazelcastInstanceImpl) {
            this.instance = hazelcastInstanceImpl;
            return this;
        }

        public MemberImpl build() {
            if (addressMap == null) {
                addressMap = newHashMap(MEMBER, address);
            }
            if (address == null) {
                address = addressMap.get(MEMBER);
            }
            return new MemberImpl(
                    addressMap,
                    address,
                    version,
                    localMember,
                    uuid,
                    attributes,
                    liteMember,
                    memberListJoinVersion,
                    instance);
        }
    }

    private static Map<EndpointQualifier, Address> newHashMap(
            EndpointQualifier member, Address address) {
        Map<EndpointQualifier, Address> result = new HashMap<>();
        result.put(member, address);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Member [");
        sb.append(address.getHost());
        sb.append("]");
        sb.append(":");
        sb.append(address.getPort());
        sb.append(" - ").append(uuid);
        // update for seatunnel, add worker and master info
        if (isLiteMember()) {
            sb.append(" [worker node]");
        } else {
            sb.append(" [master node]");
        }
        if (instance != null
                && instance.node.getClusterService().getMasterAddress() != null
                && instance.node.getClusterService().getMasterAddress().equals(address)) {
            sb.append(" [active master]");
        }
        if (localMember()) {
            sb.append(" this");
        }
        // update for seatunnel, add worker and master info end
        return sb.toString();
    }
}
