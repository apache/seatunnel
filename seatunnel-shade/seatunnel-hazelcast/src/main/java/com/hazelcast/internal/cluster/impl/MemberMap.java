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

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static com.hazelcast.internal.util.MapUtil.createLinkedHashMap;
import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableCollection;

/**
 * A special, immutable {@link MemberImpl} map type, that allows querying members using address or
 * UUID.
 */
final class MemberMap {

    static final int SINGLETON_MEMBER_LIST_VERSION = 1;

    private final int version;
    private final Map<Address, MemberImpl> addressToMemberMap;
    private final Map<UUID, MemberImpl> uuidToMemberMap;
    private final Set<MemberImpl> members;

    MemberMap(int version, Map<Address, MemberImpl> addressMap, Map<UUID, MemberImpl> uuidMap) {
        this.version = version;
        assert new HashSet<>(addressMap.values()).equals(new HashSet<>(uuidMap.values()))
                : "Maps are different! AddressMap: " + addressMap + ", UuidMap: " + uuidMap;

        this.addressToMemberMap = addressMap;
        this.uuidToMemberMap = uuidMap;
        this.members =
                Collections.unmodifiableSet(new LinkedHashSet<>(addressToMemberMap.values()));
    }

    /**
     * Creates an empty {@code MemberMap}.
     *
     * @return empty {@code MemberMap}
     */
    static MemberMap empty() {
        return new MemberMap(0, Collections.emptyMap(), Collections.emptyMap());
    }

    /**
     * Creates a singleton {@code MemberMap} including only specified member.
     *
     * @param member sole member in map
     * @return singleton {@code MemberMap}
     */
    static MemberMap singleton(MemberImpl member) {
        return new MemberMap(
                SINGLETON_MEMBER_LIST_VERSION,
                singletonMap(member.getAddress(), member),
                singletonMap(member.getUuid(), member));
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(MemberImpl... members) {
        return createNew(0, members);
    }

    /**
     * Creates a new {@code MemberMap} including given members.
     *
     * @param version version
     * @param members members
     * @return a new {@code MemberMap}
     */
    static MemberMap createNew(int version, MemberImpl... members) {
        Map<Address, MemberImpl> addressMap = createLinkedHashMap(members.length);
        Map<UUID, MemberImpl> uuidMap = createLinkedHashMap(members.length);

        for (MemberImpl member : members) {
            putMember(addressMap, uuidMap, member);
        }

        return new MemberMap(version, addressMap, uuidMap);
    }

    /**
     * Creates clone of source {@code MemberMap}, excluding given members. If source is empty, same
     * map instance will be returned. If excluded members are empty or not present in source, a new
     * map will be created containing the same members with source.
     *
     * @param source source map
     * @param excludeMembers members to exclude
     * @return clone map
     */
    static MemberMap cloneExcluding(MemberMap source, MemberImpl... excludeMembers) {
        if (source.size() == 0) {
            return source;
        }

        Map<Address, MemberImpl> addressMap = new LinkedHashMap<>(source.addressToMemberMap);
        Map<UUID, MemberImpl> uuidMap = new LinkedHashMap<>(source.uuidToMemberMap);

        for (MemberImpl member : excludeMembers) {
            MemberImpl removed = addressMap.remove(member.getAddress());
            if (removed != null) {
                uuidMap.remove(removed.getUuid());
            }

            removed = uuidMap.remove(member.getUuid());
            if (removed != null) {
                addressMap.remove(removed.getAddress());
            }
        }

        return new MemberMap(source.version + excludeMembers.length, addressMap, uuidMap);
    }

    /**
     * Creates clone of source {@code MemberMap} additionally including new members.
     *
     * @param source source map
     * @param newMembers new members to add
     * @return clone map
     */
    static MemberMap cloneAdding(MemberMap source, MemberImpl... newMembers) {
        Map<Address, MemberImpl> addressMap = new LinkedHashMap<>(source.addressToMemberMap);
        Map<UUID, MemberImpl> uuidMap = new LinkedHashMap<>(source.uuidToMemberMap);

        for (MemberImpl member : newMembers) {
            putMember(addressMap, uuidMap, member);
        }

        return new MemberMap(source.version + newMembers.length, addressMap, uuidMap);
    }

    private static void putMember(
            Map<Address, MemberImpl> addressMap, Map<UUID, MemberImpl> uuidMap, MemberImpl member) {

        MemberImpl current = addressMap.put(member.getAddress(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with address: " + member);
        }

        current = uuidMap.put(member.getUuid(), member);
        if (current != null) {
            throw new IllegalArgumentException("Replacing existing member with UUID: " + member);
        }
    }

    MemberImpl getMember(Address address) {
        return addressToMemberMap.get(address);
    }

    MemberImpl getMember(UUID uuid) {
        return uuidToMemberMap.get(uuid);
    }

    MemberImpl getMember(Address address, UUID uuid) {
        MemberImpl member1 = addressToMemberMap.get(address);
        MemberImpl member2 = uuidToMemberMap.get(uuid);

        if (member1 != null && member1.equals(member2)) {
            return member1;
        }
        return null;
    }

    boolean contains(Address address) {
        return addressToMemberMap.containsKey(address);
    }

    boolean contains(UUID uuid) {
        return uuidToMemberMap.containsKey(uuid);
    }

    Set<MemberImpl> getMembers() {
        return members;
    }

    Collection<Address> getAddresses() {
        return unmodifiableCollection(addressToMemberMap.keySet());
    }

    int size() {
        return members.size();
    }

    int getVersion() {
        return version;
    }

    MembersView toMembersView() {
        return MembersView.createNew(version, members);
    }

    MembersView toTailMembersView(MemberImpl member, boolean inclusive) {
        return MembersView.createNew(version, tailMemberSet(member, inclusive));
    }

    Set<MemberImpl> tailMemberSet(MemberImpl member, boolean inclusive) {
        ensureMemberExist(member);

        Set<MemberImpl> result = new LinkedHashSet<>();
        boolean found = false;
        for (MemberImpl m : members) {
            // update for seatunnel
            // all lite member need add to new cluster
            if (m.isLiteMember()) {
                result.add(m);
                continue;
            }

            if (!found && m.equals(member)) {
                found = true;
                if (inclusive) {
                    result.add(m);
                }
                continue;
            }

            if (found) {
                result.add(m);
            }
        }

        assert found : member + " should have been found!";

        return result;
    }

    Set<MemberImpl> headMemberSet(Member member, boolean inclusive) {
        ensureMemberExist(member);

        Set<MemberImpl> result = new LinkedHashSet<>();
        for (MemberImpl m : members) {
            if (!m.equals(member)) {
                result.add(m);
                continue;
            }

            if (inclusive) {
                result.add(m);
            }
            break;
        }

        return result;
    }

    boolean isBeforeThan(Address address1, Address address2) {
        if (address1.equals(address2)) {
            return false;
        }

        if (!addressToMemberMap.containsKey(address1)) {
            return false;
        }

        if (!addressToMemberMap.containsKey(address2)) {
            return false;
        }

        for (MemberImpl member : members) {
            if (member.getAddress().equals(address1)) {
                return true;
            }
            if (member.getAddress().equals(address2)) {
                return false;
            }
        }

        throw new AssertionError("Unreachable!");
    }

    private void ensureMemberExist(Member member) {
        if (!addressToMemberMap.containsKey(member.getAddress())) {
            throw new IllegalArgumentException(member + " not found!");
        }
        if (!uuidToMemberMap.containsKey(member.getUuid())) {
            throw new IllegalArgumentException(member + " not found!");
        }
    }
}
