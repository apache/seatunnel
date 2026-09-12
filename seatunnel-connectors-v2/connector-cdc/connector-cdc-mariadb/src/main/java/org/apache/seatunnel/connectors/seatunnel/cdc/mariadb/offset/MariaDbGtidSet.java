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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Represents a MariaDB GTID set composed of domain-server-sequence tuples. Example format:
 * "0-1-100,1-2-200"
 */
public class MariaDbGtidSet implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Map<Long, MariaDbGtid> gtidsByDomain;

    public MariaDbGtidSet(String gtidSetStr) {
        this.gtidsByDomain = new TreeMap<>();
        if (StringUtils.isNotBlank(gtidSetStr)) {
            String[] tokens = gtidSetStr.trim().split("[,\\n\\r]+");
            for (String token : tokens) {
                String trimmed = token.trim();
                if (StringUtils.isNotBlank(trimmed)) {
                    MariaDbGtid gtid = MariaDbGtid.parse(trimmed);
                    MariaDbGtid existing = gtidsByDomain.get(gtid.getDomainId());
                    if (existing == null || gtid.getSequence() > existing.getSequence()) {
                        gtidsByDomain.put(gtid.getDomainId(), gtid);
                    }
                }
            }
        }
    }

    public MariaDbGtidSet(Map<Long, MariaDbGtid> gtidsByDomain) {
        this.gtidsByDomain = new TreeMap<>(gtidsByDomain);
    }

    public Map<Long, MariaDbGtid> getGtids() {
        return Collections.unmodifiableMap(gtidsByDomain);
    }

    public MariaDbGtid getGtid(long domainId) {
        return gtidsByDomain.get(domainId);
    }

    public boolean isEmpty() {
        return gtidsByDomain.isEmpty();
    }

    public boolean isContainedWithin(MariaDbGtidSet other) {
        if (other == null) {
            return false;
        }
        if (this.isEmpty()) {
            return true;
        }
        for (MariaDbGtid thisGtid : this.gtidsByDomain.values()) {
            MariaDbGtid otherGtid = other.getGtid(thisGtid.getDomainId());
            if (otherGtid == null) {
                return false;
            }
            if (thisGtid.getSequence() > otherGtid.getSequence()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MariaDbGtidSet)) {
            return false;
        }
        MariaDbGtidSet that = (MariaDbGtidSet) o;
        return Objects.equals(gtidsByDomain, that.gtidsByDomain);
    }

    @Override
    public int hashCode() {
        return Objects.hash(gtidsByDomain);
    }

    @Override
    public String toString() {
        return gtidsByDomain.values().stream()
                .map(MariaDbGtid::toString)
                .collect(Collectors.joining(","));
    }

    /** Single MariaDB GTID representing domain_id-server_id-sequence. */
    public static class MariaDbGtid implements Serializable {
        private static final long serialVersionUID = 1L;

        private final long domainId;
        private final long serverId;
        private final long sequence;

        public MariaDbGtid(long domainId, long serverId, long sequence) {
            this.domainId = domainId;
            this.serverId = serverId;
            this.sequence = sequence;
        }

        public static MariaDbGtid parse(String gtidStr) {
            if (gtidStr == null) {
                throw new IllegalArgumentException("MariaDB GTID string must not be null");
            }
            String[] parts = gtidStr.split("-");
            if (parts.length == 3) {
                try {
                    long domainId = Long.parseLong(parts[0].trim());
                    long serverId = Long.parseLong(parts[1].trim());
                    long sequence = Long.parseLong(parts[2].trim());
                    return new MariaDbGtid(domainId, serverId, sequence);
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Invalid MariaDB GTID: " + gtidStr, e);
                }
            }
            throw new IllegalArgumentException("Invalid MariaDB GTID: " + gtidStr);
        }

        public long getDomainId() {
            return domainId;
        }

        public long getServerId() {
            return serverId;
        }

        public long getSequence() {
            return sequence;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof MariaDbGtid)) {
                return false;
            }
            MariaDbGtid that = (MariaDbGtid) o;
            return domainId == that.domainId
                    && serverId == that.serverId
                    && sequence == that.sequence;
        }

        @Override
        public int hashCode() {
            return Objects.hash(domainId, serverId, sequence);
        }

        @Override
        public String toString() {
            return domainId + "-" + serverId + "-" + sequence;
        }
    }
}
