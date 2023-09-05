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

package org.apache.seatunnel.connectors.seatunnel.redis.common;

import java.util.List;
import java.util.Set;

public interface RedisClient {

    public void set(String key, String value, long expire);

    public List<String> get(String key);

    public void hset(String key, String value, long expire);

    public List<String> hget(String key);

    public void lpush(String key, String value, long expire);

    public List<String> lrange(String key);

    public void sadd(String key, String value, long expire);

    public List<String> smembers(String key);

    public void zadd(String key, String value, long expire);

    public List<String> zrange(String key);

    public void expire(String key, long seconds);

    public Set<String> keys(String keys);

    public void close();
}
