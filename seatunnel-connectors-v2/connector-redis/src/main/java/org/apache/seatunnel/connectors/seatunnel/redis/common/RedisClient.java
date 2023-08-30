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
