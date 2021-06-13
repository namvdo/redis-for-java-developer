package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.Site;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.*;

public class SiteDaoRedisImpl implements SiteDao {
    private final JedisPool jedisPool;

    public SiteDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    // When we insert a site, we set all of its values into a single hash.
    // We then store the site's id in a set for easy access.
    @Override
    public void insert(Site site) {
        try (var jedis = jedisPool.getResource()) {
            String hashKey = RedisSchema.getSiteHashKey(site.getId());
            String siteIdKey = RedisSchema.getSiteIDsKey();
            jedis.hmset(hashKey, site.toMap());
            jedis.hmget(hashKey, "123");
            jedis.sadd(siteIdKey, hashKey);
        }
    }

    @Override
    public Site findById(long id) {
        try(var jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteHashKey(id);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            } else {
                return new Site(fields);
            }
        }
    }

    // Challenge #1
    @Override
    public Set<Site> findAll() {
        Set<Site> sites = new HashSet<>();
        try (var jedis = jedisPool.getResource()) {
            // site ID key -> hash key
            String key = RedisSchema.getSiteIDsKey();
            Set<String> siteIds = jedis.smembers(key);
            for (var entry: siteIds) {
                Map<String, String> fields = jedis.hgetAll(entry);
                if (fields != null && !fields.isEmpty()) {
                    sites.add(new Site(fields));
                }
            }
        }
        if (sites.isEmpty()) {
            return Collections.emptySet();
        }
        return sites;
    }
}
