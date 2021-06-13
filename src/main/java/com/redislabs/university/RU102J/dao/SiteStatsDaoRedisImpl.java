package com.redislabs.university.RU102J.dao;

import com.redislabs.university.RU102J.api.MeterReading;
import com.redislabs.university.RU102J.api.Site;
import com.redislabs.university.RU102J.api.SiteStats;
import com.redislabs.university.RU102J.script.CompareAndUpdateScript;
import redis.clients.jedis.*;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class SiteStatsDaoRedisImpl implements SiteStatsDao {

    private final int weekSeconds = 60 * 60 * 24 * 7;
    private final JedisPool jedisPool;
    private final CompareAndUpdateScript compareAndUpdateScript;

    public SiteStatsDaoRedisImpl(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        this.compareAndUpdateScript = new CompareAndUpdateScript(jedisPool);
    }

    // Returns the site stats for the current day
    @Override
    public SiteStats findById(long siteId) {
        return findById(siteId, ZonedDateTime.now());
    }

    @Override
    public SiteStats findById(long siteId, ZonedDateTime day) {
        try (Jedis jedis = jedisPool.getResource()) {
            String key = RedisSchema.getSiteStatsKey(siteId, day);
            Map<String, String> fields = jedis.hgetAll(key);
            if (fields == null || fields.isEmpty()) {
                return null;
            }
            return new SiteStats(fields);
        }
    }

    @Override
    public void update(MeterReading reading) {
        try (Jedis jedis = jedisPool.getResource()) {
            Long siteId = reading.getSiteId();
            ZonedDateTime day = reading.getDateTime();
            String key = RedisSchema.getSiteStatsKey(siteId, day);

            updateOptimized(jedis, key, reading);
        }
    }

    // A naive implementation of update. This implementation has
    // potential race conditions and makes several round trips to Redis.
    private void updateBasic(Jedis jedis, String key, MeterReading reading) {
        String reportingTime = ZonedDateTime.now(ZoneOffset.UTC).toString();
        jedis.hset(key, SiteStats.reportingTimeField, reportingTime);
        jedis.hincrBy(key, SiteStats.countField, 1);
        jedis.expire(key, weekSeconds);

        String maxWh = jedis.hget(key, SiteStats.maxWhField);
        if (maxWh == null || reading.getWhGenerated() > Double.valueOf(maxWh)) {
            jedis.hset(key, SiteStats.maxWhField,
                    String.valueOf(reading.getWhGenerated()));
        }

        String minWh = jedis.hget(key, SiteStats.minWhField);
        if (minWh == null || reading.getWhGenerated() < Double.valueOf(minWh)) {
            jedis.hset(key, SiteStats.minWhField,
                    String.valueOf(reading.getWhGenerated()));
        }

        String maxCapacity = jedis.hget(key, SiteStats.maxCapacityField);
        if (maxCapacity == null || getCurrentCapacity(reading) > Double.valueOf(maxCapacity)) {
            jedis.hset(key, SiteStats.maxCapacityField,
                    String.valueOf(getCurrentCapacity(reading)));
        }
    }

    // Challenge #3
    private void updateOptimized(Jedis jedis, String key, MeterReading reading) {
        try (var transaction = jedis.multi()) {
            var reportingTime = ZonedDateTime.now(ZoneOffset.UTC).toString();
            transaction.hset(key, SiteStats.reportingTimeField, reportingTime);
            transaction.hincrBy(key, SiteStats.countField, 1);
            transaction.expire(key, weekSeconds);
            compareAndUpdateScript.updateIfLess(transaction, key, SiteStats.minWhField, reading.getWhGenerated());
            compareAndUpdateScript.updateIfGreater(transaction, key, SiteStats.maxWhField, reading.getWhGenerated());
            compareAndUpdateScript.updateIfGreater(transaction, key, SiteStats.maxCapacityField, getCurrentCapacity(reading));
        }
    }

    private Double getCurrentCapacity(MeterReading reading) {
        return reading.getWhGenerated() - reading.getWhUsed();
    }

    @Override
    public int hashCode() {
       int result = Objects.hash(jedisPool);
       result = result * 31 + Integer.hashCode(weekSeconds);
       return result;
    }
}
