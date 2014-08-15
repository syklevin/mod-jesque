package com.gameleton.jesque.trigger;


import redis.clients.jedis.Jedis;
/**
 * Created by levin on 8/14/2014.
 */
public class TriggerPersistor {

    public static void save(Jedis redis, Trigger trigger) {
        redis.hmset(trigger.getRedisKey(), trigger.toRedisHash());
    }

    public static void delete(Jedis redis, String jobName ) {
        redis.del(Trigger.getRedisKeyForJobName(jobName));
        redis.zrem(Trigger.TRIGGER_NEXTFIRE_INDEX, jobName);
    }

    public static Trigger findByJobName(Jedis redis, String jobName) {
        return Trigger.fromRedisHash( redis.hgetAll( Trigger.getRedisKeyForJobName(jobName) ) );
    }


}
