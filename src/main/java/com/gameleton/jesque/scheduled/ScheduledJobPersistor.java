package com.gameleton.jesque.scheduled;

import com.gameleton.jesque.trigger.Trigger;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by levin on 8/14/2014.
 */
public class ScheduledJobPersistor {

    public static void save(Jedis redis, ScheduledJob scheduledJob ) {
        redis.hmset(scheduledJob.getRedisKey(), scheduledJob.toRedisHash());
        redis.sadd(ScheduledJob.JOB_INDEX, scheduledJob.getName());
    }

    public static void delete(Jedis redis, String name) {
        redis.del(ScheduledJob.getRedisKeyForName(name));
        redis.srem(ScheduledJob.JOB_INDEX, name);
    }

    public static ScheduledJob findByName(Jedis redis, String name) {
        ScheduledJob scheduledJob = ScheduledJob.fromRedisHash( redis.hgetAll(ScheduledJob.getRedisKeyForName(name)) );
        Trigger trigger = Trigger.fromRedisHash(redis.hgetAll(Trigger.getRedisKeyForJobName(name)));
        scheduledJob.setTrigger(trigger);
        return scheduledJob;
    }

    public static List<ScheduledJob> getAll(Jedis redis) {
        Set<String> jobNames = redis.smembers(ScheduledJob.JOB_INDEX);
        List<ScheduledJob> jobList = new ArrayList<>();
        for (String jobName : jobNames) {
            jobList.add(findByName(redis, jobName));
        }
        return jobList;
    }
}
