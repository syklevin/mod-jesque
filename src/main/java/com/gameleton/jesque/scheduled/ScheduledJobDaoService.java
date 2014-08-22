package com.gameleton.jesque.scheduled;

import com.gameleton.jesque.trigger.Trigger;
import net.greghaines.jesque.Config;
import net.greghaines.jesque.utils.JesqueUtils;
import net.greghaines.jesque.utils.PoolUtils;
import redis.clients.jedis.Jedis;
import redis.clients.util.Pool;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * Created by levin on 8/14/2014.
 */
public class ScheduledJobDaoService {

    private Config config;
    private Pool<Jedis> jedisPool;

    public ScheduledJobDaoService(Config config, Pool<Jedis> jedisPool){
        this.config = config;
        this.jedisPool = jedisPool;
    }

    public void save(final ScheduledJob scheduledJob ) {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                save(jedis, scheduledJob);
                return null;
            }
        });
    }

    public void save(final Jedis jedis, final ScheduledJob scheduledJob){
        String key = JesqueUtils.createKey(config.getNamespace(), ScheduledJob.SCHEDULED_JOB_PREFIX, scheduledJob.getName());
        jedis.hmset(key, scheduledJob.toRedisHash());
        key = JesqueUtils.createKey(config.getNamespace(), ScheduledJob.SCHEDULED_JOBS);
        jedis.sadd(key, scheduledJob.getName());
    }

    public void delete(final String name) {
        PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, Void>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public Void doWork(final Jedis jedis) throws Exception {
                delete(jedis, name);
                return null;
            }
        });
    }

    public void delete(final Jedis jedis, final String name){
        String key = JesqueUtils.createKey(config.getNamespace(), ScheduledJob.SCHEDULED_JOB_PREFIX, name);
        jedis.del(key);
        key = JesqueUtils.createKey(config.getNamespace(), ScheduledJob.SCHEDULED_JOBS);
        jedis.srem(key, name);
    }

    public ScheduledJob findByName(final String name) {
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, ScheduledJob>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public ScheduledJob doWork(final Jedis jedis) throws Exception {
                return findByName(jedis, name);
            }
        });
    }

    public ScheduledJob findByName(final Jedis jedis, final String name) {
        String key = JesqueUtils.createKey(config.getNamespace(), ScheduledJob.SCHEDULED_JOB_PREFIX, name);
        ScheduledJob scheduledJob = ScheduledJob.fromRedisHash(jedis.hgetAll(key));
        key = JesqueUtils.createKey(config.getNamespace(), Trigger.TRIGGER_PREFIX, name);
        Trigger trigger = Trigger.fromRedisHash(jedis.hgetAll(key));
        scheduledJob.setTrigger(trigger);
        return scheduledJob;
    }

    public List<ScheduledJob> getAll() {
        return PoolUtils.doWorkInPoolNicely(this.jedisPool, new PoolUtils.PoolWork<Jedis, List<ScheduledJob>>() {
            /**
             * {@inheritDoc}
             */
            @Override
            public List<ScheduledJob> doWork(final Jedis jedis) throws Exception {
                Set<String> jobNames = jedis.smembers(ScheduledJob.SCHEDULED_JOBS);
                List<ScheduledJob> jobList = new ArrayList<>();
                for (String jobName : jobNames) {
                    jobList.add(findByName(jedis, jobName));
                }
                return jobList;
            }
        });
    }
}
